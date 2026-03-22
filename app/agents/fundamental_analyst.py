"""
Input: StockAnalysisState (stock_code, market_type)
Output: StockAnalysisState (fundamental_report已填充)
Pos: 基本面分析Agent，包装FundamentalAnalyzer提供LLM增强分析
一旦我被修改，请更新我的头部注释，以及所属文件夹的md。
"""
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


class FundamentalAnalystAgent:
    """基本面分析师Agent"""

    name = "基本面分析师"

    @staticmethod
    def analyze(state: Dict[str, Any]) -> Dict[str, Any]:
        """执行基本面分析"""
        from app.analysis.fundamental_analyzer import FundamentalAnalyzer
        from app.core.ai_client import get_ai_client, chat_completion, get_completion_content

        stock_code = state['stock_code']

        try:
            analyzer = FundamentalAnalyzer()

            # 获取财务指标
            financial_data = analyzer.get_financial_indicators(stock_code)

            # 获取成长性数据
            growth_data = analyzer.get_growth_data(stock_code)

            # 计算基本面评分
            score_result = analyzer.calculate_fundamental_score(stock_code)

            result = {
                'financial_indicators': financial_data,
                'growth_data': growth_data,
                'score': score_result
            }

            # 检查是否有错误
            if isinstance(score_result, dict) and 'error' in score_result:
                return {
                    'fundamental_report': {'error': score_result['error']},
                    'execution_log': state.get('execution_log', []) + [
                        {'agent': '基本面分析师', 'status': 'failed', 'error': score_result['error']}
                    ]
                }

            # 用AI增强分析
            client = get_ai_client()
            if client:
                prompt = f"""你是资深基本面分析师。基于以下财务数据，给出专业分析：

股票代码: {stock_code}

财务指标摘要:
{_summarize_data(financial_data)}

成长性数据摘要:
{_summarize_data(growth_data)}

基本面评分:
{_summarize_data(score_result)}

请给出：
1. 财务健康度评估（偿债能力、盈利能力、运营效率）
2. 成长性分析（营收/利润增长趋势、可持续性）
3. 估值合理性判断（当前估值水平、相对行业位置）
4. 关键财务风险提示"""

                response, error = chat_completion(
                    client,
                    [{"role": "user", "content": prompt}],
                    temperature=0.7,
                    max_tokens=1200
                )
                if not error:
                    result['ai_commentary'] = get_completion_content(response)

            return {
                'fundamental_report': result,
                'execution_log': state.get('execution_log', []) + [
                    {'agent': '基本面分析师', 'status': 'success'}
                ]
            }

        except Exception as e:
            logger.error(f"基本面分析失败: {e}")
            return {
                'fundamental_report': {'error': str(e)},
                'execution_log': state.get('execution_log', []) + [
                    {'agent': '基本面分析师', 'status': 'failed', 'error': str(e)}
                ]
            }


def _summarize_data(data: Any) -> str:
    """将数据摘要为字符串，用于AI prompt"""
    if data is None:
        return "无数据"
    if isinstance(data, dict):
        lines = []
        for k, v in list(data.items())[:15]:
            lines.append(f"  {k}: {v}")
        return "\n".join(lines) if lines else "空字典"
    return str(data)[:500]
