"""
Input: StockAnalysisState (stock_code, market_type)
Output: StockAnalysisState (sentiment_report已填充)
Pos: 舆情分析Agent，包装news_fetcher提供LLM增强情绪分析
一旦我被修改，请更新我的头部注释，以及所属文件夹的md。
"""
import logging
from typing import Dict, Any, List

logger = logging.getLogger(__name__)


class SentimentAnalystAgent:
    """舆情分析师Agent"""

    name = "舆情分析师"

    @staticmethod
    def analyze(state: Dict[str, Any]) -> Dict[str, Any]:
        """执行舆情分析"""
        from app.analysis.news_fetcher import NewsFetcher
        from app.core.ai_client import get_ai_client, chat_completion, get_completion_content

        stock_code = state['stock_code']

        try:
            # 获取最新新闻
            fetcher = NewsFetcher()
            news_list = fetcher.get_latest_news(days=3, limit=20)

            # 筛选与该股票相关的新闻
            relevant_news = _filter_relevant_news(news_list, stock_code)

            result = {
                'total_news': len(news_list) if news_list else 0,
                'relevant_news_count': len(relevant_news),
                'news_items': relevant_news[:10]
            }

            # 用AI进行情绪分析
            client = get_ai_client()
            if client:
                news_text = _format_news_for_prompt(relevant_news[:10])

                prompt = f"""你是资深舆情分析师。基于以下与股票 {stock_code} 相关的最新新闻，进行舆情分析：

相关新闻数量: {len(relevant_news)}
最新新闻内容:
{news_text}

请给出：
1. 整体舆情情绪评分（1-10分，1=极度悲观，5=中性，10=极度乐观）
2. 舆情情绪判定（乐观/中性/悲观）
3. 关键舆情事件摘要（利好/利空分别列出）
4. 市场情绪可能对股价的短期影响分析
5. 需要重点关注的风险信号"""

                response, error = chat_completion(
                    client,
                    [{"role": "user", "content": prompt}],
                    temperature=0.7,
                    max_tokens=1200
                )
                if not error:
                    result['ai_commentary'] = get_completion_content(response)

            return {
                'sentiment_report': result,
                'execution_log': state.get('execution_log', []) + [
                    {'agent': '舆情分析师', 'status': 'success'}
                ]
            }

        except Exception as e:
            logger.error(f"舆情分析失败: {e}")
            return {
                'sentiment_report': {'error': str(e)},
                'execution_log': state.get('execution_log', []) + [
                    {'agent': '舆情分析师', 'status': 'failed', 'error': str(e)}
                ]
            }


def _filter_relevant_news(news_list: List, stock_code: str) -> List:
    """筛选与股票代码相关的新闻"""
    if not news_list:
        return []

    relevant = []
    for item in news_list:
        content = ""
        if isinstance(item, dict):
            content = f"{item.get('title', '')} {item.get('content', '')} {item.get('summary', '')}"
        elif isinstance(item, str):
            content = item

        if stock_code in content:
            relevant.append(item)

    # 如果没有直接相关新闻，返回最新的通用市场新闻
    if not relevant:
        return news_list[:5] if news_list else []

    return relevant


def _format_news_for_prompt(news_items: List) -> str:
    """格式化新闻列表为prompt可读文本"""
    if not news_items:
        return "暂无相关新闻"

    lines = []
    for i, item in enumerate(news_items, 1):
        if isinstance(item, dict):
            title = item.get('title', '无标题')
            date = item.get('date', item.get('publish_time', ''))
            lines.append(f"  {i}. [{date}] {title}")
        elif isinstance(item, str):
            lines.append(f"  {i}. {item[:100]}")

    return "\n".join(lines) if lines else "暂无相关新闻"
