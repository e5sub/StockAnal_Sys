"""
Input: StockAnalysisState (stock_code + 前置分析结果)
Output: StockAnalysisState (risk_assessment已填充)
Pos: app/agents/risk_manager.py - 风险管理Agent

一旦我被修改，请更新我的头部注释，以及所属文件夹的md。
"""
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


class RiskManagerAgent:
    """风险管理Agent，包装 risk_monitor.py"""

    name = "风险管理官"

    @staticmethod
    def analyze(state: Dict[str, Any]) -> Dict[str, Any]:
        from app.analysis.risk_monitor import RiskMonitor
        from app.analysis.stock_analyzer import StockAnalyzer

        stock_code = state['stock_code']
        market_type = state.get('market_type', 'A')

        try:
            analyzer = StockAnalyzer()
            rm = RiskMonitor(analyzer)
            result = rm.analyze_stock_risk(stock_code, market_type)

            return {
                'risk_assessment': result or {'error': '风险评估未返回结果'},
                'execution_log': state.get('execution_log', []) + [
                    {'agent': '风险管理官', 'status': 'success' if result else 'partial'}
                ]
            }
        except Exception as e:
            logger.error(f"风险评估失败: {e}")
            return {
                'risk_assessment': {'error': str(e)},
                'execution_log': state.get('execution_log', []) + [
                    {'agent': '风险管理官', 'status': 'failed', 'error': str(e)}
                ]
            }
