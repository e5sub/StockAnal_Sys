"""
Input: 各分析模块的方法调用
Output: LangChain @tool 包装的标准工具函数
Pos: app/core/tools.py - 所有Agent共享的工具函数注册表

一旦我被修改，请更新我的头部注释，以及所属文件夹的md。
"""
import logging
from langchain_core.tools import tool
from app.core.data_provider import get_data_provider

logger = logging.getLogger(__name__)


# === 数据获取工具 ===

@tool
def get_stock_data(stock_code: str, market_type: str = 'A', days: int = 120) -> str:
    """获取股票历史K线数据，返回最近N天的OHLCV数据摘要"""
    from datetime import datetime, timedelta
    dp = get_data_provider()
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
    try:
        df = dp.get_stock_history(stock_code, start_date, end_date)
        if df is None or df.empty:
            return f"未获取到{stock_code}的数据"
        latest = df.iloc[-1]
        summary = (
            f"股票{stock_code} 最新数据({df['date'].iloc[-1]}):\n"
            f"收盘价: {latest.get('close', 'N/A')}\n"
            f"最高价: {latest.get('high', 'N/A')}\n"
            f"最低价: {latest.get('low', 'N/A')}\n"
            f"成交量: {latest.get('volume', 'N/A')}\n"
            f"数据范围: {df['date'].iloc[0]} ~ {df['date'].iloc[-1]}, 共{len(df)}条"
        )
        return summary
    except Exception as e:
        return f"获取数据失败: {str(e)}"


@tool
def get_technical_indicators(stock_code: str, market_type: str = 'A') -> str:
    """计算股票技术指标(MA/RSI/MACD/布林带等)并返回摘要"""
    from app.analysis.stock_analyzer import StockAnalyzer
    try:
        analyzer = StockAnalyzer()
        result = analyzer.quick_analyze_stock(stock_code, market_type)
        if 'error' in result:
            return f"技术分析失败: {result['error']}"
        return str(result)
    except Exception as e:
        return f"技术分析失败: {str(e)}"


@tool
def get_fundamental_data(stock_code: str) -> str:
    """获取股票基本面数据(PE/PB/ROE/净利润等财务指标)"""
    from app.analysis.fundamental_analyzer import FundamentalAnalyzer
    try:
        fa = FundamentalAnalyzer()
        result = fa.get_financial_indicators(stock_code)
        if not result:
            return f"未获取到{stock_code}的基本面数据"
        return str(result)
    except Exception as e:
        return f"基本面数据获取失败: {str(e)}"


@tool
def get_capital_flow(stock_code: str) -> str:
    """获取股票资金流向数据(主力/北向/机构资金)"""
    from app.analysis.capital_flow_analyzer import CapitalFlowAnalyzer
    try:
        cfa = CapitalFlowAnalyzer()
        result = cfa.get_individual_fund_flow(stock_code)
        if not result:
            return f"未获取到{stock_code}的资金流向数据"
        return str(result)
    except Exception as e:
        return f"资金流向获取失败: {str(e)}"


@tool
def get_stock_news(stock_code: str, limit: int = 5) -> str:
    """获取股票相关的最新新闻和舆情信息"""
    from app.analysis.news_fetcher import news_fetcher
    try:
        news = news_fetcher.get_latest_news(days=1, limit=limit)
        if not news:
            return "暂无最新新闻"
        result = []
        for item in news[:limit]:
            result.append(f"[{item.get('time', '')}] {item.get('content', '')[:100]}")
        return '\n'.join(result)
    except Exception as e:
        return f"新闻获取失败: {str(e)}"


@tool
def get_risk_assessment(stock_code: str, market_type: str = 'A') -> str:
    """评估股票的多维度风险(波动率/趋势/反转/成交量风险)"""
    from app.analysis.risk_monitor import RiskMonitor
    from app.analysis.stock_analyzer import StockAnalyzer
    try:
        analyzer = StockAnalyzer()
        rm = RiskMonitor(analyzer)
        result = rm.analyze_stock_risk(stock_code, market_type)
        if not result:
            return f"未获取到{stock_code}的风险数据"
        return str(result)
    except Exception as e:
        return f"风险评估失败: {str(e)}"


# 工具注册表
ALL_TOOLS = [
    get_stock_data,
    get_technical_indicators,
    get_fundamental_data,
    get_capital_flow,
    get_stock_news,
    get_risk_assessment,
]

# 按职能分组
TECHNICAL_TOOLS = [get_stock_data, get_technical_indicators]
FUNDAMENTAL_TOOLS = [get_fundamental_data]
CAPITAL_FLOW_TOOLS = [get_capital_flow]
SENTIMENT_TOOLS = [get_stock_news]
RISK_TOOLS = [get_risk_assessment]
