"""
Input: 用户请求参数(stock_code, market_type, research_depth)
Output: 完整的分析状态对象，各Agent共享读写
Pos: app/agents/state.py - 所有Agent的共享状态定义

一旦我被修改，请更新我的头部注释，以及所属文件夹的md。
"""
import operator
from typing import TypedDict, Annotated, Optional, List, Dict, Any
from langgraph.graph.message import add_messages


class StockAnalysisState(TypedDict):
    """股票分析Agent系统的共享状态"""
    # 输入参数
    stock_code: str
    market_type: str  # A, HK, US
    research_depth: int  # 1-5, 控制调用哪些Agent

    # 消息历史(LangGraph标准)
    messages: Annotated[list, add_messages]

    # 各Agent分析结果
    technical_report: Optional[Dict[str, Any]]
    fundamental_report: Optional[Dict[str, Any]]
    capital_flow_report: Optional[Dict[str, Any]]
    sentiment_report: Optional[Dict[str, Any]]

    # 辩论结果
    bull_case: Optional[str]
    bear_case: Optional[str]
    debate_summary: Optional[str]

    # 风险与决策
    risk_assessment: Optional[Dict[str, Any]]
    final_decision: Optional[Dict[str, Any]]  # {action, reasoning, confidence, price_targets}

    # 元数据
    execution_log: List[Dict[str, Any]]
    progress: float  # 0.0 - 100.0
    errors: List[str]
