# -*- coding: utf-8 -*-
"""
akshare适配器 - 老王说：内部多数据源自动切换！
东财挂了切同花顺，同花顺挂了切新浪，新浪挂了切腾讯...
Input: 股票代码、日期范围等查询参数
Output: DataFrame或Dict格式的股票/财务/板块数据
Pos: app/adapters层，作为主数据源适配器被fallback_manager调度
一旦我被修改，请更新我的头部注释，以及所属文件夹的md。
"""
import akshare as ak
import pandas as pd
import logging
from typing import List, Dict, Optional
from .base_adapter import BaseAdapter

logger = logging.getLogger(__name__)


class AkshareAdapter(BaseAdapter):
    """akshare数据源适配器，支持内部多数据源冗余"""

    # 字段映射：统一不同数据源的返回格式
    FIELD_MAPPING = {
        'stock_zh_a_hist': {
            '日期': 'date', '开盘': 'open', '收盘': 'close',
            '最高': 'high', '最低': 'low', '成交量': 'volume', '成交额': 'amount'
        },
        'stock_zh_a_hist_tx': {},  # 腾讯接口字段已是英文
    }

    @property
    def name(self) -> str:
        return "akshare"

    def _format_code_for_tx(self, code: str) -> str:
        """转换股票代码为腾讯格式：000001 -> sz000001"""
        code = code.replace('.SH', '').replace('.SZ', '').replace('sh', '').replace('sz', '')
        prefix = 'sh' if code.startswith('6') else 'sz'
        return f"{prefix}{code}"

    def get_stock_history(self, code: str, start_date: str, end_date: str,
                          adjust: str = "qfq") -> pd.DataFrame:
        """获取股票历史K线 - 东财挂了自动切腾讯"""
        # 清洗并验证股票代码
        code = str(code).strip()
        for prefix in ['.SH', '.SZ', '.sh', '.sz', 'SH', 'SZ', 'sh', 'sz']:
            code = code.replace(prefix, '')
        if not code or not code.isdigit() or len(code) != 6:
            logger.error(f"无效的股票代码: {code}")
            return pd.DataFrame()

        # 尝试东财接口
        try:
            df = ak.stock_zh_a_hist(symbol=code, start_date=start_date,
                                    end_date=end_date, adjust=adjust)
            if df is not None and not df.empty:
                # 只 rename 存在的列
                mapping = {k: v for k, v in self.FIELD_MAPPING['stock_zh_a_hist'].items() if k in df.columns}
                if mapping:
                    df = df.rename(columns=mapping)
                return df
        except Exception as e:
            logger.warning(f"akshare东财接口失败(symbol={code}): {type(e).__name__}: {e}")

        # 东财挂了，切腾讯
        try:
            tx_code = self._format_code_for_tx(code)
            df = ak.stock_zh_a_hist_tx(symbol=tx_code, start_date=start_date,
                                       end_date=end_date, adjust=adjust)
            if df is not None and not df.empty:
                return df
        except Exception as e:
            logger.warning(f"akshare腾讯接口失败(symbol={code}): {type(e).__name__}: {e}")

        return pd.DataFrame()

    def get_index_stocks(self, index_code: str) -> List[str]:
        """获取指数成分股"""
        try:
            df = ak.index_stock_cons_weight_csindex(symbol=index_code)
            if df is not None and not df.empty:
                col = '成分券代码' if '成分券代码' in df.columns else df.columns[0]
                return df[col].tolist()
        except Exception as e:
            logger.warning(f"获取指数成分股失败(index={index_code}): {type(e).__name__}: {e}")
        return []

    def get_stock_info(self, code: str) -> Dict:
        """获取股票基本信息 - 东财→雪球"""
        code = code.replace('.SH', '').replace('.SZ', '').replace('sh', '').replace('sz', '')

        # 东财
        try:
            df = ak.stock_individual_info_em(symbol=code)
            if df is not None and not df.empty:
                return dict(zip(df['item'], df['value']))
        except Exception as e:
            logger.warning(f"akshare东财个股信息失败(symbol={code}): {type(e).__name__}: {e}")

        # 雪球
        try:
            df = ak.stock_individual_basic_info_xq(symbol=code)
            if df is not None and not df.empty:
                return df.to_dict('records')[0] if len(df) > 0 else {}
        except Exception as e:
            logger.warning(f"akshare雪球个股信息失败(symbol={code}): {type(e).__name__}: {e}")

        return {}

    def get_financial_data(self, code: str) -> Dict:
        """获取财务数据 - 东财→同花顺"""
        code = code.replace('.SH', '').replace('.SZ', '').replace('sh', '').replace('sz', '')

        # 东财财务分析指标
        try:
            df = ak.stock_financial_analysis_indicator(symbol=code, start_year="2023")
            if df is not None and not df.empty:
                return {'indicator': df.to_dict('records')}
        except Exception as e:
            logger.warning(f"akshare东财财务指标失败(symbol={code}): {type(e).__name__}: {e}")

        # 同花顺财务摘要
        try:
            df = ak.stock_financial_abstract_ths(symbol=code)
            if df is not None and not df.empty:
                return {'abstract': df.to_dict('records')}
        except Exception as e:
            logger.warning(f"akshare同花顺财务摘要失败(symbol={code}): {type(e).__name__}: {e}")

        return {}

    def get_board_stocks(self, board: str) -> List[str]:
        """获取板块股票列表"""
        board_map = {
            'all': 'stock_zh_a_spot_em',
            'sh': 'stock_sh_a_spot_em',
            'sz': 'stock_sz_a_spot_em',
            'bj': 'stock_bj_a_spot_em',
            'cyb': 'stock_cy_a_spot_em',
            'kcb': 'stock_kc_a_spot_em',
        }
        func_name = board_map.get(board)
        if not func_name:
            return []

        try:
            func = getattr(ak, func_name)
            df = func()
            if df is not None and not df.empty:
                col = '代码' if '代码' in df.columns else df.columns[0]
                return df[col].tolist()
        except Exception as e:
            logger.warning(f"获取板块股票列表失败(board={board}): {type(e).__name__}: {e}")
        return []

    def get_industry_list(self) -> pd.DataFrame:
        """获取行业板块列表 - 东财→同花顺"""
        # 东财
        try:
            df = ak.stock_board_industry_name_em()
            if df is not None and not df.empty:
                return df
        except Exception as e:
            logger.warning(f"akshare东财行业列表失败: {type(e).__name__}: {e}")

        # 同花顺
        try:
            df = ak.stock_board_industry_summary_ths()
            if df is not None and not df.empty:
                return df
        except Exception as e:
            logger.warning(f"akshare同花顺行业列表失败: {type(e).__name__}: {e}")

        return pd.DataFrame()

    def get_industry_stocks(self, industry: str) -> List[str]:
        """获取行业成分股"""
        try:
            df = ak.stock_board_industry_cons_em(symbol=industry)
            if df is not None and not df.empty:
                col = '代码' if '代码' in df.columns else df.columns[0]
                return df[col].tolist()
        except Exception as e:
            logger.warning(f"获取行业成分股失败(industry={industry}): {type(e).__name__}: {e}")
        return []

    def get_concept_stocks(self, concept: str) -> List[str]:
        """获取概念板块成分股代码列表"""
        # 先尝试概念板块
        try:
            df = ak.stock_board_concept_cons_em(symbol=concept)
            if df is not None and not df.empty:
                col = '代码' if '代码' in df.columns else df.columns[0]
                return df[col].tolist()
        except Exception as e:
            logger.warning(f"获取概念成分股失败(concept={concept}): {type(e).__name__}: {e}")

        # 概念失败，尝试行业板块
        try:
            df = ak.stock_board_industry_cons_em(symbol=concept)
            if df is not None and not df.empty:
                col = '代码' if '代码' in df.columns else df.columns[0]
                return df[col].tolist()
        except Exception as e:
            logger.warning(f"获取行业成分股失败(concept={concept}): {type(e).__name__}: {e}")

        return []

    def get_concept_stocks_detail(self, concept: str) -> List[Dict]:
        """获取概念板块成分股详细信息（含名称、价格等）"""
        # 先尝试概念板块
        try:
            df = ak.stock_board_concept_cons_em(symbol=concept)
            if df is not None and not df.empty:
                return self._parse_board_stocks_df(df)
        except Exception as e:
            logger.warning(f"获取概念成分股详情失败(concept={concept}): {type(e).__name__}: {e}")

        # 概念失败，尝试行业板块
        try:
            df = ak.stock_board_industry_cons_em(symbol=concept)
            if df is not None and not df.empty:
                return self._parse_board_stocks_df(df)
        except Exception as e:
            logger.warning(f"获取行业成分股详情失败(concept={concept}): {type(e).__name__}: {e}")

        return []

    def _parse_board_stocks_df(self, df) -> List[Dict]:
        """解析板块成分股DataFrame为字典列表"""
        import math
        result = []
        for _, row in df.iterrows():
            price = row.get("最新价", 0)
            change = row.get("涨跌幅", 0)
            # 处理NaN值
            price = 0 if (price is None or (isinstance(price, float) and math.isnan(price))) else float(price)
            change = 0 if (change is None or (isinstance(change, float) and math.isnan(change))) else float(change)
            item = {
                "code": str(row.get("代码", "")),
                "name": str(row.get("名称", "")),
                "price": price,
                "change_percent": change,
                "main_net_inflow": 0,
                "main_net_inflow_percent": 0
            }
            result.append(item)
        return result

    def get_capital_flow(self, code: str) -> Dict:
        """获取资金流向"""
        try:
            df = ak.stock_individual_fund_flow(stock=code, market="sh" if code.startswith('6') else "sz")
            if df is not None and not df.empty:
                return {'flow': df.to_dict('records')}
        except Exception as e:
            logger.warning(f"获取资金流向失败(code={code}): {type(e).__name__}: {e}")
        return {}

    def get_north_flow(self) -> pd.DataFrame:
        """获取北向资金"""
        try:
            df = ak.stock_hsgt_hist_em(symbol="沪股通")
            return df if df is not None else pd.DataFrame()
        except Exception as e:
            logger.warning(f"获取北向资金失败: {type(e).__name__}: {e}")
            return pd.DataFrame()

    def health_check(self) -> bool:
        """健康检查"""
        try:
            df = ak.stock_zh_a_spot_em()
            return df is not None and len(df) > 0
        except Exception as e:
            logger.warning(f"akshare健康检查失败: {type(e).__name__}: {e}")
            return False
