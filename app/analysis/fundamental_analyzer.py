# -*- coding: utf-8 -*-
"""
智能分析系统（股票） - 股票市场数据分析系统
开发者：熊猫大侠
版本：v2.1.0
许可证：MIT License
"""
# fundamental_analyzer.py
import akshare as ak
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


class FundamentalAnalyzer:
    def __init__(self):
        """初始化基础分析类"""
        self.data_cache = {}
        # 初始化统一数据层
        from app.core.data_provider import get_data_provider
        self.data_provider = get_data_provider()

    def _safe_get_column(self, df, col_options, row_idx=0, default=0):
        """安全获取DataFrame列值，支持多个候选列名"""
        if df is None or df.empty:
            return default
        for col in col_options if isinstance(col_options, list) else [col_options]:
            if col in df.columns:
                try:
                    val = df[col].iloc[row_idx]
                    return float(val) if val is not None and str(val) != '' else default
                except (IndexError, ValueError, TypeError):
                    continue
        return default

    def get_financial_indicators(self, stock_code, progress_callback=None):
        """获取财务指标数据 - 使用DataProvider统一数据层"""
        if progress_callback:
            progress_callback(5, "正在获取财务指标...")
        try:
            # 使用DataProvider获取财务数据（自动故障转移）
            fin_result = self.data_provider.get_financial_data(stock_code)
            financial_data = fin_result.get('indicator', [])
            if isinstance(financial_data, list) and len(financial_data) > 0:
                financial_data = pd.DataFrame(financial_data)
            else:
                financial_data = pd.DataFrame()

            # 获取最新估值指标（暂保留akshare直接调用，DataProvider暂不支持）
            valuation = None
            try:
                valuation = ak.stock_value_em(symbol=stock_code)
            except Exception as e:
                logger.warning(f"获取估值指标失败: {e}")

            # 整合数据（使用安全列名访问）
            indicators = {
                'pe_ttm': self._safe_get_column(valuation, ['PE(TTM)', 'PE-TTM', 'pe_ttm'], default=0),
                'pb': self._safe_get_column(valuation, ['市净率', 'PB', 'pb'], default=0),
                'ps_ttm': self._safe_get_column(valuation, ['市销率', 'PS(TTM)', 'ps_ttm'], default=0),
                'roe': self._safe_get_column(financial_data, ['加权净资产收益率(%)', '加权ROE(%)', 'ROE', 'roe'], default=0),
                'gross_margin': self._safe_get_column(financial_data, ['销售毛利率(%)', '毛利率(%)', 'gross_margin'], default=0),
                'net_profit_margin': self._safe_get_column(financial_data, ['总资产净利润率(%)', '净利润率(%)', '净资产收益率(%)', 'net_profit_margin'], default=0),
                'debt_ratio': self._safe_get_column(financial_data, ['资产负债率(%)', '负债率(%)', 'debt_ratio'], default=0)
            }
            if progress_callback:
                progress_callback(10, "财务指标获取成功")
            return indicators
        except Exception as e:
            print(f"获取财务指标出错: {str(e)}")
            if progress_callback:
                progress_callback(10, f"财务指标获取失败: {e}")
            return {}

    def get_growth_data(self, stock_code, progress_callback=None):
        """获取成长性数据"""
        if progress_callback:
            progress_callback(15, "正在获取成长性数据...")
        try:
            # 获取历年财务数据
            try:
                financial_data = ak.stock_financial_abstract(symbol=stock_code)
            except Exception as e:
                logger.warning(f"获取财务摘要数据失败: {e}")
                if progress_callback:
                    progress_callback(20, f"成长性数据获取失败: {e}")
                return {}

            if financial_data is None or financial_data.empty:
                logger.warning(f"股票 {stock_code} 财务摘要数据为空")
                if progress_callback:
                    progress_callback(20, "成长性数据为空")
                return {}

            # --- 修复：兼容不同的财务字段名 ---
            # 查找营业收入列
            revenue_col = None
            if '营业总收入' in financial_data.columns:
                revenue_col = '营业总收入'
            elif '营业收入' in financial_data.columns:
                revenue_col = '营业收入'
            
            # 查找净利润列
            profit_col = None
            if '归属母公司股东的净利润' in financial_data.columns:
                profit_col = '归属母公司股东的净利润'
            elif '净利润' in financial_data.columns:
                profit_col = '净利润'

            growth = {}
            # 仅在找到列时计算
            if revenue_col:
                revenue = financial_data[revenue_col].astype(float)
                growth['revenue_growth_3y'] = self._calculate_cagr(revenue, 3)
                growth['revenue_growth_5y'] = self._calculate_cagr(revenue, 5)
            else:
                print(f"警告: 股票 {stock_code} 未找到 '营业总收入' 或 '营业收入' 列")

            if profit_col:
                net_profit = financial_data[profit_col].astype(float)
                growth['profit_growth_3y'] = self._calculate_cagr(net_profit, 3)
                growth['profit_growth_5y'] = self._calculate_cagr(net_profit, 5)
            else:
                print(f"警告: 股票 {stock_code} 未找到 '归属母公司股东的净利润' 或 '净利润' 列")
            # --- 修复结束 ---

            if progress_callback:
                progress_callback(20, "成长性数据获取成功")
            return growth
        except Exception as e:
            # 保持现有的异常捕获，以防akshare调用本身失败
            print(f"获取成长数据出错: {str(e)}")
            if progress_callback:
                progress_callback(20, f"成长性数据获取失败: {e}")
            return {}

    def _calculate_cagr(self, series, years):
        """计算复合年增长率"""
        try:
            if len(series) < years:
                return None

            latest = series.iloc[0]
            earlier = series.iloc[min(years, len(series) - 1)]

            if earlier <= 0:
                return None

            return ((latest / earlier) ** (1 / years) - 1) * 100
        except (IndexError, ValueError, TypeError, ZeroDivisionError) as e:
            logger.warning(f"计算CAGR失败: {e}")
            return None

    def calculate_fundamental_score(self, stock_code, progress_callback=None):
        """计算基本面综合评分"""
        if progress_callback:
            progress_callback(0, "启动基本面分析模块...")

        try:
            indicators = self.get_financial_indicators(stock_code, progress_callback=progress_callback)
        except Exception as e:
            logger.error(f"获取财务指标异常: {e}")
            indicators = {}

        try:
            growth = self.get_growth_data(stock_code, progress_callback=progress_callback)
        except Exception as e:
            logger.error(f"获取成长数据异常: {e}")
            growth = {}

        if progress_callback:
            progress_callback(25, "计算基本面综合评分...")

        # 估值评分 (30分)
        valuation_score = 0
        if 'pe_ttm' in indicators and indicators['pe_ttm'] > 0:
            pe = indicators['pe_ttm']
            if pe < 15:
                valuation_score += 25
            elif pe < 25:
                valuation_score += 20
            elif pe < 35:
                valuation_score += 15
            elif pe < 50:
                valuation_score += 10
            else:
                valuation_score += 5

        # 财务健康评分 (40分)
        financial_score = 0
        if 'roe' in indicators:
            roe = indicators['roe']
            if roe > 20:
                financial_score += 15
            elif roe > 15:
                financial_score += 12
            elif roe > 10:
                financial_score += 8
            elif roe > 5:
                financial_score += 4

        if 'debt_ratio' in indicators:
            debt_ratio = indicators['debt_ratio']
            if debt_ratio < 30:
                financial_score += 15
            elif debt_ratio < 50:
                financial_score += 10
            elif debt_ratio < 70:
                financial_score += 5

        # 成长性评分 (30分)
        growth_score = 0
        if 'revenue_growth_3y' in growth and growth['revenue_growth_3y']:
            rev_growth = growth['revenue_growth_3y']
            if rev_growth > 30:
                growth_score += 15
            elif rev_growth > 20:
                growth_score += 12
            elif rev_growth > 10:
                growth_score += 8
            elif rev_growth > 0:
                growth_score += 4

        if 'profit_growth_3y' in growth and growth['profit_growth_3y']:
            profit_growth = growth['profit_growth_3y']
            if profit_growth > 30:
                growth_score += 15
            elif profit_growth > 20:
                growth_score += 12
            elif profit_growth > 10:
                growth_score += 8
            elif profit_growth > 0:
                growth_score += 4

        # 计算总分
        total_score = valuation_score + financial_score + growth_score
        
        if progress_callback:
            progress_callback(30, "基本面分析完成")

        return {
            'total': total_score,
            'valuation': valuation_score,
            'financial_health': financial_score,
            'growth': growth_score,
            'details': {
                'indicators': indicators,
                'growth': growth
            }
        }