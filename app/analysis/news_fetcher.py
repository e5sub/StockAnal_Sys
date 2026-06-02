# news_fetcher.py
# -*- coding: utf-8 -*-
"""
智能分析系统（股票） - 新闻数据获取模块
功能: 获取财联社电报新闻数据并缓存到本地，避免重复内容
"""

import os
import json
import logging
import time
import hashlib
import re
from datetime import datetime, timedelta, date, timezone
import akshare as ak
import pandas as pd
import requests

# 设置日志
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('news_fetcher')

# 自定义JSON编码器，处理日期类型
class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if pd.isna(obj):  # 处理pandas中的NaN
            return None
        return super(DateEncoder, self).default(obj)

class NewsFetcher:
    def __init__(self, save_dir="data/news"):
        """初始化新闻获取器"""
        self.save_dir = save_dir
        # 确保保存目录存在
        os.makedirs(self.save_dir, exist_ok=True)
        self.last_fetch_time = None

        # 哈希集合用于快速判断新闻是否已存在
        self.news_hashes = set()
        # 加载已有的新闻哈希
        self._load_existing_hashes()

    def _load_existing_hashes(self):
        """加载已有文件中的新闻哈希值"""
        try:
            # 获取最近7天的文件来加载哈希值
            today = datetime.now()
            for i in range(7):  # 检查最近7天的数据
                date = today - timedelta(days=i)
                filename = self.get_news_filename(date)

                if os.path.exists(filename):
                    with open(filename, 'r', encoding='utf-8') as f:
                        try:
                            news_data = json.load(f)
                            for item in news_data:
                                # 如果有哈希字段就直接使用，否则计算新的哈希
                                if 'hash' in item:
                                    self.news_hashes.add(item['hash'])
                                else:
                                    content_hash = self._calculate_hash(item['content'])
                                    self.news_hashes.add(content_hash)
                        except json.JSONDecodeError:
                            logger.warning(f"文件 {filename} 格式错误，跳过加载哈希值")

            logger.info(f"已加载 {len(self.news_hashes)} 条新闻哈希值")

            # 限制哈希集合大小，防止内存无限增长
            MAX_HASHES = 50000
            if len(self.news_hashes) > MAX_HASHES:
                # 保留最近的哈希（集合无序，但控制上限）
                self.news_hashes = set(list(self.news_hashes)[-MAX_HASHES:])
                logger.info(f"哈希集合超过上限，已截断至 {MAX_HASHES} 条")

        except Exception as e:
            logger.error(f"加载新闻哈希时出错: {str(e)}")
            # 不清空已加载的哈希，保留部分去重能力

    def _calculate_hash(self, content):
        """计算内容哈希，带文本规范化"""
        if not content:
            return None
        # 规范化：去除多余空白、统一格式
        normalized = ' '.join(str(content).split()).strip()
        return hashlib.md5(normalized.encode('utf-8')).hexdigest()

    def get_news_filename(self, date=None):
        """获取指定日期的新闻文件名"""
        if date is None:
            date = datetime.now().strftime('%Y%m%d')
        else:
            date = date.strftime('%Y%m%d')
        return os.path.join(self.save_dir, f"news_{date}.json")

    def _extract_js_object(self, html, marker):
        """从页面脚本中提取 marker 后面的 JSON 对象。"""
        start = html.find(marker)
        if start < 0:
            raise ValueError(f"页面中未找到 {marker}")

        start += len(marker)
        brace_count = 0
        in_string = False
        escaped = False

        for index, char in enumerate(html[start:], start):
            if in_string:
                if escaped:
                    escaped = False
                elif char == "\\":
                    escaped = True
                elif char == '"':
                    in_string = False
                continue

            if char == '"':
                in_string = True
            elif char == "{":
                brace_count += 1
            elif char == "}":
                brace_count -= 1
                if brace_count == 0:
                    return html[start:index + 1]

        raise ValueError("未能完整解析财联社页面数据")

    def _fetch_cls_telegraph_df(self):
        """获取财联社移动端电报页内嵌的最新 roll_data。"""
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/125.0 Safari/537.36"
            ),
            "Referer": "https://m.cls.cn/telegraph",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        response = requests.get("https://m.cls.cn/telegraph", headers=headers, timeout=10)
        response.raise_for_status()

        data_text = self._extract_js_object(response.text, "__NEXT_DATA__ = ")
        page_data = json.loads(data_text)
        roll_data = (
            page_data.get("props", {})
            .get("initialState", {})
            .get("roll_data", [])
        )

        if not roll_data:
            return pd.DataFrame()

        shanghai_tz = timezone(timedelta(hours=8))
        rows = []
        for item in roll_data:
            timestamp = item.get("ctime") or item.get("modified_time")
            if timestamp:
                publish_dt = datetime.fromtimestamp(int(timestamp), shanghai_tz)
                pub_date = publish_dt.strftime("%Y-%m-%d")
                pub_time = publish_dt.strftime("%H:%M:%S")
            else:
                pub_date = ""
                pub_time = ""

            title = item.get("title") or ""
            content = item.get("content") or item.get("brief") or title
            rows.append({
                "标题": title,
                "内容": content,
                "发布日期": pub_date,
                "发布时间": pub_time,
                "链接": item.get("shareurl") or "",
                "财联社ID": item.get("id"),
                "阅读数": item.get("reading_num"),
                "级别": item.get("level"),
            })

        return pd.DataFrame(rows)

    def _fetch_news_dataframe(self):
        """按优先级获取新闻数据，单个AKShare接口失效时自动降级。"""
        sources = [
            ("财联社电报", self._fetch_cls_telegraph_df),
            ("AKShare财联社电报", lambda: ak.stock_info_global_cls(symbol="全部")),
            ("东方财富全球快讯", lambda: getattr(ak, "stock_info_global_em")()),
            ("新浪财经全球快讯", lambda: getattr(ak, "stock_info_global_sina")()),
        ]
        last_error = None

        for source_name, fetcher in sources:
            try:
                logger.info(f"开始获取{source_name}数据")
                df = fetcher()
                if df is not None and not df.empty:
                    logger.info(f"{source_name}数据获取成功: {df.shape}")
                    return source_name, df
                logger.warning(f"{source_name}返回空数据，尝试下一个新闻源")
            except Exception as e:
                last_error = e
                logger.warning(f"{source_name}接口失败，尝试下一个新闻源: {type(e).__name__}: {e}")

        if last_error:
            raise last_error
        return "", pd.DataFrame()

    def _normalize_news_row(self, row, source_name, now):
        """统一不同新闻接口的字段结构。"""
        title = str(row.get("标题", "") or row.get("新闻标题", "") or "")
        content = str(row.get("内容", "") or row.get("摘要", "") or row.get("新闻内容", "") or "")

        if not title and content:
            title = content[:80]
        if not content and title:
            content = title

        pub_date = row.get("发布日期", "")
        pub_time = row.get("发布时间", "")

        if not pub_date and source_name.startswith("新浪"):
            pub_time = row.get("时间", "")

        if isinstance(pub_time, (datetime, date)):
            pub_time_text = pub_time.isoformat()
        else:
            pub_time_text = str(pub_time)

        if isinstance(pub_date, (datetime, date)):
            pub_date_text = pub_date.isoformat()
        else:
            pub_date_text = str(pub_date)

        if not pub_date_text and pub_time_text:
            parsed_time = pd.to_datetime(pub_time_text, errors='coerce')
            if not pd.isna(parsed_time):
                pub_date_text = parsed_time.strftime('%Y-%m-%d')
                pub_time_text = parsed_time.strftime('%H:%M:%S')

        if not pub_date_text:
            pub_date_text = now.strftime('%Y-%m-%d')
        if not pub_time_text:
            pub_time_text = now.strftime('%H:%M:%S')

        return {
            "title": title,
            "content": content,
            "date": pub_date_text,
            "time": pub_time_text,
            "datetime": f"{pub_date_text} {pub_time_text}",
            "source": source_name,
            "url": str(row.get("链接", "") or row.get("新闻链接", "") or ""),
            "fetch_time": now.strftime('%Y-%m-%d %H:%M:%S'),
        }

    def fetch_and_save(self):
        """获取新闻并保存到JSON文件，避免重复内容"""
        try:
            # 获取当前时间
            now = datetime.now()

            source_name, stock_info_global_cls_df = self._fetch_news_dataframe()

            if stock_info_global_cls_df.empty:
                logger.warning("获取的新闻数据为空")
                return False

            # 打印DataFrame的信息和类型，帮助调试
            logger.info(f"获取的数据形状: {stock_info_global_cls_df.shape}")
            logger.info(f"数据列: {stock_info_global_cls_df.columns.tolist()}")
            logger.info(f"数据类型: \n{stock_info_global_cls_df.dtypes}")

            # 计数器
            total_count = 0
            new_count = 0

            # 转换为列表字典格式并添加哈希值
            news_list = []
            for _, row in stock_info_global_cls_df.iterrows():
                total_count += 1

                news_item = self._normalize_news_row(row, source_name, now)
                content = news_item["content"]
                title = news_item["title"]

                # 组合标题和内容进行去重
                combined = f"{title}||{content}"
                content_hash = self._calculate_hash(combined)

                # 检查是否已存在相同内容的新闻
                if content_hash in self.news_hashes:
                    continue  # 跳过已存在的新闻

                # 添加新的哈希值到集合
                self.news_hashes.add(content_hash)
                new_count += 1

                # 创建新闻项并添加哈希值
                news_item["hash"] = content_hash
                news_list.append(news_item)

            # 如果没有新的新闻，直接返回
            if not news_list:
                logger.info(f"没有新的新闻数据需要保存 (共检查 {total_count} 条)")
                return True

            # 获取文件名
            filename = self.get_news_filename()

            # 如果文件已存在，则合并新旧数据
            if os.path.exists(filename):
                with open(filename, 'r', encoding='utf-8') as f:
                    try:
                        existing_data = json.load(f)
                        # 合并数据，已经确保news_list中的内容都是新的
                        merged_news = existing_data + news_list
                        # 按时间排序
                        merged_news.sort(key=lambda x: x['datetime'], reverse=True)
                    except json.JSONDecodeError:
                        logger.warning(f"文件 {filename} 格式错误，使用新数据替换")
                        merged_news = sorted(news_list, key=lambda x: x['datetime'], reverse=True)
            else:
                # 如果文件不存在，直接使用新数据
                merged_news = sorted(news_list, key=lambda x: x['datetime'], reverse=True)

            # 保存合并后的数据，使用自定义编码器处理日期
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(merged_news, f, ensure_ascii=False, indent=2, cls=DateEncoder)

            logger.info(f"成功保存 {new_count} 条新闻数据 (共检查 {total_count} 条，过滤重复 {total_count - new_count} 条)")
            self.last_fetch_time = now
            return True

        except Exception as e:
            logger.error(f"获取或保存新闻数据时出错: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())  # 打印完整的堆栈跟踪，便于调试
            return False

    def get_latest_news(self, days=1, limit=50):
        """获取最近几天的新闻数据，并去除重复项"""
        news_data = []
        today = datetime.now()
        # 记录已处理的日期，便于日志
        processed_dates = []

        # 获取指定天数内的所有新闻
        for i in range(days):
            date = today - timedelta(days=i)
            date_str = date.strftime('%Y%m%d')
            filename = self.get_news_filename(date)

            if os.path.exists(filename):
                try:
                    with open(filename, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        news_data.extend(data)
                        processed_dates.append(date_str)
                        logger.info(f"已加载 {date_str} 新闻数据 {len(data)} 条")
                except Exception as e:
                    logger.error(f"读取文件 {filename} 时出错: {str(e)}")
            else:
                logger.warning(f"日期 {date_str} 的新闻文件不存在: {filename}")

        # 排序前记录总数
        total_before_sort = len(news_data)

        # 去除重复项
        # 使用内容哈希或已有的哈希字段作为唯一标识
        unique_news = {}
        duplicate_count = 0

        for item in news_data:
            # 优先使用已有的哈希值，如果没有则组合标题+内容计算哈希
            item_hash = item.get('hash')
            if not item_hash and 'content' in item:
                combined = f"{item.get('title', '')}||{item['content']}"
                item_hash = self._calculate_hash(combined)

            # 如果是新的哈希值，则添加到结果中
            if item_hash and item_hash not in unique_news:
                unique_news[item_hash] = item
            else:
                duplicate_count += 1

        # 转换回列表并按时间排序
        deduplicated_news = list(unique_news.values())
        deduplicated_news.sort(key=lambda x: x.get('datetime', ''), reverse=True)

        # 限制返回条数
        result = deduplicated_news[:limit]

        logger.info(f"获取最近 {days} 天新闻(处理日期:{','.join(processed_dates)}), "
                    f"共 {total_before_sort} 条, 去重后 {len(deduplicated_news)} 条, "
                    f"移除重复 {duplicate_count} 条, 返回最新 {len(result)} 条")

        return result

# 单例模式的新闻获取器
news_fetcher = NewsFetcher()

def fetch_news_task():
    """执行新闻获取任务"""
    logger.info("开始执行新闻获取任务")
    news_fetcher.fetch_and_save()
    logger.info("新闻获取任务完成")

def start_news_scheduler():
    """启动新闻获取定时任务"""
    import threading
    import time

    def _run_scheduler():
        while True:
            try:
                fetch_news_task()
                # 等待10分钟
                time.sleep(1800)
            except Exception as e:
                logger.error(f"定时任务执行出错: {str(e)}")
                time.sleep(60)  # 出错后等待1分钟再试

    # 创建并启动定时任务线程
    scheduler_thread = threading.Thread(target=_run_scheduler)
    scheduler_thread.daemon = True
    scheduler_thread.start()
    logger.info("新闻获取定时任务已启动")

# 初始获取一次数据
if __name__ == "__main__":
    fetch_news_task()
