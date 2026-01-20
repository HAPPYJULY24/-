"""
DataFetcher - The Smart Engine for Quant Data Bridge
Handles all data fetching, processing, and export logic.
"""

import pandas as pd
import yfinance as yf
import ccxt
from datetime import datetime, timedelta
import os
import pytz  # 🆕 时区处理


class DataFetcher:
    """
    Core class for fetching and processing financial data from multiple sources.
    """
    
    # Timeframe mapping for different APIs
    TIMEFRAME_MAP = {
        '1m': {'yf': '1m', 'ccxt': '1m'},
        '5m': {'yf': '5m', 'ccxt': '5m'},
        '15m': {'yf': '15m', 'ccxt': '15m'},
        '1h': {'yf': '1h', 'ccxt': '1h'},
        '1d': {'yf': '1d', 'ccxt': '1d'},
        '1w': {'yf': '1wk', 'ccxt': '1w'},   # 新增：1周
        '1M': {'yf': '1mo', 'ccxt': '1M'},   # 新增：1月
        '1y': {'yf': '1y', 'ccxt': '1y'},    # 新增：1年
    }
    
    def __init__(self):
        self.last_error = None
        self.store_dir = "data/store"  # 🆕 Master DB 目录
    
    def preprocess_code(self, code: str, asset_type: str) -> str:
        """
        Preprocess asset code based on asset type.
        
        Args:
            code: Raw code from user input
            asset_type: Type of asset (Malaysia Stock, US Stock, Futures - Global, Crypto)
        
        Returns:
            Processed code ready for API call
        """
        code = code.strip()
        
        if asset_type == "Malaysia Stock":
            # If code is pure digits, append .KL suffix
            if code.isdigit():
                return f"{code}.KL"
            return code
        elif asset_type == "US Stock":
            return code
        elif asset_type == "Futures - Global":
            # 修改：期货现在直接透传用户输入，不再强制 GC=F
            return code  # 用户可以输入 GC=F, CL=F, SI=F, ES=F 等任何期货代码
        elif asset_type == "Crypto":
            return code
        
        return code
    
    def fetch_data(self, asset_type: str, code: str, timeframe: str, 
                   start_date: datetime, end_date: datetime,
                   exchange: str = None, proxy_url: str = None,
                   filter_lunch: bool = False) -> pd.DataFrame:  # 🆕 v2.0: 午休过滤开关
        """
        Main data fetching router.
        
        Args:
            asset_type: Type of asset
            code: Asset code (already preprocessed)
            timeframe: Time granularity (1m, 5m, 15m, 1h, 1d)
            start_date: Start date for data
            end_date: End date for data
            exchange: Exchange name for crypto (e.g., "Luno (Malaysia)")  # 新增
            proxy_url: Proxy URL if enabled (e.g., "http://127.0.0.1:7890")  # 新增
        
        Returns:
            DataFrame with fetched data
        
        Raises:
            Exception: If data fetching fails
        """
        self.last_error = None
        
        try:
            # 修改：Futures - Global 与股票使用相同的 yfinance 路径
            if asset_type in ["Malaysia Stock", "US Stock", "Futures - Global"]:
                df = self._fetch_stock_futures(code, timeframe, start_date, end_date)
            elif asset_type == "Crypto":
                # 传递交易所和代理参数（新增）
                df = self._fetch_crypto(code, timeframe, start_date, end_date,
                                       exchange=exchange, proxy_url=proxy_url)
            else:
                raise ValueError(f"Unknown asset type: {asset_type}")
            
            if df is None or df.empty:
                raise ValueError(f"No data found for {code}")
            
            # Standardize the dataframe
            df = self.standardize_dataframe(df)
            
            # 🆕 v2.0: 时区标准化（强制启用）
            df = self._standardize_timezone(df)
            
            # 🆕 v2.0: 午休过滤（可选，由 UI 控制）
            if filter_lunch:
                df = self._filter_lunch_break(df, asset_type)
            
            return df
        
        except Exception as e:
            self.last_error = str(e)
            raise
    
    def _fetch_stock_futures(self, code: str, timeframe: str, 
                            start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """
        Fetch data using yfinance for stocks and futures.
        
        Args:
            code: Asset code
            timeframe: Time granularity
            start_date: Start date
            end_date: End date
        
        Returns:
            DataFrame with OHLCV data
        
        Raises:
            Exception: If yfinance API fails (including minute-level restrictions)
        """
        try:
            print(f"[DEBUG] yfinance: Creating ticker for {code}")
            ticker = yf.Ticker(code)
            interval = self.TIMEFRAME_MAP[timeframe]['yf']
            
            print(f"[DEBUG] yfinance: Fetching data with interval={interval}, start={start_date}, end={end_date}")
            # yfinance download
            df = ticker.history(
                start=start_date,
                end=end_date,
                interval=interval,
                auto_adjust=False
            )
            
            print(f"[DEBUG] yfinance: Received data type: {type(df)}")
            
            # 检查返回值是否为 None（断网时可能发生）
            if df is None:
                raise Exception(
                    f"网络连接失败！\n\n"
                    f"⚠️ 无法连接到数据源服务器。\n\n"
                    f"可能原因：\n"
                    f"1. 您的电脑未连接到互联网\n"
                    f"2. 防火墙阻止了程序访问网络\n"
                    f"3. 数据源服务器暂时无法访问\n\n"
                    f"建议：\n"
                    f"- 检查您的网络连接\n"
                    f"- 确认可以访问互联网\n"
                    f"- 稍后重试"
                )
            
            print(f"[DEBUG] yfinance: Received {len(df)} rows")
            
            if df.empty:
                raise ValueError(f"No data returned from yfinance for {code}. "
                               f"Asset may not exist or date range may be invalid.")
            
            # Reset index to make Date a column
            print("[DEBUG] yfinance: Resetting index...")
            df.reset_index(inplace=True)
            print(f"[DEBUG] yfinance: DataFrame columns: {list(df.columns)}")
            print(f"[DEBUG] yfinance: First row: {df.iloc[0].to_dict() if len(df) > 0 else 'N/A'}")
            
            return df
        
        except Exception as e:
            # Catch yfinance errors including minute-level data restrictions
            error_msg = str(e)
            error_type = type(e).__name__
            print(f"[DEBUG] yfinance ERROR: {error_msg}")
            print(f"[DEBUG] Error type: {error_type}")
            
            # 检测 TypeError with NoneType（yfinance 内部断网时抛出）
            if error_type == "TypeError" and "NoneType" in error_msg:
                raise Exception(
                    f"网络连接失败！\n\n"
                    f"⚠️ 无法连接到数据源服务器。\n\n"
                    f"可能原因：\n"
                    f"1. 您的电脑未连接到互联网\n"
                    f"2. 防火墙阻止了程序访问网络\n"
                    f"3. 数据源服务器暂时无法访问\n\n"
                    f"建议：\n"
                    f"- 检查您的网络连接\n"
                    f"- 确认可以访问互联网\n"
                    f"- 检查防火墙设置\n"
                    f"- 稍后重试"
                )
            
            # 检测其他网络连接错误
            network_error_keywords = [
                'connection', 'timeout', 'network', 'unreachable',
                'failed to establish', 'timed out', 'refused',
                'no internet', 'dns', 'resolve', 'gaierror',
                'ConnectionError', 'TimeoutError', 'URLError'
            ]
            
            is_network_error = any(keyword.lower() in error_msg.lower() or keyword.lower() in error_type.lower() 
                                  for keyword in network_error_keywords)
            
            if is_network_error:
                raise Exception(
                    f"网络连接失败！\n\n"
                    f"⚠️ 无法连接到数据源服务器。\n\n"
                    f"可能原因：\n"
                    f"1. 您的电脑未连接到互联网\n"
                    f"2. 防火墙阻止了程序访问网络\n"
                    f"3. 数据源服务器暂时无法访问\n\n"
                    f"建议：\n"
                    f"- 检查您的网络连接\n"
                    f"- 确认可以访问互联网\n"
                    f"- 检查防火墙设置\n"
                    f"- 稍后重试"
                )
            
            # 翻译常见的 yfinance 错误为用户友好的中文提示
            elif "No data found" in error_msg or "No data returned" in error_msg:
                raise Exception(
                    f"找不到数据！\n\n"
                    f"可能原因：\n"
                    f"1. 股票代码 '{code}' 不存在\n"
                    f"2. 该股票在选定的日期范围内停牌\n"
                    f"3. 数据源暂时无法访问\n\n"
                    f"建议：\n"
                    f"- 检查股票代码是否正确\n"
                    f"- 尝试缩短日期范围\n"
                    f"- 稍后再试"
                )
            elif "1m data not available" in error_msg or "minute" in error_msg.lower():
                raise Exception(
                    f"分钟级数据限制！\n\n"
                    f"yfinance 仅提供最近 7-30 天的分钟级数据。\n\n"
                    f"建议：\n"
                    f"- 缩短日期范围（选择最近1个月内）\n"
                    f"- 或者选择 '1d' 时间粒度获取日线数据"
                )
            elif "Asset may not exist" in error_msg:
                raise Exception(
                    f"资产不存在！\n\n"
                    f"股票代码 '{code}' 可能不正确。\n\n"
                    f"示例：\n"
                    f"- 马股：1155（会自动添加.KL后缀）\n"
                    f"- 美股：AAPL, TSLA, MSFT"
                )
            else:
                # 其他未知错误，显示原始错误信息
                raise Exception(f"数据获取失败：{error_msg}")
    
    
    def _fetch_crypto(self, pair: str, timeframe: str, 
                     start_date: datetime, end_date: datetime,
                     exchange: str = None, proxy_url: str = None) -> pd.DataFrame:
        """
        Fetch crypto data from selected exchange with optional proxy.
        
        Args:
            pair: Trading pair (e.g., BTC/USDT)
            timeframe: Time granularity
            start_date: Start date
            end_date: End date
            exchange: Exchange name (e.g., "Luno (Malaysia)", "Binance (Global)")
            proxy_url: Proxy URL if enabled
        
        Returns:
            DataFrame with OHLCV data
        """
        # 交易所映射
        EXCHANGE_MAP = {
            "Luno (Malaysia)": ccxt.luno,
            "Binance (Global)": ccxt.binance,
            "OKX": ccxt.okx,
            "Bybit": ccxt.bybit
        }
        
        # 默认使用 Luno
        if not exchange:
            exchange = "Luno (Malaysia)"
        
        print(f"[DEBUG] Crypto: Using exchange: {exchange}")
        print(f"[DEBUG] Crypto: Proxy enabled: {proxy_url is not None}")
        
        # 获取交易所类
        exchange_class = EXCHANGE_MAP.get(exchange)
        if not exchange_class:
            raise Exception(f"不支持的交易所: {exchange}")
        
        # 配置交易所（包括代理）
        config = {}
        if proxy_url:
            config['proxies'] = {
                'http': proxy_url,
                'https': proxy_url
            }
            print(f"[DEBUG] Crypto: Proxy configured: {proxy_url}")
        
        try:
            # 创建交易所实例
            exchange_instance = exchange_class(config)
            exchange_instance.load_markets()
            
            # 获取 ccxt 时间粒度
            ccxt_timeframe = self.TIMEFRAME_MAP[timeframe]['ccxt']
            
            # 转换开始时间为时间戳
            since = int(start_date.timestamp() * 1000)
            
            print(f"[DEBUG] {exchange}: Fetching {pair} with timeframe {ccxt_timeframe}")
            
            # 获取 OHLCV 数据
            ohlcv = exchange_instance.fetch_ohlcv(
                symbol=pair,
                timeframe=ccxt_timeframe,
                since=since
            )
            
            if not ohlcv:
                raise Exception(f"从 {exchange} 获取不到数据")
            
            # 转换为 DataFrame
            df = pd.DataFrame(
                ohlcv,
                columns=['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']
            )
            
            # 转换时间戳为日期时间
            df['Date'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.drop('timestamp', axis=1, inplace=True)
            
            # 按结束日期过滤
            df = df[df['Date'] <= end_date]
            
            if df.empty:
                raise Exception(f"指定日期范围内没有数据")
            
            print(f"[DEBUG] {exchange}: Got {len(df)} rows")
            return df
            
        except Exception as e:
            error_msg = str(e)
            error_type = type(e).__name__
            print(f"[DEBUG] {exchange} ERROR: {error_msg}")
            print(f"[DEBUG] Error type: {error_type}")
            
            # 检测网络连接错误
            network_error_keywords = [
                'connection', 'timeout', 'network', 'unreachable',
                'failed to establish', 'timed out', 'refused',
                'no internet', 'dns', 'resolve', 'gaierror',
                'ConnectionError', 'TimeoutError', 'URLError',
                'RequestException', 'ConnectTimeout'
            ]
            
            is_network_error = any(keyword.lower() in error_msg.lower() or keyword.lower() in error_type.lower() 
                                  for keyword in network_error_keywords)
            
            if is_network_error:
                proxy_tip = "\n\n💡 提示：如果交易所被墙，请尝试启用代理设置。" if not proxy_url else ""
                raise Exception(
                    f"网络连接失败！\n\n"
                    f"⚠️ 无法连接到 {exchange}。\n\n"
                    f"可能原因：\n"
                    f"1. 交易所被防火墙屏蔽\n"
                    f"2. 网络连接问题\n"
                    f"3. 代理配置错误（如果已启用）\n\n"
                    f"建议：\n"
                    f"- 检查网络连接\n"
                    f"- 尝试切换到其他交易所\n"
                    f"- 启用或检查代理设置{proxy_tip}"
                )
            else:
                raise Exception(f"{exchange} 数据获取失败：{error_msg}")
    
    def standardize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize DataFrame columns and format.
        
        Args:
            df: Raw DataFrame from API
        
        Returns:
            Standardized DataFrame with columns: Date, Open, High, Low, Close, Volume
        """
        print(f"[DEBUG] Standardizing DataFrame with columns: {list(df.columns)}")
        
        # First, drop extra columns we don't need (like Adj Close, Dividends, Stock Splits)
        # Keep only the columns we want to map
        columns_to_drop = []
        for col in df.columns:
            col_lower = col.lower()
            # Drop Adj Close, Dividends, Stock Splits, etc.
            if 'adj' in col_lower or 'dividend' in col_lower or 'split' in col_lower:
                columns_to_drop.append(col)
        
        if columns_to_drop:
            print(f"[DEBUG] Dropping extra columns: {columns_to_drop}")
            df = df.drop(columns=columns_to_drop)
        
        # Rename columns to standard format
        column_mapping = {}
        
        for col in df.columns:
            col_lower = col.lower()
            if 'date' in col_lower or 'time' in col_lower or col == 'Date':
                column_mapping[col] = 'Date'
            elif 'open' in col_lower:
                column_mapping[col] = 'Open'
            elif 'high' in col_lower:
                column_mapping[col] = 'High'
            elif 'low' in col_lower:
                column_mapping[col] = 'Low'
            elif 'close' in col_lower:
                column_mapping[col] = 'Close'
            elif 'volume' in col_lower or 'vol' in col_lower:
                column_mapping[col] = 'Volume'
        
        print(f"[DEBUG] Column mapping: {column_mapping}")
        df = df.rename(columns=column_mapping)
        
        # Keep only required columns (handle case where column might not exist)
        required_cols = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        existing_cols = [col for col in required_cols if col in df.columns]
        
        print(f"[DEBUG] Keeping columns: {existing_cols}")
        df = df[existing_cols]
        
        # Verify no duplicate columns
        if len(df.columns) != len(set(df.columns)):
            duplicates = [col for col in df.columns if list(df.columns).count(col) > 1]
            print(f"[DEBUG] WARNING: Duplicate columns found: {set(duplicates)}")
            # Remove duplicates by keeping only the first occurrence
            df = df.loc[:, ~df.columns.duplicated()]
            print(f"[DEBUG] After removing duplicates, columns: {list(df.columns)}")
        
        # Convert Date to string format: YYYY-MM-DD HH:MM:SS
        if 'Date' in df.columns:
            print("[DEBUG] Converting Date column to string format...")
            df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        print(f"[DEBUG] Standardization complete. Final shape: {df.shape}, Columns: {list(df.columns)}")
        return df
    
    def _standardize_timezone(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        标准化时区为 Asia/Kuala_Lumpur (v2.0)
        
        Args:
            df: 原始DataFrame，Date列可能为UTC或无时区
        
        Returns:
            时区标准化后的DataFrame
        """
        print("[DEBUG] Standardizing timezone to Asia/Kuala_Lumpur...")
        
        KL_TZ = pytz.timezone('Asia/Kuala_Lumpur')
        
        # 确保Date列为datetime类型
        df['Date'] = pd.to_datetime(df['Date'])
        
        # 如果没有时区信息，假定为UTC
        if df['Date'].dt.tz is None:
            print("[DEBUG] No timezone info, assuming UTC")
            df['Date'] = df['Date'].dt.tz_localize('UTC')
        
        # 转换为吉隆坡时区
        df['Date'] = df['Date'].dt.tz_convert(KL_TZ)
        print(f"[DEBUG] Timezone converted. Sample: {df['Date'].iloc[0]}")
        
        # 移除时区信息，保留本地时间（避免Parquet兼容性问题）
        df['Date'] = df['Date'].dt.tz_localize(None)
        
        # 🔧 FIX: 转换回字符串格式，确保与 analyze_gaps() 兼容
        df['Date'] = df['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        print("[DEBUG] Timezone standardization complete")
        return df
    
    def _filter_lunch_break(self, df: pd.DataFrame, asset_type: str) -> pd.DataFrame:
        """
        过滤午休时段 (12:30-14:30 MYT) - 黑名单策略 (v2.0)
        
        策略：剔除午休噪音，保留所有其他时间（包括盘前盘后）
        适用于：Malaysia Stock + FKLI/FCPO
        
        Args:
            df: 原始DataFrame
            asset_type: 资产类型
        
        Returns:
            过滤后的DataFrame
        """
        # 只对马股资产过滤
        if asset_type not in ["Malaysia Stock", "Futures - Global"]:
            print(f"[DEBUG] Skipping lunch filter for {asset_type}")
            return df
        
        print(f"[DEBUG] Applying lunch break filter for {asset_type}")
        
        # 确保Date列为datetime
        df['Date'] = pd.to_datetime(df['Date'])
        
        # 提取小时和分钟
        df['_hour'] = df['Date'].dt.hour
        df['_minute'] = df['Date'].dt.minute
        
        # 🔥 黑名单策略：定义午休时段（要被剔除的）
        is_lunch_break = (
            (df['_hour'] == 12) & (df['_minute'] > 30)  # 12:31 - 12:59
        ) | (
            (df['_hour'] == 13)                          # 13:00 - 13:59
        ) | (
            (df['_hour'] == 14) & (df['_minute'] < 30)  # 14:00 - 14:29
        )
        
        # 过滤：保留所有非午休时段的数据
        filtered_df = df[~is_lunch_break].copy()  # 🎯 注意这里是 ~（取反）
        
        # 删除临时列
        filtered_df.drop(['_hour', '_minute'], axis=1, inplace=True)
        
        # 🔧 FIX: 转换回字符串格式，确保与后续方法兼容
        filtered_df['Date'] = filtered_df['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        removed_count = len(df) - len(filtered_df)
        print(f"[DEBUG] Filtered {removed_count} lunch break records")
        
        return filtered_df
    
    def smart_update(self, symbol: str, asset_type: str, timeframe: str,
                     start_date: datetime = None, end_date: datetime = None,
                     exchange: str = None, proxy_url: str = None) -> pd.DataFrame:
        """
        智能增量更新策略 (v2.0 - Master DB)
        
        工作流程：
        1. 检查 data/store/{symbol}_{timeframe}.parquet 是否存在
        2. 如果存在，读取最后一条记录的时间戳
        3. 下载 last_date+1 到 end_date 的新数据
        4. 合并去重，覆盖保存到 Master DB
        5. 如果不存在，执行全量下载
        
        Args:
            symbol: 资产代码（已预处理）
            asset_type: 资产类型
            timeframe: 时间粒度
            start_date: 开始日期（仅用于全量下载）
            end_date: 结束日期（默认为今天）
            exchange: 交易所（加密货币）
            proxy_url: 代理URL
        
        Returns:
            合并后的完整DataFrame
        """
        # 确保目录存在
        os.makedirs(self.store_dir, exist_ok=True)
        
        # 生成 Master DB 文件名（固定，不带日期）
        filename = f"{symbol}_{timeframe}.parquet"
        filepath = os.path.join(self.store_dir, filename)
        
        # 默认结束日期为今天
        if end_date is None:
            end_date = datetime.now()
        
        # 检查本地 Master DB 是否存在
        if os.path.exists(filepath):
            print(f"[DEBUG] Found Master DB: {filepath}")
            
            try:
                # 读取现有数据
                existing_df = pd.read_parquet(filepath)
                
                # 获取最后一条记录的日期
                existing_df['Date'] = pd.to_datetime(existing_df['Date'])
                last_date = existing_df['Date'].max()
                
                print(f"[DEBUG] Last record date: {last_date}")
                print(f"[DEBUG] Existing records: {len(existing_df)}")
                
                # 下载增量数据 (last_date+1 到 end_date)
                incremental_start = last_date + timedelta(days=1)
                
                # 如果增量开始时间已经超过结束时间，说明没有新数据
                if incremental_start > end_date:
                    print("[DEBUG] No new data needed, returning existing Master DB")
                    # 🔧 FIX: 转换为字符串格式再返回
                    existing_df['Date'] = existing_df['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
                    return existing_df
                
                print(f"[DEBUG] Incremental download: {incremental_start} to {end_date}")
                
                # 获取增量数据（调用原有的 fetch_data）
                new_df = self.fetch_data(
                    asset_type=asset_type,
                    code=symbol,
                    timeframe=timeframe,
                    start_date=incremental_start,
                    end_date=end_date,
                    exchange=exchange,
                    proxy_url=proxy_url
                )
                
                if new_df.empty:
                    print("[DEBUG] No new data fetched, returning existing Master DB")
                    # 🔧 FIX: 转换为字符串格式再返回
                    existing_df['Date'] = existing_df['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
                    return existing_df
                
                print(f"[DEBUG] Fetched {len(new_df)} new records")
                
                # 合并数据
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                
                # 去重（保留最新）
                combined_df['Date'] = pd.to_datetime(combined_df['Date'])
                combined_df = combined_df.drop_duplicates(subset=['Date'], keep='last')
                combined_df = combined_df.sort_values('Date').reset_index(drop=True)
                
                # 🔧 FIX: 转换回字符串格式
                combined_df['Date'] = combined_df['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
                
                print(f"[DEBUG] After merge and dedup: {len(combined_df)} total records")
                
            except Exception as e:
                print(f"[DEBUG] Error reading Master DB: {str(e)}")
                print("[DEBUG] Falling back to full download")
                
                # 如果读取失败，执行全量下载
                if start_date is None:
                    start_date = end_date - timedelta(days=365)  # 默认1年
                
                combined_df = self.fetch_data(
                    asset_type=asset_type,
                    code=symbol,
                    timeframe=timeframe,
                    start_date=start_date,
                    end_date=end_date,
                    exchange=exchange,
                    proxy_url=proxy_url
                )
        
        else:
            print(f"[DEBUG] No Master DB found, executing full download")
            
            # 首次下载：全量
            if start_date is None:
                start_date = end_date - timedelta(days=365)  # 默认1年
            
            combined_df = self.fetch_data(
                asset_type=asset_type,
                code=symbol,
                timeframe=timeframe,
                start_date=start_date,
                end_date=end_date,
                exchange=exchange,
                proxy_url=proxy_url
            )
        
        # 保存到 Master DB（覆盖）
        combined_df.to_parquet(filepath, index=False, compression='snappy')
        print(f"[DEBUG] Master DB updated: {filepath}")
        
        return combined_df
    

    def analyze_gaps(self, df: pd.DataFrame, requested_start: datetime, 
                     requested_end: datetime) -> tuple[bool, str]:
        """
        Analyze data gaps and determine if warning is needed.
        
        Args:
            df: Fetched DataFrame
            requested_start: User-requested start date
            requested_end: User-requested end date
        
        Returns:
            Tuple of (has_warning, warning_message)
            - has_warning: True if gap > 3 days
            - warning_message: Warning text to display
        """
        if df.empty:
            return True, "数据为空"
        
        # Get actual start date (first row)
        first_date_str = df.iloc[0]['Date']
        actual_start = datetime.strptime(first_date_str, '%Y-%m-%d %H:%M:%S')
        
        # Calculate difference
        diff = actual_start - requested_start
        
        # 3-day tolerance
        if diff.days > 3:
            warning_msg = f"警告：数据不完整。源数据开始于 {first_date_str}，请求开始于 {requested_start.strftime('%Y-%m-%d')}"
            return True, warning_msg
        
        return False, "数据获取成功！覆盖率 100%"
    
    def export_to_csv(self, df: pd.DataFrame, code: str, timeframe: str, 
                      start_date: datetime) -> str:
        """
        Export DataFrame to CSV file.
        
        Args:
            df: DataFrame to export
            code: Asset code
            timeframe: Timeframe used
            start_date: Start date used
        
        Returns:
            Path to saved CSV file
        """
        # Create filename: {Code}_{Timeframe}_{StartDate}.csv
        start_str = start_date.strftime('%Y%m%d')
        filename = f"{code}_{timeframe}_{start_str}.csv"
        
        # Save to current directory or a data folder
        output_dir = "exported_data"
        os.makedirs(output_dir, exist_ok=True)
        filepath = os.path.join(output_dir, filename)
        
        # Export without index
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        
        return filepath
    
    def export_to_parquet(self, df: pd.DataFrame, code: str, timeframe: str, 
                          start_date: datetime) -> str:
        """
        导出DataFrame为Parquet格式 (v2.0)
        
        Args:
            df: 要导出的DataFrame
            code: 资产代码
            timeframe: 时间粒度
            start_date: 开始日期（用于文件名）
        
        Returns:
            导出文件的完整路径
        """
        # 生成文件名（带日期）
        start_str = start_date.strftime('%Y%m%d')
        filename = f"{code}_{timeframe}_{start_str}.parquet"
        
        # 导出目录
        output_dir = "exported_data"
        os.makedirs(output_dir, exist_ok=True)
        filepath = os.path.join(output_dir, filename)
        
        # 确保Date列为datetime类型（Parquet要求）
        df_export = df.copy()
        if df_export['Date'].dtype == 'object':
            df_export['Date'] = pd.to_datetime(df_export['Date'])
        
        # 导出
        df_export.to_parquet(
            filepath,
            engine='pyarrow',
            compression='snappy',  # 压缩算法
            index=False
        )
        
        print(f"[DEBUG] Parquet exported to: {filepath}")
        return filepath
