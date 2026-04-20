"""CCXT Adapter for cryptocurrency exchanges."""
import pandas as pd
import ccxt
from datetime import datetime
from .base_adapter import BaseAdapter


class CCXTAdapter(BaseAdapter):
    """
    Adapter for cryptocurrency exchanges via CCXT.
    
    Supports:
    - Luno (Malaysia)
    - Binance (Global)
    - OKX
    - Bybit
    """
    
    # Exchange mapping
    EXCHANGE_MAP= {
        "Luno (Malaysia)": ccxt.luno,
        "Binance (Global)": ccxt.binance,
        "OKX": ccxt.okx,
        "Bybit": ccxt.bybit
    }
    
    def fetch(self, code: str, timeframe: str, start_date: datetime, 
              end_date: datetime, exchange: str = None, proxy_url: str = None,
              filter_lunch: bool = False, asset_type: str = "Crypto",
              **kwargs) -> pd.DataFrame:
        """
        Fetch crypto data from selected exchange with optional proxy.
        
        Args:
            code: Trading pair (e.g., 'BTC/USDT')
            timeframe: Time granularity (1m, 5m, 15m, 1h, 1d)
            start_date: Start date
            end_date: End date
            exchange: Exchange name (e.g., "Luno (Malaysia)", "Binance (Global)")
            proxy_url: Proxy URL if enabled (e.g., "http://127.0.0.1:7890")
            filter_lunch: Whether to apply lunch break filtering (not applicable for crypto)
            asset_type: Asset type (for filtering logic, not used for crypto)
        
        Returns:
            DataFrame with OHLCV data
        
        Raises:
            Exception: If CCXT API fails with user-friendly Chinese error
        """
        # Default to Luno if no exchange specified
        if not exchange:
            exchange = "Luno (Malaysia)"
        
        print(f"[DEBUG] Crypto: Using exchange: {exchange}")
        print(f"[DEBUG] Crypto: Proxy enabled: {proxy_url is not None}")
        
        # Get exchange class
        exchange_class = self.EXCHANGE_MAP.get(exchange)
        if not exchange_class:
            raise Exception(f"不支持的交易所: {exchange}")
        
        # Configure exchange with optional proxy and robust settings
        config = {
            'timeout': 30000,  # 30 seconds timeout
            'enableRateLimit': True,
            'headers': {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
        }
        
        if proxy_url:
            config['proxies'] = {
                'http': proxy_url,
                'https': proxy_url
            }
            print(f"[DEBUG] Crypto: Proxy configured: {proxy_url}")
        
        try:
            # Create exchange instance
            exchange_instance = exchange_class(config)
            exchange_instance.load_markets()
            
            # Get CCXT timeframe
            ccxt_timeframe = self.TIMEFRAME_MAP[timeframe]['ccxt']
            
            # Convert start time to timestamp (milliseconds)
            since = int(start_date.timestamp() * 1000)
            
            print(f"[DEBUG] {exchange}: Fetching {code} with timeframe {ccxt_timeframe}")
            
            # Fetch OHLCV data
            ohlcv = exchange_instance.fetch_ohlcv(
                symbol=code,
                timeframe=ccxt_timeframe,
                since=since
            )
            
            if not ohlcv:
                raise Exception(f"从 {exchange} 获取不到数据")
            
            # Convert to DataFrame
            df = pd.DataFrame(
                ohlcv,
                columns=['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']
            )
            
            # Convert timestamp to datetime
            df['Date'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.drop('timestamp', axis=1, inplace=True)
            
            # Filter by end date
            df = df[df['Date'] <= end_date]
            
            if df.empty:
                raise Exception(f"指定日期范围内没有数据")
            
            print(f"[DEBUG] {exchange}: Got {len(df)} rows")
            
            # Standardize timezone (CRITICAL: must be called)
            df = self._standardize_timezone(df)
            
            # Note: Lunch break filtering not applicable for crypto (24/7 markets)
            # But we keep the parameter for API consistency
            
            return df
            
        except Exception as e:
            error_msg = str(e)
            error_type = type(e).__name__
            
            print(f"[DEBUG] {exchange} ERROR: {error_msg}")
            print(f"[DEBUG] Error type: {error_type}")
            
            # Check for network errors
            if self._is_network_error(error_msg, error_type):
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
                # If already translated, re-raise
                if "从" in error_msg and "获取不到数据" in error_msg:
                    raise
                if "指定日期范围内没有数据" in error_msg:
                    raise
                # Otherwise wrap the error
                raise Exception(f"{exchange} 数据获取失败：{error_msg}")
    
    def _is_network_error(self, error_msg: str, error_type: str) -> bool:
        """Check if error is network-related."""
        network_keywords = [
            'connection', 'timeout', 'network', 'unreachable',
            'failed to establish', 'timed out', 'refused',
            'no internet', 'dns', 'resolve', 'gaierror',
            'ConnectionError', 'TimeoutError', 'URLError',
            'RequestException', 'ConnectTimeout'
        ]
        
        return any(
            keyword.lower() in error_msg.lower() or keyword.lower() in error_type.lower()
            for keyword in network_keywords
        )
