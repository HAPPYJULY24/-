"""
YFinance Adapter
Handles data fetching from Yahoo Finance for stocks and global futures.
"""

import pandas as pd
import yfinance as yf
from datetime import datetime
from .base_adapter import BaseAdapter


class YFinanceAdapter(BaseAdapter):
    """
    Adapter for Yahoo Finance data source.
    
    Supports:
    - Malaysia Stock (e.g., 1155.KL)
    - US Stock (e.g., AAPL, TSLA)
    - Global Futures (e.g., GC=F, CL=F, ES=F)
    """
    
    def fetch(self, code: str, timeframe: str, start_date: datetime, 
              end_date: datetime, filter_lunch: bool = False, 
              asset_type: str = "US Stock", exchange: str = None, 
              **kwargs) -> pd.DataFrame:
        """
        Fetch data from Yahoo Finance.
        
        Args:
            code: Asset code (e.g., 'AAPL', '1155.KL', 'GC=F')
            timeframe: Time granularity (1m, 5m, 15m, 1h, 1d)
            start_date: Start date
            end_date: End date
            filter_lunch: Whether to apply lunch break filtering
            asset_type: Type of asset for filtering logic
            exchange: Optional exchange (ignored for yfinance)
            **kwargs: Additional arguments
        
        Returns:
            DataFrame with OHLCV data
        
        Raises:
            Exception: If yfinance API fails or data integrity check fails
        """
        from logic.localization import tr
        
        try:
            print(f"[DEBUG] yfinance: Creating ticker for {code}")
            ticker = yf.Ticker(code)
            interval = self.TIMEFRAME_MAP[timeframe]['yf']
            
            print(f"[DEBUG] yfinance: Fetching data with interval={interval}")
            
            # Fetch data from yfinance
            df = ticker.history(
                start=start_date,
                end=end_date,
                interval=interval,
                auto_adjust=False
            )
            
            # Check for None (network failure)
            if df is None:
                raise Exception(self._get_network_error_message())
            
            print(f"[DEBUG] yfinance: Received {len(df)} rows")
            
            if df.empty:
                raise ValueError(
                    f"{tr('messages.error_title')}: {code}\n"
                    f"{tr('messages.no_data_found_details')}"
                )
            
            # Reset index to make Date a column
            df.reset_index(inplace=True)
            print(f"[DEBUG] yfinance: DataFrame columns: {list(df.columns)}")
            
            # Log first timestamp for debugging
            first_ts_raw = df['Date'].iloc[0] if 'Date' in df.columns else df.index[0]
            print(f"[DEBUG] Raw first timestamp: {first_ts_raw}")
            
            # Standardize columns first
            df = self._standardize_columns(df)
            
            # --- Timezone Sanitization (Fix for Double Timezone Offset) ---
            # Ensure Date is datetime
            df['Date'] = pd.to_datetime(df['Date'])
            
            # Check if naive
            is_naive = df['Date'].dt.tz is None
            
            if is_naive:
                if asset_type == "Malaysia Stock" or ".KL" in code:
                    print("[DEBUG] Detected Naive Timestamp for Malaysia Stock. Assuming already Asia/Kuala_Lumpur.")
                    # Direct localization to MYT (Do NOT convert from UTC)
                    df['Date'] = df['Date'].dt.tz_localize('Asia/Kuala_Lumpur')
                else:
                    # For other assets, fallback to BaseAdapter behavior (Assume UTC)
                    # Or better, trust yfinance returns UTC for US/Global if it's naive?
                    # Actually, yfinance often returns naive local time for US stocks too.
                    # But for now, we strictly fix the reported Bursa issue.
                    print("[DEBUG] Detected Naive Timestamp for non-MY asset. Assuming UTC (BaseAdapter behavior).")
                    df['Date'] = df['Date'].dt.tz_localize('UTC')
            
            # Now call standard timezone normalization
            # Since we manually localized above, _standardize_timezone will just convert (if needed) and strip tz
            df = self._standardize_timezone(df)
            
            final_ts_localized = df['Date'].iloc[0]
            print(f"[DEBUG] Final localized timestamp: {final_ts_localized}")

            # --- Bursa Market Hours Hard-Constraint ---
            if asset_type == "Malaysia Stock" or ".KL" in code:
                self._validate_bursa_hours(df, timeframe)

            # Apply lunch break filtering if requested
            if filter_lunch:
                df = self._filter_lunch_break(df, asset_type)
            
            return df
            
        except Exception as e:
            # Re-raise integrity errors directly
            if "Data Integrity Error" in str(e):
                raise

            # Re-raise "No data found" errors directly (avoid false positive network error)
            # The error message contains "Network Issue" which trips _is_network_error
            if "No data found" in str(e) or "未找到数据" in str(e):
                raise
                
            # Generic error handling (simplified for brevity as we focus on the fix)
            error_msg = str(e)
            print(f"[DEBUG] yfinance ERROR: {error_msg}")
            
            if self._is_network_error(error_msg, type(e).__name__):
                raise Exception(self._get_network_error_message())
            
            raise

    def _validate_bursa_hours(self, df: pd.DataFrame, timeframe: str):
        """
        Validate that all data points fall within Bursa trading hours (09:00 - 17:00).
        Skips validation for daily/weekly/monthly timeframes where timestamps are 00:00:00.
        """
        # Skip validation for non-intraday timeframes (Daily/Weekly/Monthly)
        # yfinance uses '1m', '2m', '5m', '15m', '30m', '60m', '90m', '1h', '1d', '5d', '1wk', '1mo', '3mo'
        # We only want to validate intraday (minutes/hours)
        if timeframe in ['1d', '5d', '1w', '1wk', '1mo', '1M', '3mo']:
            print(f"[DEBUG] Skipping market hours validation for daily/weekly timeframe: {timeframe}")
            return

        # Ensure Date is datetime
        temp_dates = pd.to_datetime(df['Date'])
        
        # Extract hours
        hours = temp_dates.dt.hour
        minutes = temp_dates.dt.minute
        
        # Define invalid ranges: Before 09:00 or After 17:00
        # Check for < 09:00
        mask_too_early = hours < 9
        
        # Check for > 17:00 (Strict close)
        # 17:00 exactly is allowed allowed as closing auction/price? 
        # Usually market closes at 17:00, last trade might be slightly after?
        # Let's be strict: > 17:00 (i.e. 17:01 is bad, 18:00 is definitely bad)
        # Actually 17:00:00 is fine. 17:01:00 is suspicious.
        # But specifically 18:00 and 22:00 were the bug symptoms.
        mask_too_late = (hours > 17) | ((hours == 17) & (minutes > 10)) # Allow 10 mins buffer for closing auction
        
        invalid_mask = mask_too_early | mask_too_late
        
        if invalid_mask.any():
            invalid_rows = df[invalid_mask]
            first_invalid = invalid_rows.iloc[0]
            invalid_time = first_invalid['Date']
            
            error_msg = (
                f"Data Integrity Error: Found trading activity outside Bursa market hours!\n"
                f"Time: {invalid_time} (Expected 09:00-17:10)\n"
                f"Validation failed for Malaysia Stock.\n"
                f"System detected possible timezone misalignment (Double Offset)."
            )
            print(f"[ERROR] {error_msg}")
            raise ValueError(error_msg)
        
        print("[DEBUG] Bursa Market Hours Validation Passed ✅")

    def _is_network_error(self, error_msg: str, error_type: str) -> bool:
        """Check if error is network-related."""
        network_keywords = [
            'connection', 'timeout', 'network', 'unreachable',
            'failed to establish', 'timed out', 'refused',
            'no internet', 'dns', 'resolve', 'gaierror',
            'ConnectionError', 'TimeoutError', 'URLError'
        ]
        
        return any(
            keyword.lower() in error_msg.lower() or keyword.lower() in error_type.lower()
            for keyword in network_keywords
        )
    
    def _get_network_error_message(self) -> str:
        """Get standardized network error message in Chinese."""
        return (
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
