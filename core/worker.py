"""
Worker thread for async data fetching operations.
"""

from PyQt6.QtCore import QThread, pyqtSignal
from datetime import datetime
from .data_fetcher import DataFetcher
import pandas as pd


class FetchWorker(QThread):
    """
    QThread worker that runs DataFetcher logic in background.
    Emits signals to communicate with the UI.
    """
    
    # Signals
    success = pyqtSignal(pd.DataFrame, bool, str, str)  # df, has_warning, warning_msg, csv_path
    error = pyqtSignal(str)  # error_message
    finished = pyqtSignal()  # completion signal
    
    def __init__(self, asset_type: str, code: str, timeframe: str, 
                 start_date: datetime, end_date: datetime,
                 exchange: str = None, proxy_url: str = None,
                 use_smart_update: bool = False, filter_lunch: bool = False):  # 🆕 v2.0
        """
        Initialize worker with fetch parameters (v2.0 - 支持增量更新和午休过滤)
        
        Args:
            asset_type: Type of asset
            code: Asset code (already preprocessed)
            timeframe: Time granularity
            start_date: Start date
            end_date: End date
            exchange: Exchange name for crypto
            proxy_url: Proxy URL if enabled
            use_smart_update: 启用增量更新 (v2.0)
            filter_lunch: 过滤午休时段 (v2.0)
        """
        super().__init__()
        self.asset_type = asset_type
        self.code = code
        self.timeframe = timeframe
        self.start_date = start_date
        self.end_date = end_date
        self.exchange = exchange
        self.proxy_url = proxy_url
        self.use_smart_update = use_smart_update  # 🆕 v2.0
        self.filter_lunch = filter_lunch  # 🆕 v2.0
        self.fetcher = DataFetcher()
    
    def run(self):
        """
        Execute data fetching in background thread.
        Emits success or error signals based on result.
        """
        try:
            print(f"[DEBUG] Worker started")
            print(f"[DEBUG] Asset: {self.asset_type}, Code: {self.code}, Timeframe: {self.timeframe}")
            print(f"[DEBUG] Date range: {self.start_date} to {self.end_date}")
            
            # 🆕 v2.0: 根据设置选择增量更新或全量下载
            if self.use_smart_update:
                print("[DEBUG] Step 1: Using Smart Update (Incremental)...")
                df = self.fetcher.smart_update(
                    symbol=self.code,
                    asset_type=self.asset_type,
                    timeframe=self.timeframe,
                    start_date=self.start_date,
                    end_date=self.end_date,
                    exchange=self.exchange,
                    proxy_url=self.proxy_url
                )
                # smart_update 已经包含了时区标准化，需要手动应用午休过滤
                if self.filter_lunch:
                    print("[DEBUG] Applying lunch filter to smart_update result...")
                    df = self.fetcher._filter_lunch_break(df, self.asset_type)
            else:
                print("[DEBUG] Step 1: Using Standard Fetch (Full Download)...")
                df = self.fetcher.fetch_data(
                    self.asset_type,
                    self.code,
                    self.timeframe,
                    self.start_date,
                    self.end_date,
                    exchange=self.exchange,
                    proxy_url=self.proxy_url,
                    filter_lunch=self.filter_lunch  # 🆕 v2.0: 传递午休过滤参数
                )
            print(f"[DEBUG] Step 1 Complete: Fetched {len(df)} rows")
            
            # Check if data is empty
            if df is None or df.empty:
                print("[DEBUG] ERROR: DataFrame is empty!")
                self.error.emit("未获取到任何数据。请检查资产代码和日期范围。")
                return
            
            # Analyze gaps
            print("[DEBUG] Step 2: Analyzing data gaps...")
            has_warning, warning_msg = self.fetcher.analyze_gaps(
                df, self.start_date, self.end_date
            )
            print(f"[DEBUG] Step 2 Complete: has_warning={has_warning}")
            
            # DO NOT export CSV automatically anymore
            # User will export manually by clicking export button
            print("[DEBUG] Data fetch complete. CSV export will be done manually by user.")
            
            # Emit success signal (without csv_path)
            print("[DEBUG] Emitting success signal...")
            self.success.emit(df, has_warning, warning_msg, "")  # Empty csv_path
            print("[DEBUG] Success signal emitted!")
        
        except Exception as e:
            # Emit error signal with detailed information
            import traceback
            error_details = traceback.format_exc()
            print(f"[DEBUG] EXCEPTION CAUGHT: {str(e)}")
            print(f"[DEBUG] Full traceback:\n{error_details}")
            self.error.emit(f"{str(e)}\n\n详细信息:\n{error_details}")
        
        finally:
            # Always emit finished signal
            print("[DEBUG] Worker finished, emitting finished signal...")
            self.finished.emit()

