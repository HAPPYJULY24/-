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
                 exchange: str = None, proxy_url: str = None):  # 新增参数
        """
        Initialize worker with fetch parameters.
        
        Args:
            asset_type: Type of asset
            code: Asset code (already preprocessed)
            timeframe: Time granularity
            start_date: Start date
            end_date: End date
            exchange: Exchange name for crypto (新增)
            proxy_url: Proxy URL if enabled (新增)
        """
        super().__init__()
        self.asset_type = asset_type
        self.code = code
        self.timeframe = timeframe
        self.start_date = start_date
        self.end_date = end_date
        self.exchange = exchange  # 新增
        self.proxy_url = proxy_url  # 新增
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
            
            # Fetch data
            print("[DEBUG] Step 1: Starting data fetch...")
            df = self.fetcher.fetch_data(
                self.asset_type,
                self.code,
                self.timeframe,
                self.start_date,
                self.end_date,
                exchange=self.exchange,  # 新增
                proxy_url=self.proxy_url  # 新增
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

