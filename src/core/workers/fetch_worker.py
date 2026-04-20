"""
Worker thread for async data fetching operations.
"""

from PyQt6.QtCore import QThread, pyqtSignal
from datetime import datetime
from typing import Optional
import pickle
from src.quant_bridge.data_fetcher_facade import DataFetcher
import pandas as pd


class FetchWorker(QThread):
    """
    QThread worker that runs DataFetcher logic in background.
    Emits signals to communicate with the UI.
    """
    
    # Signals - Relay serialized bytes to avoid pandas/file I/O crash paths.
    success = pyqtSignal(object, bool, str, str)  # payload_bytes, has_warning, warning_msg, csv_path
    error = pyqtSignal(str)  # error_message
    
    def __init__(
        self, 
        asset_type: str, 
        code: str, 
        timeframe: str, 
        start_date: datetime, 
        end_date: datetime,
        exchange: Optional[str] = None, 
        proxy_url: Optional[str] = None,
        use_smart_update: bool = False, 
        apply_session_filter: bool = False,  # Renamed from filter_lunch
        custom_session: Optional[tuple] = None  # New: (start_time, end_time)
    ) -> None:
        """
        Initialize worker with fetch parameters (v2.5 - Session Filtering)
        """
        super().__init__()
        self.asset_type: str = asset_type
        self.code: str = code
        self.timeframe: str = timeframe
        self.start_date: datetime = start_date
        self.end_date: datetime = end_date
        self.exchange: Optional[str] = exchange
        self.proxy_url: Optional[str] = proxy_url
        self.use_smart_update: bool = use_smart_update
        self.apply_session_filter: bool = apply_session_filter
        self.custom_session: Optional[tuple] = custom_session
        self.fetcher: DataFetcher = DataFetcher()
    
    def run(self) -> None:
        """
        Execute data fetching in background thread.
        Emits success or error signals based on result.
        """
        try:
            print(f"[DEBUG] Worker started")
            print(f"[DEBUG] Asset: {self.asset_type}, Code: {self.code}, Timeframe: {self.timeframe}")
            print(f"[DEBUG] Date range: {self.start_date} to {self.end_date}")
            
            # v2.0/2.5: Smart Update or Full Fetch
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
                # Apply session filter manually for smart update
                if self.apply_session_filter:
                    print("[DEBUG] Applying session filter to smart_update result...")
                    df = self.fetcher.apply_market_session_filter(df, self.asset_type, self.custom_session)
            else:
                print("[DEBUG] Step 1: Using Standard Fetch (Full Download)...")
                # Legacy fetch_data doesn't support custom_session directly in signature easily without breaking facade
                # So we fetch first (raw), then apply filter if needed?
                # Actually, fetch_data calls fetch, which calls adapter.adapter handles generic kwargs?
                # However, DataFetcher.apply_market_session_filter is the new logic.
                # Adapters might have their own old logic or we centralized it in Facade?
                # In facade.fetch(), it calls adapter.fetch(..., filter_lunch=...).
                # But BaseAdapter.fetch also has _filter_lunch_break.
                # The user Task 1 said: "Rename _filter_lunch_break to apply_market_session_filter".
                # I did that in Facade.
                # However, adapters (YFinanceAdapter) inherit from BaseAdapter.
                # BaseAdapter still has _filter_lunch_break?
                # If I rely on Facade's apply_market_session_filter, I should probably
                # disable the adapter's internal filtering (pass filter_lunch=False) 
                # and apply it here in Worker or Facade wrapper.
                
                # Let's see... implementation in Facade:
                # def fetch(..., filter_lunch=False, ...):
                #    ... adapter.fetch(..., filter_lunch=filter_lunch, ...)
                
                # If I want to use the NEW logic in Facade (which I just wrote), 
                # I should NOT rely on adapter.fetch's filter_lunch argument anymore.
                # I should fetch RAW data (filter_lunch=False) and then call apply_market_session_filter in Facade.
                
                # Let's fetch raw
                df = self.fetcher.fetch_data(
                    self.asset_type,
                    self.code,
                    self.timeframe,
                    self.start_date,
                    self.end_date,
                    exchange=self.exchange,
                    proxy_url=self.proxy_url,
                    filter_lunch=False # Disable adapter-level filtering
                )
                
                # Apply new Facade-level filtering
                if self.apply_session_filter:
                    print(f"[DEBUG] Applying session filter (Custom: {self.custom_session})...")
                    df = self.fetcher.apply_market_session_filter(df, self.asset_type, self.custom_session)
                
            print(f"[DEBUG] Step 1 Complete: Fetched {len(df)} rows")
            
            # Check if data is empty
            if df is None or df.empty:
                print("[DEBUG] ERROR: DataFrame is empty!")
                self.error.emit("未获取到任何数据。请检查资产代码和日期范围。")
                return
            
            # Phase 1/2 Refactoring:
            # Metadata extraction is now handled implicitly inside DataFetcherFacade.fetch.
            # currency conversion is also performed there. No need for worker intervention.
            
            # Analyze gaps
            print("[DEBUG] Step 2: Analyzing data gaps...")
            has_warning, warning_msg = self.fetcher.analyze_gaps(
                df, self.start_date, self.end_date
            )
            print(f"[DEBUG] Step 2 Complete: has_warning={has_warning}")
            
            # DO NOT export CSV automatically anymore
            # User will export manually by clicking export button
            print("[DEBUG] Data fetch complete. CSV export will be done manually by user.")
            
            # Emit success signal using in-memory bytes payload.
            print("[DEBUG] Emitting success signal (in-memory pickle payload)...")
            # Serialize plain-Python tabular payload instead of DataFrame object.
            # This avoids passing pandas objects across thread boundaries.
            safe_df = df.where(pd.notna(df), None)
            relay_payload = {
                "columns": list(safe_df.columns),
                "rows": safe_df.to_dict(orient="records"),
                "row_count": len(safe_df),
            }
            payload = pickle.dumps(relay_payload, protocol=pickle.HIGHEST_PROTOCOL)
            self.success.emit(payload, has_warning, warning_msg, "")
            print(f"[DEBUG] Success signal emitted! Payload bytes: {len(payload)}")
        
        except Exception as e:
            # Emit error signal with detailed information
            import traceback
            error_details = traceback.format_exc()
            print(f"[DEBUG] EXCEPTION CAUGHT: {str(e)}")
            print(f"[DEBUG] Full traceback:\n{error_details}")
            self.error.emit(f"{str(e)}\n\n详细信息:\n{error_details}")
        
        finally:
            # Let QThread emit its native finished signal after run() returns.
            print("[DEBUG] Worker finished, returning from run()...")

