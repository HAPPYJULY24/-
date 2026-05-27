from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, 
                             QLabel, QGroupBox, QCheckBox, QLineEdit, 
                             QPushButton, QComboBox, QDateEdit, 
                             QMessageBox, QButtonGroup, QRadioButton, QTimeEdit)
from PyQt6.QtCore import Qt, QDate, QRegularExpression, QTime
from PyQt6.QtGui import QFont, QRegularExpressionValidator
from datetime import datetime

from ..status_banner import StatusBanner
from ..data_grid import DataGrid
from src.core.workers.fetch_worker import FetchWorker
from src.quant_bridge import DataFetcher
from logic.localization import tr


from utils.validators import validate_code, validate_date_range

class FetcherTab(QWidget):
    """
    Tab for fetching financial data.
    """
    def __init__(self):
        super().__init__()
        self.fetcher = DataFetcher()
        self.current_worker = None
        self.current_df = None
        self.current_rows = []
        self.current_columns = []
        self.current_code = None
        self.current_timeframe = None
        self.current_start_date = None
        
        self.init_ui()
        
    def init_ui(self):
        """Initialize the UI layout for the Fetcher tab."""
        main_layout = QVBoxLayout()
        main_layout.setSpacing(15)
        main_layout.setContentsMargins(10, 10, 10, 10)
        
        # Status banner
        self.status_banner = StatusBanner()
        main_layout.addWidget(self.status_banner)
        
        # Input configuration panel
        config_panel = self._create_config_panel()
        main_layout.addWidget(config_panel)
        
        # Network Settings
        proxy_group = QGroupBox("网络设置 (Network Settings)")
        proxy_group.setCheckable(True)
        proxy_group.setChecked(False)
        proxy_layout = QHBoxLayout()
        
        self.proxy_enabled = QCheckBox("启用代理 (Enable Proxy)")
        self.proxy_enabled.setChecked(False)
        proxy_layout.addWidget(self.proxy_enabled)
        
        proxy_url_label = QLabel("代理 URL:")
        proxy_layout.addWidget(proxy_url_label)
        
        self.proxy_url_input = QLineEdit()
        self.proxy_url_input.setPlaceholderText("http://127.0.0.1:7890")
        self.proxy_url_input.setText("http://127.0.0.1:7890")
        self.proxy_url_input.setEnabled(False)
        proxy_layout.addWidget(self.proxy_url_input)
        
        self.proxy_enabled.toggled.connect(self.proxy_url_input.setEnabled)
        
        proxy_group.setLayout(proxy_layout)
        main_layout.addWidget(proxy_group)
        
        # Advanced Settings
        advanced_group = QGroupBox("高级设置 (Advanced Settings) - v2.0")
        advanced_group.setCheckable(True)
        advanced_group.setChecked(False)
        advanced_layout = QVBoxLayout()
        
        self.incremental_update_checkbox = QCheckBox("✨ 启用增量更新 (Incremental Update)")
        self.incremental_update_checkbox.setChecked(False)
        self.incremental_update_checkbox.setToolTip(
            "开启后，将从本地 Master DB 读取历史数据，仅下载最新数据。\n"
            "可节省80%下载时间和网络流量。"
        )
        advanced_layout.addWidget(self.incremental_update_checkbox)
        
        # Session Filter Checkbox
        self.apply_session_filter_checkbox = QCheckBox(tr("ui.apply_session_filter"))
        self.apply_session_filter_checkbox.toggled.connect(self._on_session_filter_toggled)
        advanced_layout.addWidget(self.apply_session_filter_checkbox)
        
        # Custom Session Input (Hidden by default)
        self.session_container = QWidget()
        session_layout = QHBoxLayout()
        session_layout.setContentsMargins(0, 0, 0, 0)
        
        session_label = QLabel(tr("ui.custom_session"))
        self.session_start = QTimeEdit()
        self.session_start.setDisplayFormat("HH:mm")
        self.session_start.setTime(QTime(20, 0))
        
        self.session_end = QTimeEdit()
        self.session_end.setDisplayFormat("HH:mm")
        self.session_end.setTime(QTime(4, 0))
        
        session_layout.addWidget(session_label)
        session_layout.addWidget(self.session_start)
        session_layout.addWidget(QLabel("-"))
        session_layout.addWidget(self.session_end)
        session_layout.addStretch()
        
        self.session_container.setLayout(session_layout)
        self.session_container.hide()
        advanced_layout.addWidget(self.session_container)
        
        advanced_group.setLayout(advanced_layout)
        main_layout.addWidget(advanced_group)
        
        # Data preview section
        preview_header = QHBoxLayout()
        preview_label = QLabel("数据预览 (前5行 & 后5行)")
        preview_label.setStyleSheet("font-size: 14px; font-weight: bold;")
        preview_header.addWidget(preview_label)
        
        self.row_count_label = QLabel("")
        self.row_count_label.setStyleSheet("font-size: 12px; color: #888;")
        preview_header.addWidget(self.row_count_label)
        preview_header.addStretch()
        
        main_layout.addLayout(preview_header)
        
        self.data_grid = DataGrid()
        main_layout.addWidget(self.data_grid)
        
        self.setLayout(main_layout)
        
        # Initial validation check
        self._validate_request()
        
    def _create_config_panel(self) -> QGroupBox:
        """Create the input configuration panel."""
        group = QGroupBox("输入配置")
        main_layout = QVBoxLayout()
        main_layout.setSpacing(10)
        
        # Row 1: Asset Type
        asset_row = QHBoxLayout()
        asset_label = QLabel("资产类型:")
        font = asset_label.font()
        font.setBold(True)
        font.setPointSize(10)
        asset_label.setFont(font)
        asset_label.setFixedWidth(70)
        asset_row.addWidget(asset_label)
        
        self.asset_button_group = QButtonGroup()
        self.radio_my_stock = QRadioButton("马股")
        self.radio_us_stock = QRadioButton("美股")
        self.radio_gold = QRadioButton("国际期货 (YF)")
        self.radio_bursa_futures = QRadioButton("Bursa期货 (TV)")
        self.radio_crypto = QRadioButton("加密货币")
        
        self.asset_button_group.addButton(self.radio_my_stock, 0)
        self.asset_button_group.addButton(self.radio_us_stock, 1)
        self.asset_button_group.addButton(self.radio_gold, 2)
        self.asset_button_group.addButton(self.radio_bursa_futures, 3)
        self.asset_button_group.addButton(self.radio_crypto, 4)
        
        asset_row.addWidget(self.radio_my_stock)
        asset_row.addWidget(self.radio_us_stock)
        asset_row.addWidget(self.radio_gold)
        asset_row.addWidget(self.radio_bursa_futures)
        asset_row.addWidget(self.radio_crypto)
        asset_row.addStretch()
        
        self.radio_my_stock.setChecked(True)
        self.radio_my_stock.toggled.connect(self._on_asset_type_changed)
        self.radio_us_stock.toggled.connect(self._on_asset_type_changed)
        self.radio_gold.toggled.connect(self._on_asset_type_changed)
        self.radio_bursa_futures.toggled.connect(self._on_asset_type_changed)
        self.radio_crypto.toggled.connect(self._on_asset_type_changed)
        
        main_layout.addLayout(asset_row)
        
        # Row 2: Code, Exchange, Timeframe
        input_row = QHBoxLayout()
        
        code_label = QLabel("代码:")
        font = code_label.font()
        font.setBold(True)
        font.setPointSize(10)
        code_label.setFont(font)
        code_label.setFixedWidth(70)
        input_row.addWidget(code_label)
        
        self.code_input = QLineEdit()
        self.code_input.setPlaceholderText("例如: 1155")
        self.code_input.setMaximumWidth(150)
        input_row.addWidget(self.code_input)
        
        # Validators
        self.malaysia_validator = QRegularExpressionValidator(QRegularExpression(r"^\d{0,4}$"))
        self.us_validator = QRegularExpressionValidator(QRegularExpression(r"^[A-Za-z.]{0,10}$"))
        self.futures_validator = QRegularExpressionValidator(QRegularExpression(r"^[A-Za-z0-9=\-.\^]{0,15}$"))
        self.bursa_futures_validator = QRegularExpressionValidator(QRegularExpression(r"^[A-Za-z0-9!]+$"))
        self.crypto_validator = QRegularExpressionValidator(QRegularExpression(r"^[A-Za-z0-9/]{0,20}$"))
        self.code_input.setValidator(self.malaysia_validator)
        
        self.exchange_label = QLabel("交易所:")
        font = self.exchange_label.font()
        font.setBold(True)
        font.setPointSize(10)
        self.exchange_label.setFont(font)
        self.exchange_label.setFixedWidth(60)
        self.exchange_label.hide()
        input_row.addWidget(self.exchange_label)
        
        self.exchange_combo = QComboBox()
        self.exchange_combo.addItems(["Luno (Malaysia)", "Binance (Global)", "OKX", "Bybit"])
        self.exchange_combo.setMaximumWidth(150)
        self.exchange_combo.hide()
        input_row.addWidget(self.exchange_combo)
        
        input_row.addSpacing(20)
        
        timeframe_label = QLabel("时间粒度:")
        font = timeframe_label.font()
        font.setBold(True)
        font.setPointSize(10)
        timeframe_label.setFont(font)
        input_row.addWidget(timeframe_label)
        
        self.timeframe_combo = QComboBox()
        self.timeframe_combo.addItems(['1m', '5m', '15m', '1h', '1d', '1w', '1M', '1y'])
        self.timeframe_combo.setCurrentText('1d')
        self.timeframe_combo.setMaximumWidth(100)
        input_row.addWidget(self.timeframe_combo)
        
        input_row.addStretch()
        main_layout.addLayout(input_row)
        
        # Row 3: Date Range
        date_row = QHBoxLayout()
        
        start_label = QLabel("开始日期:")
        font = start_label.font()
        font.setBold(True)
        font.setPointSize(10)
        start_label.setFont(font)
        date_row.addWidget(start_label)
        
        self.start_date = QDateEdit()
        self.start_date.setCalendarPopup(True)
        self.start_date.setDate(QDate.currentDate().addMonths(-1))
        self.start_date.setDisplayFormat("yyyy-MM-dd")
        self.start_date.setMaximumWidth(120)
        date_row.addWidget(self.start_date)
        
        date_row.addSpacing(20)
        
        end_label = QLabel("结束日期:")
        font = end_label.font()
        font.setBold(True)
        font.setPointSize(10)
        end_label.setFont(font)
        date_row.addWidget(end_label)
        
        self.end_date = QDateEdit()
        self.end_date.setCalendarPopup(True)
        self.end_date.setDate(QDate.currentDate())
        self.end_date.setDisplayFormat("yyyy-MM-dd")
        self.end_date.setMaximumWidth(120)
        date_row.addWidget(self.end_date)
        
        date_row.addStretch()
        main_layout.addLayout(date_row)
        
        # Row 4: Buttons
        button_row = QHBoxLayout()
        
        self.fetch_button = QPushButton("获取数据 (Fetch Data)")
        self.fetch_button.clicked.connect(self._on_fetch_clicked)
        self.fetch_button.setMinimumHeight(35)
        button_row.addWidget(self.fetch_button)
        
        self.export_csv_button = QPushButton("导出 CSV (Export CSV)")
        self.export_csv_button.clicked.connect(lambda: self._on_export_clicked('csv'))
        self.export_csv_button.setEnabled(False)
        self.export_csv_button.setMinimumHeight(35)
        button_row.addWidget(self.export_csv_button)
        
        self.export_parquet_button = QPushButton("📦 导出 Parquet")
        self.export_parquet_button.clicked.connect(lambda: self._on_export_clicked('parquet'))
        self.export_parquet_button.setEnabled(False)
        self.export_parquet_button.setMinimumHeight(35)
        self.export_parquet_button.setMinimumHeight(35)
        button_row.addWidget(self.export_parquet_button)
        
        # New Save to Master DB Button (v2.6)
        self.save_db_button = QPushButton("💾 保存至数据中心 (Save to Master DB)")
        self.save_db_button.clicked.connect(self._on_save_db_clicked)
        self.save_db_button.setEnabled(False)
        self.save_db_button.setMinimumHeight(35)
        self.save_db_button.setStyleSheet("background-color: #2c3e50; color: white; font-weight: bold;")
        button_row.addWidget(self.save_db_button)
        
        button_row.addStretch()
        main_layout.addLayout(button_row)
        
        # Validation Feedback Label
        self.lbl_validator_msg = QLabel("")
        self.lbl_validator_msg.setWordWrap(True)
        self.lbl_validator_msg.setMaximumHeight(40)
        main_layout.addWidget(self.lbl_validator_msg)
        
        group.setLayout(main_layout)
        
        # Connect signals for validation
        self.timeframe_combo.currentIndexChanged.connect(self._validate_request)
        self.start_date.dateChanged.connect(self._validate_request)
        
        return group

    def _validate_request(self, *args):
        """
        Defensive Validation Logic (v2.0)
        Prevents invalid Yahoo Finance API requests based on known limits.
        """
        timeframe = self.timeframe_combo.currentText()
        start_qdate = self.start_date.date()
        today = QDate.currentDate()
        days_diff = start_qdate.daysTo(today)
        
        # Scenario A: Intraday (5m-1h) > 60 days
        if timeframe in ['5m', '15m', '1h'] and days_diff > 60:
            msg = tr("messages.intraday_limit_60d_v2")
            if "messages." in msg:
                msg = f"Intraday data ({timeframe}) is only available for the LAST 60 days from today.\nYour State Date is {days_diff} days ago."
            
            self.lbl_validator_msg.setText(f"⚠ {msg}")
            self.lbl_validator_msg.setStyleSheet("color: red; font-weight: bold;")
            self.fetch_button.setEnabled(False)
            return

        # Scenario B: 1m Critical > 7 days
        if timeframe == '1m' and days_diff > 7:
            msg = tr("messages.minute_limit_7d_v2")
            if "messages." in msg:
                 msg = f"1-minute data is only available for the LAST 7 days from today.\nYour Start Date is {days_diff} days ago."

            self.lbl_validator_msg.setText(f"⚠ {msg}")
            self.lbl_validator_msg.setStyleSheet("color: red; font-weight: bold;")
            self.fetch_button.setEnabled(False)
            return

        # Scenario C: Timeframe > Date Range (e.g., 1y timeframe for 1d range)
        min_days = self._get_min_days_for_timeframe(timeframe)
        if days_diff < min_days:
            msg = tr("messages.insufficient_date_range").format(tf=timeframe, min_days=min_days)
            # Fallback if translation key missing
            if "messages." in msg:
                msg = f"Timeframe '{timeframe}' requires at least {min_days} days range."
            
            self.lbl_validator_msg.setText(f"⚠ {msg}")
            self.lbl_validator_msg.setStyleSheet("color: red; font-weight: bold;")
            self.fetch_button.setEnabled(False)
            return

        # Scenario D: Safe
        self.lbl_validator_msg.setText("")
        self.lbl_validator_msg.setStyleSheet("")
        self.fetch_button.setEnabled(True)

    def _get_min_days_for_timeframe(self, timeframe: str) -> int:
        """Get minimum required days for a timeframe."""
        mapping = {
            '1m': 0, '5m': 0, '15m': 0, '1h': 0,  # Intraday needs < 1 day effectively, but days_diff is dates
            '1d': 0,
            '1w': 7,
            '1M': 28,
            '1y': 365
        }
        return mapping.get(timeframe, 0)

    def _get_selected_asset_type(self) -> str:
        if self.radio_my_stock.isChecked(): return "Malaysia Stock"
        elif self.radio_us_stock.isChecked(): return "US Stock"
        elif self.radio_gold.isChecked(): return "Futures - Global"
        elif self.radio_bursa_futures.isChecked(): return "Bursa Futures (TV)"
        elif self.radio_crypto.isChecked(): return "Crypto"
        return "Malaysia Stock"

    def _on_asset_type_changed(self):
        asset_type = self._get_selected_asset_type()
        if asset_type == "Malaysia Stock":
            self.code_input.setValidator(self.malaysia_validator)
            self.code_input.setPlaceholderText("例如: 1155")
            self.exchange_label.hide()
            self.exchange_combo.hide()
        elif asset_type == "US Stock":
            self.code_input.setValidator(self.us_validator)
            self.code_input.setPlaceholderText("例如: AAPL, TSLA")
            self.exchange_label.hide()
            self.exchange_combo.hide()
        elif asset_type == "Futures - Global":
            self.code_input.setValidator(None)
            self.code_input.setPlaceholderText("例如: GC=F, CL=F")
            self.exchange_label.hide()
            self.exchange_combo.hide()
        elif asset_type == "Bursa Futures (TV)":
            self.code_input.setValidator(self.bursa_futures_validator)
            self.code_input.setPlaceholderText("例如: FCPO1!, FKLI1!")
            self.exchange_label.hide()
            self.exchange_combo.hide()
        elif asset_type == "Crypto":
            self.code_input.setValidator(self.crypto_validator)
            self.code_input.setPlaceholderText("例如: BTC/USDT")
            self.exchange_label.show()
            self.exchange_combo.show()
        self.code_input.clear()
        
        # Session Filter Logic (v2.5)
        if asset_type in ["Malaysia Stock", "Bursa Futures (TV)"]:
            # Automatic filtering (Enforced)
            self.apply_session_filter_checkbox.blockSignals(True) # Prevent toggling logic during setup
            self.apply_session_filter_checkbox.setChecked(True)
            self.apply_session_filter_checkbox.setEnabled(False)
            self.apply_session_filter_checkbox.blockSignals(False)
            self.apply_session_filter_checkbox.setToolTip(tr("tips.session_filter_my"))
            self.session_container.hide()
        elif asset_type in ["Crypto", "US Stock", "Futures - Global"]:
            # Optional custom filtering
            self.apply_session_filter_checkbox.blockSignals(True)
            self.apply_session_filter_checkbox.setChecked(False) # Default OFF
            self.apply_session_filter_checkbox.setEnabled(True)
            self.apply_session_filter_checkbox.blockSignals(False)
            self.apply_session_filter_checkbox.setToolTip(tr("tips.session_filter_custom"))
            self.session_container.hide()

    def _on_session_filter_toggled(self, checked):
        """Handle session filter toggle."""
        asset_type = self._get_selected_asset_type()
        supports_custom = asset_type in ["Crypto", "US Stock", "Futures - Global"]
        
        if checked and supports_custom:
            self.session_container.show()
        else:
            self.session_container.hide()

    def _on_fetch_clicked(self):
        try:
            self.status_banner.hide()
            asset_type = self._get_selected_asset_type()
            raw_code = self.code_input.text().strip()
            timeframe = self.timeframe_combo.currentText()
            
            start_qdate = self.start_date.date()
            end_qdate = self.end_date.date()
            start_date = datetime(start_qdate.year(), start_qdate.month(), start_qdate.day())
            end_date = datetime(end_qdate.year(), end_qdate.month(), end_qdate.day())
            
            is_valid, error_msg = validate_code(raw_code, asset_type)
            if not is_valid:
                QMessageBox.warning(self, "输入错误", error_msg)
                return
            
            is_valid, error_msg = validate_date_range(start_date, end_date)
            if not is_valid:
                QMessageBox.warning(self, "日期错误", error_msg)
                return
            
            preprocess_info = self.fetcher.preprocess_code(raw_code, asset_type)
            
            # Unpack the string if it's returning the Phase 1 metadata dict
            if isinstance(preprocess_info, dict):
                processed_code = preprocess_info.get('processed_code', raw_code)
            else:
                processed_code = preprocess_info
            self.fetch_button.setEnabled(False)
            self.fetch_button.setText("获取中...")
            
            self.current_code = processed_code
            self.current_timeframe = timeframe
            self.current_start_date = start_date
            
            exchange = None
            if asset_type == "Crypto":
                exchange = self.exchange_combo.currentText()
                
            proxy_url = None
            if self.proxy_enabled.isChecked():
                proxy_url = self.proxy_url_input.text().strip()
            
            use_smart_update = self.incremental_update_checkbox.isChecked()
            apply_session_filter = self.apply_session_filter_checkbox.isChecked()
            
            custom_session = None
            if apply_session_filter and self.session_container.isVisible():
                 start_t = self.session_start.time()
                 end_t = self.session_end.time()
                 from datetime import time
                 custom_session = (
                     time(start_t.hour(), start_t.minute()),
                     time(end_t.hour(), end_t.minute())
                 )
            
            # Stability mode:
            # Execute fetch synchronously in UI thread to avoid cross-thread
            # native crashes observed in this environment.
            self._run_fetch_sync(
                asset_type=asset_type,
                processed_code=processed_code,
                timeframe=timeframe,
                start_date=start_date,
                end_date=end_date,
                exchange=exchange,
                proxy_url=proxy_url,
                use_smart_update=use_smart_update,
                apply_session_filter=apply_session_filter,
                custom_session=custom_session,
            )
            
        except Exception as e:
            QMessageBox.critical(self, "程序错误", str(e))
            self.fetch_button.setEnabled(True)
            self.fetch_button.setText("获取数据 (Fetch Data)")

    def _on_fetch_success(self, payload, has_warning, warning_msg, csv_path):
        import pickle
        try:
            # 1. Decode in-memory payload from worker thread.
            if not isinstance(payload, (bytes, bytearray)):
                raise TypeError(f"中转数据类型异常: {type(payload)}")
            decoded = pickle.loads(payload)
            if not isinstance(decoded, dict):
                raise TypeError("中转数据结构异常")
            columns = decoded.get("columns", [])
            rows = decoded.get("rows", [])
            row_count = int(decoded.get("row_count", len(rows)))
                
        except Exception as e:
            self._on_fetch_error(f"无法从中转数据加载结果: {str(e)}")
            return
            
        self.current_df = None
        self.current_columns = list(columns)
        self.current_rows = list(rows)
        # Restore preview rendering (safe path using plain-Python records).
        self.data_grid.display_records(self.current_columns, self.current_rows)
        self.row_count_label.setText(f"共 {row_count} 条数据")
        self.export_csv_button.setEnabled(True)
        self.export_parquet_button.setEnabled(True)
        self.save_db_button.setEnabled(True)
        
        if has_warning:
            self.status_banner.show_warning(warning_msg)
        else:
            self.status_banner.show_success(warning_msg + " | 点击 '导出' 保存")
            
        # Keep auto-processing disabled for now; it can be re-enabled after
        # we confirm full stability on your machine.
        # self._try_auto_process_data()

    def _on_fetch_error(self, error_msg):
        self.status_banner.show_error(error_msg)
        self.data_grid.setRowCount(0)
        if "\n" in error_msg and len(error_msg) > 100:
             QMessageBox.warning(self, "数据获取错误", f"详细错误信息:\n\n{error_msg[:600]}")

    def _on_fetch_finished(self):
        self.fetch_button.setEnabled(True)
        self.fetch_button.setText("获取数据 (Fetch Data)")
        if self.current_worker:
            # Avoid blocking/re-entrancy around finished callback; thread is already done.
            self.current_worker.deleteLater()
            self.current_worker = None

    def _run_fetch_sync(
        self,
        asset_type,
        processed_code,
        timeframe,
        start_date,
        end_date,
        exchange,
        proxy_url,
        use_smart_update,
        apply_session_filter,
        custom_session,
    ):
        """Run fetch pipeline synchronously (threadless stability path)."""
        from PyQt6.QtWidgets import QApplication
        try:
            QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)
            QApplication.processEvents()

            if use_smart_update:
                df = self.fetcher.smart_update(
                    symbol=processed_code,
                    asset_type=asset_type,
                    timeframe=timeframe,
                    start_date=start_date,
                    end_date=end_date,
                    exchange=exchange,
                    proxy_url=proxy_url,
                )
                if apply_session_filter:
                    df = self.fetcher.apply_market_session_filter(df, asset_type, custom_session)
            else:
                df = self.fetcher.fetch_data(
                    asset_type,
                    processed_code,
                    timeframe,
                    start_date,
                    end_date,
                    exchange=exchange,
                    proxy_url=proxy_url,
                    filter_lunch=False,
                )
                if apply_session_filter:
                    df = self.fetcher.apply_market_session_filter(df, asset_type, custom_session)

            if df is None or df.empty:
                self._on_fetch_error("未获取到任何数据。请检查资产代码和日期范围。")
                return

            has_warning, warning_msg = self.fetcher.analyze_gaps(df, start_date, end_date)
            payload = {
                "columns": list(df.columns),
                "rows": df.where(df.notna(), None).to_dict(orient="records"),
                "row_count": len(df),
            }

            import pickle
            relay = pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL)
            self._on_fetch_success(relay, has_warning, warning_msg, "")
        except Exception as e:
            self._on_fetch_error(str(e))
        finally:
            QApplication.restoreOverrideCursor()
            self._on_fetch_finished()

    def _try_auto_process_data(self):
        from pathlib import Path
        store_dir = Path("datacenter/RawData")
        
        # Support deep folder structure (v2.6 Master DB Refactor)
        fcpo_files = list(store_dir.rglob("*FCPO1!_15m.parquet"))
        zl_files = list(store_dir.rglob("*ZL1!_15m.parquet"))
        
        if fcpo_files and zl_files:
            self.status_banner.show_info("正在生成对齐后的数据集...")
            from src.core.data_processor import DataProcessor
            processor = DataProcessor(store_dir="datacenter/RawData", output_dir="datacenter/RawData/alignment")
            # Logic to invoke processor can go here if needed as a separate thread or call

    def _on_export_clicked(self, format_type='csv'):
        if not self.current_rows:
            return
        
        # This part requires access to file dialog. 
        # Since this logic might be better placed in main window or passed down,
        # but for now we implement it here.
        from PyQt6.QtWidgets import QFileDialog
        
        default_name = f"{self.current_code}_{self.current_timeframe}_{self.current_start_date.strftime('%Y%m%d')}"
        if format_type == 'csv':
            file_filter = "CSV Files (*.csv)"
            default_name += ".csv"
        else:
            file_filter = "Parquet Files (*.parquet)"
            default_name += ".parquet"
            
        file_path, _ = QFileDialog.getSaveFileName(self, f"导出 {format_type.upper()}", default_name, file_filter)
        
        if file_path:
            try:
                import pandas as pd
                df = pd.DataFrame(self.current_rows, columns=self.current_columns)
                if format_type == 'csv':
                    df.to_csv(file_path, index=False)
                else:
                    df.to_parquet(file_path, index=False)
                QMessageBox.information(self, "导出成功", f"文件已保存至:\n{file_path}")
            except Exception as e:
                QMessageBox.critical(self, "导出失败", str(e))

    def _on_save_db_clicked(self):
        """Save current data to Master DB (Data Center)."""
        if not self.current_rows:
            return
        
        try:
            import pandas as pd
            df = pd.DataFrame(self.current_rows, columns=self.current_columns)
            # Determine asset type for folder structure
            asset_type = self._get_selected_asset_type()
            
            # Call Facade to save
            saved_path = self.fetcher.save_to_master_db(
                df,
                asset_type, 
                self.current_code, 
                self.current_timeframe
            )
            
            self.status_banner.show_success(f"已保存至数据中心: {saved_path}")
            QMessageBox.information(self, "保存成功", 
                                  f"数据已成功保存至 Master DB！\n\n"
                                  f"路径: {saved_path}\n"
                                  f"您可以在 '数据管理中心' (Data Manager) 中查看。")
            
        except Exception as e:
            QMessageBox.critical(self, "保存失败", f"无法保存到 Master DB:\n{str(e)}")
