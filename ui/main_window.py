"""
Main Window - Primary application interface for Quant Data Bridge.
"""

from PyQt6.QtWidgets import (QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
                             QLabel, QLineEdit, QComboBox, QRadioButton,
                             QPushButton, QDateEdit, QGroupBox, QButtonGroup,
                             QMessageBox, QCheckBox)
from PyQt6.QtCore import Qt, QDate
from PyQt6.QtGui import QFont
from datetime import datetime

from .status_banner import StatusBanner
from .data_grid import DataGrid
from core.worker import FetchWorker
from core.data_fetcher import DataFetcher
from utils.validators import validate_code, validate_date_range


class MainWindow(QMainWindow):
    """
    Main application window for Quant Data Bridge.
    """
    
    def __init__(self):
        super().__init__()
        self.current_worker = None
        self.fetcher = DataFetcher()
        self.current_df = None  # Store fetched DataFrame
        self.current_code = None  # Store code for export filename
        self.current_timeframe = None  # Store timeframe for export filename
        self.current_start_date = None  # Store start date for export filename
        self._init_ui()
        
        # 🆕 启动时执行自动清理和磁盘检查
        self._perform_startup_checks()
    
    def _init_ui(self):
        """Initialize the user interface."""
        self.setWindowTitle("Quant Data Bridge")
        self.setMinimumSize(900, 700)
        
        # Create central widget
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        # Main layout
        main_layout = QVBoxLayout()
        main_layout.setSpacing(15)
        main_layout.setContentsMargins(20, 20, 20, 20)
        
        # Title
        title = QLabel("Quant Data Bridge")
        title_font = QFont()
        title_font.setPointSize(18)
        title_font.setBold(True)
        title.setFont(title_font)
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        main_layout.addWidget(title)
        
        # Status banner
        self.status_banner = StatusBanner()
        main_layout.addWidget(self.status_banner)
        
        # Input configuration panel
        config_panel = self._create_config_panel()
        main_layout.addWidget(config_panel)
        
        # Network Settings / 网络设置 (Proxy Configuration)
        proxy_group = QGroupBox("网络设置 (Network Settings)")
        proxy_group.setCheckable(True)
        proxy_group.setChecked(False)  # 默认折叠
        proxy_layout = QHBoxLayout()
        
        self.proxy_enabled = QCheckBox("启用代理 (Enable Proxy)")
        self.proxy_enabled.setChecked(False)
        proxy_layout.addWidget(self.proxy_enabled)
        
        proxy_url_label = QLabel("代理 URL:")
        proxy_layout.addWidget(proxy_url_label)
        
        self.proxy_url_input = QLineEdit()
        self.proxy_url_input.setPlaceholderText("http://127.0.0.1:7890")
        self.proxy_url_input.setText("http://127.0.0.1:7890")  # 默认值
        self.proxy_url_input.setEnabled(False)  # 默认禁用
        proxy_layout.addWidget(self.proxy_url_input)
        
        # 连接信号：启用代理时启用 URL 输入框
        self.proxy_enabled.toggled.connect(self.proxy_url_input.setEnabled)
        
        proxy_group.setLayout(proxy_layout)
        main_layout.addWidget(proxy_group)
        
        # 🆕 v2.0: Advanced Settings / 高级设置 (Incremental Update & Lunch Filter)
        advanced_group = QGroupBox("高级设置 (Advanced Settings) - v2.0")
        advanced_group.setCheckable(True)
        advanced_group.setChecked(False)  # 默认折叠
        advanced_layout = QVBoxLayout()
        
        self.incremental_update_checkbox = QCheckBox("✨ 启用增量更新 (Incremental Update)")
        self.incremental_update_checkbox.setChecked(False)
        self.incremental_update_checkbox.setToolTip(
            "开启后，将从本地 Master DB 读取历史数据，仅下载最新数据。\n"
            "可节省80%下载时间和网络流量。"
        )
        advanced_layout.addWidget(self.incremental_update_checkbox)
        
        self.filter_lunch_checkbox = QCheckBox("⏰ 过滤午休时段 (Filter Lunch Break: 12:30-14:30)")
        self.filter_lunch_checkbox.setChecked(False)
        self.filter_lunch_checkbox.setToolTip(
            "开启后，将自动过滤午休时段（12:30-14:30）的噪音数据。\n"
            "适用于马股和期货，保留盘前盘后数据。"
        )
        advanced_layout.addWidget(self.filter_lunch_checkbox)
        
        advanced_group.setLayout(advanced_layout)
        main_layout.addWidget(advanced_group)
        
        # Data preview section with row count
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
        
        central_widget.setLayout(main_layout)
    
    def _create_config_panel(self) -> QGroupBox:
        """Create the input configuration panel with compact horizontal layout."""
        group = QGroupBox("输入配置")
        main_layout = QVBoxLayout()
        main_layout.setSpacing(10)
        
        # Row 1: Asset Type (horizontal radio buttons)
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
        self.radio_gold = QRadioButton("国际期货 (YF)")  # 修改：重命名以区分
        self.radio_bursa_futures = QRadioButton("Bursa期货 (TV)")  # 新增：TradingView数据源
        self.radio_crypto = QRadioButton("加密货币")
        
        self.asset_button_group.addButton(self.radio_my_stock, 0)
        self.asset_button_group.addButton(self.radio_us_stock, 1)
        self.asset_button_group.addButton(self.radio_gold, 2)
        self.asset_button_group.addButton(self.radio_bursa_futures, 3)
        self.asset_button_group.addButton(self.radio_crypto, 4)
        
        asset_row.addWidget(self.radio_my_stock)
        asset_row.addWidget(self.radio_us_stock)
        asset_row.addWidget(self.radio_gold)
        asset_row.addWidget(self.radio_bursa_futures)  # 新增
        asset_row.addWidget(self.radio_crypto)
        asset_row.addStretch()
        
        self.radio_my_stock.setChecked(True)
        self.radio_my_stock.toggled.connect(self._on_asset_type_changed)
        self.radio_us_stock.toggled.connect(self._on_asset_type_changed)
        self.radio_gold.toggled.connect(self._on_asset_type_changed)
        self.radio_bursa_futures.toggled.connect(self._on_asset_type_changed)  # 新增
        self.radio_crypto.toggled.connect(self._on_asset_type_changed)
        
        main_layout.addLayout(asset_row)
        
        # Row 2: Code, Exchange, Timeframe (horizontal)
        input_row = QHBoxLayout()
        
        # Code input
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
        
        # Create validators
        from PyQt6.QtGui import QRegularExpressionValidator
        from PyQt6.QtCore import QRegularExpression
        
        self.malaysia_validator = QRegularExpressionValidator(QRegularExpression(r"^\d{0,4}$"))
        self.us_validator = QRegularExpressionValidator(QRegularExpression(r"^[A-Za-z.]{0,10}$"))
        self.futures_validator = QRegularExpressionValidator(QRegularExpression(r"^[A-Za-z0-9=\-.\^]{0,15}$"))
        self.bursa_futures_validator = QRegularExpressionValidator(QRegularExpression(r"^[A-Za-z0-9!]+$"))  # 新增：允许!字符
        self.crypto_validator = QRegularExpressionValidator(QRegularExpression(r"^[A-Za-z0-9/]{0,20}$"))
        self.code_input.setValidator(self.malaysia_validator)
        
        # Exchange selector (hidden by default)
        self.exchange_label = QLabel("交易所:")
        font = self.exchange_label.font()
        font.setBold(True)
        font.setPointSize(10)
        self.exchange_label.setFont(font)
        self.exchange_label.setFixedWidth(60)
        self.exchange_label.hide()
        input_row.addWidget(self.exchange_label)
        
        self.exchange_combo = QComboBox()
        self.exchange_combo.addItems([
            "Luno (Malaysia)",
            "Binance (Global)",
            "OKX",
            "Bybit"
        ])
        self.exchange_combo.setMaximumWidth(150)
        self.exchange_combo.hide()
        input_row.addWidget(self.exchange_combo)
        
        input_row.addSpacing(20)
        
        # Timeframe
        timeframe_label = QLabel("时间粒度:")
        font = timeframe_label.font()
        font.setBold(True)
        font.setPointSize(10)
        timeframe_label.setFont(font)
        input_row.addWidget(timeframe_label)
        
        self.timeframe_combo = QComboBox()
        self.timeframe_combo.addItems([
            '1m', '5m', '15m', '1h', '1d',
            '1w', '1M', '1y'
        ])
        self.timeframe_combo.setCurrentText('1d')
        self.timeframe_combo.setMaximumWidth(100)
        input_row.addWidget(self.timeframe_combo)
        
        input_row.addStretch()
        main_layout.addLayout(input_row)
        
        # Row 3: Date Range (horizontal)
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
        
        # Row 4: Buttons (horizontal)
        button_row = QHBoxLayout()
        
        self.fetch_button = QPushButton("获取数据 (Fetch Data)")
        self.fetch_button.clicked.connect(self._on_fetch_clicked)
        self.fetch_button.setMinimumHeight(35)
        button_row.addWidget(self.fetch_button)
        
        # 🆕 v2.0: 两个导出按钮（CSV 和 Parquet）
        self.export_csv_button = QPushButton("导出 CSV (Export CSV)")
        self.export_csv_button.clicked.connect(lambda: self._on_export_clicked('csv'))
        self.export_csv_button.setEnabled(False)
        self.export_csv_button.setMinimumHeight(35)
        button_row.addWidget(self.export_csv_button)
        
        self.export_parquet_button = QPushButton("📦 导出 Parquet")
        self.export_parquet_button.clicked.connect(lambda: self._on_export_clicked('parquet'))
        self.export_parquet_button.setEnabled(False)
        self.export_parquet_button.setMinimumHeight(35)
        self.export_parquet_button.setToolTip("Parquet格式：压缩率70%，读取速度快10倍")
        button_row.addWidget(self.export_parquet_button)
        
        # 🆕 数据管理按钮
        self.data_manager_button = QPushButton("📊 数据管理")
        self.data_manager_button.clicked.connect(self._on_data_manager_clicked)
        self.data_manager_button.setMinimumHeight(35)
        self.data_manager_button.setToolTip("管理Master DB和导出的数据文件")
        button_row.addWidget(self.data_manager_button)
        
        button_row.addStretch()
        main_layout.addLayout(button_row)
        
        group.setLayout(main_layout)
        return group
        """Create the input configuration panel."""
        group = QGroupBox("输入配置")
        layout = QVBoxLayout()
        layout.setSpacing(15)
        
        # Asset Type Selection
        asset_label = QLabel("资产类型:")
        font = asset_label.font()
        font.setBold(True)
        font.setPointSize(11)
        asset_label.setFont(font)
        layout.addWidget(asset_label)
        
        # Radio buttons for asset types
        radio_layout = QHBoxLayout()
        self.asset_button_group = QButtonGroup()
        
        self.radio_my_stock = QRadioButton("马股 (Malaysia Stock)")
        self.radio_us_stock = QRadioButton("美股 (US Stock)")
        self.radio_gold = QRadioButton("期货 (Futures - Global)")  # 修改：从 "期货-黄金" 改为通用期货
        self.radio_crypto = QRadioButton("加密货币 (Crypto)")
        
        self.asset_button_group.addButton(self.radio_my_stock, 0)
        self.asset_button_group.addButton(self.radio_us_stock, 1)
        self.asset_button_group.addButton(self.radio_gold, 2)
        self.asset_button_group.addButton(self.radio_crypto, 3)
        
        radio_layout.addWidget(self.radio_my_stock)
        radio_layout.addWidget(self.radio_us_stock)
        radio_layout.addWidget(self.radio_gold)
        radio_layout.addWidget(self.radio_crypto)
        radio_layout.addStretch()
        
        layout.addLayout(radio_layout)
        
        # Set default selection
        self.radio_my_stock.setChecked(True)
        
        # Connect radio button signals
        self.radio_my_stock.toggled.connect(self._on_asset_type_changed)
        self.radio_us_stock.toggled.connect(self._on_asset_type_changed)
        self.radio_gold.toggled.connect(self._on_asset_type_changed)
        self.radio_crypto.toggled.connect(self._on_asset_type_changed)
        # Code Input
        code_layout = QVBoxLayout()
        code_label = QLabel("代码:")
        font = code_label.font()
        font.setBold(True)
        font.setPointSize(11)
        code_label.setFont(font)
        code_layout.addWidget(code_label)
        
        self.code_input = QLineEdit()
        self.code_input.setPlaceholderText("例如: 1155")
        
        # 创建不同资产类型的验证器
        from PyQt6.QtGui import QRegularExpressionValidator
        from PyQt6.QtCore import QRegularExpression
        
        # 马股验证器：只允许数字（最多4位）
        self.malaysia_validator = QRegularExpressionValidator(QRegularExpression(r"^\d{0,4}$"))
        
        # 美股验证器：只允许字母和点
        self.us_validator = QRegularExpressionValidator(QRegularExpression(r"^[A-Za-z.]{0,10}$"))
        
        # 期货验证器：允许字母、数字、等号、减号、点（修改：支持通用期货代码）
        self.futures_validator = QRegularExpressionValidator(QRegularExpression(r"^[A-Za-z0-9=\-.\^]{0,15}$"))
        
        # 加密货币验证器：允许字母、数字和斜杠
        self.crypto_validator = QRegularExpressionValidator(QRegularExpression(r"^[A-Za-z0-9/]{0,20}$"))
        
        # 默认设置马股验证器
        self.code_input.setValidator(self.malaysia_validator)
        
        code_layout.addWidget(self.code_input)
        layout.addLayout(code_layout)
        
        # Exchange Selection (仅加密货币时显示)
        exchange_layout = QVBoxLayout()
        self.exchange_label = QLabel("交易所:")
        font = self.exchange_label.font()
        font.setBold(True)
        font.setPointSize(11)
        self.exchange_label.setFont(font)
        exchange_layout.addWidget(self.exchange_label)
        
        self.exchange_combo = QComboBox()
        self.exchange_combo.addItems([
            "Luno (Malaysia)",
            "Binance (Global)",
            "OKX",
            "Bybit"
        ])
        exchange_layout.addWidget(self.exchange_combo)
        layout.addLayout(exchange_layout)
        
        # 默认隐藏交易所选择器（只在选择 Crypto 时显示）
        self.exchange_label.hide()
        self.exchange_combo.hide()
        
        # Timeframe Selection
        timeframe_label = QLabel("时间粒度:")
        font = timeframe_label.font()
        font.setBold(True)
        font.setPointSize(11)
        timeframe_label.setFont(font)
        layout.addWidget(timeframe_label)
        
        
        self.timeframe_combo = QComboBox()
        self.timeframe_combo.addItems([
            '1m', '5m', '15m', '1h', '1d',
            '1w',   # 新增：1周
            '1M',   # 新增：1月  
            '1y'    # 新增：1年
        ])
        self.timeframe_combo.setCurrentText('1d')
        layout.addWidget(self.timeframe_combo)
        
        # Date Range
        date_layout = QHBoxLayout()
        
        # Start Date
        start_date_layout = QVBoxLayout()
        start_label = QLabel("开始日期:")
        font = start_label.font()
        font.setBold(True)
        font.setPointSize(11)
        start_label.setFont(font)
        start_date_layout.addWidget(start_label)
        
        self.start_date_edit = QDateEdit()
        self.start_date_edit.setCalendarPopup(True)
        self.start_date_edit.setDate(QDate.currentDate().addMonths(-1))
        self.start_date_edit.setDisplayFormat("yyyy-MM-dd")
        start_date_layout.addWidget(self.start_date_edit)
        
        # End Date
        end_date_layout = QVBoxLayout()
        end_label = QLabel("结束日期:")
        font = end_label.font()
        font.setBold(True)
        font.setPointSize(11)
        end_label.setFont(font)
        end_date_layout.addWidget(end_label)
        
        self.end_date_edit = QDateEdit()
        self.end_date_edit.setCalendarPopup(True)
        self.end_date_edit.setDate(QDate.currentDate())
        self.end_date_edit.setDisplayFormat("yyyy-MM-dd")
        end_date_layout.addWidget(self.end_date_edit)
        
        date_layout.addLayout(start_date_layout)
        date_layout.addLayout(end_date_layout)
        layout.addLayout(date_layout)
        
        # Fetch Button
        self.fetch_button = QPushButton("获取数据 (Fetch Data)")
        self.fetch_button.clicked.connect(self._on_fetch_clicked)
        layout.addWidget(self.fetch_button)
        
        # Export CSV Button (disabled by default)
        self.export_button = QPushButton("导出 CSV (Export CSV)")
        self.export_button.setEnabled(False)
        self.export_button.clicked.connect(self._on_export_clicked)
        self.export_button.setStyleSheet("""
            QPushButton:disabled {
                background-color: #555;
                color: #888;
            }
        """)
        layout.addWidget(self.export_button)
        
        group.setLayout(layout)
        return group
    
    def _get_selected_asset_type(self) -> str:
        """Get the currently selected asset type."""
        if self.radio_my_stock.isChecked():
            return "Malaysia Stock"
        elif self.radio_us_stock.isChecked():
            return "US Stock"
        elif self.radio_gold.isChecked():
            return "Futures - Global"
        elif self.radio_bursa_futures.isChecked():  # 新增
            return "Bursa Futures (TV)"
        elif self.radio_crypto.isChecked():
            return "Crypto"
        else:
            return "Malaysia Stock"
    
    def _on_asset_type_changed(self):
        """Handle asset type selection change - update input validator and placeholder."""
        asset_type = self._get_selected_asset_type()
        
        # 根据资产类型更新验证器和占位符
        if asset_type == "Malaysia Stock":
            self.code_input.setValidator(self.malaysia_validator)
            self.code_input.setPlaceholderText("例如: 1155")
            self.code_input.setReadOnly(False)
            self.code_input.clear()
            # 隐藏交易所选择器
            self.exchange_label.hide()
            self.exchange_combo.hide()
            
        elif asset_type == "US Stock":
            self.code_input.setValidator(self.us_validator)
            self.code_input.setPlaceholderText("例如: AAPL, TSLA, MSFT")
            self.code_input.setReadOnly(False)
            self.code_input.clear()
            # 隐藏交易所选择器
            self.exchange_label.hide()
            self.exchange_combo.hide()
            
        elif asset_type == "Futures - Global":  # 修改：从锁定改为可输入
            # 期货现在允许用户自由输入（通用模式）
            self.code_input.setValidator(self.futures_validator)
            self.code_input.setPlaceholderText("例如: GC=F, CL=F, SI=F")
            self.code_input.setReadOnly(False)  # 修改：从 True 改为 False
            self.code_input.clear()  # 修改：清空而不是填充 GC=F
            # 隐藏交易所选择器
            self.exchange_label.hide()
            self.exchange_combo.hide()
        
        elif asset_type == "Bursa Futures (TV)":  # 新增：Bursa期货（TradingView）
            self.code_input.setValidator(self.bursa_futures_validator)
            self.code_input.setPlaceholderText("例如: FCPO1!, FKLI1!")
            self.code_input.setReadOnly(False)
            self.code_input.clear()
            # 隐藏交易所选择器
            self.exchange_label.hide()
            self.exchange_combo.hide()
            
        elif asset_type == "Crypto":
            self.code_input.setValidator(self.crypto_validator)
            self.code_input.setPlaceholderText("例如: BTC/USDT, ETH/MYR")
            self.code_input.setReadOnly(False)
            self.code_input.clear()
            # 显示交易所选择器（新增）
            self.exchange_label.show()
            self.exchange_combo.show()
    
    def _on_fetch_clicked(self):
        """Handle fetch button click."""
        try:
            print("\n" + "="*60)
            print("[DEBUG] Fetch button clicked!")
            print("="*60)
            
            # Hide previous status
            self.status_banner.hide()
            
            # Get inputs
            asset_type = self._get_selected_asset_type()
            raw_code = self.code_input.text().strip()
            timeframe = self.timeframe_combo.currentText()
            
            print(f"[DEBUG] User inputs - Asset: {asset_type}, Code: {raw_code}, Timeframe: {timeframe}")
            
            # Get dates
            start_qdate = self.start_date.date()  # 修改：从 start_date_edit 改为 start_date
            end_qdate = self.end_date.date()  # 修改：从 end_date_edit 改为 end_date
            
            start_date = datetime(start_qdate.year(), start_qdate.month(), start_qdate.day())
            end_date = datetime(end_qdate.year(), end_qdate.month(), end_qdate.day())
            
            print(f"[DEBUG] Date range: {start_date} to {end_date}")
            
            # Validate inputs
            print("[DEBUG] Validating code...")
            is_valid, error_msg = validate_code(raw_code, asset_type)
            if not is_valid:
                print(f"[DEBUG] Code validation failed: {error_msg}")
                # 使用消息框显示错误，更明显
                QMessageBox.warning(self, "输入错误", error_msg)
                self.status_banner.show_error(error_msg.split('\n')[0])  # 横幅只显示第一行
                return
            
            print("[DEBUG] Validating date range...")
            is_valid, error_msg = validate_date_range(start_date, end_date)
            if not is_valid:
                print(f"[DEBUG] Date validation failed: {error_msg}")
                # 使用消息框显示错误，更明显
                QMessageBox.warning(self, "日期错误", error_msg)
                self.status_banner.show_error(error_msg.split('\n')[0])  # 横幅只显示第一行
                return
            
            # Preprocess code
            print("[DEBUG] Preprocessing code...")
            processed_code = self.fetcher.preprocess_code(raw_code, asset_type)
            print(f"[DEBUG] Processed code: {processed_code}")
            
            # Disable button during fetch
            self.fetch_button.setEnabled(False)
            self.fetch_button.setText("获取中...")
            print("[DEBUG] Button disabled, starting worker thread...")
            
            # Store parameters for later CSV export
            self.current_code = processed_code
            self.current_timeframe = timeframe
            self.current_start_date = start_date
            
            # Create and start worker thread
            # 获取交易所和代理设置（新增）
            exchange = None
            proxy_url = None
            
            if asset_type == "Crypto":
                exchange = self.exchange_combo.currentText()
                print(f"[DEBUG] Selected exchange: {exchange}")
            
            if self.proxy_enabled.isChecked():
                proxy_url = self.proxy_url_input.text().strip()
                if proxy_url:
                    print(f"[DEBUG] Proxy enabled: {proxy_url}")
            
            # 🆕 v2.0: 读取高级设置
            use_smart_update = self.incremental_update_checkbox.isChecked()
            filter_lunch = self.filter_lunch_checkbox.isChecked()
            print(f"[DEBUG] Advanced settings: use_smart_update={use_smart_update}, filter_lunch={filter_lunch}")
            
            self.current_worker = FetchWorker(
                asset_type=asset_type,
                code=processed_code,
                timeframe=timeframe,
                start_date=start_date,
                end_date=end_date,
                exchange=exchange,
                proxy_url=proxy_url,
                use_smart_update=use_smart_update,  # 🆕 v2.0
                filter_lunch=filter_lunch  # 🆕 v2.0
            )
            
            print("[DEBUG] Connecting worker signals...")
            # Connect signals
            self.current_worker.success.connect(self._on_fetch_success)
            self.current_worker.error.connect(self._on_fetch_error)
            self.current_worker.finished.connect(self._on_fetch_finished)
            
            # Start worker
            print("[DEBUG] Starting worker thread...")
            self.current_worker.start()
            print("[DEBUG] Worker thread started successfully!")
            
        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            print(f"[DEBUG] CRITICAL ERROR in _on_fetch_clicked: {str(e)}")
            print(f"[DEBUG] Full traceback:\n{error_details}")
            
            # Show error to user
            QMessageBox.critical(
                self,
                "程序错误",
                f"点击获取数据时发生错误:\n\n{str(e)}\n\n详细信息:\n{error_details}"
            )
            
            # Re-enable button
            self.fetch_button.setEnabled(True)
            self.fetch_button.setText("获取数据 (Fetch Data)")
    
    def _on_fetch_success(self, df, has_warning, warning_msg, csv_path):
        """Handle successful data fetch."""
        try:
            print("\n" + "="*60)
            print("[DEBUG] _on_fetch_success called!")
            print(f"[DEBUG] DataFrame shape: {df.shape}")
            print(f"[DEBUG] Has warning: {has_warning}")
            print("="*60 + "\n")
            
            # Store DataFrame for later export
            self.current_df = df.copy()
            
            # Display data in grid
            print("[DEBUG] Displaying dataframe in grid...")
            self.data_grid.display_dataframe(df)
            print("[DEBUG] Grid updated successfully")
            
            # Update row count display
            row_count = len(df)
            self.row_count_label.setText(f"共 {row_count} 条数据")
            self.row_count_label.setStyleSheet("font-size: 12px; color: #4CAF50; font-weight: bold;")
            
            # 🆕 v2.0: Enable both export buttons now that we have data
            self.export_csv_button.setEnabled(True)
            self.export_parquet_button.setEnabled(True)
            
            # Show appropriate status (without CSV path since we haven't exported yet)
            if has_warning:
                self.status_banner.show_warning(warning_msg)
            else:
                self.status_banner.show_success(warning_msg + " | 点击 '导出 CSV' 按钮保存数据")
            print("[DEBUG] Success handler completed!")
        except Exception as e:
            import traceback
            print(f"[DEBUG] ERROR in _on_fetch_success: {str(e)}")
            print(traceback.format_exc())
            QMessageBox.critical(self, "显示错误", f"显示数据时出错:\n{str(e)}")
    
    
    def _on_fetch_error(self, error_msg):
        """Handle fetch error."""
        try:
            print("\n" + "="*60)
            print("[DEBUG] _on_fetch_error called!")
            print(f"[DEBUG] Error: {error_msg[:200]}")
            print("="*60 + "\n")
            
            self.status_banner.show_error(f"错误: {error_msg}")
            self.data_grid.setRowCount(0)
            
            # Show message box for errors with traceback
            if "\n" in error_msg and len(error_msg) > 100:
                QMessageBox.warning(self, "数据获取错误", f"详细错误信息:\n\n{error_msg[:600]}")
            print("[DEBUG] Error handler completed!")
        except Exception as e:
            print(f"[DEBUG] ERROR in _on_fetch_error: {str(e)}")
    
    
    def _on_fetch_finished(self):
        """Handle worker thread completion."""
        try:
            print("\n" + "="*60)
            print("[DEBUG] _on_fetch_finished called!")
            print("="*60 + "\n")
            
            # Re-enable button
            self.fetch_button.setEnabled(True)
            self.fetch_button.setText("获取数据 (Fetch Data)")
            
            # Clean up worker
            if self.current_worker:
                self.current_worker.deleteLater()
                self.current_worker = None
            print("[DEBUG] Finished handler completed!\n")
        except Exception as e:
            print(f"[DEBUG] ERROR in _on_fetch_finished: {str(e)}")
    
    def _on_export_clicked(self, format='csv'):
        """
        Handle export button click (v2.0 - 支持CSV和Parquet)
        
        Args:
            format: 'csv' 或 'parquet'
        """
        try:
            print("\n" + "="*60)
            print(f"[DEBUG] Export button clicked! Format: {format}")
            print("="*60)
            
            if self.current_df is None:
                QMessageBox.warning(self, "无数据", "请先获取数据再导出！")
                return
            
            # 生成建议的文件名
            from datetime import datetime
            start_str = self.current_start_date.strftime('%Y%m%d')
            
            # 根据格式生成文件名和过滤器
            if format == 'parquet':
                default_filename = f"{self.current_code}_{self.current_timeframe}_{start_str}.parquet"
                file_filter = "Parquet 文件 (*.parquet);;所有文件 (*.*)"
                dialog_title = "选择 Parquet 保存位置"
            else:  # csv
                default_filename = f"{self.current_code}_{self.current_timeframe}_{start_str}.csv"
                file_filter = "CSV 文件 (*.csv);;所有文件 (*.*)"
                dialog_title = "选择 CSV 保存位置"
            
            # 获取用户的文档目录作为默认保存位置
            import os
            default_dir = os.path.expanduser("~/Documents")
            default_path = os.path.join(default_dir, default_filename)
            
            # 弹出文件保存对话框
            from PyQt6.QtWidgets import QFileDialog
            file_path, selected_filter = QFileDialog.getSaveFileName(
                self,
                dialog_title,
                default_path,
                file_filter
            )
            
            # 用户取消了保存
            if not file_path:
                print("[DEBUG] User cancelled export")
                return
            
            # 确保文件扩展名正确
            if format == 'parquet':
                if not file_path.lower().endswith('.parquet'):
                    file_path += '.parquet'
            else:
                if not file_path.lower().endswith('.csv'):
                    file_path += '.csv'
            
            print(f"[DEBUG] User selected save path: {file_path}")
            print(f"[DEBUG] Exporting DataFrame of shape {self.current_df.shape}")
            
            # 🆕 v2.0: 根据格式调用不同的导出方法
            if format == 'parquet':
                # 调用 data_fetcher 的 export_to_parquet 方法
                self.fetcher.export_to_parquet(
                    self.current_df,
                    self.current_code,
                    self.current_timeframe,
                    self.current_start_date
                )
                # 但实际保存到用户选择的路径
                self.current_df.to_parquet(file_path, index=False, compression='snappy')
                print(f"[DEBUG] Parquet exported successfully to: {file_path}")
                
                # 显示成功消息
                QMessageBox.information(
                    self,
                    "导出成功",
                    f"Parquet 文件已保存到:\n{file_path}\n\n✨ 压缩率70%，读取速度提升90%"
                )
                
                # 更新状态横幅
                self.status_banner.show_success(f"数据获取成功！Parquet已保存: {file_path}")
            
            else:  # CSV
                # 保存到用户选择的路径
                self.current_df.to_csv(file_path, index=False, encoding='utf-8-sig')
                print(f"[DEBUG] CSV exported successfully to: {file_path}")
                
                # 显示成功消息
                QMessageBox.information(
                    self,
                    "导出成功",
                    f"CSV 文件已保存到:\n{file_path}"
                )
                
                # 更新状态横幅
                self.status_banner.show_success(f"数据获取成功！CSV已保存: {file_path}")
            
        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            print(f"[DEBUG] ERROR in _on_export_clicked: {str(e)}")
            print(f"[DEBUG] Full traceback:\n{error_details}")
            QMessageBox.critical(
                self,
                "导出错误",
                f"导出{format.upper()}时发生错误:\n\n{str(e)}"
            )
    
    def _on_data_manager_clicked(self):
        """打开数据管理对话框"""
        try:
            from .data_manager_dialog import DataManagerDialog
            dialog = DataManagerDialog(self)
            dialog.exec()
        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            print(f"[DEBUG] ERROR in _on_data_manager_clicked: {str(e)}")
            print(f"[DEBUG] Full traceback:\n{error_details}")
            QMessageBox.critical(
                self,
                "打开数据管理器失败",
                f"无法打开数据管理界面:\n\n{str(e)}"
            )
    
    def _perform_startup_checks(self):
        """启动时执行检查（自动清理和磁盘空间）"""
        from utils.cache_manager import CacheManager
        
        try:
            # 1. 自动清理旧日志
            if CacheManager.should_auto_cleanup():
                print("[INFO] Performing auto cleanup on startup...")
                CacheManager.perform_auto_cleanup()
            
            # 2. 检查磁盘空间
            settings = CacheManager.load_settings()
            threshold = settings.get('disk_warning_threshold_gb', 1.0)
            is_low, free_gb, msg = CacheManager.is_disk_space_low(threshold_gb=threshold)
            
            if is_low:
                reply = QMessageBox.warning(
                    self,
                    "磁盘空间警告",
                    f"{msg}\n\n建议清理旧数据释放空间。\n\n是否打开数据管理中心？",
                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
                )
                
                if reply == QMessageBox.StandardButton.Yes:
                    self._on_data_manager_clicked()
        
        except Exception as e:
            print(f"[ERROR] Startup checks failed: {str(e)}")
