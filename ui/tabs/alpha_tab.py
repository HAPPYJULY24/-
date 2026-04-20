from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel, 
                             QPushButton, QComboBox, QPlainTextEdit, QGroupBox, 
                             QFormLayout, QDoubleSpinBox, QSplitter, 
                             QTableWidget, QTableWidgetItem, QHeaderView, QListWidget, QListWidgetItem,
                             QSizePolicy, QMessageBox, QScrollArea, QProgressBar, QApplication, QLineEdit, QTabWidget)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QFont, QColor, QCursor

import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
import os
from pathlib import Path

# Set backend to avoid issues
matplotlib.use('QtAgg')
plt.style.use('dark_background')  # Force dark mode friendly charts

from src.quant_bridge import AlphaEngine
from ui.widgets.alpha_charts import AlphaCharts
from utils.cache_manager import CacheManager
from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel, 
                             QPushButton, QComboBox, QPlainTextEdit, QGroupBox, 
                             QFormLayout, QDoubleSpinBox, QSplitter, 
                             QTableWidget, QTableWidgetItem, QHeaderView, QListWidget, QListWidgetItem,
                             QSizePolicy, QMessageBox, QScrollArea, QProgressBar, QApplication, QLineEdit, QTabWidget, QCheckBox)



class AlphaWorker(QThread):
    """
    Worker thread for running the Alpha Engine pipeline (V3.0).
    """
    finished = pyqtSignal(object)  # Emits result dict
    error = pyqtSignal(str)
    log = pyqtSignal(str)
    
    def __init__(
        self, 
        data_path: str, 
        expression: str, 
        config: dict, 
        periods: list
    ) -> None:
        """
        Initialize worker with alpha pipeline parameters.
        
        Args:
            data_path: Path to data file (CSV or Parquet)
            expression: Alpha factor expression
            config: Configuration dictionary
            periods: List of holding periods to analyze
        """
        super().__init__()
        self.data_path: str = data_path
        self.expression: str = expression
        self.config: dict = config
        self.periods: list = periods
        
    def run(self) -> None:
        """Execute alpha pipeline in background thread."""
        try:
            self.log.emit(f"Loading data from {os.path.basename(self.data_path)}...")
            if self.data_path.endswith('.parquet'):
                df = pd.read_parquet(self.data_path)
            else:
                df = pd.read_csv(self.data_path)
            
            # --- Column Normalization (Case-Insensitive) ---
            # Lowercase all columns to ensure consistency with UI display
            df.columns = [str(c).lower() for c in df.columns]

            # Map common names to standard lowercase
            col_map = {
                'last': 'close', 'price': 'close',
                'vol': 'volume',
                'date': 'datetime', 'time': 'datetime' 
            }
            df.rename(columns=col_map, inplace=True)
            
            # Duplicate suffixed OHLCV names only when there is a single candidate.
            # Multi-asset aligned files must use the explicit Target Return column.
            close_candidates = [c for c in df.columns if c.endswith(('_close', '.close', ' close'))]
            open_candidates = [c for c in df.columns if c.endswith(('_open', '.open', ' open'))]
            high_candidates = [c for c in df.columns if c.endswith(('_high', '.high', ' high'))]
            low_candidates = [c for c in df.columns if c.endswith(('_low', '.low', ' low'))]
            volume_candidates = [c for c in df.columns if c.endswith(('_volume', '.volume', ' volume', '_vol', '.vol'))]
            for c in list(df.columns):
                if 'close' not in df.columns and len(close_candidates) == 1 and c == close_candidates[0]:
                    df['close'] = df[c]
                elif 'open' not in df.columns and len(open_candidates) == 1 and c == open_candidates[0]:
                    df['open'] = df[c]
                elif 'high' not in df.columns and len(high_candidates) == 1 and c == high_candidates[0]:
                    df['high'] = df[c]
                elif 'low' not in df.columns and len(low_candidates) == 1 and c == low_candidates[0]:
                    df['low'] = df[c]
                elif 'volume' not in df.columns and len(volume_candidates) == 1 and c == volume_candidates[0]:
                    df['volume'] = df[c]

            # --- Auto-Drop Zero Volume Rows ---
            if self.config.get('auto_drop_zero_vol', False):
                if 'volume' in df.columns:
                    initial_rows = len(df)
                    df = df[df['volume'] > 0].copy()
                    dropped = initial_rows - len(df)
                    if dropped > 0:
                        self.log.emit(f"Auto-Drop: Removed {dropped} rows with Volume=0.")
                else:
                    self.log.emit("Auto-Drop: 'volume' column not found, skipping filter.")
            
            # CRITICAL FIX: Ensure 'datetime' is available as a column for the engine
            if 'datetime' not in df.columns:
                # Check if index is datetime
                if isinstance(df.index, pd.DatetimeIndex):
                    self.log.emit("Resetting DatetimeIndex to 'datetime' column...")
                    df = df.reset_index()
                    col_name = df.columns[0]
                    for col in [col_name, 'Date', 'date', 'Time', 'time', 'index']:
                        if col in df.columns:
                            try:
                                if pd.api.types.is_datetime64_any_dtype(df[col]):
                                    df.rename(columns={col: 'datetime'}, inplace=True)
                                    break
                            except:
                                pass
                else:
                     # Check for string columns that look like date
                     for col in ['Date', 'date', 'Time', 'time']:
                         if col in df.columns:
                             self.log.emit(f"Converting '{col}' to datetime...")
                             try:
                                df[col] = pd.to_datetime(df[col])
                                df.rename(columns={col: 'datetime'}, inplace=True)
                                break
                             except:
                                pass

            if 'datetime' not in df.columns:
                self.log.emit("WARNING: No 'datetime' column found. Time-series IC will be flat global IC.")
            
            self.log.emit(f"Data ready: {len(df)} rows. Columns: {list(df.columns)}")
            
            self.engine = AlphaEngine()
            
            self.log.emit(f"Executing pipeline (Periods: {self.periods})...")
            if self.config.get('risk_factors'):
                try:
                    result = self.engine.process_pipeline(
                        df, 
                        self.expression, 
                        self.config,
                        periods=self.periods
                    )
                except KeyError as e:
                    # Catch Case-Sensitivity Discrepancy
                    # "Close" vs "close" mismatch
                    missing_key = str(e).strip("'")
                    raise ValueError(
                        f"列名未对齐，请确保使用小写 / Column name mismatch, use lowercase: {missing_key}\n"
                        f"Tip: System normalizes standard columns to lowercase (open, high, low, close, volume)."
                    ) from e
            else:
                 # Skip neutralization if no risk factors
                 result = self.engine.process_pipeline(
                        df, 
                        self.expression, 
                        self.config,
                        periods=self.periods
                    )    
            self.log.emit("Pipeline evaluation complete.")
            self.finished.emit(result)
            
        except Exception as e:
            import traceback
            self.error.emit(str(e) + "\n" + traceback.format_exc())

class AlphaTab(QWidget):
    """
    Tab for Alpha Research (Phase 3 - V3.0).
    """
    def __init__(self):
        super().__init__()
        self.init_ui()
        self.current_worker = None
        self.current_result = None # Store result for export
        
    def init_ui(self):
        main_layout = QHBoxLayout()
        main_layout.setContentsMargins(10, 10, 10, 10)
        
        # === Left Panel with ScrollArea ===
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setFixedWidth(350) # Compact left panel
        scroll_area.setFrameShape(QScrollArea.Shape.NoFrame)
        
        left_container = QWidget()
        left_layout = QVBoxLayout(left_container)
        left_layout.setContentsMargins(0, 0, 10, 0)
        
        left_splitter = QSplitter(Qt.Orientation.Vertical)
        left_layout.addWidget(left_splitter)
        
        # --- Top Widget: Data & Expression ---
        top_widget = QWidget()
        top_layout = QVBoxLayout(top_widget)
        top_layout.setContentsMargins(0, 0, 0, 0)
        
        # 1. Title
        title = QLabel("Alpha Lab (Ultimate)")
        title_font = QFont()
        title_font.setPointSize(14)
        title_font.setBold(True)
        title.setFont(title_font)
        top_layout.addWidget(title)
        
        # 2. Data Loader
        data_group = QGroupBox("1. Data Source")
        data_layout = QFormLayout()
        
        self.data_combo = QComboBox()
        self.refresh_data_files()
        self.data_combo.currentIndexChanged.connect(self._on_data_selected)
        
        # Horizontal layout for File Select + Refresh
        file_h_layout = QHBoxLayout()
        file_h_layout.addWidget(self.data_combo)
        
        self.refresh_btn = QPushButton("🔄 刷新列表")
        self.refresh_btn.setToolTip("Reload file list from disk")
        self.refresh_btn.clicked.connect(self.refresh_data_files)
        # self.refresh_btn.setFixedWidth(80) 
        file_h_layout.addWidget(self.refresh_btn)
        
        data_layout.addRow("File:", file_h_layout)
        
        # New: Info Label for Data Audit
        self.info_label = QLabel("Ready")
        self.info_label.setStyleSheet("color: gray; font-size: 10px;")
        self.info_label.setWordWrap(True)
        data_layout.addRow("", self.info_label)

        self.target_return_combo = QComboBox()
        self.target_return_combo.setToolTip("Price column used to calculate forward returns and export backtest close.")
        data_layout.addRow("Target Return:", self.target_return_combo)

        self.only_overlap_chk = QCheckBox("Only evaluate overlap rows")
        self.only_overlap_chk.setToolTip("When is_overlap exists, evaluate alpha only on rows where all aligned markets overlap.")
        self.only_overlap_chk.setChecked(False)
        data_layout.addRow("", self.only_overlap_chk)
        
        data_group.setLayout(data_layout)
        top_layout.addWidget(data_group)
        
        # 3. Factor Expression
        expr_group = QGroupBox("2. Factor Expression")
        expr_layout = QVBoxLayout()
        expr_label = QLabel("Example: df['factor'] = df['close'] / df['open']")
        expr_label.setStyleSheet("color: gray; font-size: 10px;")
        expr_layout.addWidget(expr_label)
        
        self.expression_input = QPlainTextEdit()
        self.expression_input.setPlaceholderText("Enter python code...")
        self.expression_input.setMinimumHeight(100)
        expr_layout.addWidget(self.expression_input)
        
        expr_group.setLayout(expr_layout)
        top_layout.addWidget(expr_group)
        
        left_splitter.addWidget(top_widget)
        
        # --- Bottom Widget: Preproc, Neut, Run ---
        bottom_widget = QWidget()
        bottom_layout = QVBoxLayout(bottom_widget)
        bottom_layout.setContentsMargins(0, 10, 0, 0)
        
        # 4. Preprocessing
        prep_group = QGroupBox("3. Preprocessing")
        prep_layout = QFormLayout()
        
        self.method_combo = QComboBox()
        self.method_combo.addItems(["3-Sigma", "MAD", "Quantile"])
        prep_layout.addRow("Method:", self.method_combo)
        
        self.quantile_lb = QDoubleSpinBox()
        self.quantile_lb.setRange(0, 1)
        self.quantile_lb.setValue(0.01)
        self.quantile_lb.setSingleStep(0.01)
        prep_layout.addRow("Quantile LB:", self.quantile_lb)

        self.quantile_ub = QDoubleSpinBox()
        self.quantile_ub.setRange(0, 1)
        self.quantile_ub.setValue(0.99)
        self.quantile_ub.setSingleStep(0.01)
        prep_layout.addRow("Quantile UB:", self.quantile_ub)
        
        # New: Auto-Drop Checkbox
        self.auto_drop_chk = QCheckBox("Auto-drop Zero Volume Rows")
        self.auto_drop_chk.setToolTip("Automatically remove rows where Volume is 0 (e.g. holidays or non-trading days)")
        self.auto_drop_chk.setChecked(False) # Default off, logic will set it
        prep_layout.addRow("", self.auto_drop_chk)
        
        prep_group.setLayout(prep_layout)
        bottom_layout.addWidget(prep_group)

        # 5. Neutralization
        neut_group = QGroupBox("4. Neutralization")
        neut_layout = QVBoxLayout()
        
        self.risk_list = QListWidget() 
        self.risk_list.setMinimumHeight(100) 
        self.risk_list.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        neut_layout.addWidget(QLabel("Select Risk Factors:"))
        neut_layout.addWidget(self.risk_list)
        
        ridge_layout = QHBoxLayout()
        ridge_layout.addWidget(QLabel("Ridge Alpha:"))
        self.ridge_alpha = QDoubleSpinBox()
        self.ridge_alpha.setRange(0, 10)
        self.ridge_alpha.setValue(1.0)
        ridge_layout.addWidget(self.ridge_alpha)
        neut_layout.addLayout(ridge_layout)
        
        neut_group.setLayout(neut_layout)
        bottom_layout.addWidget(neut_group)
        
        # 6. Evaluation Config (V3.0)
        eval_group = QGroupBox("5. Evaluation Config")
        eval_layout = QFormLayout()
        
        self.periods_input = QLineEdit("1, 3, 5, 10, 20")
        self.periods_input.setPlaceholderText("e.g. 1, 5, 10")
        eval_layout.addRow("Periods:", self.periods_input)
        
        eval_group.setLayout(eval_layout)
        bottom_layout.addWidget(eval_group)

        # 7. Execute Button & Progress
        btn_layout = QHBoxLayout()
        
        self.run_button = QPushButton("🚀 运行分析")
        self.run_button.setMinimumHeight(45)
        self.run_button.setStyleSheet("background-color: #4CAF50; color: white; font-weight: bold;")
        self.run_button.clicked.connect(self._run_pipeline)
        btn_layout.addWidget(self.run_button)
        
        self.export_button = QPushButton("💾 Export to Backtest")
        self.export_button.setMinimumHeight(45)
        self.export_button.setStyleSheet("background-color: #2196F3; color: white; font-weight: bold;")
        self.export_button.setEnabled(False)
        self.export_button.clicked.connect(self._export_signal)
        btn_layout.addWidget(self.export_button)

        # New: Save Signal Button
        self.save_signal_button = QPushButton("💾 保存信号")
        self.save_signal_button.setMinimumHeight(45)
        self.save_signal_button.setStyleSheet("background-color: #9C27B0; color: white; font-weight: bold;")
        self.save_signal_button.setEnabled(False)
        self.save_signal_button.clicked.connect(self._save_signal_to_db)
        btn_layout.addWidget(self.save_signal_button)
        
        bottom_layout.addLayout(btn_layout)
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 0) # Indeterminate
        self.progress_bar.setVisible(False)
        self.progress_bar.setStyleSheet("QProgressBar { border: 2px solid grey; border-radius: 5px; text-align: center; } QProgressBar::chunk { background-color: #05B8CC; width: 20px; }")
        bottom_layout.addWidget(self.progress_bar)
        
        # 8. Log
        log_group = QGroupBox("Logs")
        log_layout = QVBoxLayout()
        self.log_view = QPlainTextEdit()
        self.log_view.setReadOnly(True)
        self.log_view.setMinimumHeight(80)
        log_layout.addWidget(self.log_view)
        log_group.setLayout(log_layout)
        bottom_layout.addWidget(log_group)
        
        left_splitter.addWidget(bottom_widget)
        scroll_area.setWidget(left_container)
        scroll_area.setMinimumWidth(450) # Force a minimum width to prevent UI clipping
        
        # === Right Panel: Charts Widget (Phase 5B.3: Extracted) ===
        self.charts = AlphaCharts()
        
        # === Metrics KPI Panel (BUG-1 fix: was dead code, now wired up) ===
        self._setup_metrics_panel()
        
        # === Main Layout: Split view (Left Controls + Right Charts) ===
        main_splitter = QSplitter(Qt.Orientation.Horizontal)
        main_splitter.addWidget(scroll_area)  # Left panel
        main_splitter.addWidget(self.charts)  # Right panel (extracted widget)
        main_splitter.setStretchFactor(0, 2)  # Left panel smaller
        main_splitter.setStretchFactor(1, 3)  # Right panel larger (charts)
        
        main_layout = QHBoxLayout()
        main_layout.addWidget(main_splitter)
        self.setLayout(main_layout)
        
        # Initial populate manually
        self._on_data_selected(0)

    def refresh_data_files(self):
        """Scan data/processed/ AND Master DB for parquet files."""
        self.data_combo.clear()
        
        # 1. Processed Data (Aligned)
        processed_dir = Path("data/processed")
        if processed_dir.exists():
            files = list(processed_dir.glob("*.parquet"))
            for f in files:
                self.data_combo.addItem(f"[Processed] {f.name}", str(f.absolute()))
                
        # 2. Master DB Data (Raw)
        try:
            # Use CacheManager to get info (optimized recursive)
            master_files, _, _ = CacheManager.get_master_db_info()
            for info in master_files:
                display_name = f"[Master] {info['code']} ({info['timeframe']})"
                # Store absolute path
                self.data_combo.addItem(display_name, info['filepath'])
        except Exception as e:
            print(f"Error loading Master DB files: {e}")
            
        if self.data_combo.count() == 0:
            self.data_combo.addItem("No data found")
            if hasattr(self, 'run_button'):
                self.run_button.setEnabled(False)

    @staticmethod
    def _infer_timeframe_from_text(text: str) -> str:
        text = str(text).lower()
        if any(token in text for token in ['1d', 'daily', '(d)', '(day)']):
            return 'daily'
        if any(token in text for token in ['1m', '3m', '5m', '15m', '30m', '60m', 'minute', 'min']):
            return 'intraday'
        return 'unknown'

    @staticmethod
    def _is_price_candidate(col: str) -> bool:
        col = str(col).lower()
        return col in {'close', 'last', 'price'} or col.endswith(('_close', '.close', ' close'))

    def _checked_risk_factors(self):
        factors = []
        for i in range(self.risk_list.count()):
            item = self.risk_list.item(i)
            if item.checkState() == Qt.CheckState.Checked:
                factors.append(item.data(Qt.ItemDataRole.UserRole) or item.text())
        return factors

    def _on_data_selected(self, index):
        """Load columns and audit data."""
        filepath = self.data_combo.currentData() # Get from UserRole
        
        if not filepath or filepath == "No data found":
            self.run_button.setEnabled(False)
            self.info_label.setText("")
            self.target_return_combo.clear()
            self.only_overlap_chk.setChecked(False)
            self.only_overlap_chk.setEnabled(False)
            return
            
        path = Path(filepath)
        if not path.exists():
            self.info_label.setText(f"File not found: {path.name}")
            return

        try:
            # 1. Column Loading (Full Read for columns only? Or use pyarrow schema)
            import pyarrow.parquet as pq
            
            # Read schema for columns
            parquet_file = pq.ParquetFile(path)
            schema_names = parquet_file.schema.names
            num_rows = parquet_file.metadata.num_rows
            
            # Filter non-feature columns and normalize to lowercase for display
            # BUG-4 fix: also exclude non-numeric, _symbol suffix, and near-constant columns
            exclude = ['datetime', 'symbol', 'date', 'time', 'factor', 'next_ret', 'index']
            
            # Build a type-aware column list using pyarrow schema
            import pyarrow as pa
            schema = parquet_file.schema_arrow
            
            valid_columns = []
            for field in schema:
                col_lower = field.name.lower()
                # Skip system/reserved columns
                if col_lower in exclude:
                    continue
                # Skip _symbol suffix identifier columns (e.g., zl1!_symbol, myx-fcpo1!_symbol)
                if col_lower.endswith('_symbol'):
                    continue
                # Skip non-numeric types (strings, booleans, binary)
                if not pa.types.is_integer(field.type) and not pa.types.is_floating(field.type):
                    continue
                valid_columns.append(col_lower)
            
            valid_columns = sorted(list(set(valid_columns)))

            price_candidates = [c for c in valid_columns if self._is_price_candidate(c)]
            self.target_return_combo.clear()
            if price_candidates:
                ordered_prices = sorted(
                    price_candidates,
                    key=lambda c: (c not in {'close', 'last', 'price'}, c)
                )
                for col in ordered_prices:
                    self.target_return_combo.addItem(col, col)
                self.target_return_combo.setEnabled(True)
            else:
                self.target_return_combo.addItem("No price column found", "")
                self.target_return_combo.setEnabled(False)
            
            # Detect near-constant columns (variance ≈ 0) by reading a small sample
            near_constant_cols = set()
            if valid_columns:
                try:
                    sample_df = pd.read_parquet(path, columns=[
                        c for c in schema_names if c.lower() in valid_columns
                    ])
                    for col in sample_df.columns:
                        col_std = sample_df[col].std()
                        if col_std is not None and (pd.isna(col_std) or col_std < 1e-10):
                            near_constant_cols.add(col.lower())
                except Exception:
                    pass  # If sample read fails, skip constant detection
            
            self.risk_list.clear()
            for col in valid_columns:
                if col in near_constant_cols:
                    item = QListWidgetItem(f"⚠️ {col} (常量)")
                    item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
                    item.setCheckState(Qt.CheckState.Unchecked)
                    item.setToolTip("Near-constant column — neutralization has no effect")
                    item.setForeground(QColor("#888"))
                    item.setData(Qt.ItemDataRole.UserRole, col)
                else:
                    item = QListWidgetItem(col)
                    item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
                    item.setCheckState(Qt.CheckState.Unchecked)
                    item.setData(Qt.ItemDataRole.UserRole, col)
                self.risk_list.addItem(item)
                
            self.run_button.setEnabled(True)
            self._log(f"Loaded {len(valid_columns)} numeric columns from {path.name} (excluded {len(schema_names) - len(valid_columns) - len(exclude)} non-numeric/system columns)")
            
            # 2. Data Audit (Optimized) & Mode Detection
            is_master = "[Master]" in self.data_combo.currentText()
            is_processed = "[Processed]" in self.data_combo.currentText()
            
            mode_text = ""
            mode_color = "gray"
            
            if is_master:
                mode_text = "🔒 Mode: Single Asset"
                mode_color = "#4CAF50" # Green
                self.auto_drop_chk.setChecked(self._infer_timeframe_from_text(self.data_combo.currentText()) == 'daily')
            elif is_processed:
                mode_text = "🔗 Mode: Aligned / Multi-Asset"
                mode_color = "#2196F3" # Blue
                self.auto_drop_chk.setChecked(False) # Default OFF for Aligned (presumably clean)

            has_overlap = any(c.lower() == 'is_overlap' for c in schema_names)
            self.only_overlap_chk.setEnabled(has_overlap)
            self.only_overlap_chk.setChecked(bool(has_overlap and is_processed))
                
            # Audit Volume (Partial Read)
            # Only read 'Volume' column if it exists to check for 0s
            vol_msg = ""
            try:
                # Check if volume column exists (case-insensitive)
                vol_col = next((c for c in schema_names if c.lower() == 'volume'), None)
                if vol_col:
                    # Read only volume column
                    df_vol = pd.read_parquet(path, columns=[vol_col])
                    zero_vol_count = (df_vol[vol_col] == 0).sum()
                    
                    if zero_vol_count > 0:
                        vol_msg = f"<br><span style='color:orange'>⚠️ Contains {zero_vol_count} rows with Volume=0. Auto-drop recommended.</span>"
                    else:
                        vol_msg = "<br><span style='color:green'>✔ Volume check passed.</span>"
            except Exception as e:
                vol_msg = f"<br>Volume check error: {str(e)}"

            # Get Date Range (from metadata statistics if available, or reading columns)
            # Pyarrow metadata stats might have min/max for columns
            # Try to read 'Date' or index column
            date_range_str = ""
            try:
                # Try to find date col
                date_col = next((c for c in schema_names if c.lower() in ['date', 'datetime', 'time']), None)
                if date_col:
                     # Reading full date column is fast enough? 
                     # Let's try to read min/max from statistics if available (often not written)
                     # Fallback to reading column
                     df_date = pd.read_parquet(path, columns=[date_col])
                     min_date = df_date[date_col].min()
                     max_date = df_date[date_col].max()
                     date_range_str = f"{pd.to_datetime(min_date).date()} ~ {pd.to_datetime(max_date).date()}"
            except:
                pass
                
            self.info_label.setText(
                f"<b>{mode_text}</b><br>"
                f"Rows: {num_rows} | Date: {date_range_str}"
                f"{vol_msg}"
            )
            self.info_label.setStyleSheet(f"border-left: 3px solid {mode_color}; padding-left: 5px; color: #ccc;")
            
        except Exception as e:
            self._log(f"Error reading {path.name}: {e}")
            self.info_label.setText(f"Error: {str(e)}")

    def _log(self, message):
        self.log_view.appendPlainText(f"[{pd.Timestamp.now().strftime('%H:%M:%S')}] {message}")

    def _set_loading_state(self, is_loading):
        """Toggle UI state during execution."""
        if is_loading:
            self.run_button.setEnabled(False)
            self.run_button.setText("⏳ Calculating...")
            self.run_button.setStyleSheet("background-color: #FFA500; color: white; font-weight: bold;")
            self.progress_bar.setVisible(True)
            QApplication.setOverrideCursor(QCursor(Qt.CursorShape.WaitCursor))
        else:
            self.run_button.setEnabled(True)
            self.run_button.setText("🚀 运行分析")
            self.run_button.setStyleSheet("background-color: #4CAF50; color: white; font-weight: bold;")
            self.progress_bar.setVisible(False)
            QApplication.restoreOverrideCursor()

    def _run_pipeline(self):
        # 1. Validation
        expression = self.expression_input.toPlainText().strip()
        if not expression:
            QMessageBox.warning(self, "Input Error", "Please enter a factor expression.")
            return
        if "df['factor']" not in expression and 'df["factor"]' not in expression:
             QMessageBox.warning(self, "Input Error", "Expression must assign to df['factor'].\nExample: df['factor'] = ...")
             return
             
        data_file_path = self.data_combo.currentData()
        if not data_file_path:
            return
        data_path = str(data_file_path)
        
        # Parse Periods
        periods_str = self.periods_input.text()
        try:
            periods = sorted({int(p.strip()) for p in periods_str.split(',') if p.strip()})
            if not periods or any(p <= 0 for p in periods):
                raise ValueError
        except:
             QMessageBox.warning(self, "Input Error", "Periods must be positive integers (e.g. 1, 5, 10).")
             return

        q_lb = self.quantile_lb.value()
        q_ub = self.quantile_ub.value()
        if not (0 <= q_lb < q_ub <= 1):
            QMessageBox.warning(self, "Input Error", "Quantile bounds must satisfy 0 <= LB < UB <= 1.")
            return

        target_return_col = self.target_return_combo.currentData()
        if not target_return_col:
            QMessageBox.warning(self, "Input Error", "Please select a Target Return price column.")
            return
        
        # 2. Config Construction
        risk_factors = self._checked_risk_factors()
        
        config = {
            'winsor_method': self.method_combo.currentText(),
            'quantile_lb': q_lb,
            'quantile_ub': q_ub,
            'risk_factors': risk_factors,
            'ridge_alpha': self.ridge_alpha.value(),
            'auto_drop_zero_vol': self.auto_drop_chk.isChecked(), # Pass config
            'target_return_col': target_return_col,
            'only_overlap': self.only_overlap_chk.isChecked()
        }
        
        # 3. Execution
        self.log_view.clear()
        self._log(f"Starting pipeline with periods {periods}...")
        self._log(f"Target return column: {target_return_col}")
        self._set_loading_state(True)
        self.export_button.setEnabled(False)
        self.save_signal_button.setEnabled(False)
        self.current_result = None
        
        self.current_worker = AlphaWorker(data_path, expression, config, periods)
        self.current_worker.log.connect(self._log)
        self.current_worker.error.connect(self._on_worker_error)
        self.current_worker.finished.connect(self._on_worker_finished)
        self.current_worker.start()

    def _on_worker_error(self, message):
        self._set_loading_state(False)
        self._log(f"ERROR: {message}")
        QMessageBox.critical(self, "Execution Error", message)

    def _on_worker_finished(self, result):
        """Handle worker completion."""
        # Robust Cleanup: Wait for the thread to fully exit its C++ loop before destroying
        if hasattr(self, 'current_worker') and self.current_worker:
            self.current_worker.wait()
            self.current_worker.deleteLater()
            self.current_worker = None
            
        self._log("Processing complete. Rendering charts (This may take a moment)...")
        self.current_result = result
        self._log(f"Target return column used: {result.get('target_return_col', 'N/A')}")

        ic_decay = result.get('ic_decay_table', pd.DataFrame())
        self.period_combo.blockSignals(True)
        self.period_combo.clear()
        if not ic_decay.empty:
            for p in sorted(ic_decay.index.tolist()):
                self.period_combo.addItem(str(p))
            self.period_combo.setCurrentIndex(0)
        self.period_combo.blockSignals(False)
        
        # Pump events to refresh the UI text
        QApplication.processEvents()
        
        # Update charts using the new widget
        self._update_charts(result)
        
        # Update KPI Metrics Table (BUG-1 fix: was dead code, now wired up)
        self._update_metrics_table_view()
        
        # Turn off loading state (single call — BUG-8 fix)
        self._set_loading_state(False)
        self.export_button.setEnabled(True)
        self.save_signal_button.setEnabled(True)
        
        QMessageBox.information(self, "Success", "Alpha pipeline executed successfully!")

    def _setup_metrics_panel(self):
        """Create the Metrics KPI table and period selector inside the Charts widget. (BUG-1 fix)"""
        from PyQt6.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QLabel, QComboBox, QTableWidget, QTableWidgetItem, QHeaderView
        
        metrics_tab = QWidget()
        metrics_layout = QVBoxLayout(metrics_tab)
        metrics_layout.setContentsMargins(5, 5, 5, 5)
        
        # Period selector
        selector_layout = QHBoxLayout()
        selector_layout.addWidget(QLabel("Period:"))
        self.period_combo = QComboBox()
        self.period_combo.setMinimumWidth(80)
        self.period_combo.currentIndexChanged.connect(lambda: self._update_metrics_table_view())
        selector_layout.addWidget(self.period_combo)
        selector_layout.addStretch()
        metrics_layout.addLayout(selector_layout)
        
        # KPI Table
        self.metrics_table = QTableWidget()
        self.metrics_table.setColumnCount(4)
        self.metrics_table.setHorizontalHeaderLabels(["Metric", "Value", "Description", "Rating"])
        self.metrics_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.metrics_table.verticalHeader().setVisible(False)
        self.metrics_table.setAlternatingRowColors(True)
        self.metrics_table.setStyleSheet(
            "QTableWidget { gridline-color: #444; }"
            "QTableWidget::item { padding: 4px; }"
        )
        metrics_layout.addWidget(self.metrics_table)
        
        # Insert as first tab in charts widget for maximum visibility
        self.charts.insertTab(0, metrics_tab, "📊 Metrics KPI")
        self.charts.setCurrentIndex(0)

    def _update_metrics_table_view(self):
        """Populate Metrics KPI table from the current result. (BUG-1 fix: was dead code)"""
        if not self.current_result: return
        
        ic_decay = self.current_result.get('ic_decay_table', pd.DataFrame())
        metrics_v1 = self.current_result.get('metrics', {})
        
        # Populate period combo if empty
        if self.period_combo.count() == 0 and not ic_decay.empty:
            for p in ic_decay.index:
                self.period_combo.addItem(str(p))
        
        current_p_text = self.period_combo.currentText()
        if not current_p_text and not ic_decay.empty:
             current_p_text = str(ic_decay.index[0])
             
        # Prepare Rows
        rows = []
        
        if current_p_text and not ic_decay.empty:
            try:
                period = int(current_p_text)
                row_data = ic_decay.loc[period]
                
                rows.append(('Rank IC Mean', row_data['Rank IC']))
                rows.append(('ICIR', row_data['ICIR']))
                rows.append(('T-Stat', row_data['T-Stat']))
                rows.append(('P-Value', row_data['P-Value']))
                rows.append(('Sample N', row_data['N']))
                
                if metrics_v1:
                     rows.insert(2, ('Win Rate', metrics_v1.get('Win Rate', 0)))

            except Exception as e:
                self._log(f"Error parse metrics: {e}")
        else:
            for k, v in metrics_v1.items():
                rows.append((k, v))
                
        self.metrics_table.setRowCount(len(rows))
        
        for i, (k, v) in enumerate(rows):
            self.metrics_table.setItem(i, 0, QTableWidgetItem(k))
            
            # Value Formatting
            if pd.isna(v):
                value_str = "N/A"
            else:
                if k == 'Win Rate': value_str = f"{v*100:.1f}%"
                elif k == 'T-Stat': value_str = f"{v:.2f}"
                elif k == 'Sample N': value_str = f"{int(v)}"
                elif k == 'P-Value': value_str = f"{v:.4e}" if v < 0.001 else f"{v:.4f}"
                else: value_str = f"{v:.4f}"
            
            self.metrics_table.setItem(i, 1, QTableWidgetItem(value_str))
            
            # Description Logic
            description = ""
            rating = "Neutral"
            bg_color = None
            text_color = None
            
            if k == 'Rank IC Mean':
                if abs(v) > 0.05:
                    description = "Strong Signal"
                    rating = "Strong"
                    bg_color = QColor(50, 100, 50) if v > 0 else QColor(100, 50, 50) 
                else:
                    description = "Weak Signal"
            elif k == 'ICIR':
                if abs(v) > 1.0: 
                    description = "Very Stable"
                    rating = "Excellent"
                    bg_color = QColor(50, 100, 50)
                elif abs(v) > 0.5:
                    description = "Stable"
            elif k == 'P-Value':
                if v < 0.05:
                    description = "Significant (<0.05)"
                    rating = "Pass"
                    bg_color = QColor(50, 100, 50)
                else:
                    description = "Not Significant"
                    text_color = QColor("gray")
            elif k == 'Win Rate':
                if v > 0.55:
                    description = "Consistent"
                    rating = "Good"
                    bg_color = QColor(50, 100, 50)
                elif v > 0.45:
                    description = "Average"
                else:
                    description = "Unstable"
                    text_color = QColor("#FF9800")
            
            self.metrics_table.setItem(i, 2, QTableWidgetItem(description))
            
            rating_item = QTableWidgetItem(rating)
            if bg_color:
                rating_item.setBackground(bg_color)
                rating_item.setForeground(QColor("white"))
            elif text_color:
                rating_item.setForeground(text_color)
            self.metrics_table.setItem(i, 3, rating_item)

    def _update_charts(self, result):
        """
        Update charts with analysis results (Phase 5B.3: Using extracted widget).
        
        Args:
            result: Dictionary containing analysis results from AlphaEngine
        """
        # Update charts using the new widget (Convenience method)
        self.charts.update_all_charts(result)
        
        self.log_view.appendPlainText("[INFO] Charts updated successfully.")

    def _save_signal_to_db(self):
        """Save factor signal and DRAFT JSON config (Baton Relay Package)"""
        if not self.current_result: 
             QMessageBox.warning(self, "Error", "No results to save. Please run the pipeline first.")
             return
        
        df = self.current_result.get('signal_df', pd.DataFrame())
        if df.empty: 
             QMessageBox.warning(self, "Error", "Result dataframe is empty.")
             return

        from ui.export_alpha_dialog import ExportAlphaDialog
        from PyQt6.QtWidgets import QFileDialog
        
        dialog = ExportAlphaDialog(self)
        if dialog.exec() != dialog.DialogCode.Accepted:
            return
            
        export_data = dialog.get_export_data()
        stg_id = export_data["strategy_id"]
        stg_name = export_data["strategy_name"]
        save_mode = export_data["save_mode"]
        
        # Determine local_dir if needed
        local_dir_path = None
        if save_mode in ['local', 'both']:
            local_dir_path = QFileDialog.getExistingDirectory(
                self, "Select Local Export Directory", "", QFileDialog.Option.ShowDirsOnly
            )
            if not local_dir_path:
                return

        folder_name = f"{stg_id}_{stg_name}"
        parquet_filename = f"{stg_id}_data.parquet"
        json_filename = f"{stg_id}_config.json"
        
        try:
            # 1. 导出 Parquet 信号文件
            save_df = AlphaEngine.prepare_signal_export(df)
            if save_df.empty:
                raise ValueError("Resulting signal is empty after dropping warm-up period.")
            
            # 2. 收集环境与流水线上下文
            universe = "unknown"
            timeframe = "unknown"
            data_text = self.data_combo.currentText()
            if "]" in data_text:
                universe = data_text.split("]")[1].strip()  # 简单提取
            
            expr = self.expression_input.toPlainText().strip()
            winsor = self.method_combo.currentText()
            qlb = self.quantile_lb.value()
            qub = self.quantile_ub.value()
            risk_factors = self._checked_risk_factors()
            ridge_alpha = self.ridge_alpha.value()
            auto_drop = self.auto_drop_chk.isChecked()
            
            # 3. 构造接力棒 JSON DRAFT 载体
            from src.core.models.strategy_config import StrategyConfig, StrategyMetadata, EnvironmentConfig, AlphaPipelineConfig
            
            stg_config = StrategyConfig(
                metadata=StrategyMetadata(strategy_id=stg_id, strategy_name=stg_name),
                environment_config=EnvironmentConfig(universe=universe, timeframe=timeframe),
                alpha_pipeline=AlphaPipelineConfig(
                    expression=expr,
                    winsor_method=winsor,
                    quantile_lb=qlb,
                    quantile_ub=qub,
                    risk_factors=risk_factors,
                    ridge_alpha=ridge_alpha,
                    auto_drop_zero_vol=auto_drop
                )
            )
            
            paths_created = []
            
            # Save to Data Center (which physically goes to DataCenter/Alpha_data)
            dc_base = None
            if save_mode in ['data_center', 'both']:
                dc_base = Path("DataCenter/Alpha_data") / folder_name
                dc_base.mkdir(parents=True, exist_ok=True)
                save_df.to_parquet(dc_base / parquet_filename, index=False)
                stg_config.to_json(str(dc_base / json_filename))
                paths_created.append(f"Data Center (Alpha):\n- {dc_base / parquet_filename}")
                
            # Save to Local
            if save_mode in ['local', 'both']:
                local_base = Path(local_dir_path) / folder_name
                local_base.mkdir(parents=True, exist_ok=True)
                save_df.to_parquet(local_base / parquet_filename, index=False)
                stg_config.to_json(str(local_base / json_filename))
                paths_created.append(f"Local:\n- {local_base / parquet_filename}")
            
            msg = "\n\n".join(paths_created)
            QMessageBox.information(self, "Success", f"Baton Relay Package Saved Successfully!\n\n{msg}")
                
        except Exception as e:
            import traceback
            QMessageBox.critical(self, "Save Error", f"Could not package strategy:\n{str(e)}\n\n{traceback.format_exc()}")

    def _export_signal(self):
        if not self.current_result: return
        df = self.current_result.get('signal_df', pd.DataFrame())
        if df.empty: return
        
        # Use existing exported_data/backtest as default dir
        default_dir = Path("exported_data/backtest")
        default_dir.mkdir(parents=True, exist_ok=True)
        default_filename = f"alpha_signal_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        
        from PyQt6.QtWidgets import QFileDialog
        file_path, _ = QFileDialog.getSaveFileName(
            self,
            "Export Signal to Backtest",
            str(default_dir / default_filename),
            "Parquet Files (*.parquet);;All Files (*)"
        )

        if not file_path:
            return
            
        try:
            export_df = AlphaEngine.prepare_signal_export(df)
            export_df.to_parquet(file_path, index=False)
            QMessageBox.information(self, "Export Success", f"Signal exported to:\n{file_path}\nRows: {len(export_df)}\nFormat: Parquet")
        except Exception as e:
             QMessageBox.critical(self, "Export Error", str(e))
