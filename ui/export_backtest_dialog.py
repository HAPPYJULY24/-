from PyQt6.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QLabel, 
                             QLineEdit, QRadioButton, QPushButton, QButtonGroup,
                             QFormLayout, QGroupBox, QMessageBox)
from PyQt6.QtCore import Qt
from datetime import datetime

class ExportBacktestDialog(QDialog):
    """
    Wizard Dialog for exporting backtest results.
    Collects folder/file names and the desired save destination.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Export Backtest Info")
        self.setMinimumWidth(400)
        self._init_ui()

    def _init_ui(self):
        layout = QVBoxLayout(self)

        # 1. Naming Group
        naming_group = QGroupBox("📄 命名设置 (Naming Settings)")
        form_layout = QFormLayout()

        # Generate timestamps for placeholders
        current_time = datetime.now()
        self.default_folder = f"backtest_{current_time.strftime('%Y%m%d_%H%M%S')}"
        self.default_trade_log = "trade_log"
        self.default_dna = "strategy_dna"

        self.folder_input = QLineEdit()
        self.folder_input.setPlaceholderText(self.default_folder)
        self.folder_input.setToolTip("Leave empty to use the auto-generated timestamp name")
        form_layout.addRow("Folder Name:", self.folder_input)

        self.trade_log_input = QLineEdit()
        self.trade_log_input.setPlaceholderText(self.default_trade_log)
        self.trade_log_input.setToolTip("Base name only, extension will be added automatically")
        form_layout.addRow("Trade Log Name:", self.trade_log_input)

        self.dna_input = QLineEdit()
        self.dna_input.setPlaceholderText(self.default_dna)
        self.dna_input.setToolTip("Base name only, extension will be added automatically")
        form_layout.addRow("Strategy DNA Name:", self.dna_input)

        naming_group.setLayout(form_layout)
        layout.addWidget(naming_group)

        # 2. Save Mode Group
        mode_group = QGroupBox("📍 保存方式 (Save Destination)")
        mode_layout = QVBoxLayout()

        self.btn_data_center = QRadioButton("保存至数据中心 (Save to Data Center)")
        self.btn_data_center.setChecked(True)
        self.btn_local = QRadioButton("数据导出 (Local Export)")
        self.btn_both = QRadioButton("两者都保存 (Save Both)")

        self.mode_group = QButtonGroup(self)
        self.mode_group.addButton(self.btn_data_center, 0)
        self.mode_group.addButton(self.btn_local, 1)
        self.mode_group.addButton(self.btn_both, 2)

        mode_layout.addWidget(self.btn_data_center)
        mode_layout.addWidget(self.btn_local)
        mode_layout.addWidget(self.btn_both)

        mode_group.setLayout(mode_layout)
        layout.addWidget(mode_group)

        # 3. Buttons
        btn_layout = QHBoxLayout()
        self.btn_cancel = QPushButton("Cancel")
        self.btn_cancel.clicked.connect(self.reject)
        self.btn_export = QPushButton("Export")
        self.btn_export.setDefault(True)
        self.btn_export.setStyleSheet("background-color: #4CAF50; color: white; font-weight: bold;")
        self.btn_export.clicked.connect(self.accept)

        btn_layout.addStretch()
        btn_layout.addWidget(self.btn_cancel)
        btn_layout.addWidget(self.btn_export)

        layout.addLayout(btn_layout)

    def get_export_data(self) -> dict:
        """
        Returns the clean, fallback-applied inputs when the dialog is accepted.
        """
        folder_name = self.folder_input.text().strip() or self.default_folder
        trade_log_name = self.trade_log_input.text().strip() or self.default_trade_log
        dna_name = self.dna_input.text().strip() or self.default_dna

        mode_id = self.mode_group.checkedId()
        if mode_id == 0:
            save_mode = "data_center"
        elif mode_id == 1:
            save_mode = "local"
        else:
            save_mode = "both"

        return {
            "folder_name": folder_name,
            "trade_log_base": trade_log_name,
            "dna_base": dna_name,
            "save_mode": save_mode
        }
