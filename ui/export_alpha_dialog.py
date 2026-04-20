from PyQt6.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QLabel, 
                             QLineEdit, QRadioButton, QPushButton, QButtonGroup,
                             QFormLayout, QGroupBox, QMessageBox)
from PyQt6.QtCore import Qt

class ExportAlphaDialog(QDialog):
    """
    Wizard Dialog for exporting Alpha strategies (Baton Relay Package).
    Collects Strategy ID and Name, and the desired save destination.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Save Strategy Package")
        self.setMinimumWidth(400)
        self._init_ui()

    def _init_ui(self):
        layout = QVBoxLayout(self)

        # 1. Naming Group
        naming_group = QGroupBox("📄 命名设置 (Naming Settings)")
        form_layout = QFormLayout()

        self.default_id = "STG_001"
        self.default_name = "My_Alpha_Factor"

        self.id_input = QLineEdit()
        self.id_input.setPlaceholderText(self.default_id)
        self.id_input.setToolTip("Strategy ID (e.g. STG_001)")
        form_layout.addRow("Strategy ID:", self.id_input)

        self.name_input = QLineEdit()
        self.name_input.setPlaceholderText(self.default_name)
        self.name_input.setToolTip("Strategy Name")
        form_layout.addRow("Strategy Name:", self.name_input)

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
        stg_id = self.id_input.text().strip() or self.default_id
        stg_name = self.name_input.text().strip() or self.default_name

        mode_id = self.mode_group.checkedId()
        if mode_id == 0:
            save_mode = "data_center"
        elif mode_id == 1:
            save_mode = "local"
        else:
            save_mode = "both"

        return {
            "strategy_id": stg_id,
            "strategy_name": stg_name,
            "save_mode": save_mode
        }
