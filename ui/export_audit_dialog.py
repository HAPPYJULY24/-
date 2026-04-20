from PyQt6.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QLabel,
                             QLineEdit, QRadioButton, QPushButton, QButtonGroup,
                             QFormLayout, QGroupBox, QMessageBox)
from PyQt6.QtCore import Qt
from datetime import datetime


class ExportAuditDialog(QDialog):
    """
    Wizard dialog for exporting Risk Audit results.
    Collects a folder name and save destination.
    Each audit track (BASE, ORIGINAL, OVERRIDE) is saved as a separate CSV
    inside the folder.
    """

    def __init__(self, has_override: bool = False, parent=None):
        super().__init__(parent)
        self.has_override = has_override
        self.setWindowTitle("Export Audit Log")
        self.setMinimumWidth(420)
        self._init_ui()

    def _init_ui(self):
        layout = QVBoxLayout(self)

        # ── Naming ──────────────────────────────────────────────────
        naming_group = QGroupBox("📄 命名设置 (Naming Settings)")
        form = QFormLayout()

        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        self._default_folder = f"audit_{ts}"

        self.folder_input = QLineEdit()
        self.folder_input.setPlaceholderText(self._default_folder)
        self.folder_input.setToolTip("Leave empty to use the auto-generated timestamp name")
        form.addRow("Folder Name:", self.folder_input)

        naming_group.setLayout(form)
        layout.addWidget(naming_group)

        # ── Files that will be created ───────────────────────────────
        info_group = QGroupBox("📁 将生成的文件 (Files to be created)")
        info_layout = QVBoxLayout()
        files = [
            "• BASE_trade_log.csv",
            "• ORIGINAL_trade_log.csv",
        ]
        if self.has_override:
            files.append("• OVERRIDE_trade_log.csv")
        for f in files:
            lbl = QLabel(f)
            lbl.setStyleSheet("color:#A5D6A7; font-family:monospace;")
            info_layout.addWidget(lbl)
        info_group.setLayout(info_layout)
        layout.addWidget(info_group)

        # ── Save destination ─────────────────────────────────────────
        mode_group = QGroupBox("📍 保存方式 (Save Destination)")
        mode_layout = QVBoxLayout()

        self.btn_data_center = QRadioButton("保存至数据中心 (Save to Data Center)")
        self.btn_data_center.setChecked(True)
        self.btn_local = QRadioButton("数据导出 (Local Export)")
        self.btn_both = QRadioButton("两者都保存 (Save Both)")

        self._mode_group = QButtonGroup(self)
        self._mode_group.addButton(self.btn_data_center, 0)
        self._mode_group.addButton(self.btn_local, 1)
        self._mode_group.addButton(self.btn_both, 2)

        for btn in (self.btn_data_center, self.btn_local, self.btn_both):
            mode_layout.addWidget(btn)
        mode_group.setLayout(mode_layout)
        layout.addWidget(mode_group)

        # ── Buttons ──────────────────────────────────────────────────
        btn_layout = QHBoxLayout()
        self.btn_cancel = QPushButton("Cancel")
        self.btn_cancel.clicked.connect(self.reject)
        self.btn_export = QPushButton("Export")
        self.btn_export.setDefault(True)
        self.btn_export.setStyleSheet(
            "background-color:#4CAF50; color:white; font-weight:bold;")
        self.btn_export.clicked.connect(self.accept)

        btn_layout.addStretch()
        btn_layout.addWidget(self.btn_cancel)
        btn_layout.addWidget(self.btn_export)
        layout.addLayout(btn_layout)

    def get_export_data(self) -> dict:
        folder = self.folder_input.text().strip() or self._default_folder
        mode_id = self._mode_group.checkedId()
        save_mode = {0: "data_center", 1: "local", 2: "both"}.get(mode_id, "data_center")
        return {"folder_name": folder, "save_mode": save_mode}
