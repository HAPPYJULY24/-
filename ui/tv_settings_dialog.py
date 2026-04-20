"""
Settings Dialog - TradingView credential configuration UI
"""

from PyQt6.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QLabel, 
                             QLineEdit, QPushButton, QGroupBox, QCheckBox,
                             QMessageBox)
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QFont

from logic.localization import tr
from utils.config_manager import ConfigManager


class TvSettingsDialog(QDialog):
    """
    设置对话框 - 配置 TradingView 凭据
    """
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.config_manager = ConfigManager()
        self._init_ui()
        self._load_current_settings()
    
    def _init_ui(self):
        """初始化用户界面"""
        self.setWindowTitle(tr("tv_settings.title"))
        self.setMinimumWidth(500)
        self.setModal(True)  # 模态对话框
        
        layout = QVBoxLayout()
        layout.setSpacing(15)
        layout.setContentsMargins(20, 20, 20, 20)
        
        # 标题标签
        title_label = QLabel(tr("tv_settings.header"))
        title_font = QFont()
        title_font.setPointSize(12)
        title_font.setBold(True)
        title_label.setFont(title_font)
        layout.addWidget(title_label)
        
        info_label = QLabel(tr("tv_settings.info"))
        info_label.setWordWrap(True)
        info_label.setStyleSheet("color: #666; font-size: 10pt;")
        layout.addWidget(info_label)
        
        # TradingView 凭据配置组
        tv_group = QGroupBox(tr("tv_settings.group_title"))
        tv_layout = QVBoxLayout()
        
        # 启用开关
        self.enable_checkbox = QCheckBox(tr("tv_settings.enable_checkbox"))
        self.enable_checkbox.toggled.connect(self._on_enable_toggled)
        tv_layout.addWidget(self.enable_checkbox)
        
        # 用户名
        username_layout = QHBoxLayout()
        username_label = QLabel(tr("tv_settings.username_label"))
        username_label.setMinimumWidth(100)
        username_layout.addWidget(username_label)
        
        self.username_input = QLineEdit()
        self.username_input.setPlaceholderText(tr("tv_settings.username_placeholder"))
        username_layout.addWidget(self.username_input)
        tv_layout.addLayout(username_layout)
        
        # 密码
        password_layout = QHBoxLayout()
        password_label = QLabel(tr("tv_settings.password_label"))
        password_label.setMinimumWidth(100)
        password_layout.addWidget(password_label)
        
        self.password_input = QLineEdit()
        self.password_input.setEchoMode(QLineEdit.EchoMode.Password)
        self.password_input.setPlaceholderText(tr("tv_settings.password_placeholder"))
        password_layout.addWidget(self.password_input)
        
        # 显示/隐藏密码按钮
        self.show_password_btn = QPushButton("👁")
        self.show_password_btn.setMaximumWidth(40)
        self.show_password_btn.setCheckable(True)
        self.show_password_btn.toggled.connect(self._toggle_password_visibility)
        password_layout.addWidget(self.show_password_btn)
        
        tv_layout.addLayout(password_layout)
        
        # 安全提示
        security_note = QLabel(
            tr("tv_settings.security_note", path=self.config_manager.get_config_file_path())
        )
        security_note.setWordWrap(True)
        security_note.setStyleSheet("color: #999; font-size: 9pt; padding: 10px;")
        tv_layout.addWidget(security_note)
        
        tv_group.setLayout(tv_layout)
        layout.addWidget(tv_group)
        
        # 按钮区域
        button_layout = QHBoxLayout()
        button_layout.addStretch()
        
        # 测试连接按钮
        self.test_btn = QPushButton(tr("tv_settings.test_btn"))
        self.test_btn.clicked.connect(self._test_connection)
        button_layout.addWidget(self.test_btn)
        
        # 清除凭据按钮
        self.clear_btn = QPushButton(tr("tv_settings.clear_btn"))
        self.clear_btn.clicked.connect(self._clear_credentials)
        button_layout.addWidget(self.clear_btn)
        
        # 保存按钮
        self.save_btn = QPushButton(tr("tv_settings.save_btn"))
        self.save_btn.clicked.connect(self._save_settings)
        self.save_btn.setDefault(True)
        button_layout.addWidget(self.save_btn)
        
        # 取消按钮
        cancel_btn = QPushButton(tr("tv_settings.cancel_btn"))
        cancel_btn.clicked.connect(self.reject)
        button_layout.addWidget(cancel_btn)
        
        layout.addLayout(button_layout)
        
        self.setLayout(layout)
    
    def _load_current_settings(self):
        """加载当前设置"""
        username, password, enabled = self.config_manager.get_tradingview_credentials()
        
        self.username_input.setText(username)
        self.password_input.setText(password)
        self.enable_checkbox.setChecked(enabled)
        
        # 触发启用状态更新
        self._on_enable_toggled(enabled)
    
    def _on_enable_toggled(self, checked: bool):
        """启用开关改变回调"""
        self.username_input.setEnabled(checked)
        self.password_input.setEnabled(checked)
        self.show_password_btn.setEnabled(checked)
        self.test_btn.setEnabled(checked)
    
    def _toggle_password_visibility(self, checked: bool):
        """切换密码可见性"""
        if checked:
            self.password_input.setEchoMode(QLineEdit.EchoMode.Normal)
            self.show_password_btn.setText("🙈")
        else:
            self.password_input.setEchoMode(QLineEdit.EchoMode.Password)
            self.show_password_btn.setText("👁")
    
    def _test_connection(self):
        """测试 TradingView 连接"""
        username = self.username_input.text().strip()
        password = self.password_input.text().strip()
        
        if not username or not password:
            QMessageBox.warning(
                self,
                tr("tv_settings.input_error_title"),
                tr("tv_settings.input_error_message")
            )
            return
        
        # 显示测试对话框
        QMessageBox.information(
            self,
            tr("tv_settings.test_title"),
            tr("tv_settings.test_message", username=username)
        )
    
    def _clear_credentials(self):
        """清除凭据"""
        reply = QMessageBox.question(
            self,
            tr("tv_settings.clear_confirm_title"),
            tr("tv_settings.clear_confirm_message"),
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            self.username_input.clear()
            self.password_input.clear()
            self.enable_checkbox.setChecked(False)
            
            if self.config_manager.clear_tradingview_credentials():
                QMessageBox.information(self, tr("common.success"), tr("tv_settings.clear_success"))
            else:
                QMessageBox.warning(self, tr("common.error"), tr("tv_settings.clear_error"))
    
    def _save_settings(self):
        """保存设置"""
        username = self.username_input.text().strip()
        password = self.password_input.text().strip()
        enabled = self.enable_checkbox.isChecked()
        
        # 验证输入
        if enabled and (not username or not password):
            QMessageBox.warning(
                self,
                tr("tv_settings.validation_error_title"),
                tr("tv_settings.validation_error_message")
            )
            return
        
        # 保存配置
        if self.config_manager.save_tradingview_credentials(username, password, enabled):
            QMessageBox.information(
                self,
                tr("tv_settings.save_success_title"),
                tr("tv_settings.save_success_message")
            )
            self.accept()
        else:
            QMessageBox.critical(
                self,
                tr("tv_settings.save_error_title"),
                tr("tv_settings.save_error_message")
            )
