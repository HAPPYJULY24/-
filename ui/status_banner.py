"""
Status Banner - Displays success, warning, and error messages.
"""

from PyQt6.QtWidgets import QFrame, QLabel, QHBoxLayout, QPushButton
from PyQt6.QtCore import Qt, QTimer
from PyQt6.QtGui import QFont


class StatusBanner(QFrame):
    """
    A banner widget for displaying status messages with color-coded backgrounds.
    Features:
    - Auto-hide after 20 seconds
    - Close button (X) in top-right corner
    - Fixed maximum height to prevent UI compression
    """
    
    def __init__(self):
        super().__init__()
        
        # 创建自动隐藏定时器（20秒）- 必须在 _init_ui() 之前创建
        self.auto_hide_timer = QTimer()
        self.auto_hide_timer.timeout.connect(self.hide)
        self.auto_hide_timer.setSingleShot(True)  # 只触发一次
        
        # 初始化UI
        self._init_ui()
    
    def _init_ui(self):
        """Initialize the banner UI."""
        # 设置固定最大高度，防止压缩其他UI元素
        self.setMaximumHeight(120)  # 限制最大高度
        self.setMinimumHeight(0)  # 隐藏时高度为0
        
        # 创建水平布局
        layout = QHBoxLayout()
        layout.setContentsMargins(15, 10, 15, 10)
        layout.setSpacing(10)
        
        # 状态消息标签
        self.message_label = QLabel()
        self.message_label.setWordWrap(True)  # 允许文字换行
        self.message_label.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        
        # 设置字体
        font = QFont()
        font.setPointSize(10)
        self.message_label.setFont(font)
        
        # 关闭按钮 (X)
        self.close_button = QPushButton("✕")
        self.close_button.setFixedSize(24, 24)
        self.close_button.clicked.connect(self.hide)
        self.close_button.setStyleSheet("""
            QPushButton {
                background-color: transparent;
                border: none;
                color: #333;
                font-size: 16px;
                font-weight: bold;
                border-radius: 12px;
            }
            QPushButton:hover {
                background-color: rgba(0, 0, 0, 0.1);
            }
            QPushButton:pressed {
                background-color: rgba(0, 0, 0, 0.2);
            }
        """)
        
        # 添加到布局
        layout.addWidget(self.message_label, 1)  # 消息标签占据剩余空间
        layout.addWidget(self.close_button, 0, Qt.AlignmentFlag.AlignTop)  # 关闭按钮靠右上
        
        self.setLayout(layout)
        
        # 默认隐藏
        self.hide()
    
    def show_success(self, message: str):
        """
        Display a success message (green background).
        
        Args:
            message: Success message to display
        """
        self.message_label.setText(f"✓ {message}")
        self.setStyleSheet("""
            QFrame {
                background-color: #d4edda;
                border: 1px solid #c3e6cb;
                border-radius: 5px;
            }
            QLabel {
                color: #155724;
            }
        """)
        self.show()
        self._start_auto_hide_timer()
    
    def show_warning(self, message: str):
        """
        Display a warning message (yellow background).
        
        Args:
            message: Warning message to display
        """
        self.message_label.setText(f"⚠ {message}")
        self.setStyleSheet("""
            QFrame {
                background-color: #fff3cd;
                border: 1px solid #ffeeba;
                border-radius: 5px;
            }
            QLabel {
                color: #856404;
            }
        """)
        self.show()
        self._start_auto_hide_timer()
    
    def show_error(self, message: str):
        """
        Display an error message (red background).
        
        Args:
            message: Error message to display
        """
        self.message_label.setText(f"✕ {message}")
        self.setStyleSheet("""
            QFrame {
                background-color: #f8d7da;
                border: 1px solid #f5c6cb;
                border-radius: 5px;
            }
            QLabel {
                color: #721c24;
            }
        """)
        self.show()
        self._start_auto_hide_timer()
    
    def _start_auto_hide_timer(self):
        """启动20秒自动隐藏定时器"""
        # 重启定时器（如果已经在运行，会先停止）
        self.auto_hide_timer.stop()
        self.auto_hide_timer.start(20000)  # 20秒 = 20000毫秒
    
    def hide(self):
        """隐藏横幅并停止定时器"""
        # 停止自动隐藏定时器
        self.auto_hide_timer.stop()
        # 调用父类的hide方法
        super().hide()
