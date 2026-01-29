"""
Quant Data Bridge - Main entry point for the application.
生产版本 - 包含日志、资源路径处理和启动画面
"""

import sys
import os
import logging
from datetime import datetime
from PyQt6.QtWidgets import QApplication, QSplashScreen, QLabel
from PyQt6.QtCore import Qt, QTimer
from PyQt6.QtGui import QFont
import qdarktheme
from ui.main_window import MainWindow


# ==================== 1. 资源路径自适应 ====================
def resource_path(relative_path):
    """
    获取资源文件的绝对路径（兼容开发环境和 PyInstaller 打包环境）
    
    Args:
        relative_path: 相对路径
    
    Returns:
        绝对路径
    """
    try:
        # PyInstaller 创建临时文件夹，将路径存储在 _MEIPASS 中
        base_path = sys._MEIPASS
        logging.info(f"Running in PyInstaller mode, base_path: {base_path}")
    except AttributeError:
        # 开发环境：使用当前脚本所在目录
        base_path = os.path.abspath(".")
        logging.info(f"Running in development mode, base_path: {base_path}")
    
    full_path = os.path.join(base_path, relative_path)
    logging.debug(f"Resource path resolved: {relative_path} -> {full_path}")
    return full_path


# ==================== 2. 健壮的日志系统 ====================
def setup_logging():
    """
    配置日志系统 - 记录到文件和控制台
    日志文件保存在 exe 所在目录（而非临时解压目录）
    """
    # 获取 exe 所在目录（或开发环境的当前目录）
    if getattr(sys, 'frozen', False):
        # 打包后：exe 所在目录
        log_dir = os.path.dirname(sys.executable)
    else:
        # 开发环境：当前脚本目录
        log_dir = os.path.abspath(".")
    
    log_file = os.path.join(log_dir, "app_run.log")
    
    # 配置日志格式
    log_format = '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
    
    # 准备日志处理器列表
    handlers = [
        # 文件处理器 - 始终记录到文件（UTF-8）
        logging.FileHandler(log_file, encoding='utf-8', mode='a')
    ]
    
    # 安全地添加控制台处理器 (仅当有控制台窗口时)
    # 在 PyInstaller --noconsole 模式下，sys.stdout 可能为 None 或无法写入
    if sys.stdout is not None:
        try:
            import io
            # 尝试使用 buffer 以获得更好的编码控制 (Windows终端修复)
            if hasattr(sys.stdout, 'buffer'):
                stream = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
            else:
                stream = sys.stdout
                
            console_handler = logging.StreamHandler(stream)
            console_handler.setFormatter(logging.Formatter(log_format))
            handlers.append(console_handler)
        except Exception:
            # 如果设置控制台日志失败，静默失败，不要让程序崩溃
            pass
    
    # 配置根日志记录器
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=handlers
    )
    
    logging.info("=" * 60)
    logging.info("Quant Data Bridge 应用启动")
    logging.info(f"日志文件位置: {log_file}")
    logging.info(f"Python 版本: {sys.version}")
    logging.info(f"运行模式: {'打包模式 (PyInstaller)' if getattr(sys, 'frozen', False) else '开发模式'}")
    logging.info("=" * 60)


def global_exception_handler(exc_type, exc_value, exc_traceback):
    """
    全局异常捕获器 - 捕获所有未处理的异常并记录到日志
    防止程序在打包后静默崩溃（Silent Crash）
    
    Args:
        exc_type: 异常类型
        exc_value: 异常值
        exc_traceback: 异常堆栈
    """
    if issubclass(exc_type, KeyboardInterrupt):
        # 允许 Ctrl+C 正常退出
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    
    # 记录严重错误到日志
    logging.critical("=" * 60)
    logging.critical("捕获到未处理的异常！程序即将崩溃:")
    logging.critical(f"异常类型: {exc_type.__name__}")
    logging.critical(f"异常信息: {exc_value}")
    logging.critical("完整堆栈跟踪:", exc_info=(exc_type, exc_value, exc_traceback))
    logging.critical("=" * 60)
    
    # 调用系统默认的异常处理器（可选，用于开发时看到错误）
    sys.__excepthook__(exc_type, exc_value, exc_traceback)


# ==================== 3. 启动加载提示 ====================
def create_splash_screen():
    """
    创建启动画面（Splash Screen）
    显示 "正在加载 Quant Data Bridge..." 提示用户程序正在启动
    
    Returns:
        QSplashScreen 对象
    """
    # 创建一个简单的文本标签作为启动画面
    splash_label = QLabel("正在加载 Quant Data Bridge...\n\n请稍候")
    splash_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
    splash_label.setStyleSheet("""
        QLabel {
            background-color: #2b2b2b;
            color: #ffffff;
            font-size: 16px;
            font-weight: bold;
            padding: 40px;
            border: 2px solid #007bff;
            border-radius: 10px;
        }
    """)
    
    # 设置字体
    font = QFont()
    font.setPointSize(14)
    font.setBold(True)
    splash_label.setFont(font)
    
    # 创建启动画面
    splash = QSplashScreen(splash_label.grab(), Qt.WindowType.WindowStaysOnTopHint)
    splash.setWindowFlags(Qt.WindowType.FramelessWindowHint | Qt.WindowType.WindowStaysOnTopHint)
    
    logging.info("启动画面已创建并显示")
    return splash


# ==================== 主函数 ====================
def main():
    """主程序入口点"""
    try:
        # 1. 初始化日志系统
        setup_logging()
        
        # 2. 设置全局异常处理器
        sys.excepthook = global_exception_handler
        logging.info("全局异常处理器已安装")
        
        # 3. 创建 Qt 应用
        logging.info("正在初始化 Qt 应用...")
        app = QApplication(sys.argv)
        
        # 设置应用元数据
        app.setApplicationName("Quant Data Bridge")
        app.setOrganizationName("Quant Tools")
        logging.info("应用元数据已设置")
        
        # 4. 显示启动画面
        splash = create_splash_screen()
        splash.show()
        app.processEvents()  # 强制刷新显示启动画面
        
        # 5. 应用深色主题
        logging.info("正在应用深色主题...")
        app.setStyleSheet(qdarktheme.load_stylesheet())
        logging.info("深色主题已应用")
        
        # 6. 创建主窗口（这一步可能需要几秒钟）
        logging.info("正在创建主窗口...")
        window = MainWindow()
        logging.info("主窗口创建成功")
        
        # 7. 2秒后关闭启动画面并显示主窗口
        def finish_loading():
            splash.close()
            window.show()
            logging.info("主窗口已显示，启动画面已关闭")
        
        # 使用定时器延迟关闭启动画面（让用户看到加载提示）
        QTimer.singleShot(2000, finish_loading)
        
        # 8. 运行应用主循环
        logging.info("进入应用主循环")
        exit_code = app.exec()
        
        # 9. 程序正常退出
        logging.info(f"应用正常退出，退出代码: {exit_code}")
        logging.info("=" * 60)
        return exit_code
        
    except Exception as e:
        # 捕获主函数中的任何异常
        logging.critical(f"主函数发生严重错误: {str(e)}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

