# Quant Data Bridge - PyInstaller 打包指南

## 📦 打包步骤

### 1. 安装 PyInstaller
```bash
pip install pyinstaller
```

### 2. 打包方式（三选一）

#### 方式 A：使用配置文件打包（推荐）
```bash
pyinstaller Quant_Data_Bridge.spec
```

#### 方式 B：一行命令打包（简化版）
```bash
pyinstaller --onefile --windowed --name="Quant_Data_Bridge" main.py
```

#### 方式 C：详细参数打包（完整控制）
```bash
pyinstaller --onefile \
    --windowed \
    --name="Quant_Data_Bridge" \
    --hidden-import=PyQt6 \
    --hidden-import=qdarktheme \
    --hidden-import=yfinance \
    --hidden-import=ccxt \
    --hidden-import=pandas \
    main.py
```

### 3. 打包输出位置
- 可执行文件位于: `dist/Quant_Data_Bridge.exe`
- 中间文件位于: `build/` 目录（可删除）
- PyInstaller 规格文件: `Quant_Data_Bridge.spec`

---

## 🔧 已实现的生产级功能

### ✅ 1. 健壮的日志系统
- **日志文件**: 程序启动时自动在 exe 所在目录创建 `app_run.log`
- **记录内容**:
  - 应用启动/退出信息
  - Python 版本和运行模式（开发/打包）
  - 所有 INFO 及以上级别的日志
  - 未捕获的异常完整堆栈跟踪
- **全局异常捕获**: 使用 `sys.excepthook` 捕获所有崩溃并记录日志

### ✅ 2. 资源路径自适应
- **resource_path() 函数**: 自动处理开发环境和打包环境的路径差异
- **用法示例**:
  ```python
  from main import resource_path
  
  # 读取配置文件（开发环境: ./config.json, 打包后: _MEIPASS/config.json）
  config_file = resource_path("config.json")
  with open(config_file, 'r') as f:
      config = json.load(f)
  ```

### ✅ 3. 启动加载提示（Splash Screen）
- **显示时长**: 2 秒
- **显示内容**: "正在加载 Quant Data Bridge... 请稍候"
- **样式**: 深色主题，蓝色边框，居中显示

---

## 📝 日志文件示例

打开 `app_run.log` 可以看到：

```
2026-01-01 15:40:00 - INFO - main.py:60 - ============================================================
2026-01-01 15:40:00 - INFO - main.py:61 - Quant Data Bridge 应用启动
2026-01-01 15:40:00 - INFO - main.py:62 - 日志文件位置: D:\Quant_Data_Bridge\app_run.log
2026-01-01 15:40:00 - INFO - main.py:63 - Python 版本: 3.10.0 (...)
2026-01-01 15:40:00 - INFO - main.py:64 - 运行模式: 打包模式 (PyInstaller)
2026-01-01 15:40:00 - INFO - main.py:65 - ============================================================
2026-01-01 15:40:00 - INFO - main.py:145 - 全局异常处理器已安装
2026-01-01 15:40:01 - INFO - main.py:148 - 正在初始化 Qt 应用...
2026-01-01 15:40:02 - INFO - main.py:159 - 启动画面已创建并显示
2026-01-01 15:40:02 - INFO - main.py:163 - 正在应用深色主题...
2026-01-01 15:40:03 - INFO - main.py:168 - 正在创建主窗口...
2026-01-01 15:40:04 - INFO - main.py:170 - 主窗口创建成功
2026-01-01 15:40:06 - INFO - main.py:177 - 主窗口已显示，启动画面已关闭
2026-01-01 15:40:06 - INFO - main.py:181 - 进入应用主循环
```

---

## 🐛 调试建议

### 开发阶段（显示控制台）
在 `Quant_Data_Bridge.spec` 中设置:
```python
console=True,  # 显示控制台窗口，可以看到 print 和日志输出
```

### 生产环境（隐藏控制台）
```python
console=False,  # 不显示控制台，用户看到的是纯净的 GUI
```

### 查看崩溃日志
如果用户报告闪退：
1. 让用户发送 **exe 所在目录** 的 `app_run.log` 文件
2. 查找 `CRITICAL` 级别的日志
3. 查看完整的异常堆栈跟踪

---

## 📋 打包检查清单

打包前确认：
- [x] 日志系统已配置（setup_logging）
- [x] 全局异常处理器已安装（sys.excepthook）
- [x] 资源路径使用 resource_path() 包裹
- [x] 启动画面正常显示（QSplashScreen）
- [x] requirements.txt 包含所有依赖
- [x] 测试开发环境运行正常
- [x] 设置 console=False（生产版）

打包后测试：
- [ ] 双击 exe 能正常启动
- [ ] 启动画面显示 2 秒
- [ ] 主窗口正常加载
- [ ] 获取数据功能正常
- [ ] 导出 CSV 功能正常
- [ ] app_run.log 文件在 exe 同目录生成
- [ ] 关闭程序后日志记录退出信息

---

## 🎯 文件大小优化

预期 exe 文件大小: **约 150-200 MB**

如果需要进一步减小体积：
1. 在 spec 文件中排除不需要的库
2. 使用 UPX 压缩 (`upx=True`)
3. 考虑使用 `--onedir` 模式（文件多但启动更快）

---

## 🚀 分发给同事

1. 将 `dist/Quant_Data_Bridge.exe` 发送给同事
2. 告知：首次运行可能需要等待 3-10 秒（会显示"正在加载..."）
3. 如果遇到问题，请发送 `app_run.log` 文件

---

## 📞 技术支持

如果打包过程中遇到问题：
- 检查 `build/` 目录下的错误日志
- 确保所有依赖都在 `hiddenimports` 列表中
- 尝试在开发环境中先运行 `python main.py` 确认无错误
