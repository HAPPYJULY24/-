# Quant Data Bridge - PyInstaller 打包指南

## 🚀 打包前准备（必读）

### 1. 清理项目
在打包之前，请先清理项目缓存和构建产物：

```bash
# 方式1: 使用清理脚本（推荐）
python cleanup.py

# 方式2: 手动删除
# 删除以下目录：app/, build/, dist/, __pycache__/
```

### 2. 验证功能
确保以下功能在开发环境中正常工作：
- ✅ 数据获取（股票、期货、加密货币）
- ✅ 数据导出（CSV、Parquet）
- ✅ 数据管理中心（预览、批量导出、删除）
- ✅ v2.0 功能（时区标准化、增量更新、午休过滤）

---

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

### ✅ 4. v2.0 新功能（增强版）
- **📊 数据管理中心**:
  - 数据预览（双击查看前10行）
  - 批量导出（带进度条）
  - Master DB管理（查看、删除、统计）
- **⚡ 增量更新**: Master DB存储历史数据，下次只下载新数据
- **🌏 时区标准化**: 所有数据统一存储为Asia/Kuala_Lumpur时区
- **🗑️ 自动清理**: 启动时自动清理旧日志（config/settings.json配置）
- **💾 磁盘监控**: 低空间警告，主动提示清理
- **🎨 Parquet导出**: 高效压缩格式，体积减小70%

---

## 📝 日志文件示例

打开 `app_run.log` 可以看到：

```
2026-01-20 15:40:00 - INFO - main.py:60 - ============================================================
2026-01-20 15:40:00 - INFO - main.py:61 - Quant Data Bridge 应用启动
2026-01-20 15:40:00 - INFO - main.py:62 - 日志文件位置: D:\Quant_Data_Bridge\app_run.log
2026-01-20 15:40:00 - INFO - main.py:63 - Python 版本: 3.10.0 (...)
2026-01-20 15:40:00 - INFO - main.py:64 - 运行模式: 打包模式 (PyInstaller)
2026-01-20 15:40:00 - INFO - main.py:65 - ============================================================
2026-01-20 15:40:00 - INFO - main.py:145 - 全局异常处理器已安装
2026-01-20 15:40:01 - INFO - main.py:148 - 正在初始化 Qt 应用...
2026-01-20 15:40:02 - INFO - main.py:159 - 启动画面已创建并显示
2026-01-20 15:40:02 - INFO - main.py:163 - 正在应用深色主题...
2026-01-20 15:40:03 - INFO - main.py:168 - 正在创建主窗口...
2026-01-20 15:40:04 - INFO - main.py:170 - 主窗口创建成功
2026-01-20 15:40:06 - INFO - main.py:177 - 主窗口已显示，启动画面已关闭
2026-01-20 15:40:06 - INFO - main.py:181 - 进入应用主循环
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

### 打包前确认：
- [x] 日志系统已配置（setup_logging）
- [x] 全局异常处理器已安装（sys.excepthook）
- [x] 资源路径使用 resource_path() 包裹
- [x] 启动画面正常显示（QSplashScreen）
- [x] requirements.txt 包含所有依赖
- [x] 已执行 `python cleanup.py` 清理缓存
- [x] 测试开发环境运行正常
- [x] 设置 console=False（生产版）

### 打包后测试：
- [ ] 双击 exe 能正常启动
- [ ] 启动画面显示 2 秒
- [ ] 主窗口正常加载
- [ ] 获取数据功能正常（测试股票、期货、加密货币）
- [ ] 导出功能正常（CSV 和 Parquet）
- [ ] **数据管理中心**功能正常：
  - [ ] 预览数据（双击文件）
  - [ ] 批量导出（选择目录）
  - [ ] 删除文件
  - [ ] 磁盘空间显示
- [ ] 自动清理功能正常（启动时检查日志）
- [ ] config/settings.json 自动生成
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

## 🚀 分发给用户

### 1. 准备分发包
将以下文件/文件夹打包：
- `Quant_Data_Bridge.exe` (主程序)
- `README.md` (使用说明，可选)

### 2. 首次运行说明
告知用户：
- 首次运行可能需要等待 3-10 秒（会显示"正在加载..."）
- 程序会在exe同目录自动创建：
  - `app_run.log` - 运行日志
  - `config/` - 配置文件夹
  - `data/store/` - Master DB数据存储
  - `exported_data/` - 导出的数据文件

### 3. 数据管理
- Master DB 会自动保存历史数据实现增量更新
- 使用"📊 数据管理"功能查看和管理本地数据
- 可通过数据管理中心清理不需要的文件

### 4. 故障排查
如果遇到问题，请发送以下文件：
- `app_run.log` - 完整日志
- `config/settings.json` - 配置文件（如果存在）

---

## 📞 技术支持

如果打包过程中遇到问题：
- 检查 `build/` 目录下的错误日志
- 确保所有依赖都在 `hiddenimports` 列表中
- 尝试在开发环境中先运行 `python main.py` 确认无错误
- 使用 `python cleanup.py` 清理后重新打包

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
