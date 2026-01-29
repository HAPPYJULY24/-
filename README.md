# Quant Data Bridge

一个现代化的量化数据获取与处理平台，支持多源金融数据获取、清洗、对齐，专为量化回测设计。

## ✨ 核心特性 (v2.0)

### 1. 多源数据获取
- **TradingView (Futures)** - 通过 `tvDatafeed` 获取全球期货数据 (CBOT, Bursa, COMEX 等)
  - ✅ **自动交易所识别** - 输入 `ZL1!` 自动切换 CBOT，`FCPO1!` 自动切换 MYX
  - ✅ **超长历史数据** - 智能突破 5000 根限制，自动分段下载并拼接
- **Yahoo Finance** - 获取全球股票、外汇、贵金属
- **CCXT** - 支持 Binance, OKX, Bybit, Luno 等加密货币交易所

### 2. 🔬 Data Alignment Studio (数据对齐实验室)
一个交互式的数据处理工具，专门解决"多品种对齐难"的问题。
- **任意对齐** - 选择任意两个 Parquet 文件进行对齐 (如 FCPO vs ZL, BTC vs ETH)
- **智能时区** - 自动检测并将所有数据统一转换为 UTC
- **动态列名** - 自动提取 Symbol 并重命名列 (如 `FCPO_Close`, `ZL_Close`)
- **Forward Fill** - 智能填补不同交易时间造成的空缺
- **实时预览** - 立即查看前50行+后50行结果，支持导出 CSV/Parquet

### 3. 数据存储与格式
- **Parquet (主存储)** - 使用高效的 Parquet 格式存储历史数据，体积小、读取快
- **CSV (导出)** - 支持导出为通用 CSV 格式
- **Master DB** - 增量更新模式，只下载最新的数据

---

## 🚀 快速开始

### 1. 安装依赖
```bash
pip install -r requirements.txt
```

### 2. 运行应用
```bash
python main.py
```

### 3. 使用场景示例

#### 场景 A: 准备跨品种套利数据 (FCPO vs ZL)
1. **下载数据**:
   - 选择 "Bursa期货 (TV)"
   - 输入 `FCPO1!` (时间粒度 15m) → 下载
   - 输入 `ZL1!` (自动识别 CBOT) → 下载
2. **数据对齐**:
   - 菜单栏点击 **"🔧 工具"** → **"🔬 Data Alignment Studio"**
   - 选择 Asset A: `FCPO1!_15m.parquet`
   - 选择 Asset B: `ZL1!_15m.parquet`
   - 点击 "🚀 开始对齐"
3. **导出结果**:
   - 预览对齐结果（红色高亮缺失值）
   - 点击 "💾 导出结果" 保存为 CSV 用于回测

#### 场景 B: 获取加密货币数据（使用代理）
1. 选择 "加密货币" → 交易所 `Binance`
2. 输入 `BTC/USDT`
3. 展开 "网络设置" → 启用代理 (`http://127.0.0.1:7890`)
4. 点击下载

---

## 📂 项目结构

```
├── main.py                 # 应用入口
├── core/                   # 核心引擎
│   ├── data_fetcher.py     # 数据获取 (支持 TV, YF, CCXT)
│   ├── data_processor.py   # 数据对齐与处理 (Pandas)
│   └── worker.py           # 异步线程
├── ui/                     # 用户界面
│   ├── main_window.py      # 主窗口
│   ├── alignment_dialog.py # 数据对齐实验室 (新)
│   └── settings_dialog.py  # 配置对话框
├── data/
│   ├── store/              # Master DB (Parquet)
│   └── processed/          # 处理后的数据
└── exported_data/          # 导出的 CSV 文件
```

## 🛠️ 技术栈
- **GUI**: PyQt6 (现代化 Material 风格)
- **Data**: pandas, numpy, pyarrow
- **Feed**: tvdatafeed, yfinance, ccxt
- **Network**: requests (支持 HTTP/HTTPS 代理)

## 📦 打包发布
使用 PyInstaller 打包为独立 EXE：
```bash
pyinstaller Quant_Data_Bridge.spec
```

---
**Quant Data Bridge** - 专注于解决量化数据的"最后一公里"问题。
