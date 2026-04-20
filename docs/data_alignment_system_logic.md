# 数据对齐系统 (Data Alignment System) 逻辑揭秘

本文档详细说明了 Quant Data Bridge 中量化数据对齐子系统（Data Alignment Studio / DataProcessor）的底层处理逻辑、执行时序及关键技术（内含 V2.6 及 Phase 3 优化更新）。

## 1. 业务场景与目标

在量化投研中，跨资产套利（如马来西亚棕榈油 FCPO 与芝加哥豆油 ZL）或计算相对强弱指标时，通常需要将这两个资产不同时区、不同交易时段的行情数据拼接在同一时间轴上。**数据对齐系统**存在的目的就是：
1. **解决时区错位**（如 MYT 与 EST 混杂）。
2. **解决交易时间段差异**（如外盘 24 小时交易，内盘存在午休停盘）。
3. **清洗节假日脏数据**（即非交易日但有开收盘价、Volume 为 0 的废线）。
4. **填补因停盘导致的价格空洞**，确保信号能连续生成。

---

## 2. 架构层级划分

系统分为两个主要层级：
- **核心逻辑层 (Core Logic)**: `src/core/data_processor.py` -> `DataProcessor` 类。负责加载、重采样、时区转换、脏数据清洗、拼接合并以及数据填充。
- **UI交互层 (GUI/Studio)**: `ui/alignment_dialog.py` -> `AlignmentDialog`。利用异步子线程 (`AlignmentWorker`) 调用 Core 层代码，并提供表格级的可视化预览和手工控制面板。

---

## 3. 标准化对齐流水线 (10 步解析)

当用户在界面点击“执行对齐”或代码中触发 `align_custom_files` 时，系统严格遵循以下 10 步进行：

### 步骤 1: 扫描并加载 (Loading / Scan)
通过 `rglob` 递归扫描 `data/store` 数据中心内任意深度的独立资产（Parquet 格式）。根据选取的文件，在代码层动态提取出对应的行情代码（Symbol，例如 `FCPO1!`、`ZL1!`）。
- *技术点*: 强制要求原始数据必须包含时间索引 `Date` 或者将其提升为标准的 DatetimeIndex 以便后续处理。

### 步骤 2: 时间轴统一（Timezone Synchronization / Killer Fix）
马来西亚期货、美股等具有不同的交易所时间。如果 Parquet 的时间索引已预先附带时区信息（Timezone-aware），系统会**自动执行 `tz_convert('UTC')`** 将所有时间一律平滑转换为世界标准时间 (UTC)。
若系统发现时间是 Naive (无时区关联)，则假设它们已经在数据抓取层（Phase 1 处理）经过了本地化同步处理，跳过该转换。

### 步骤 3: 假期废线清洗 (Non-Trading Day Cleaning)
当探测到聚合周期为日线 (`1d`) 时：
系统自动触发 `clean_non_trading_days`。该函数识别并彻底剔除具有 `Volume == 0` 的行。
> **原理**: 如果由于某种抓取历史遗留导致资产 A 在某地公共假日存在空白 K 线或平价 K 线，但本身成交量为 0，程序会提前删去该行，防止其污染合并后的技术指标跨期计算。

### 步骤 4: 重命名隔离 (Column Prefix Injection)
为了防止两份 OHLCV 表合并后产生列名乱跑和冲突（例如都有 `Close`、`Volume`），系统会将原有行情列加盖资产名称前缀。
- **处理前**: `Open`, `High`, `Low`, `Close`, `Volume`
- **处理后**: `{SymbolA}_Open`, `{SymbolB}_Close`...

### 步骤 5: 外连接拼接 (Outer Join Merge)
由于两个不同资产可能在不同时间点都有独立报价，故绝不可以使用 Inner Join（会抹杀一边的独立行情时间段）。
代码使用 `pd.concat([df_a, df_b], axis=1, join='outer')`，以确保资产 A 或者是 资产 B 的任何一个有效时间戳都能留入全新的时间轴上。

### 步骤 6: 价格盲区修补 (Forward Fill Strategy / Killer Fix)
合并后一定会在没有交集的时间节点上产生大量 `NaN`。此时默认套用 **前向填充 (ffill)** 逻辑。
- 这意味着如果马来西亚休市，但美国大豆油在狂飙，FCPO 的对应时刻 K 线不会是 NaN，而是继承前一个交易日的有效收盘价。
- **灵活度**: 系统支持仅修补 Asset A，或仅修补 Asset B，或双向修补。在生成基于价差的 Spread Alpha 因子时，它完美杜绝了因 `NaN` 导致运算抛出的缺失问题。

### 步骤 7: 双核判定游标 (Overlap Flag Analysis)
系统会新创造一列特殊的真伪列 `is_overlap`。
它的判断标准是当行时间戳：`(Asset_A_Close 不为 NaN) AND (Asset_B_Close 不为 NaN)` 则标为 `True`。
这对于之后的风险控制与回测极度重要，交易引擎可通过过滤 `is_overlap == True` 严格限定**只能在双市场都活跃的时候发单/计算信号**。

### 步骤 8: 数据完整度分析与归档生成 (Statistics & Metric)
计算包含合并后的资产时间跨度（天数），以及各自的数据覆盖率（Coverage Percentage）。将 `is_overlap` 行数及占比打印到后台调试终端做审查。
最后将处理完毕的 DataFrame 输出为高度压缩的 `.parquet` 文件，放置入专门的存放目录或数据中心的 `ALIGNED` 子分片。

### 步骤 9: 回退为时间列 (Index Reset)
由于大部分轻量 UI 直接读取列单，需要把时间索引回撤重置：调用 `reset_index()`，将其变回普通的 `Date` 数据列，以免造成界面预览报错或导出到 Excel 无法带时间的尴尬。

### 步骤 10: 分片切割预览 (GUI Optimization)
针对体积高达数十 MB 级的分钟 Parquet 重混文件，系统仅切割并传输 `df.head(50)` + `df.tail(50)` 生成 `preview_df`，借此实现前端 QTableWidget 毫秒级的渲染与高亮 NaN 的着色分析功能。

---

## 4. 特殊场景：外汇维度融合对齐 (Phase 3 Integration)
此系统机制与 Phase 3 更新存在联动扩展。
尽管本处理逻辑 (`DataProcessor`) 用于两两资产时间轴交叉对齐，但在**数据抓取源头层**（`DataFetcherFacade.save_to_master_db`），系统会预先进行类似机制的强拉平降维合并。
1. 在爬取如大宗商品与股票时，会利用 `pd.merge_asof(direction='backward')` 方式强制挂载同期最近的美元兑外汇走势（`USD_MYR`汇率）。
2. 在存储前执行去重和空体积检查，这极大缓解了此对齐工作室内不必要的资源开销。

## 5. 项目代码定位参考
- 启动类层：`ui/tabs/fetcher_tab.py` (内嵌简易自动对齐)
- 分析工作室界面层：`ui/alignment_dialog.py` -> `_start_alignment` 及结果接收
- 全局基建方法核心层：`src/core/data_processor.py` -> `align_custom_files()` 

（报告书写日期：由 AI 在 Phase 3+ 研发阶段针对 V2.6+ 底层进行反编译撰写）
