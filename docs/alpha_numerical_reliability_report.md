# AlphaTab 数值可靠性校验修复报告

生成时间：2026-04-20

范围：
- `src/core/engines/alpha_engine.py`
- `ui/tabs/alpha_tab.py`
- `tests/test_alpha_metrics.py`

验证命令：

```powershell
python -m compileall src/core/engines/alpha_engine.py ui/tabs/alpha_tab.py tests/test_alpha_metrics.py
conda run -n QuantLab python -m pytest tests/test_alpha_metrics.py -q
```

验证结果：

```text
7 passed
```

说明：测试仍有 pandas/numpy 对常量相关性和 `groupby.apply` 的 warning，这些 warning 来自测试专门构造的无效截面与 pandas 未来行为提示；核心数值断言全部通过。

## 1. 多资产 close 目标收益列不能再随便猜

修改前风险代码形态：

```python
if 'close' not in df.columns and c.endswith(('_close', '.close', ' close')):
    df['close'] = df[c]
```

问题：FCPO/ZL aligned 数据中多个 `_close` 列并存时，系统会把第一个匹配列复制成 `close`，IC 和分位数收益可能评价到错误标的。

修改后：

```python
price_col = config.get('target_return_col')
if price_col:
    price_col = str(price_col).lower()
    if price_col not in df.columns:
        raise ValueError(f"Target return column '{price_col}' not found in data.")
else:
    price_col = next((c for c in ['close', 'last', 'price'] if c in df.columns), None)
```

证据位置：
- `src/core/engines/alpha_engine.py:168`
- `ui/tabs/alpha_tab.py:246` 增加 `Target Return` 下拉框
- `ui/tabs/alpha_tab.py:669` 运行前强制选择目标收益列
- `ui/tabs/alpha_tab.py:684` 将 `target_return_col` 传入 Engine
- `tests/test_alpha_metrics.py:83` 验证显式 `a_close` 被用于 forward return
- `tests/test_alpha_metrics.py:110` 验证没有目标列时不会猜多个 `_close`

补充：Worker 和导出清洗现在只在“唯一一个 suffixed close 候选”时才自动映射为 `close`，多资产 aligned 文件必须走显式 Target Return。

## 2. period 已限制为正整数

修改前风险代码形态：

```python
periods = [int(p.strip()) for p in periods_str.split(',') if p.strip()]
df[col_name] = df[price_col].shift(-p) / df[price_col] - 1
```

问题：`p=0` 得到全 0 收益，`p<0` 变成历史收益。

修改后：

```python
periods = sorted({int(p) for p in periods})
if not periods or any(p <= 0 for p in periods):
    raise ValueError("Periods must be positive integers.")
```

证据位置：
- `src/core/engines/alpha_engine.py:40`
- `ui/tabs/alpha_tab.py:656`
- `tests/test_alpha_metrics.py:65` 验证 `0/-1` 会抛错

## 3. Time-Series IC 统计口径已统一

修改前风险代码形态：

```python
rank_ic_mean = global_spearman
rank_ic_std = rolling_rank_ic.std()
t_stat = rolling_rank_ic.mean() / ...
```

问题：Rank IC、ICIR、T-stat 不来自同一条样本序列。

修改后主评估表口径：

```python
rolling_rank_ic = ranked_factor.rolling(window=window).corr(ranked_ret)
rolling_rank_ic = rolling_rank_ic.replace([np.inf, -np.inf], np.nan).dropna()
rank_ic_mean = rolling_rank_ic.mean()
rank_ic_std = rolling_rank_ic.std()
n_samples = int(rolling_rank_ic.count())
t_stat = newey_west_t_stat(rolling_rank_ic)
```

证据位置：
- `src/core/engines/alpha_engine.py:280`
- `src/core/engines/alpha_engine.py:288`
- `src/core/engines/alpha_engine.py:291`

专业指标中也补充区分：
- `rank_ic_mean` 使用 rolling Rank IC 均值
- `global_rank_ic_mean` 单独保存全样本 Spearman

证据位置：
- `src/core/engines/alpha_engine.py:499`
- `src/core/engines/alpha_engine.py:521`
- `src/core/engines/alpha_engine.py:532`

## 4. NaN/无效 IC 截面不再按 0 计入

修改前风险代码形态：

```python
if len(group) < 2:
    return pd.Series({'Rank_IC': 0, 'IC': 0})
n_samples = len(ic_daily)
```

问题：无效截面把均值拉向 0，并虚增样本数。

修改后：

```python
valid = group[['factor', ret_c]].replace([np.inf, -np.inf], np.nan).dropna()
if len(valid) < 2 or valid['factor'].nunique() < 2 or valid[ret_c].nunique() < 2:
    return pd.Series({'Rank_IC': np.nan, 'IC': np.nan})
...
ic_daily = ic_daily.replace([np.inf, -np.inf], np.nan).dropna(subset=['Rank_IC'])
n_samples = int(ic_daily['Rank_IC'].count())
```

证据位置：
- `src/core/engines/alpha_engine.py:219`
- `src/core/engines/alpha_engine.py:265`
- `tests/test_alpha_metrics.py:149` 验证无效截面被丢弃，`N == 1`

## 5. forward return 计算前已强制排序

修改前风险代码形态：

```python
df[col_name] = df.groupby('symbol')[price_col].transform(lambda x: x.shift(-p) / x - 1)
```

问题：若 parquet 行顺序不是时间顺序，shift 会错位。

修改后：

```python
if 'symbol' in df.columns and 'datetime' in df.columns:
    df = df.sort_values(['symbol', 'datetime']).copy()
elif 'datetime' in df.columns:
    df = df.sort_values('datetime').copy()
```

证据位置：
- `src/core/engines/alpha_engine.py:186`
- `tests/test_alpha_metrics.py:83` 用乱序输入验证 A 在 2024-01-01 的 `ret_1 == 0.10`

## 6. 中性化后已重新标准化 factor

修改前风险代码形态：

```python
group.loc[valid_group.index, 'factor'] = resids
```

问题：Ridge residual 尺度改变，导出到 Backtest 后阈值策略仍把 factor 当标准分数使用。

修改后：

```python
df['factor_neutralized'] = df['factor']
if is_panel:
    df = df.groupby('datetime', group_keys=False).apply(standardize_factor_group)
else:
    df = standardize_factor_group(df)
```

并保留层次字段：

```python
df['factor_raw'] = df['factor']
group['factor_winsor'] = group['factor']
df['factor_neutralized'] = df['factor']
```

证据位置：
- `src/core/engines/alpha_engine.py:80`
- `src/core/engines/alpha_engine.py:118`
- `src/core/engines/alpha_engine.py:159`

## 7. 信号导出两个业务错误已修复

修改前风险代码形态：

```python
save_df = AlphaEngine.prepare_signal_export(df, window=14)
df = self.current_result.get('preview_df', pd.DataFrame())
```

问题：
- `prepare_signal_export()` 不接受 `window` 参数，保存会报错。
- Export to Backtest 使用 `preview_df`，只导出前 100 行。

修改后：

```python
save_df = AlphaEngine.prepare_signal_export(df)
...
df = self.current_result.get('signal_df', pd.DataFrame())
export_df = AlphaEngine.prepare_signal_export(df)
```

证据位置：
- `ui/tabs/alpha_tab.py:903` 保存包使用全量 `signal_df`
- `ui/tabs/alpha_tab.py:935` 删除 `window=14`
- `ui/tabs/alpha_tab.py:999` Backtest 导出使用全量 `signal_df`
- `ui/tabs/alpha_tab.py:1019` 导出统一走 `prepare_signal_export()`
- `tests/test_alpha_metrics.py:171` 验证导出清洗保留全量有效行

## 8. Quantile LB/UB 已约束 LB < UB

修改前风险代码形态：

```python
lower = group['factor'].quantile(config['quantile_lb'])
upper = group['factor'].quantile(config['quantile_ub'])
```

问题：UI 可输入 `LB >= UB`，winsor 边界语义反转。

修改后：

```python
if not (0 <= q_lb < q_ub <= 1):
    raise ValueError("Quantile bounds must satisfy 0 <= LB < UB <= 1.")
```

证据位置：
- `src/core/engines/alpha_engine.py:44`
- `ui/tabs/alpha_tab.py:663`

## 9. Auto-drop Zero Volume 不再对所有 Master 默认开启

修改前风险代码形态：

```python
if is_master:
    self.auto_drop_chk.setChecked(True)
```

问题：分钟级 Master 中零成交 bar 可能是有效 intraday bar，不应默认删除。

修改后：

```python
self.auto_drop_chk.setChecked(
    self._infer_timeframe_from_text(self.data_combo.currentText()) == 'daily'
)
```

证据位置：
- `ui/tabs/alpha_tab.py:434`
- `ui/tabs/alpha_tab.py:563`

## 10. `is_overlap` 已接入 Alpha 评估逻辑

修改前风险代码形态：

```python
# is_overlap generated upstream, but not used by AlphaEngine
```

问题：ffill 的非重叠时段也参与评估，跨市场因子可能被停盘/填充数据污染。

修改后：

```python
if bool(config.get('only_overlap', False)) and 'is_overlap' in df.columns:
    df = df[df['is_overlap'] == True].copy()
```

UI 默认：

```python
has_overlap = any(c.lower() == 'is_overlap' for c in schema_names)
self.only_overlap_chk.setEnabled(has_overlap)
self.only_overlap_chk.setChecked(bool(has_overlap and is_processed))
```

证据位置：
- `src/core/engines/alpha_engine.py:76`
- `ui/tabs/alpha_tab.py:250`
- `ui/tabs/alpha_tab.py:569`
- `ui/tabs/alpha_tab.py:685`
- `tests/test_alpha_metrics.py:126` 验证 overlap 过滤后仅保留 overlap 行

## 11. Metrics period 下拉框已在新结果时重建

修改前风险代码形态：

```python
if self.period_combo.count() == 0 and not ic_decay.empty:
    self.period_combo.addItem(...)
```

问题：第二次运行 periods 不同时，UI 可能沿用旧 period。

修改后：

```python
self.period_combo.blockSignals(True)
self.period_combo.clear()
if not ic_decay.empty:
    for p in sorted(ic_decay.index.tolist()):
        self.period_combo.addItem(str(p))
self.period_combo.blockSignals(False)
```

证据位置：
- `ui/tabs/alpha_tab.py:720`

## 12. Factor Coverage 已拆成原始因子覆盖率和收益覆盖率

修改前风险代码形态：

```python
valid_df = df.dropna(subset=[factor_name, returns_name])
metrics['coverage'] = len(valid_df) / len(df)
```

问题：`df` 已经提前 drop 过 factor NaN，coverage 不是原始因子覆盖率。

修改后：

```python
original_row_count = len(df)
factor_valid_count = int(df['factor'].notna().sum())
...
'coverage_metrics': {
    'raw_rows': original_row_count,
    'factor_valid_rows': factor_valid_count,
    'factor_coverage': factor_valid_count / original_row_count if original_row_count > 0 else 0.0,
    'analysis_rows': len(df),
    'return_valid_rows': int(df[ret_col_primary].notna().sum()) if ret_col_primary and ret_col_primary in df.columns else 0,
}
```

专业指标中也拆分：

```python
metrics['factor_coverage'] = factor_valid_rows / total_rows if total_rows > 0 else 0.0
metrics['return_coverage'] = return_valid_rows / total_rows if total_rows > 0 else 0.0
metrics['coverage'] = metrics['return_coverage']
```

证据位置：
- `src/core/engines/alpha_engine.py:49`
- `src/core/engines/alpha_engine.py:73`
- `src/core/engines/alpha_engine.py:404`
- `src/core/engines/alpha_engine.py:617`
- `tests/test_alpha_metrics.py:126` 验证 `raw_rows/factor_valid_rows/analysis_rows` 分离

## 回归测试覆盖

新增/补强测试：

```text
test_alpha_metrics
test_periods_must_be_positive
test_forward_returns_are_sorted_and_use_explicit_target_column
test_ambiguous_suffixed_close_is_not_guessed_without_target_column
test_overlap_filter_and_factor_coverage_are_reported_separately
test_invalid_panel_ic_groups_are_dropped_from_n
test_prepare_signal_export_uses_full_clean_signal_rows
```

测试文件：`tests/test_alpha_metrics.py`

## 剩余建议

1. 后续可消除 pandas `groupby.apply` deprecation warning，避免未来 pandas 版本升级时出现行为变化。
2. 可把 `target_return_col` 写入策略 JSON 配置模型，当前 parquet 信号已按目标列生成 `close`，但 JSON 配置仍未显式记录该字段。
3. 可增加 UI 自动化测试，覆盖 Target Return 下拉框、Overlap 默认值、period combo 重建和导出全量行数。
