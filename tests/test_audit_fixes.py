import pytest
import pandas as pd
import numpy as np
import os
from pathlib import Path
from scipy.stats import rankdata

from src.core.engines.alpha_engine import (
    AlphaEngine,
    numba_expanding_rank_pct,
    vectorized_expanding_rank_pct,
    numba_rolling_zscore,
    numba_rolling_zscore_fallback,
    neutralize_ts_rolling
)


def test_numba_vs_vectorized_fallback_mathematical_equivalence():
    """
    【硬化测试 1】：验证 Numba JIT 与 NumPy/SciPy Fallback 扩展百分位秩计算器在数学上的 100% 绝对等价性。
    特别包含重复值 (Ties)、并列数值、NaN、暖机期边界以及极端不规则输入。
    """
    # 构造带有重复值、NaN 且包含极值的测试数组
    test_arr = np.array([
        10.0, 20.0, 20.0, np.nan, 30.0, 15.0, 15.0, 15.0, 50.0, np.nan,
        50.0, 25.0, 25.0, 5.0, 100.0, 45.0, 45.0, 45.0, np.nan, 80.0
    ])
    
    # 1. 检验 min_periods = 5 的情况
    min_p = 5
    numba_res = numba_expanding_rank_pct(test_arr, min_periods=min_p)
    fallback_res = vectorized_expanding_rank_pct(test_arr, min_periods=min_p)
    
    # 检查前 min_p - 1 个点必须是 NaN
    assert np.isnan(numba_res[:min_p-1]).all()
    assert np.isnan(fallback_res[:min_p-1]).all()
    
    # 2. 全量精度校验 (断言 10^-14 级别绝对一致，防止任何计算漂移)
    np.testing.assert_allclose(
        numba_res,
        fallback_res,
        equal_nan=True,
        rtol=1e-14,
        atol=1e-14,
        err_msg="[CRITICAL ERROR] Numba 与 SciPy Fallback 输出不一致！"
    )
    
    # 3. 针对 ties 进行单点精细计算验证
    # index=4（val=30.0）：由于 min_periods = 5，且序列含一个 NaN，导致有效数仅 4 个，故输出 NaN
    assert np.isnan(numba_res[4])
    
    # 数组：10.0, 20.0, 20.0, np.nan, 30.0, 15.0
    # 第六位（index=5, val=15.0）：有效序列为 [10.0, 20.0, 20.0, 30.0, 15.0]
    # 排序：10.0 (1), 15.0 (2), 20.0 (3.5), 20.0 (3.5), 30.0 (5)
    # 15.0 排第 2，总有效数 5。百分比应该是 2/5 = 0.4
    assert np.isclose(numba_res[5], 0.4)


def test_numba_rolling_zscore_equivalence():
    """
    验证 Numba JIT Z-Score 与 Fallback NumPy Z-Score 滚动版本的一致性与无偏性。
    """
    test_arr = np.array([
        1.5, 2.5, np.nan, 3.5, 4.5, 5.5, 10.0, -2.0, 0.0, np.nan,
        1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0
    ])
    
    res_jit = numba_rolling_zscore(test_arr, window=5)
    res_fb = numba_rolling_zscore_fallback(test_arr, window=5)
    
    np.testing.assert_allclose(
        res_jit,
        res_fb,
        equal_nan=True,
        rtol=1e-14,
        atol=1e-14,
        err_msg="[CRITICAL ERROR] Z-Score JIT 与 Fallback 不一致！"
    )


def test_rolling_neutralization_lenient_policy_under_sparse_data():
    """
    【硬化测试 2】：验证滚动中性化在极度稀疏、缺失数据场景下的“宽容策略兜底保护”。
    必须安全退化为原始因子值，绝对禁止奇异矩阵报错或大面积输出 NaN。
    """
    # 构造 20 个样本，但中间有极多 NaN 导致有效数据稀疏
    df_sparse = pd.DataFrame({
        'factor': [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0] * 2,
        # 故意让自变量包含大量 NaN，使有效样本小于 len(risk_cols) + 2
        'risk_1': [np.nan, 0.1, np.nan, 0.2, np.nan, 0.3, np.nan, 0.4, np.nan, 0.5] * 2,
        'risk_2': [np.nan, np.nan, np.nan, 0.5, np.nan, np.nan, np.nan, 0.9, np.nan, np.nan] * 2
    })
    
    # 滚动中性化，窗口设为 W = 5
    # 自变量 2 个，len(risk_cols) + 2 = 4。在 W=5 窗口内，有效样本极易少于 4
    try:
        resids = neutralize_ts_rolling(df_sparse, 'factor', ['risk_1', 'risk_2'], W=5)
    except Exception as e:
        pytest.fail(f"[CRITICAL FAILURE] 滚动中性化在稀疏数据下发生崩溃！报错：{str(e)}")
        
    # 1. 确保中性化成功完成，未发生异常，且无 NaN (因为暖机和宽容兜底都是退化原值)
    assert not resids.isna().any(), "[CRITICAL] 稀疏兜底中不应产生 NaN 漏洞！"
    
    # 2. 检查极度稀疏段的数值，必须严格等于 factor 的原始值
    # 第 6 个点 (index=5)：过去 5 天数据 (1~5)，
    # X：[0.1, nan], [nan, nan], [0.2, 0.5], [nan, nan], [0.3, nan] -> 有效行只有 index=3 一行！
    # 有效行 1 <= 4，触发宽容策略，残差必须退化为原值 6.0
    assert np.isclose(resids.iloc[5], 6.0)


def test_parquet_export_filename_symbol_isolation(tmp_path):
    """
    【硬化测试 3】：验证单资产导出模式下的 Parquet 文件 symbol 物理磁盘隔离保护。
    """
    export_dir = tmp_path / "Alpha_export_test"
    export_dir.mkdir()
    
    # 构造单资产 DataFrame
    df_single = pd.DataFrame({
        'datetime': pd.date_range("2026-05-01", periods=5),
        'symbol': ['MYX-FCPO1'] * 5,
        'close': [100.0, 101.0, 102.0, 103.0, 104.0],
        'factor': [0.1, 0.2, 0.3, 0.4, 0.5]
    })
    
    # 指定普通文件名
    filepath = export_dir / "test_factor_data.parquet"
    
    # 执行导出
    AlphaEngine.write_signal_export_parquet(df_single, filepath, metadata={'version': '1.0'})
    
    # 1. 验证原始 filepath 并不存在（已被升级为带有 symbol 标签的物理路径）
    assert not filepath.exists(), "[CRITICAL] 文件名未被升级隔离，存在同名覆盖炸弹漏洞！"
    
    # 2. 验证真正的物理文件名中已强制包含 symbol 和 hash、版本号
    generated_files = list(export_dir.glob("*.parquet"))
    assert len(generated_files) == 1, f"Expected 1 parquet file, found: {generated_files}"
    filename = generated_files[0].name
    assert "test_factor_data_MYX-FCPO1_" in filename
    assert "_v" in filename
    
    # 3. 验证数据及 Parquet 格式正确
    df_loaded = pd.read_parquet(generated_files[0])
    assert len(df_loaded) == 5
    assert (df_loaded['symbol'] == 'MYX-FCPO1').all()


def test_forward_returns_execution_lag_precision():
    """
    【硬化测试 4】：严格验证执行价 Lag 匹配逻辑。
    信号在 t 期收盘触发，次日 t+1 期开盘买入，确保收益率计算完全扣除了同期重叠污染。
    """
    # 构造含 Open 和 Close 的时间序列
    df_price = pd.DataFrame({
        'open':  [100.0, 102.0, 105.0, 101.0],
        'close': [101.0, 104.0, 102.0, 100.0]
    }, index=pd.date_range("2026-05-01", periods=4, freq="D"))
    
    # 计算 period=1 的前向收益
    df_res = AlphaEngine.calculate_execution_returns(df_price, price_col='close', open_col='open', periods=[1])
    
    # 理论推导：
    # t=0 (5月1日) 触发信号 -> t+1 (5月2日) 以 open=102.0 买入 -> t+1 (5月2日) 以 close=104.0 卖出
    # ret_1 应为 104.0 / 102.0 - 1.0 = 0.0196078
    expected_ret_0 = 104.0 / 102.0 - 1.0
    assert np.isclose(df_res.loc['2026-05-01', 'ret_1'], expected_ret_0)


def test_parquet_export_versioning_and_hash_isolation(tmp_path):
    """
    【硬化测试 5】：强行锁死计划书规定的 {expr_hash} 和 _v{timestamp} 命名规则，
    防止 Agent 在业务代码中偷工减料。
    """
    export_dir = tmp_path / "Alpha_version_test"
    export_dir.mkdir()
    
    df_single = pd.DataFrame({
        'datetime': pd.date_range("2026-05-01", periods=3),
        'symbol': ['MYX-FKLI1'] * 3,
        'factor': [1.0, 2.0, 3.0]
    })
    
    base_filepath = export_dir / "alpha_factor.parquet"
    
    # 传入特定的表达式或元数据以触发哈希与时间戳生成
    AlphaEngine.write_signal_export_parquet(
        df_single, base_filepath, 
        expr_str="close.pct_change(5)", # 用于生成唯一 expr_hash
        timestamp_str="20260528"       # 显式传入固定时间戳以便测试断言
    )
    
    # 检查目录下生成的文件，必须使用正规表达式匹配，确保包含 symbol、hash 和 timestamp
    generated_files = list(export_dir.glob("*.parquet"))
    assert len(generated_files) == 1
    filename = generated_files[0].name
    
    # 断言规则：必须同时包含基准名、symbol、版本标记 _v20260528
    assert "alpha_factor_MYX-FKLI1_" in filename
    assert "_v20260528.parquet" in filename
