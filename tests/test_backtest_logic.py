import pytest
import pandas as pd
import numpy as np
from src.core.engines.bt_vectorized import VectorizedBacktest
from src.core.engines.bt_event_driven import EventDrivenBacktest
from logic.risk_manager_interceptor import RiskManager, RiskConfig
from src.core.models.trade import TradeDirection

def test_vectorized_lookahead_bias_elimination():
    """
    Verify that vectorized stop loss look-ahead bias is eliminated
    and the Next Open execution mode PnL calculations are correct.
    """
    dates = pd.date_range(start="2026-01-01", periods=5, freq="D")
    df = pd.DataFrame({
        'open': [100.0, 105.0, 102.0, 108.0, 110.0],
        'high': [102.0, 106.0, 103.0, 109.0, 112.0],
        'low':  [99.0,  101.0, 95.0,  106.0, 108.0],  # 01-03 低点为 95.0
        'close': [101.0, 103.0, 98.0,  107.0, 111.0], # 01-02 收盘 103.0 进场
        'factor': [0.0, 0.0, 0.0, 0.0, 0.0],
        'atr': [2.0, 2.0, 2.0, 2.0, 2.0],
        'adx': [25.0, 25.0, 25.0, 25.0, 25.0]
    }, index=dates)
    
    # 01-02 产生信号，01-03至01-05为持仓状态
    df['pos'] = [0, 0, 1, 1, 1] 
    
    engine = VectorizedBacktest()
    
    res_close = engine._apply_stop_loss(
        df=df,
        execution_mode='Close',
        multiplier=25.0,
        sl_pct=2.0  # 止损线 100.94
    )
    
    # 01-03 作为持仓第一天，日内触及 95.0，必须正确触发止损！
    assert res_close.loc[dates[2], 'is_sl_triggered'] == True, \
        "Critical Bug: Stop loss was suppressed on the first holding bar!"
        
    # 验证信号激发当天（01-02）是否安全（没有被赋予止损触发）
    assert res_close.loc[dates[1], 'is_sl_triggered'] == False, \
        "Look-ahead Bias: Stop loss triggered on the signal generation bar itself!"

    # 2. Next Open Mode Verification
    df_open = df.copy()
    df_open['pos'] = [0, 0, 0, 1, 1]  # Next Open 模式下 T+2 开始显示持仓
    
    res_open = engine._apply_stop_loss(
        df=df_open,
        execution_mode='Next Open',
        multiplier=25.0,
        sl_pct=2.0  # 止损线 102.0 * 0.98 = 99.96
    )
    
    # 01-04 是持仓第一天，实际进场开盘价必须为 01-03 的开盘价 102.0
    assert res_open.loc[dates[3], 'entry_price'] == 102.0, \
        f"Next Open entry price error: expected 102.0, got {res_open.loc[dates[3], 'entry_price']}"
        
    # 01-04 的首日标准持仓收益 (open[3] - entry_price) * mult * pos = (108 - 102) * 25 * 1 = 150.0
    # 它不应该被抹平为 0
    assert res_open.loc[dates[3], 'normal_pnl'] == 150.0, \
        f"Next Open first bar PnL error: expected 150.0, got {res_open.loc[dates[3], 'normal_pnl']}"


def test_event_driven_short_position_stop_loss():
    """
    Verify that short positions are correctly synchronized with the Risk Manager
    as negative integers, preventing ghost stop-outs.
    """
    # Create test dataset where price goes down (profitable for a short position)
    dates = pd.date_range(start="2026-01-01", periods=5, freq="D")
    df = pd.DataFrame({
        'open': [100.0, 105.0, 102.0, 98.0, 95.0],
        'high': [101.0, 106.0, 103.0, 99.0, 96.0],
        'low':  [99.0,  101.0, 95.0,  94.0, 92.0],
        'close': [101.0, 103.0, 98.0,  95.0, 93.0],
        'factor': [0.0, 0.0, 0.0, 0.0, 0.0],
        'atr': [2.0, 2.0, 2.0, 2.0, 2.0],
        'adx': [25.0, 25.0, 25.0, 25.0, 25.0]
    }, index=dates)
    
    # Trigger a short signal
    df['signal'] = [0, -1, 0, 0, 0]
    
    engine = EventDrivenBacktest()
    cfg = RiskConfig(
        initial_capital=100000.0,
        initial_margin=5000.0,
        risk_target_pct=2.0,
        max_position_size=20,
        multiplier=25.0,
        adx_filter_enabled=False
    )
    
    res = engine.run(
        df=df.copy(),
        asset_symbol="TEST",
        RiskManagerClass=lambda *a, **kw: RiskManager(cfg),
        multiplier=25.0,
        commission=15.0,
        slippage=0.0,
        initial_capital=100000.0,
        initial_margin=5000.0,
        allow_lunch=True,
        allow_overnight=True,
        execution_mode='Close',
        risk_params={'max_lots': 2, 'sl_pct': 5.0}  # 5% stop loss
    )
    
    trades = res['trades']
    
    # If the ghost stop loss is fixed, the short position should NOT trigger 'Intra_SL'
    # because the market price went down. It should hold the position and close by Signal/End of Data.
    assert len(trades) == 1, "Expected exactly 1 trade to be recorded."
    assert trades.iloc[0]['exit_reason'] in ["Signal_Close", "End_Of_Data"], \
        f"[Ghost Stop Loss Check] Short position incorrectly triggered stop out: {trades.iloc[0]['exit_reason']}!"


def test_double_sided_friction_costs():
    """
    Verify that double-sided friction costs are correctly charged
    on entry and exit, and logged correctly.
    """
    dates = pd.date_range(start="2026-01-01", periods=3, freq="D")
    df = pd.DataFrame({
        'open': [100.0, 102.0, 105.0],
        'high': [101.0, 103.0, 106.0],
        'low':  [99.0,  101.0, 104.0],
        'close': [101.0, 103.0, 105.0],
        'factor': [0.0, 0.0, 0.0],
        'atr': [2.0, 2.0, 2.0],
        'adx': [25.0, 25.0, 25.0]
    }, index=dates)
    
    # LONG Entry signal on 01-01 -> Entry at 102.0 on 01-02.
    # Exit signal on 01-02 -> Exit at 105.0 on 01-03.
    df['signal'] = [1, 0, 0] 
    
    engine = EventDrivenBacktest()
    cfg = RiskConfig(
        initial_capital=100000.0,
        initial_margin=5000.0,
        risk_target_pct=2.0,
        max_position_size=20,
        multiplier=25.0,
        adx_filter_enabled=False
    )
    
    commission = 15.0
    slippage = 1.0  # Slippage absolute cost per lot = slippage * multiplier = 25.0
    # Single-side cost = 15.0 + 25.0 = 40.0 per lot
    # For 2 lots, single-side cost = 80.0
    # Double-side cost = 160.0
    
    res = engine.run(
        df=df.copy(),
        asset_symbol="TEST",
        RiskManagerClass=lambda *a, **kw: RiskManager(cfg),
        multiplier=25.0,
        commission=commission,
        slippage=slippage,
        initial_capital=100000.0,
        initial_margin=5000.0,
        allow_lunch=True,
        allow_overnight=True,
        execution_mode='Close',
        risk_params={'max_lots': 2}
    )
    
    trades = res['trades']
    assert len(trades) == 1
    trade = trades.iloc[0]
    
    # Entry=102.0, Exit=105.0, Lots=2, Mult=25.0
    # Gross PnL = (105.0 - 102.0) * 25.0 * 2 = 150.0
    # Net PnL = 150.0 - double_side_cost (160.0) = -10.0
    assert trade['net_pnl'] == -10.0, f"Expected net PnL of -10.0, got {trade['net_pnl']}"
    
    # Verify settled Net Profit
    assert res['metrics']['Total Net Profit'] == -10.0, f"Expected settled Net Profit of -10.0, got {res['metrics']['Total Net Profit']}"


def test_maintenance_margin_buffer():
    """
    Verify that account is NOT liquidated when equity drops below initial margin
    but stays above maintenance margin (80% of initial margin).
    Verify that liquidation triggers when equity drops below maintenance margin.
    """
    cfg = RiskConfig(
        initial_capital=10000.0,
        initial_margin=5000.0,
        risk_target_pct=10.0,
        max_position_size=1,
        multiplier=1.0,
        adx_filter_enabled=False
    )
    
    rm = RiskManager(cfg)
    # Sync a position with large used margin (e.g., 12000) to isolate margin check from drawdown check
    # Initial margin required: 12000.0. Maintenance margin: 9600.0.
    
    # 1. Equity = 9700.0 (Below 12000.0, but above 9600.0, drawdown = 3% < 20%)
    rm.sync_account_state(balance=9700.0, equity=9700.0, current_pos=1, used_margin=12000.0)
    assert rm.state.is_liquidated == False, "Account should not be liquidated in the maintenance cushion zone!"
    
    # 2. Equity = 9500.0 (Below 9600.0, drawdown = 5% < 20%)
    rm.sync_account_state(balance=9500.0, equity=9500.0, current_pos=1, used_margin=12000.0)
    assert rm.state.is_liquidated == True, "Account must be liquidated when equity falls below maintenance margin!"
    assert "Margin Call Liquidation" in rm.state.liquidation_reason or "Margin Call" in rm.state.liquidation_reason


def test_drawdown_circuit_breakers():
    """
    Verify peak drawdown (>35%) and daily drawdown (>20%) circuit breakers,
    and verify Gap Risk protection using yesterday's closing equity.
    """
    cfg = RiskConfig(
        initial_capital=100000.0,
        initial_margin=5000.0,
        risk_target_pct=1.0,
        max_position_size=20,
        multiplier=1.0,
        adx_filter_enabled=False
    )
    
    rm = RiskManager(cfg)
    
    # --- PART A: Peak Drawdown ---
    # Day 1: Equity reaches 200,000
    rm.sync_account_state(balance=200000.0, equity=200000.0, current_pos=0, used_margin=0.0, current_date=pd.Timestamp("2026-01-01"))
    # Equity drops to 129,000 (drawdown = (200k - 129k)/200k = 35.5% > 35%)
    rm.sync_account_state(balance=129000.0, equity=129000.0, current_pos=0, used_margin=0.0, current_date=pd.Timestamp("2026-01-01"))
    assert rm.state.is_liquidated == True
    assert "Peak Drawdown Breach" in rm.state.liquidation_reason
    
    # --- PART B: Daily Drawdown & Gap Risk ---
    rm2 = RiskManager(cfg)
    
    # Day 1: Equity starts at 100,000, ends at 90,000
    rm2.sync_account_state(balance=100000.0, equity=100000.0, current_pos=0, used_margin=0.0, current_date=pd.Timestamp("2026-01-01 09:00:00"))
    rm2.sync_account_state(balance=90000.0, equity=90000.0, current_pos=0, used_margin=0.0, current_date=pd.Timestamp("2026-01-01 17:00:00"))
    
    # Day 2: Day opens. If there is a massive overnight gap risk:
    # Say, first bar equity drops to 70,000 (from yesterday's close of 90,000).
    # Daily drawdown should be calculated using yesterday's close (90,000) as daily baseline!
    # Daily drawdown = (90,000 - 70,000) / 90,000 = 22.2% > 20%
    rm2.sync_account_state(balance=70000.0, equity=70000.0, current_pos=0, used_margin=0.0, current_date=pd.Timestamp("2026-01-02 09:00:00"))
    assert rm2.state.is_liquidated == True
    assert "Daily Drawdown Breach" in rm2.state.liquidation_reason


def test_non_datetime_index_ui_processing():
    """
    Verify that _get_timestamps in RiskDashboardCharts fallback works perfectly 
    on non-DatetimeIndex (e.g. RangeIndex or string indices), returning raw integers.
    """
    from ui.widgets.risk_dashboard_charts import RiskDashboardCharts
    
    # 1. Datetime Index Case
    dates = pd.date_range(start="2026-01-01", periods=3, freq="D")
    df_dt = pd.DataFrame({'equity': [100, 101, 102]}, index=dates)
    
    # 2. String Index Case
    df_str = pd.DataFrame({'equity': [100, 101, 102]}, index=['bar_1', 'bar_2', 'bar_3'])
    
    # 3. Integer RangeIndex Case
    df_int = pd.DataFrame({'equity': [100, 101, 102]})
    
    # Instantiate widget python object securely without requiring active Qt loop
    chart = RiskDashboardCharts.__new__(RiskDashboardCharts)
    
    ts_dt = chart._get_timestamps(df_dt)
    assert len(ts_dt) == 3
    assert isinstance(ts_dt, np.ndarray)
    assert ts_dt[0] == 1767225600
    
    ts_str = chart._get_timestamps(df_str)
    assert len(ts_str) == 3
    assert (ts_str == np.array([0, 1, 2])).all(), "Expected standard integer range fallback for string index!"
    
    ts_int = chart._get_timestamps(df_int)
    assert len(ts_int) == 3
    assert (ts_int == np.array([0, 1, 2])).all(), "Expected standard integer range fallback for RangeIndex!"
