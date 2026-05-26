import pytest
import pandas as pd
import numpy as np
from src.core.engines.bt_event_driven import EventDrivenBacktest
from logic.risk_manager_interceptor import RiskManager, RiskConfig

def test_be_event_driven_close_mode_execution_price():
    """
    Verify that in Event-Driven engine, under 'Close' execution mode:
    - Fills occur strictly at the closing price of the previous bar (the signal bar close).
    - Under 'Next Open' execution mode, fills occur strictly at the opening price of the next bar.
    """
    dates = pd.date_range(start="2026-05-01", periods=4, freq="D")
    
    # Dataset with 4 periods to satisfy the single-bar minimum holding time constraint (i > entered_this_bar_index)
    # Bar 0 (05-01): Close = 101.0. LONG signal (1) generated.
    # Bar 1 (05-02): Open = 105.0, Close = 103.0. Keep holding (signal = 1). Entry executed here at Bar 1.
    # Bar 2 (05-03): Open = 108.0, Close = 106.0. CLOSE signal (0) generated.
    # Bar 3 (05-04): Open = 110.0, Close = 109.0. Close executed here at Bar 3.
    df = pd.DataFrame({
        'open': [100.0, 105.0, 108.0, 110.0],
        'high': [102.0, 106.0, 109.0, 111.0],
        'low':  [99.0,  102.0, 106.0, 108.0],
        'close': [101.0, 103.0, 106.0, 109.0],
        'factor': [0.0, 0.0, 0.0, 0.0],
        'signal': [1, 1, 0, 0],
        'atr': [2.0, 2.0, 2.0, 2.0],
        'adx': [25.0, 25.0, 25.0, 25.0]
    }, index=dates)
    
    engine = EventDrivenBacktest()
    cfg = RiskConfig(
        initial_capital=100000.0,
        initial_margin=5000.0,
        risk_target_pct=2.0,
        max_position_size=20,
        multiplier=25.0,
        adx_filter_enabled=False
    )
    
    # -------------------------------------------------------------
    # 1. VERIFY CLOSE EXECUTION MODE (Should fill at previous close)
    # -------------------------------------------------------------
    res_close = engine.run(
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
        risk_params={'max_lots': 1}
    )
    
    trades_close = res_close['trades']
    assert len(trades_close) == 1
    trade_close = trades_close.iloc[0]
    
    # Signal generated at Bar 0 -> Entry at Close of Bar 0 = 101.0
    # Exit signal generated at Bar 2 -> Exit at Close of Bar 2 = 106.0
    assert trade_close['entry_price'] == 101.0, \
        f"Close mode entry price error: expected 101.0, got {trade_close['entry_price']}"
    assert trade_close['exit_price'] == 106.0, \
        f"Close mode exit price error: expected 106.0, got {trade_close['exit_price']}"
        
    # Gross PnL = (106.0 - 101.0) * 25.0 * 1 = 125.0
    # Net PnL = 125.0 - double cost (commission*2 = 30.0) = 95.0
    assert trade_close['net_pnl'] == 95.0, \
        f"Close mode Net PnL error: expected 95.0, got {trade_close['net_pnl']}"

    # -------------------------------------------------------------
    # 2. VERIFY NEXT OPEN EXECUTION MODE (Should fill at next open)
    # -------------------------------------------------------------
    res_open = engine.run(
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
        execution_mode='Next Open',
        risk_params={'max_lots': 1}
    )
    
    trades_open = res_open['trades']
    assert len(trades_open) == 1
    trade_open = trades_open.iloc[0]
    
    # Signal generated at Bar 0 -> Entry at Open of Bar 1 = 105.0
    # Exit signal generated at Bar 2 -> Exit at Open of Bar 3 = 110.0
    assert trade_open['entry_price'] == 105.0, \
        f"Next Open mode entry price error: expected 105.0, got {trade_open['entry_price']}"
    assert trade_open['exit_price'] == 110.0, \
        f"Next Open mode exit price error: expected 110.0, got {trade_open['exit_price']}"
        
    # Gross PnL = (110.0 - 105.0) * 25.0 * 1 = 125.0
    # Net PnL = 125.0 - 30.0 = 95.0
    assert trade_open['net_pnl'] == 95.0, \
        f"Next Open mode Net PnL error: expected 95.0, got {trade_open['net_pnl']}"
