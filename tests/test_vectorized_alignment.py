import pytest
import pandas as pd
import numpy as np
from src.core.engines.bt_vectorized import VectorizedBacktest

def test_next_open_first_bar_stop_loss():
    """
    Verify that under Next Open mode, the first bar of the trade (T+1) 
    is correctly monitored for stop loss breaches using the entry bar's low/high,
    and PnL calculations are aligned mathematically.
    """
    dates = pd.date_range(start="2026-05-01", periods=4, freq="D")
    
    # Let's set up a scenario:
    # Bar 0 (T=0): Signal generated (signal = 1, pos_raw = 1)
    # Bar 1 (T=1): Next Open entry at open[1] = 100.0.
    #              During T=1, price crashes to low[1] = 95.0.
    #              If sl_pct is 2.0%, the stop loss price is 100.0 * 0.98 = 98.0.
    #              The stop loss MUST trigger at Bar 1 (T=1) because low[1] = 95.0 < 98.0!
    df = pd.DataFrame({
        'open': [100.0, 100.0, 90.0, 92.0],
        'high': [102.0, 101.0, 91.0, 93.0],
        'low':  [99.0,  95.0,  88.0, 91.0],  # Bar 1 low is 95.0, below stop loss 98.0
        'close': [100.0, 96.0,  89.0, 92.0],
        'factor': [0.0, 0.0, 0.0, 0.0],
        'atr': [2.0, 2.0, 2.0, 2.0],
        'adx': [25.0, 25.0, 25.0, 25.0]
    }, index=dates)
    
    df['pos_raw'] = [1, 1, 1, 1]  # Raw signal is active from Bar 0
    
    engine = VectorizedBacktest()
    
    # Run _calculate_pnl directly to test Next Open shift alignment
    df_pnl = engine._calculate_pnl(
        df=df.copy(),
        execution_mode='Next Open',
        multiplier=25.0,
        sl_pct=2.0, # 2.0% stop loss
        commission=15.0,
        slippage=0.0
    )
    
    # 1. Position alignment check: T+1 (Bar 1) should have active pos = 1 (shifted by 1 from pos_raw[0])
    assert df_pnl.loc[dates[1], 'pos'] == 1, \
        f"Position at Bar 1 should be 1, got {df_pnl.loc[dates[1], 'pos']}"
        
    # 2. Stop loss trigger check: Bar 1 (first holding bar) must trigger stop loss!
    assert df_pnl.loc[dates[1], 'exit_type'] == 'Intra-bar SL', \
        f"Exit type at Bar 1 should be 'Intra-bar SL', got {df_pnl.loc[dates[1], 'exit_type']}"
    assert df_pnl.loc[dates[1], 'is_sl_triggered'] == True, \
        "Stop loss was not triggered on the first holding bar T+1!"
        
    # 3. PnL calculation check: Since it triggered SL, the realized PnL should be based on stop price 98.0
    # Entry price at Bar 1: open[1] = 100.0. Stop price: 98.0.
    # PnL = (98.0 - 100.0) * 25.0 * 1 = -50.0.
    assert df_pnl.loc[dates[1], 'gross_pnl'] == -50.0, \
        f"Realized PnL at Bar 1 should be -50.0, got {df_pnl.loc[dates[1], 'gross_pnl']}"
        
    # 4. Subsequent bars (Bar 2+) should be stopped out (pos = 0)
    assert df_pnl.loc[dates[2], 'pos'] == 0, \
        f"Position at Bar 2 should be 0 (stopped out), got {df_pnl.loc[dates[2], 'pos']}"
    assert df_pnl.loc[dates[2], 'exit_type'] == 'Post SL', \
        f"Exit type at Bar 2 should be 'Post SL', got {df_pnl.loc[dates[2], 'exit_type']}"
