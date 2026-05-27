import pytest
import pandas as pd
import numpy as np
from logic.risk_manager_interceptor import RiskManager as Interceptor, RiskConfig
from src.core.models.order import OrderRequest

def test_nan_swallowing_bypass_prevention():
    """Verify that any NaN parameters propagate correctly to trigger risk rejection rather than bypassing checks."""
    cfg = RiskConfig(
        initial_capital=100000.0,
        initial_margin=5000.0,
        risk_target_pct=1.0,
        max_position_size=20,
        multiplier=25.0,
        adx_filter_enabled=True,
        leverage_limit=2.0
    )
    rm = Interceptor(cfg)
    
    # 1. Test NaN ADX (Layer 1 Regime Bypass Prevention)
    order_nan_adx = OrderRequest(
        symbol="FCPO",
        volume=1,
        direction=1,
        price=4000.0,
        atr=2.0,
        adx=float('nan')
    )
    response = rm.validate_order(order_nan_adx)
    assert response.approved == False
    assert "ADX value is NaN" in response.reason
    
    # Disable ADX check for subsequent layers
    rm.config.adx_filter_enabled = False
    rm.params['use_adx'] = False
    
    # 2. Test NaN Equity (Layer 3 Margin & Layer 4 Leverage Bypass Prevention)
    rm.state.equity = float('nan')
    order_entry = OrderRequest(
        symbol="FCPO",
        volume=1,
        direction=1,
        price=4000.0,
        atr=2.0
    )
    response = rm.validate_order(order_entry)
    assert response.approved == False
    assert "NaN_EXPOSURE_BREACH" in response.reason

def test_integer_index_no_daily_reset():
    """Verify that integer indexes (non-datetime) do not trigger daily baseline reset on every single bar."""
    cfg = RiskConfig(
        initial_capital=100000.0,
        initial_margin=5000.0,
        risk_target_pct=1.0,
        max_position_size=20,
        multiplier=25.0,
        adx_filter_enabled=False,
        leverage_limit=2.0
    )
    rm = Interceptor(cfg)
    
    # Simulate passive state sync on consecutive integer-indexed bars
    # Bar 0
    rm.sync_account_state(balance=100000.0, equity=100000.0, current_pos=0, used_margin=0.0, current_date=0)
    assert rm._daily_baseline_equity == 100000.0
    
    # Bar 1 - equity drops, but it is same day (integer indexes are treated as same day / no reset)
    rm.sync_account_state(balance=95000.0, equity=95000.0, current_pos=0, used_margin=0.0, current_date=1)
    # If the bug is fixed, daily baseline remains 100000.0 (it does not reset to 95000.0 or 100000.0 from last bar)
    assert rm._daily_baseline_equity == 100000.0
    
    # Assert daily drawdown is computed against initial 100k, not reset bar-by-bar
    daily_dd = (rm._daily_baseline_equity - rm.state.equity) / rm._daily_baseline_equity
    assert daily_dd == 0.05  # 5% drawdown relative to start baseline

def test_state_mutation_overwrite_protection():
    """Verify that once liquidated, subsequent passive syncs do not overwrite the original liquidation reason."""
    cfg = RiskConfig(
        initial_capital=100000.0,
        initial_margin=5000.0,
        risk_target_pct=1.0,
        max_position_size=20,
        multiplier=25.0,
        adx_filter_enabled=False,
        leverage_limit=2.0
    )
    rm = Interceptor(cfg)
    
    # Simulate a Margin Call Liquidation without triggering peak drawdown breach
    rm.state.equity = 3000.0
    rm.state.used_margin = 10000.0 # margin_level = 3000 / (10000 * 0.8) = 3000 / 8000 = 0.375 < 1.0
    rm._high_water_mark = 3500.0
    rm._daily_baseline_equity = 3500.0
    rm._last_bar_equity = 3500.0
    
    rm.sync_account_state(balance=10000.0, equity=3000.0, current_pos=2, used_margin=10000.0, current_date=pd.Timestamp("2026-05-01"))
    
    assert rm.state.is_liquidated == True
    assert "Margin Call Liquidation" in rm.state.liquidation_reason
    original_reason = rm.state.liquidation_reason
    
    # Subsequent passive state sync (with a drawdown that would trigger peak drawdown breach)
    rm.sync_account_state(balance=10000.0, equity=2000.0, current_pos=0, used_margin=0.0, current_date=pd.Timestamp("2026-05-02"))
    
    # The reason must remain the original Margin Call Liquidation reason, not overwritten by Drawdown Breach
    assert rm.state.is_liquidated == True
    assert rm.state.liquidation_reason == original_reason
