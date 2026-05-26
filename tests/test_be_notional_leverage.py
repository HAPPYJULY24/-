import pytest
from logic.risk_manager_interceptor import RiskManager, RiskConfig
from src.core.models.order import OrderRequest, OrderResponse

def test_be_notional_leverage_and_truncation():
    """
    Verify that the corrected Layer 4 leverage calculation uses notional exposure
    (abs(pos) * price * multiplier) / equity, and successfully triggers
    Smart Truncation to adjust volume to fit the leverage limit.
    """
    # Strategy DNA configuration
    cfg = RiskConfig(
        initial_capital=100000.0,      # Account equity = 100,000
        initial_margin=5000.0,         # Margin per lot = 5,000
        risk_target_pct=10.0,          # High risk per trade to allow large target lots
        max_position_size=20,          # Max size cap = 20
        multiplier=25.0,               # Contract multiplier (FCPO = 25.0)
        adx_filter_enabled=False,      # Disable ADX filter for simplicity
        leverage_limit=2.0             # Limit leverage strictly to 2.0x Notional Value
    )
    
    rm = RiskManager(cfg)
    
    # Assert initial state
    assert rm.state.equity == 100000.0
    assert rm.state.free_margin == 100000.0
    
    # Test Scenario A: Small order that doesn't breach the limit
    # Notional value of 1 lot = 1 * 4000.0 * 25.0 = 100,000.0 (1.0x leverage relative to 100k equity)
    order_small = OrderRequest(
        symbol="FCPO",
        volume=1,
        direction=1, # LONG
        price=4000.0,
        atr=2.0
    )
    
    response_small = rm.validate_order(order_small)
    assert response_small.approved == True
    assert response_small.adjusted_volume == 1, "Should approve 1 lot (1.0x leverage) since it is <= 2.0x limit"
    
    # Reset State for the next test
    rm.state.current_pos = 0
    rm.state.used_margin = 0.0
    rm.audit_log = []
    
    # Test Scenario B: Massive order that breaches leverage limit (5 lots = 5.0x leverage)
    # Target is 5 lots (Notional exposure = 500,000.0, which is 5.0x leverage)
    # The leverage limit is 2.0x, which corresponds to exactly 2 lots.
    # The interceptor should dynamically truncate 5 lots down to 2 lots!
    order_large = OrderRequest(
        symbol="FCPO",
        volume=5,
        direction=1, # LONG
        price=4000.0,
        atr=2.0
    )
    
    response_large = rm.validate_order(order_large)
    assert response_large.approved == True
    assert response_large.adjusted_volume == 2, \
        f"Leverage check failed: expected 5 lots to be truncated to 2 lots, got {response_large.adjusted_volume}"
        
    # Verify that the adjustment was properly logged in the audit trail with leverage limit context
    adjustments = [log for log in rm.audit_log if log['Type'] == 'Order_Adjusted']
    assert len(adjustments) == 1, "Should log exactly 1 adjustment in audit trail"
    assert "Leverage Breach" in adjustments[0]['Reason'], "Should indicate leverage breach prevention"
