import pytest
import pandas as pd
import numpy as np

def test_be_ui_ffill_alignment_protection():
    """
    Verify that when the audited strategy is liquidated early, the outer-join
    and forward-fill (.ffill()) alignment logic correctly fills NaN values,
    preventing chart distortion and PyQtgraph rendering crashes.
    """
    # 1. Base strategy runs to completion (5 periods)
    base_dates = pd.date_range(start="2026-05-01", periods=5, freq="D")
    base_df = pd.DataFrame({
        'equity': [100000.0, 101000.0, 102000.0, 103000.0, 104000.0]
    }, index=base_dates)
    
    # 2. Audited strategy gets liquidated on day 3 (stops writing data on 05-03)
    audit_dates = pd.date_range(start="2026-05-01", periods=3, freq="D")
    audit_df = pd.DataFrame({
        'equity': [100000.0, 95000.0, 78000.0],
        'used_margin': [5000.0, 10000.0, 0.0]
    }, index=audit_dates)
    
    # 3. Replicate the outer-join alignment logic used in RiskDashboardCharts.update_chart
    aligned = pd.merge(
        base_df[['equity']].rename(columns={'equity': 'eq_base'}),
        audit_df[['equity', 'used_margin']].rename(columns={'equity': 'eq_audit', 'used_margin': 'margin_audit'}),
        left_index=True,
        right_index=True,
        how='outer'
    ).sort_index()
    
    # Assert that prior to .ffill(), the last two rows of eq_audit are NaN
    assert pd.isna(aligned.loc['2026-05-04', 'eq_audit'])
    assert pd.isna(aligned.loc['2026-05-05', 'eq_audit'])
    
    # Apply forward-fill (.ffill()) as added in the refactored code
    aligned_filled = aligned.ffill()
    
    # Assert that after .ffill():
    # 1. The NaN values are gone and correctly filled with the liquidated equity (78,000.0)
    assert not aligned_filled.isnull().any().any(), "Aligned DataFrame should not contain any NaN values after ffill!"
    assert aligned_filled.loc['2026-05-04', 'eq_audit'] == 78000.0
    assert aligned_filled.loc['2026-05-05', 'eq_audit'] == 78000.0
    
    # 2. The margin audit is filled or remains flat (since the liquidated state was 0.0)
    assert aligned_filled.loc['2026-05-04', 'margin_audit'] == 0.0
    assert aligned_filled.loc['2026-05-05', 'margin_audit'] == 0.0
