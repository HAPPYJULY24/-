import pytest
import pandas as pd
import numpy as np
from logic.risk_manager_interceptor import RiskManager as Interceptor, RiskConfig
from src.core.models.order import OrderRequest
from ui.widgets.risk_dashboard_charts import RiskDashboardCharts
from PyQt6.QtWidgets import QApplication
import sys

def test_negative_free_margin_floor_division_clamp():
    """验证可用资金为负数时，Layer 3 不会通过地板除计算出负数成交手数。"""
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
    
    # 模拟穿仓前夕，equity=2000.0, used_margin=5000.0, 导致 free_margin = -3000.0
    rm.state.equity = 2000.0
    rm.state.used_margin = 5000.0
    rm.state.current_pos = 1
    
    # 发送一个增仓或开新仓申请
    order = OrderRequest(
        symbol="FCPO",
        volume=1,
        direction=1,
        price=4000.0,
        atr=2.0
    )
    
    # 验证 Layer 3 的拦截情况：应安全拒绝，绝对不能调整出负的手数 (如 -1 手)
    response = rm.validate_order(order)
    assert response.approved == False
    assert response.adjusted_volume == 0
    assert "Margin Call" in response.reason or "Zero affordable lots" in response.reason


def test_is_exit_reversal_leakage_protection():
    """验证当 is_exit 订单规模超过持仓或方向不对时，会被裁剪到持仓额度，绝对无法开出反向新仓。"""
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
    
    # 当前持仓: Short -2手
    rm.state.current_pos = -2
    rm.state.used_margin = 10000.0
    
    # 场景 1：以 is_exit=True 提交 LONG 买入 10手 (试图反向开仓 8手)
    order_excessive = OrderRequest(
        symbol="FCPO",
        volume=10,
        direction=1, # LONG
        price=4000.0,
        atr=2.0,
        is_exit=True
    )
    
    response = rm.validate_order(order_excessive)
    assert response.approved == True
    # 验证交易量被裁剪为 2手，仅仅用于平仓，禁止额外开仓
    assert response.adjusted_volume == 2
    assert rm.state.current_pos == 0
    assert rm.state.used_margin == 0.0
    
    # 场景 2：空仓状态下发送 exit 订单，应直接拒绝
    rm.state.current_pos = 0
    order_empty = OrderRequest(
        symbol="FCPO",
        volume=2,
        direction=1,
        price=4000.0,
        atr=2.0,
        is_exit=True
    )
    response_empty = rm.validate_order(order_empty)
    assert response_empty.approved == False
    assert "No active position" in response_empty.reason


def test_bankrupt_position_sizing_protection():
    """验证爆仓账户(equity <= 0)在 Layer 2 测算时不会开出 1 手的保底手数。"""
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
    
    # 强行设置负净值 (爆仓账户)
    rm.state.equity = -500.0
    
    order = OrderRequest(
        symbol="FCPO",
        volume=5,
        direction=1,
        price=4000.0,
        atr=2.0
    )
    
    # Layer 2 应该立即拦截并判定 target_lots 为 0 或 Reject
    response = rm.validate_order(order)
    assert response.approved == False
    assert response.adjusted_volume == 0


def test_ui_charts_event_swallowing_and_cleanup():
    """验证 PyQtGraph 双轴图表已关闭交互，防止事件吞噬，并且销毁时可以安全释放。"""
    app = QApplication.instance() or QApplication(sys.argv)
    
    charts = RiskDashboardCharts()
    
    # 验证 p2 初始化后被正确设置为不可交互，防止吞噬底层 PlotItem 的拖动事件
    base_df = pd.DataFrame({'equity': [100000.0, 102000.0]}, index=pd.date_range("2026-05-01", periods=2))
    audit_df = pd.DataFrame({'equity': [100000.0, 95000.0], 'used_margin': [5000.0, 5000.0]}, index=pd.date_range("2026-05-01", periods=2))
    
    charts.update_chart(base_df, audit_df)
    
    assert charts.p2 is not None
    # 确认 p2 的交互属性已被关闭
    assert charts.p2.handleMouse == False or not charts.p2.isInteractive()
    
    # 销毁测试
    charts.deleteLater()
