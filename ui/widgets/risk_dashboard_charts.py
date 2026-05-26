"""
RiskDashboardCharts Widget - Risk Audit Visualization Component

Extracted from risk_tab.py to improve maintainability and reusability.
Phase 5B.2: UI Refactor conforming to Single Responsibility Principle.

Uses pyqtgraph for dual-axis plotting (Equity + Margin).
"""

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import QWidget, QVBoxLayout
import pyqtgraph as pg
import pandas as pd
import numpy as np


class RiskDashboardCharts(QWidget):
    """
    Reusable risk dashboard visualization widget.
    
    Provides dual-axis chart:
    - Left Axis: Base Strategy (gray dashed) vs Audited Strategy (green solid)
    - Right Axis: Used Margin (blue fill) with Stress Zone (red fill when >80% utilization)
    """
    
    def __init__(self):
        super().__init__()
        self._setup_chart()
    
    def _setup_chart(self):
        """Initialize pyqtgraph PlotWidget with dual axis."""
        layout = QVBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        
        # Create main plot widget
        self.plot_widget = pg.PlotWidget()
        self.plot_widget.setBackground('#1e1e1e')
        self.plot_widget.setLabel('left', 'Equity (RM)')
        self.plot_widget.setLabel('bottom', 'Time')
        self.plot_widget.addLegend()
        
        layout.addWidget(self.plot_widget)
        self.setLayout(layout)
        
        # Second viewbox for margin (right axis) - created on first plot
        self.p2 = None
    
    def _get_timestamps(self, df):
        """Convert DataFrame index to Unix timestamp array with fallback for non-datetime index."""
        if df.empty:
            return np.array([])
        if isinstance(df.index, pd.DatetimeIndex):
            try:
                ts = df.index.astype(np.int64) // 10**9
                return ts.values if hasattr(ts, 'values') else ts
            except Exception:
                pass
        return np.arange(len(df))
    
    def update_chart(self, base_df, audit_df):
        """
        Update risk comparison chart with outer-join timeline alignment to prevent
        physical stretching or distortion due to early strategy liquidation.
        
        Args:
            base_df: DataFrame with 'equity' column (base strategy)
            audit_df: DataFrame with 'equity' and 'used_margin' columns (audited strategy)
        """
        # Clear existing plots
        self.plot_widget.clear()
        
        if base_df.empty or audit_df.empty:
            return
            
        # 1. Align time series using Outer Join to keep timelines mathematically locked
        aligned = pd.merge(
            base_df[['equity']].rename(columns={'equity': 'eq_base'}),
            audit_df[['equity', 'used_margin']].rename(columns={'equity': 'eq_audit', 'used_margin': 'margin_audit'}),
            left_index=True,
            right_index=True,
            how='outer'
        ).sort_index().ffill()
        
        # 2. Extract synchronized time coordinates
        idx = aligned.index
        if isinstance(idx, pd.DatetimeIndex):
            try:
                ts = idx.astype(np.int64) // 10**9
                x_axis_data = ts.values if hasattr(ts, 'values') else ts
            except Exception:
                x_axis_data = np.arange(len(aligned))
        else:
            x_axis_data = np.arange(len(aligned))
            
        y_base_equity = aligned['eq_base'].values
        y_audit_equity = aligned['eq_audit'].values
        y_audit_margin = aligned['margin_audit'].values
        
        # 3. Plot Base Equity (Gray Dashed Line)
        self.plot_widget.plot(
            x=x_axis_data,
            y=y_base_equity,
            pen=pg.mkPen(color='#666666', width=1.5, style=Qt.PenStyle.DashLine),
            name="Base Strategy"
        )
        
        # 4. Plot Audited Equity (Green Solid Line)
        self.plot_widget.plot(
            x=x_axis_data,
            y=y_audit_equity,
            pen=pg.mkPen(color='#4CAF50', width=2),
            name="Audited Strategy"
        )
        
        # 5. Setup Right Axis for Margin ViewBox
        p1 = self.plot_widget.getPlotItem()
        
        if self.p2 is None:
            # First time: create second ViewBox
            self.p2 = pg.ViewBox()
            p1.showAxis('right')
            p1.scene().addItem(self.p2)
            p1.getAxis('right').linkToView(self.p2)
            p1.getAxis('right').setLabel('Used Margin (RM)', color='#2196F3')
            
            # Link to underlying ViewBox instead of PlotItem
            self.p2.setXLink(p1.vb)
            
            # Disable X mouse events, only allow Y control to prevent event blocking
            self.p2.setMouseEnabled(x=False, y=True)
            
            # Connect resize signal safely (GC-defended & signal accumulation proofed)
            try:
                p1.vb.sigResized.disconnect(self._update_right_axis_geometry)
            except TypeError:
                pass
            p1.vb.sigResized.connect(self._update_right_axis_geometry)
        else:
            # Clear previous margin plots
            self.p2.clear()
        
        # 6. Plot Margin on Right Axis
        # Safely convert nan values in margin (which can occur after outer join) to 0.0 for fills
        y_audit_margin_safe = np.nan_to_num(y_audit_margin, nan=0.0)
        
        # Blue Fill: Normal Margin Usage
        margin_curve = pg.PlotCurveItem(x=x_axis_data, y=y_audit_margin_safe, pen=pg.mkPen(color='#2196F3', width=1))
        margin_fill = pg.FillBetweenItem(
            curve1=margin_curve,
            curve2=pg.PlotCurveItem(x=x_axis_data, y=np.zeros(len(x_axis_data)), pen=None),
            brush=pg.mkBrush(color=(33, 150, 243, 50))  # Blue with alpha
        )
        self.p2.addItem(margin_fill)
        
        # Red Fill: Stress Zone (Margin/Equity > 80%)
        # Safely convert nan values in equity to 0.0
        y_audit_equity_safe = np.nan_to_num(y_audit_equity, nan=0.0)
        utilization = np.zeros_like(y_audit_equity_safe)
        mask = y_audit_equity_safe != 0
        utilization[mask] = y_audit_margin_safe[mask] / y_audit_equity_safe[mask]
        
        stress_y = y_audit_margin_safe.copy()
        stress_y[utilization <= 0.8] = 0.0  # Hide normal usage
        
        stress_curve = pg.PlotCurveItem(x=x_axis_data, y=stress_y, pen=None)
        stress_fill = pg.FillBetweenItem(
            curve1=stress_curve,
            curve2=pg.PlotCurveItem(x=x_axis_data, y=np.zeros(len(x_axis_data)), pen=None),
            brush=pg.mkBrush(color=(255, 82, 82, 100))  # Red with alpha
        )
        self.p2.addItem(stress_fill)
        
        # Force geometry update using member method
        self._update_right_axis_geometry()
    
    def clear(self):
        """Clear all charts."""
        self.plot_widget.clear()
        if self.p2 is not None:
            self.p2.clear()
            
    def _update_right_axis_geometry(self):
        """Synchronize the geometry of the right ViewBox to match the main PlotItem's ViewBox."""
        p1 = self.plot_widget.getPlotItem()
        if p1 is not None and self.p2 is not None:
            self.p2.setGeometry(p1.vb.sceneBoundingRect())
            self.p2.linkedViewChanged(p1.vb, self.p2.XAxis)
