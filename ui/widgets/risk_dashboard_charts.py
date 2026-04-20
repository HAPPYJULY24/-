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
        """Convert DataFrame index to Unix timestamp array."""
        if df.empty:
            return np.array([])
        return df.index.astype(np.int64).values // 10**9
    
    def update_chart(self, base_df, audit_df):
        """
        Update risk comparison chart.
        
        Args:
            base_df: DataFrame with 'equity' column (base strategy, no risk manager)
            audit_df: DataFrame with 'equity' and 'used_margin' columns (audited strategy)
        """
        # Clear existing plots
        self.plot_widget.clear()
        
        x_base = self._get_timestamps(base_df)
        x_audit = self._get_timestamps(audit_df)
        
        # 1. Base Equity (Gray Dashed Line)
        if not base_df.empty:
            self.plot_widget.plot(
                x=x_base,
                y=base_df['equity'].values,
                pen=pg.mkPen(color='#666666', width=1, style=Qt.PenStyle.DashLine),
                name="Base Strategy"
            )
        
        # 2. Audited Equity (Green Solid Line)
        if not audit_df.empty:
            self.plot_widget.plot(
                x=x_audit,
                y=audit_df['equity'].values,
                pen=pg.mkPen(color='#4CAF50', width=2),
                name="Audited Strategy"
            )
        
        # 3. Setup Right Axis for Margin
        p1 = self.plot_widget.getPlotItem()
        
        if self.p2 is None:
            # First time: create second ViewBox
            self.p2 = pg.ViewBox()
            p1.showAxis('right')
            p1.scene().addItem(self.p2)
            p1.getAxis('right').linkToView(self.p2)
            p1.getAxis('right').setLabel('Used Margin (RM)', color='#2196F3')
            self.p2.setXLink(p1)
            
            # Connect resize signal
            def updateViews():
                self.p2.setGeometry(p1.vb.sceneBoundingRect())
                self.p2.linkedViewChanged(p1.vb, self.p2.XAxis)
            
            p1.vb.sigResized.connect(updateViews)
        else:
            # Clear previous margin plots
            self.p2.clear()
        
        # 4. Plot Margin on Right Axis
        if not audit_df.empty and 'used_margin' in audit_df.columns:
            margin = audit_df['used_margin'].values
            equity = audit_df['equity'].values
            
            # Blue Fill: Normal Margin Usage
            margin_curve = pg.PlotCurveItem(x=x_audit, y=margin, pen=pg.mkPen(color='#2196F3', width=1))
            margin_fill = pg.FillBetweenItem(
                curve1=margin_curve,
                curve2=pg.PlotCurveItem(x=x_audit, y=np.zeros(len(x_audit)), pen=None),
                brush=pg.mkBrush(color=(33, 150, 243, 50))  # Blue with alpha
            )
            self.p2.addItem(margin_fill)
            
            # Red Fill: Stress Zone (Margin/Equity > 80%)
            utilization = np.zeros_like(equity)
            mask = equity != 0
            utilization[mask] = margin[mask] / equity[mask]
            
            stress_y = margin.copy()
            stress_y[utilization <= 0.8] = 0  # Hide normal usage
            
            stress_curve = pg.PlotCurveItem(x=x_audit, y=stress_y, pen=None)
            stress_fill = pg.FillBetweenItem(
                curve1=stress_curve,
                curve2=pg.PlotCurveItem(x=x_audit, y=np.zeros(len(x_audit)), pen=None),
                brush=pg.mkBrush(color=(255, 82, 82, 100))  # Red with alpha
            )
            self.p2.addItem(stress_fill)
            
            # Force geometry update
            self.p2.setGeometry(p1.vb.sceneBoundingRect())
            self.p2.linkedViewChanged(p1.vb, self.p2.XAxis)
    
    def clear(self):
        """Clear all charts."""
        self.plot_widget.clear()
        if self.p2 is not None:
            self.p2.clear()
