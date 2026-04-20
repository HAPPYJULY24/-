"""
BacktestCharts Widget - Reusable Visualization Component

Extracted from backtest_tab.py to improve maintainability and reusability.
Phase 5B.1: UI Refactor符合Single Responsibility Principle.
"""

from PyQt6.QtWidgets import QWidget, QTabWidget
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
import matplotlib.pyplot as plt


class BacktestCharts(QTabWidget):
    """
    Reusable backtest visualization widget.
    
    Provides four chart tabs:
    1. Equity Curve - Shows equity growth and margin level
    2. Position Analysis - Step chart of position status
    3. Drawdown - Area chart of drawdown percentage
    4. Risk Indicators - ATR and ADX indicators
    """
    
    def __init__(self):
        super().__init__()
        self._setup_charts()
    
    def _setup_charts(self):
        """Initialize all chart tabs."""
        # 1. Equity Curve
        self.equity_fig = plt.figure()
        self.equity_canvas = FigureCanvas(self.equity_fig)
        self.addTab(self.equity_canvas, "Equity Curve")
        
        # 2. Net PnL Distribution
        self.dist_fig = plt.figure()
        self.dist_canvas = FigureCanvas(self.dist_fig)
        self.addTab(self.dist_canvas, "Net PnL Distribution")
        
        # 3. Drawdown
        self.dd_fig = plt.figure()
        self.dd_canvas = FigureCanvas(self.dd_fig)
        self.addTab(self.dd_canvas, "Drawdown")
        
        # 4. Risk Indicators
        self.risk_fig = plt.figure()
        self.risk_canvas = FigureCanvas(self.risk_fig)
        self.addTab(self.risk_canvas, "Risk Indicators")
    
    def update_equity_curve(self, results):
        """
        Update equity curve chart using MtM equity from the engine.
        
        Args:
            results: Backtest results dictionary containing 'equity_curve' DataFrame
        """
        self.equity_fig.clear()
        ax1 = self.equity_fig.add_subplot(111)
        
        df_full = results.get('equity_curve', None)
        
        if df_full is None or df_full.empty or 'equity' not in df_full.columns:
            ax1.set_title("Equity Curve (No Data)")
            self.equity_canvas.draw()
            return
        
        # Read initial capital from engine metrics (Fix Issue #4)
        initial_capital = results.get('metrics', {}).get(
            'Initial Capital', float(df_full['equity'].iloc[0]) or 100000.0)
        
        # Plot MtM equity curve (includes floating PnL)
        eq = df_full['equity'].values
        ax1.plot(df_full.index, eq, color='#4CAF50', linewidth=2, label='MtM Equity')
        
        # Plot initial capital reference line
        ax1.axhline(y=initial_capital, color='#888888', linestyle='--', 
                    linewidth=1, label=f'Initial Capital ({initial_capital:,.0f})', alpha=0.5)
        
        # Plot Maintenance Level (Red Line) if available
        if 'maint_level' in df_full.columns:
            ax1.plot(df_full.index, df_full['maint_level'], color='#FF5252', 
                    linestyle='--', linewidth=1, label='Margin Call Level', alpha=0.5)
        
        ax1.set_title("Mark-to-Market Equity Curve")
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        self.equity_fig.autofmt_xdate()
        self.equity_canvas.draw()
    
    def update_pnl_distribution(self, results):
        """
        Update Net PnL distribution chart (histogram).
        
        Args:
            results: Backtest results dictionary containing 'trade_log'
        """
        self.dist_fig.clear()
        ax2 = self.dist_fig.add_subplot(111)
        
        trade_log = results.get('trade_log', None)
        if trade_log is None or trade_log.empty:
            ax2.set_title("Net PnL Distribution (No Trades)")
            self.dist_canvas.draw()
            return
            
        pnl_data = trade_log['net_pnl']
        
        # Plot histogram
        n, bins, patches = ax2.hist(pnl_data, bins=30, edgecolor='black', alpha=0.7)
        
        # Color bins based on profit/loss
        for c, p in zip(bins, patches):
            if c >= 0:
                p.set_facecolor('#4CAF50')  # Green for profit
            else:
                p.set_facecolor('#FF5252')  # Red for loss
                
        # Draw vertical line at 0
        ax2.axvline(x=0, color='gray', linestyle='--', linewidth=2, label='Break Even (RM 0)')
        
        ax2.set_title("Net PnL Distribution")
        ax2.set_xlabel("Net PnL (RM)")
        ax2.set_ylabel("Frequency")
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        self.dist_canvas.draw()
    
    def update_drawdown(self, results):
        """
        Update drawdown chart using MtM equity curve from the engine.
        
        Uses the continuous equity series (with floating PnL) to compute
        drawdown at every bar, matching the MDD metric exactly.
        
        Args:
            results: Backtest results dictionary containing 'equity_curve' DataFrame
        """
        self.dd_fig.clear()
        ax3 = self.dd_fig.add_subplot(111)
        
        df_full = results.get('equity_curve', None)
        
        if df_full is None or df_full.empty or 'equity' not in df_full.columns:
            ax3.set_title("Drawdown (No Data)")
            self.dd_canvas.draw()
            return
        
        # Use pre-computed drawdown from engine if available, else compute
        if 'drawdown_pct' in df_full.columns:
            dd_pct = df_full['drawdown_pct'].values * 100  # Convert to percentage
        else:
            equity = df_full['equity']
            peak = equity.cummax()
            dd_pct = ((equity - peak) / peak * 100).values
        
        # Invert sign so drawdown shows as positive downward area
        dd_display = [-v for v in dd_pct]  # Make positive for fill_between
        
        # Fill area showing MtM drawdown
        ax3.fill_between(df_full.index, dd_display, 0, color='#FF5252', alpha=0.6)
        ax3.plot(df_full.index, dd_display, color='#D32F2F', linewidth=0.8)
        
        # Mark max drawdown point
        max_dd_idx = max(range(len(dd_display)), key=lambda i: dd_display[i])
        max_dd_val = dd_display[max_dd_idx]
        if max_dd_val > 0:
            ax3.annotate(f'Max DD: {max_dd_val:.2f}%',
                        xy=(df_full.index[max_dd_idx], max_dd_val),
                        fontsize=9, color='#D32F2F', fontweight='bold',
                        xytext=(0, 10), textcoords='offset points',
                        ha='center')
        
        ax3.set_title("Mark-to-Market Drawdown (%)")
        ax3.set_ylabel("Drawdown %")
        ax3.grid(True, alpha=0.3)
        self.dd_fig.autofmt_xdate()
        self.dd_canvas.draw()
    
    def update_risk_indicators(self, df):
        """
        Update ATR/ADX risk indicator charts.
        
        Args:
            df: DataFrame with 'atr' and/or 'adx' columns
        """
        self.risk_fig.clear()
        ax4 = self.risk_fig.add_subplot(111)
        
        has_risk_data = False
        
        # Plot ATR if available
        if 'atr' in df.columns:
            ax4.plot(df.index, df['atr'], label='ATR(14)', color='cyan', linewidth=1.5)
            has_risk_data = True
        
        # Plot ADX on second Y-axis if available
        if 'adx' in df.columns:
            ax4_2 = ax4.twinx()
            ax4_2.plot(df.index, df['adx'], label='ADX(14)', 
                      color='magenta', linestyle='--', linewidth=1.5)
            ax4_2.axhline(20, color='gray', linestyle=':', alpha=0.5)
            ax4_2.legend(loc='upper right')
            has_risk_data = True
        
        if has_risk_data:
            ax4.set_title("Risk Indicators (ATR & ADX)")
            ax4.legend(loc='upper left')
            ax4.grid(True, alpha=0.3)
            self.risk_fig.autofmt_xdate()
            self.risk_canvas.draw()
    
    def update_all_charts(self, results):
        """
        Convenience method to update all charts at once.
        
        Args:
            results: Dict with metrics, trade_log, and equity_curve dataframe
        """
        df = results.get('equity_curve', None)
        
        # Pass the full results dict to the PnL charts so they can use Trade_Log
        self.update_equity_curve(results)
        self.update_pnl_distribution(results)
        self.update_drawdown(results)
        
        # Background charts still rely on full df indexing
        if df is not None:
            self.update_risk_indicators(df)
