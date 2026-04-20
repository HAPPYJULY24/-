"""
Vectorized Backtest Module
Fast signal-based PnL calculation without RiskManager integration.
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, Optional


class VectorizedBacktest:
    """
    Vectorized backtesting engine for fast signal-based strategy evaluation.
    
    Uses numpy vectorization for maximum performance.
    No RiskManager integration - pure signal-driven execution.
    """
    
    def __init__(self):
        """Initialize vectorized backtest engine."""
        self.logger = logging.getLogger(__name__)
    
    def run(self, df: pd.DataFrame, multiplier: float, commission: float, slippage: float,
            initial_capital: float,
            initial_margin: float, maintenance_margin_rate: float = 0.8,
            allow_lunch: bool = True, allow_overnight: bool = True,
            execution_mode: str = 'Close', risk_target: float = 0.02, sl_pct: float = 0.0,
            max_lots: int = 20, pressure_test: bool = False) -> Dict:
        """
        Execute vectorized backtest.

        IMPORTANT: This engine is a pressure-test plugin.
        It returns only aggregate metrics (Net Profit, Drawdown, etc.).
        It does NOT produce trade-level data. Use EventDrivenBacktest for
        high-fidelity standard backtests.
        
        Args:
            df: DataFrame with 'close' and 'factor' columns
            multiplier: Contract multiplier (e.g., 25 for FCPO, 50 for FKLI)
            commission: Commission per lot
            slippage: Slippage in ticks
            initial_capital: Starting capital
            initial_margin: Initial margin per lot
            maintenance_margin_rate: Margin call threshold (default 0.8)
            allow_lunch: Allow positions during lunch hours
            allow_overnight: Allow overnight positions
            execution_mode: 'Close' or 'Next Open'
            risk_target: Risk percentage for position sizing (0 = fixed 1 lot)
            sl_pct: Stop loss percentage (0 = off)
            max_lots: Maximum lots per position
            pressure_test: Set to True when called from run_pressure_test dispatcher
        
        Returns:
            Dictionary with aggregate metrics only: metrics, signals.
            Does NOT contain 'trades' or detailed equity_curve.
        """
        if not pressure_test:
            self.logger.warning(
                "[VECTORIZED] Called without pressure_test=True. "
                "This engine must only be used for slippage pressure scanning. "
                "Use EventDrivenBacktest for standard backtests."
            )
        
        # 1. Prepare Data
        df = self._prepare_dataframe(df)
        
        # 2. Calculate Risk Indicators
        if 'atr' not in df.columns:
            df['atr'] = self._calculate_atr(df)
        if 'adx' not in df.columns:
            df['adx'] = self._calculate_adx(df)
            
        # Prevent look-ahead bias: 
        # Shift ATR so that when evaluating conditions/stops during bar T, 
        # we only use volatility information known at the end of bar T-1.
        df['atr'] = df['atr'].shift(1).fillna(0)
        
        # 3. Generate Signals
        if 'signal' not in df.columns:
            df['signal'] = 0
        df['signal'] = df['signal'].fillna(0).astype(int)
        
        # 4. Apply Trading Hours Filter
        df = self._filter_trading_hours(df, allow_lunch, allow_overnight)
        
        # 5. Position Sizing
        df = self._calculate_position_size(df, risk_target, initial_capital, multiplier, max_lots)
        
        # 6. Execution & PnL Logic
        df = self._calculate_pnl(df, execution_mode, multiplier, sl_pct, commission, slippage)
        
        # 7. Equity Curve & Margin (internal only — not exposed in return dict)
        df = self._calculate_equity_and_margin(df, initial_capital, initial_margin, maintenance_margin_rate)
        
        # 8. Calculate aggregate metrics (NO trade-level data)
        metrics = self._calculate_metrics(df, initial_capital)
        
        # Return aggregate-only result dict — no 'trades', no 'equity_curve'
        return {
            "metrics": metrics,
            "signals": df['signal'],
        }
    
    def _prepare_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare and validate DataFrame."""
        # Handle column name variations
        if 'close' not in df.columns:
            for col in ['Close', 'CLOSE', 'price', 'Price', 'last', 'Last']:
                if col in df.columns:
                    df = df.rename(columns={col: 'close'})
                    break
        
        # Robust ADX/ATR detection (handle suffixes from AlphaEngine)
        if 'adx' not in df.columns:
            for col in df.columns:
                if str(col).lower().startswith('adx_'):
                    df['adx'] = df[col]
                    break
        
        if 'atr' not in df.columns:
            for col in df.columns:
                if str(col).lower().startswith('atr_'):
                    df['atr'] = df[col]
                    break

        if 'open' not in df.columns: df['open'] = df['close']
        if 'high' not in df.columns: df['high'] = df['close']
        if 'low' not in df.columns: df['low'] = df['close']
        if 'last' not in df.columns: df['last'] = df['close']
        
        if not {'close', 'factor'}.issubset(df.columns):
            raise ValueError(f"DataFrame must contain 'close' and 'factor' columns.\nFound: {list(df.columns)}")
        
        df = df.copy()
        if 'datetime' in df.columns:
            df['datetime'] = pd.to_datetime(df['datetime'])
            df = df.set_index('datetime')
        
        return df
    
    def _calculate_atr(self, df: pd.DataFrame, window: int = 14) -> pd.Series:
        """Calculate Average True Range (ATR)."""
        high = df['high']
        low = df['low']
        close = df['close']
        prev_close = close.shift(1)
        
        tr1 = high - low
        tr2 = (high - prev_close).abs()
        tr3 = (low - prev_close).abs()
        
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.ewm(alpha=1/window, min_periods=window, adjust=False).mean()
        return atr.fillna(0)
    
    def _calculate_adx(self, df: pd.DataFrame, window: int = 14) -> pd.Series:
        """Calculate Average Directional Index (ADX)."""
        high = df['high']
        low = df['low']
        close = df['close']
        
        up_move = high.diff()
        down_move = low.diff().mul(-1)
        
        pos_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
        neg_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
        
        # Wilder's Smoothing for ADX components
        pos_dm = pd.Series(pos_dm, index=df.index).ewm(alpha=1/window, min_periods=window, adjust=False).mean()
        neg_dm = pd.Series(neg_dm, index=df.index).ewm(alpha=1/window, min_periods=window, adjust=False).mean()
        
        tr1 = high - low
        tr2 = (high - close.shift(1)).abs()
        tr3 = (low - close.shift(1)).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.ewm(alpha=1/window, min_periods=window, adjust=False).mean()
        
        pos_di = 100 * (pos_dm / atr)
        neg_di = 100 * (neg_dm / atr)
        
        # Avoid division by zero
        denom = pos_di + neg_di
        denom = denom.replace(0, np.nan)
        
        dx = 100 * (pos_di - neg_di).abs() / denom
        adx = dx.ewm(alpha=1/window, min_periods=window, adjust=False).mean().fillna(0)
        return adx
    
    def _filter_trading_hours(self, df: pd.DataFrame, allow_lunch: bool, 
                              allow_overnight: bool) -> pd.DataFrame:
        """Filter trading hours to avoid gap risk."""
        if 'datetime' not in df.columns and not isinstance(df.index, pd.DatetimeIndex):
            return df
        
        df = df.copy()
        if 'datetime' in df.columns:
            df['datetime'] = pd.to_datetime(df['datetime'])
            df.set_index('datetime', drop=False, inplace=True)
        
        times = df.index.time
        
        if not allow_lunch:
            lunch_start = pd.Timestamp("12:30").time()
            mask_lunch_exit = (times == lunch_start)
            if mask_lunch_exit.any():
                df.loc[mask_lunch_exit, 'signal'] = 0
        
        if not allow_overnight:
            market_close = pd.Timestamp("18:00").time()
            night_close = pd.Timestamp("23:30").time()
            
            mask_day_exit = (times == market_close)
            mask_night_exit = (times == night_close)
            
            if mask_day_exit.any():
                df.loc[mask_day_exit, 'signal'] = 0
            if mask_night_exit.any():
                df.loc[mask_night_exit, 'signal'] = 0
        
        return df
    
    def _calculate_position_size(self, df: pd.DataFrame, risk_target: float,
                                 initial_capital: float, multiplier: float,
                                 max_lots: int) -> pd.DataFrame:
        """Calculate position size based on risk target."""
        if risk_target > 0:
            # Volatility-based position sizing
            risk_amount = initial_capital * (risk_target / 100.0)
            contract_size = df['atr'] * multiplier
            contract_size = contract_size.replace(0, np.inf)
            
            lots = risk_amount / contract_size
            lots = lots.fillna(0).astype(int)
            lots = lots.clip(upper=max_lots)
            
            df['pos_raw'] = df['signal'] * lots
        else:
            # Default 1 lot per signal
            df['pos_raw'] = df['signal']
        
        return df
    
    def _calculate_pnl(self, df: pd.DataFrame, execution_mode: str, multiplier: float,
                       sl_pct: float, commission: float, slippage: float) -> pd.DataFrame:
        """Calculate PnL with optional stop loss."""
        # Execution & PnL Logic
        if execution_mode == 'Next Open':
            df['price_change'] = df['open'].diff()
            df['pos'] = df['pos_raw'].shift(2).fillna(0)
            df['exec_price'] = df['open']
        else:
            df['price_change'] = df['close'].diff()
            df['pos'] = df['pos_raw'].shift(1).fillna(0)
            df['exec_price'] = df['close']
        
        df['exit_type'] = 'Signal'
        
        # Stop Loss Logic (Vectorized)
        if sl_pct > 0:
            df = self._apply_stop_loss(df, sl_pct, multiplier, execution_mode)
        else:
            # Basic PnL
            df['gross_pnl'] = df['price_change'] * multiplier * df['pos']
        
        # Recalculate PnL for non-stop bars
        mask_normal = (df['exit_type'] == 'Signal')
        df.loc[mask_normal, 'gross_pnl'] = (df.loc[mask_normal, 'price_change'] * 
                                             multiplier * df.loc[mask_normal, 'pos'])
        
        # Transaction Costs
        df['pos_change'] = df['pos'].diff().abs().fillna(0)
        cost_per_lot = commission + (slippage * multiplier)
        df['cost'] = df['pos_change'] * cost_per_lot
        df['net_pnl'] = df['gross_pnl'] - df['cost']
        
        return df
    
    def _apply_stop_loss(self, df: pd.DataFrame, sl_pct: float, multiplier: float,
                         execution_mode: str) -> pd.DataFrame:
        """Apply vectorized stop loss logic."""
        # Identify trade IDs
        df['trade_id'] = (df['pos'] != df['pos'].shift(1).fillna(0)).cumsum()
        
        # Get entry prices
        entry_prices = df.groupby('trade_id')['exec_price'].transform('first')
        df['entry_price'] = entry_prices
        
        # Calculate SL prices
        sl_prices = np.where(df['pos'] > 0,
                            df['entry_price'] * (1 - sl_pct/100.0),
                            df['entry_price'] * (1 + sl_pct/100.0))
        
        # Check for hits
        hit_long = (df['pos'] > 0) & (df['low'] < sl_prices)
        hit_short = (df['pos'] < 0) & (df['high'] > sl_prices)
        is_hit = hit_long | hit_short
        
        # Cumulative hits per trade
        cum_hits = pd.Series(is_hit, index=df.index).groupby(df['trade_id']).cumsum()
        first_hit_mask = (cum_hits == 1) & (is_hit)
        post_hit_mask = (cum_hits > 0) & (~first_hit_mask)
        
        # Apply stop logic
        df.loc[post_hit_mask, 'pos'] = 0
        df.loc[post_hit_mask, 'gross_pnl'] = 0
        
        # Calculate SL PnL
        if execution_mode == 'Next Open':
            ref_price = df['open'].shift(1)
        else:
            ref_price = df['close'].shift(1)
        
        sl_pnl = (sl_prices - ref_price) * multiplier * df['pos']
        df.loc[first_hit_mask, 'gross_pnl'] = sl_pnl
        df.loc[first_hit_mask, 'exit_type'] = 'Intra-bar SL'
        
        return df
    
    def _calculate_equity_and_margin(self, df: pd.DataFrame, initial_capital: float,
                                     initial_margin: float, maintenance_margin_rate: float) -> pd.DataFrame:
        """Calculate equity curve and margin requirements."""
        df['equity'] = initial_capital + df['net_pnl'].cumsum()
        df['used_margin'] = df['pos'].abs() * initial_margin
        df['maint_level'] = df['used_margin'] * maintenance_margin_rate
        df['is_liquidated'] = df['equity'] < df['maint_level']
        
        return df
    
    def _calculate_metrics(self, df: pd.DataFrame, initial_capital: float) -> Dict:
        """
        Calculate aggregate performance metrics for pressure tests.
        Returns only top-level summary metrics: Net Profit, Drawdown, Ratios, Trade Count.
        No trade-level data is exposed.
        """
        total_net_profit = df['net_pnl'].sum()
        final_equity = df['equity'].iloc[-1] if not df.empty else initial_capital
        total_trades = df[df['pos_change'] > 0].shape[0]
        
        # Drawdown
        df['peak'] = df['equity'].cummax()
        df['drawdown'] = df['equity'] - df['peak']
        df['drawdown_pct'] = df['drawdown'] / df['peak']
        
        max_drawdown_rm = df['drawdown'].min()
        max_drawdown_pct = df['drawdown_pct'].min()
        
        # Sharpe Ratio (daily)
        daily_pnl = df['net_pnl'].resample('D').sum() if len(df) > 2 else pd.Series(dtype=float)
        daily_pnl = daily_pnl[daily_pnl != 0]
        
        if len(daily_pnl) > 1:
            mean_ret = daily_pnl.mean()
            std_ret = daily_pnl.std()
            sharpe = (mean_ret / std_ret) * np.sqrt(252) if std_ret != 0 else 0.0
        else:
            sharpe = 0.0
        
        # Calmar Ratio
        days = (df.index[-1] - df.index[0]).days if len(df) > 1 else 1
        years = max(days / 365.25, 0.01)
        annual_return_pct = (final_equity / initial_capital) ** (1 / years) - 1 if final_equity > 0 else -1.0
        calmar = annual_return_pct / abs(max_drawdown_pct) if max_drawdown_pct != 0 else 0.0
        
        # Margin Status
        first_liquidation = df[df['is_liquidated']].first_valid_index()
        liquidation_msg = "Safe" if not first_liquidation else f"MARGIN CALL at {first_liquidation}!"
        
        if total_trades == 0:
            raise Exception("Total Trades recorded: 0. Check filters or signal logic.")
        
        return {
            "Total Net Profit": round(total_net_profit, 2),
            "Max Drawdown (RM)": round(max_drawdown_rm, 2),
            "Max Drawdown %": round(max_drawdown_pct * 100, 2),
            "Sharpe Ratio": round(sharpe, 3),
            "Calmar Ratio": round(calmar, 3),
            "Total Trades": total_trades,
            "Margin Status": liquidation_msg,
        }


# Auto-register to engine registry on module load
try:
    from .engine_registry import EngineRegistry
    EngineRegistry.register('vectorized', VectorizedBacktest)
except ImportError:
    pass  # Registry not available yet
