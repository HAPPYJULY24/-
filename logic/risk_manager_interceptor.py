"""
Risk Manager - Interceptor Pattern
Upgraded from Puppet Mode to active order validation.
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, Tuple, Optional
import pandas as pd
import numpy as np
import json

@dataclass
class RiskConfig:
    """Immutable Risk Constraints derived strictly from Strategy DNA."""
    initial_capital: float
    initial_margin: float
    risk_target_pct: float
    max_position_size: int
    multiplier: float
    adx_filter_enabled: bool
    adx_threshold: float = 20.0  # Optional fallback if needed by legacy

    @staticmethod
    def from_dna(json_path: str) -> "RiskConfig":
        """Loads and parses the strategy_dna.json file into a RiskConfig."""
        with open(json_path, 'r', encoding='utf-8') as f:
            dna = json.load(f)
            
        return RiskConfig(
            initial_capital=float(dna["environment"].get("initial_capital", 100000.0)),
            initial_margin=float(dna["backtest_risk_settings"].get("initial_margin", 5000.0)),
            risk_target_pct=float(dna["backtest_risk_settings"].get("risk_target_pct", 1.0)),
            max_position_size=int(dna["backtest_risk_settings"]["max_position_size"]),
            multiplier=float(dna["friction_costs"]["multiplier"]),
            adx_filter_enabled=bool(dna["execution_constraints"]["adx_filter_enabled"])
        )

# Import Phase 1 models
import sys
from pathlib import Path
project_root = Path(__file__).parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.core.models.asset import get_asset_config
from src.core.models.order import OrderRequest, OrderResponse

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


@dataclass
class Account_State:
    """Account state tracking."""
    balance: float
    equity: float
    used_margin: float = 0.0
    current_pos: int = 0  # +ve for Long, -ve for Short
    max_drawdown: float = 0.0
    is_liquidated: bool = False
    liquidation_reason: str = ""
    
    @property
    def maintenance_margin(self) -> float:
        """Dynamic maintenance margin baseline (80% of used initial margin)"""
        return self.used_margin * 0.8
    
    @property
    def free_margin(self) -> float:
        return self.equity - self.used_margin
    
    @property
    def margin_level(self) -> float:
        maint = self.maintenance_margin
        if maint == 0:
            return float('inf')
        return self.equity / maint


class RiskManager:
    """
    Risk Manager - Interceptor Pattern.
    
    Validates all order requests through 3-layer pipeline:
    1. Regime Check (ADX filtering)
    2. Position Sizing (ATR-based with margin check)
    3. Margin Sufficiency (using AssetConfig)
    """
    
    def __init__(self, config: RiskConfig):
        """
        Initialize RiskManager.
        100% DNA-driven initialization. No loose kwargs.
        """
        self.config = config
        
        # Initialize Sovereign State Ledger
        self.state = Account_State(
            balance=config.initial_capital, 
            equity=config.initial_capital
        )
        
        # Fallback alias for older references expecting self.params
        self.params = {
            'use_adx': config.adx_filter_enabled,
            'adx_threshold': config.adx_threshold,
            'margin_call_level': 1.1,
            'buffer_ratio': 0.9,
            'margin_per_lot': config.initial_margin,
            'risk_per_trade': config.risk_target_pct / 100.0
        }
        self.multiplier = config.multiplier
        self.initial_capital = config.initial_capital
        
        # Drawdown monitoring baseline
        self._high_water_mark = config.initial_capital
        self._daily_baseline_equity = config.initial_capital
        self._last_bar_equity = config.initial_capital
        self._current_day = None
        
        # Audit Log
        self.audit_log = []
    
    # ============================================================================
    # BACKWARD COMPATIBILITY (Puppet Mode Methods)
    # ============================================================================
    
    def sync_account_state(self, balance: float, equity: float, current_pos: int, used_margin: float, current_date=None):
        """
        Puppet Mode: Passive State Sync (BACKWARD COMPATIBLE).
        Accept state from BacktestEngine.
        """
        self.state.balance = balance
        self.state.equity = equity
        self.state.current_pos = current_pos
        self.state.used_margin = used_margin
        
        # 1. Daily baseline tracking with Gap Risk Protection
        if current_date is not None:
            bar_day = current_date.date() if hasattr(current_date, 'date') else str(current_date)[:10]
            if self._current_day != bar_day:
                # Today's baseline is yesterday's final closing equity to capture Overnight Gap risk
                self._daily_baseline_equity = getattr(self, '_last_bar_equity', equity)
                self._current_day = bar_day
                
        # Cache current Bar equity for the next day's reset
        self._last_bar_equity = equity
            
        # 2. Update High-Water Mark
        if equity > self._high_water_mark:
            self._high_water_mark = equity
            
        # 3. Compute drawdown metrics
        peak_drawdown = (self._high_water_mark - equity) / self._high_water_mark if self._high_water_mark > 0 else 0.0
        daily_drawdown = (self._daily_baseline_equity - equity) / self._daily_baseline_equity if self._daily_baseline_equity > 0 else 0.0
        
        # 4. Check for liquidations (Drawdown & Margin Call)
        if peak_drawdown > 0.35:
            self.state.is_liquidated = True
            self.state.liquidation_reason = f"Peak Drawdown Breach: {peak_drawdown:.2%} > 35%"
            return
            
        if daily_drawdown > 0.20:
            self.state.is_liquidated = True
            self.state.liquidation_reason = f"Daily Drawdown Breach: {daily_drawdown:.2%} > 20%"
            return
            
        # 5. Margin Call Liquidation check (using Maintenance Margin)
        if self.state.margin_level < 1.0:
            self.state.is_liquidated = True
            self.state.liquidation_reason = (
                f"Margin Call Liquidation: Equity ({self.state.equity:.2f}) "
                f"fell below Maintenance Margin ({self.state.maintenance_margin:.2f})"
            )
    
    def check_regime(self, row: pd.Series, signal: int) -> int:
        """
        BACKWARD COMPATIBLE: Legacy regime check.
        For use in old code that doesn't use OrderRequest.
        """
        if not self.params['use_adx']:
            return signal
        
        if signal == 0:
            return 0
        
        if 'ADX' not in row:
            return signal
        
        if row['ADX'] < self.params['adx_threshold']:
            self.audit_log.append({
                'Date': row.get('Date') if isinstance(row, dict) else row.name,
                'Type': 'Regime_Audit',
                'Action': 'Blocked',
                'Details': f"ADX {row['ADX']:.2f} < {self.params['adx_threshold']}"
            })
            return 0
        
        return signal
    
    def calculate_lots(self, atr: float, stop_loss_dist: float = 0.0) -> int:
        """
        BACKWARD COMPATIBLE: Legacy position sizing.
        For use in old code that doesn't use OrderRequest.
        """
        if atr is None or pd.isna(atr) or atr <= 0:
            return 0
        
        sizing_equity = min(self.initial_capital, self.state.equity)
        risk_amount = sizing_equity * self.params['risk_per_trade']
        volatility_value = atr * self.multiplier
        
        if volatility_value == 0:
            return 0
        
        target_lots = int(risk_amount / volatility_value)
        
        # Margin check
        if self.params['margin_per_lot'] > 0:
            max_allowed_lots = int((self.state.free_margin * self.params['buffer_ratio']) / 
                                  self.params['margin_per_lot'])
        else:
            max_allowed_lots = 999
        
        final_lots = min(target_lots, max_allowed_lots, 20)  # Hard cap at 20
        final_lots = max(0, final_lots)
        
        if final_lots < target_lots:
            self.audit_log.append({
                'Type': 'Position_Sizing',
                'Action': 'Reduced',
                'Details': f"Target {target_lots} -> {final_lots} (Safety Cap/Margin)"
            })
        
        return final_lots
    
    def check_intra_bar(
        self, 
        row: pd.Series, 
        entry_price: float, 
        sl_price: float, 
        tp_price: Optional[float] = None
    ) -> Tuple[bool, str, float]:
        """BACKWARD COMPATIBLE: Intra-bar stop loss monitor."""
        current_pos = self.state.current_pos
        
        if current_pos == 0:
            return False, "", 0.0
        
        open_p = row['open']
        high_p = row['high']
        low_p = row['low']
        
        # Margin level warning
        margin_level = self.state.margin_level
        if margin_level < self.params['margin_call_level']:
            logger.critical(f"\033[91mCRITICAL ALERT: Low Margin Level {margin_level:.2f}!\033[0m")
        
        # LONG positions
        if current_pos > 0:
            if open_p < sl_price:
                return True, "Gap_SL", open_p
            if low_p < sl_price:
                return True, "Intra_SL", sl_price
            if tp_price and high_p > tp_price:
                return True, "Intra_TP", tp_price
        
        # SHORT positions
        elif current_pos < 0:
            if open_p > sl_price:
                return True, "Gap_SL", open_p
            if high_p > sl_price:
                return True, "Intra_SL", sl_price
            if tp_price and low_p < tp_price:
                return True, "Intra_TP", tp_price
        
        return False, "", 0.0
    
    # ============================================================================
    # NEW INTERCEPTOR PATTERN METHODS
    # ============================================================================
    
    def validate_order(self, order: OrderRequest) -> OrderResponse:
        """
        3-Layer Order Validation Pipeline (INTERCEPTOR PATTERN).
        
        Pipeline:
        1. Layer 1: check_regime_layer (ADX filtering)
        2. Layer 2: calculate_risk_pos_layer (ATR-based sizing + margin check)
        3. Layer 3: check_margin_layer (AssetConfig-based margin sufficiency)
        
        Args:
            order: OrderRequest object
        
        Returns:
            OrderResponse (approved/rejected with reason)
        """
        # Wind-control Interceptor Check: Reject all new entries if liquidated, but allow exit orders
        if getattr(self.state, 'is_liquidated', False):
            if not getattr(order, 'is_exit', False):
                reason = getattr(self.state, 'liquidation_reason', "Account Liquidated")
                response = OrderResponse.reject(reason=f"Rejected: {reason}. Opening forbidden.")
                self._log_rejection(order, response)
                return response
            
        # Layer 1: Regime Check
        regime_response = self._check_regime_layer(order)
        if not regime_response.approved:
            self._log_rejection(order, regime_response)
            return regime_response
        
        # Layer 2: Position Sizing
        sizing_response = self._calculate_risk_pos_layer(order)
        if not sizing_response.approved:
            self._log_rejection(order, sizing_response)
            return sizing_response
        
        # Update volume based on sizing
        adjusted_order = order
        if sizing_response.adjusted_volume < order.volume:
            self._log_adjustment(order, sizing_response)
        
        # Layer 3: Margin Sufficiency
        margin_response = self._check_margin_layer(adjusted_order, sizing_response.adjusted_volume)
        if not margin_response.approved:
            self._log_rejection(order, margin_response)
            return margin_response
            
        if margin_response.adjusted_volume < sizing_response.adjusted_volume:
            self._log_adjustment(order, margin_response)
            
        final_approved_volume = margin_response.adjusted_volume
        
        # All layers passed
        
        # --- CRITICAL: MARGIN LOCKING (NET EXPOSURE) ---
        order_sign = 1 if order.direction_str == 'LONG' else -1
        new_pos = self.state.current_pos + (final_approved_volume * order_sign)
        
        self.state.current_pos = new_pos
        self.state.used_margin = abs(new_pos) * self.config.initial_margin
        
        final_response = OrderResponse.approve(
            volume=final_approved_volume,
            reason="Validated: All checks passed. Net Margin Locked."
        )
        self._log_approval(order, final_response)
        return final_response
    
    def _check_regime_layer(self, order: OrderRequest) -> OrderResponse:
        """Layer 1: ADX Regime Filter."""
        if not self.params['use_adx']:
            return OrderResponse.approve(order.volume, "Regime: ADX check disabled")
        
        if order.adx < self.params['adx_threshold']:
            return OrderResponse.reject(
                reason=f"Regime: ADX too low ({order.adx:.2f} < {self.params['adx_threshold']})",
                details={'adx': order.adx, 'threshold': self.params['adx_threshold']}
            )
        
        return OrderResponse.approve(order.volume, f"Regime: ADX {order.adx:.2f} OK")
    
    def _calculate_risk_pos_layer(self, order: OrderRequest) -> OrderResponse:
        """Layer 2: Sovereign Sizing based on DNA's risk_target_pct and max_position_size."""
        if order.atr is None or pd.isna(order.atr) or order.atr <= 0:
            return OrderResponse.reject(
                reason=f"Position Sizing: Invalid ATR ({order.atr})",
                details={'atr': order.atr}
            )
        
        # Calculate raw risk amount
        risk_amount = self.state.equity * (self.config.risk_target_pct / 100.0)
        volatility_value = order.atr * self.config.multiplier
        
        if volatility_value <= 0:
            return OrderResponse.reject(reason="Invalid volatility mapping")
            
        target_lots = int(risk_amount / volatility_value)
        
        # Absolute Veto: Max Position Size
        if target_lots > self.config.max_position_size:
            adjusted_vol = min(self.config.max_position_size, order.volume)
            return OrderResponse.adjust(
                original_volume=order.volume,
                adjusted_volume=adjusted_vol,
                reason=f"Position Sizing: Target {target_lots} exceeds DNA max ({self.config.max_position_size})"
            )
            
        # Ensure we don't exceed the requested volume either
        final_lots = min(target_lots, order.volume)
        if final_lots == 0:
            return OrderResponse.reject(reason="Position Sizing: Zero lots calculated")
            
        return OrderResponse.approve(final_lots, "Position Sizing OK")
    
    def _check_margin_layer(self, order: OrderRequest, approved_volume: int) -> OrderResponse:
        """Layer 3: Absolute Directional Margin Verification & Smart Truncation."""
        order_sign = 1 if order.direction_str == 'LONG' else -1
        curr_pos = self.state.current_pos
        
        # 1. Determine Closing vs New Exposure
        if curr_pos == 0 or (curr_pos > 0 and order_sign > 0) or (curr_pos < 0 and order_sign < 0):
            closing_lots = 0
            new_lots = approved_volume
        else:
            if abs(curr_pos) >= approved_volume:
                closing_lots = approved_volume
                new_lots = 0
            else:
                closing_lots = abs(curr_pos)
                new_lots = approved_volume - abs(curr_pos)
                
        # 2. Calculate Effective Margin
        freed_margin = closing_lots * self.config.initial_margin
        effective_free_margin = self.state.free_margin + freed_margin
        required_margin = new_lots * self.config.initial_margin
        
        # 3. Affordability Check
        if required_margin > effective_free_margin:
            # Smart Truncation
            if self.config.initial_margin > 0:
                affordable_new_lots = int(effective_free_margin // self.config.initial_margin)
            else:
                affordable_new_lots = new_lots
                
            affordable_total_lots = closing_lots + affordable_new_lots
            
            if affordable_total_lots == 0:
                return OrderResponse.reject(
                    reason="[LEVERAGE_WARNING] Margin Call: Zero affordable lots",
                    details={'required_margin': required_margin, 'effective_free_margin': effective_free_margin}
                )
            else:
                return OrderResponse.adjust(
                    original_volume=approved_volume,
                    adjusted_volume=affordable_total_lots,
                    reason="[LEVERAGE_WARNING] Adjust: Margin Call Prevention - Reduced to affordable lots",
                    details={'required_margin': required_margin, 'effective_free_margin': effective_free_margin, 'affordable_new_lots': affordable_new_lots, 'closing_lots': closing_lots}
                )
                
        return OrderResponse.approve(
            approved_volume,
            f"Margin Verified (closing: {closing_lots}, new: {new_lots}, required: {required_margin:.2f})",
            details={'required_margin': required_margin, 'freed_margin': freed_margin, 'new_lots': new_lots}
        )
    
    def _log_rejection(self, order: OrderRequest, response: OrderResponse):
        """Log rejected order to audit trail."""
        self.audit_log.append({
            'Type': 'Order_Rejected',
            'Symbol': order.symbol,
            'Direction': order.direction_str,
            'Requested_Volume': order.volume,
            'Reason': response.reason,
            'Details': response.details
        })
        logger.warning(f"🚫 [ORDER REJECTED] {order.symbol} {order.direction_str} x{order.volume}: {response.reason}")
    
    def _log_adjustment(self, order: OrderRequest, response: OrderResponse):
        """Log adjusted order to audit trail."""
        self.audit_log.append({
            'Type': 'Order_Adjusted',
            'Symbol': order.symbol,
            'Direction': order.direction_str,
            'Requested_Volume': order.volume,
            'Approved_Volume': response.adjusted_volume,
            'Reason': response.reason,
            'Details': response.details
        })
        logger.info(f"⚠️ [ORDER ADJUSTED] {order.symbol} {order.direction_str}: {order.volume} -> {response.adjusted_volume}")
    
    def _log_approval(self, order: OrderRequest, response: OrderResponse):
        """Log approved order to audit trail."""
        self.audit_log.append({
            'Type': 'Order_Approved',
            'Symbol': order.symbol,
            'Direction': order.direction_str,
            'Volume': response.adjusted_volume,
            'Reason': response.reason
        })
        logger.info(f"✅ [ORDER APPROVED] {order.symbol} {order.direction_str} x{response.adjusted_volume}")
    
    def get_audit_dataframe(self) -> pd.DataFrame:
        """Get audit log as DataFrame."""
        return pd.DataFrame(self.audit_log)
