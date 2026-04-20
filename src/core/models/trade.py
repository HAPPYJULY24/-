"""
Trade and Position Models
Defines trade records and position tracking with PnL calculations.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Literal
from enum import Enum


class TradeDirection(Enum):
    """Trade direction enumeration."""
    LONG = "LONG"
    SHORT = "SHORT"


class TradeStatus(Enum):
    """Trade status enumeration."""
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    CANCELLED = "CANCELLED"


@dataclass
class Trade:
    """
    Represents a single trade execution (entry or exit).
    
    Attributes:
        trade_id: Unique trade identifier
        symbol: Asset symbol
        direction: Trade direction (LONG/SHORT)
        entry_time: Entry timestamp
        entry_price: Entry price
        lots: Number of contracts/lots
        multiplier: Contract multiplier
        initial_margin: Margin per lot
        exit_time: Exit timestamp (None if still open)
        exit_price: Exit price (None if still open)
        commission: Total commission paid
        slippage: Total slippage cost
        pnl: Realized profit/loss (None if still open)
        status: Trade status
        metadata: Additional trade information
    """
    trade_id: str
    symbol: str
    direction: TradeDirection
    entry_time: datetime
    entry_price: float
    lots: int
    multiplier: float
    initial_margin: float
    exit_time: Optional[datetime] = None
    exit_price: Optional[float] = None
    commission: float = 0.0
    slippage: float = 0.0
    pnl: Optional[float] = None
    status: TradeStatus = TradeStatus.OPEN
    metadata: dict = field(default_factory=dict)
    
    def close_trade(self, exit_time: datetime, exit_price: float, 
                    commission: float = 0.0, slippage: float = 0.0):
        """
        Close the trade and calculate realized PnL.
        
        Args:
            exit_time: Exit timestamp
            exit_price: Exit price
            commission: Exit commission
            slippage: Exit slippage
        """
        self.exit_time = exit_time
        self.exit_price = exit_price
        self.commission += commission
        self.slippage += slippage
        self.status = TradeStatus.CLOSED
        
        # Calculate PnL
        price_diff = (exit_price - self.entry_price) if self.direction == TradeDirection.LONG \
                     else (self.entry_price - exit_price)
        gross_pnl = price_diff * self.lots * self.multiplier
        self.pnl = gross_pnl - self.commission - self.slippage
    
    def calculate_unrealized_pnl(self, current_price: float) -> float:
        """
        Calculate unrealized PnL for open position.
        
        Args:
            current_price: Current market price
            
        Returns:
            Unrealized PnL (excluding commission/slippage)
        """
        if self.status != TradeStatus.OPEN:
            return 0.0
        
        price_diff = (current_price - self.entry_price) if self.direction == TradeDirection.LONG \
                     else (self.entry_price - current_price)
        return price_diff * self.lots * self.multiplier
    
    def to_dict(self) -> dict:
        """Convert to dictionary representation."""
        return {
            'trade_id': self.trade_id,
            'symbol': self.symbol,
            'direction': self.direction.value,
            'entry_time': self.entry_time.isoformat() if self.entry_time else None,
            'entry_price': self.entry_price,
            'lots': self.lots,
            'multiplier': self.multiplier,
            'initial_margin': self.initial_margin,
            'exit_time': self.exit_time.isoformat() if self.exit_time else None,
            'exit_price': self.exit_price,
            'commission': self.commission,
            'slippage': self.slippage,
            'pnl': self.pnl,
            'status': self.status.value,
            'metadata': self.metadata
        }


@dataclass
class Position:
    """
    Represents a real-time position tracker with margin and PnL calculations.
    
    Attributes:
        symbol: Asset symbol
        direction: Position direction (LONG/SHORT)
        lots: Current number of lots held
        avg_entry_price: Average entry price
        multiplier: Contract multiplier
        initial_margin_per_lot: Initial margin per lot
        maintenance_margin_per_lot: Maintenance margin per lot
        open_trades: List of associated open trades
        metadata: Additional position information
    """
    symbol: str
    direction: TradeDirection
    lots: int
    avg_entry_price: float
    multiplier: float
    initial_margin_per_lot: float
    maintenance_margin_per_lot: Optional[float] = None
    entry_time: Optional[datetime] = None
    open_trades: list = field(default_factory=list)
    metadata: dict = field(default_factory=dict)
    
    def __post_init__(self):
        """Set maintenance margin to 80% if not specified."""
        if self.maintenance_margin_per_lot is None:
            self.maintenance_margin_per_lot = self.initial_margin_per_lot * 0.8
    
    def calculate_unrealized_pnl(self, current_price: float) -> float:
        """
        Calculate unrealized profit/loss for the position.
        
        Args:
            current_price: Current market price
            
        Returns:
            Unrealized PnL in currency units
        """
        if self.lots == 0:
            return 0.0
        
        price_diff = (current_price - self.avg_entry_price) if self.direction == TradeDirection.LONG \
                     else (self.avg_entry_price - current_price)
        return price_diff * self.lots * self.multiplier

    def calculate_pnl(self, current_price: float) -> float:
        """Alias for calculate_unrealized_pnl."""
        return self.calculate_unrealized_pnl(current_price)
    
    def calculate_pnl_at_price(self, price: float) -> float:
        """Alias for calculate_unrealized_pnl with price."""
        return self.calculate_unrealized_pnl(price)
    
    @property
    def avg_price(self) -> float:
        """Alias for property-style access in backtest engines."""
        return self.avg_entry_price

    @property
    def margin_used(self) -> float:
        """Alias for property-style access in backtest engines."""
        return self.calculate_margin_used()

    def calculate_margin_used(self) -> float:
        """
        Calculate total margin currently used by this position.
        
        Returns:
            Total margin in currency units (initial margin * lots)
        """
        return self.initial_margin_per_lot * self.lots
    
    def calculate_maintenance_margin(self) -> float:
        """
        Calculate maintenance margin requirement.
        
        Returns:
            Maintenance margin in currency units
        """
        return self.maintenance_margin_per_lot * self.lots
    
    def add_lots(self, additional_lots: int, entry_price: float):
        """
        Add lots to position and update average entry price.
        
        Args:
            additional_lots: Number of lots to add
            entry_price: Entry price of new lots
        """
        if additional_lots <= 0:
            return
        
        total_cost_old = self.avg_entry_price * self.lots
        total_cost_new = entry_price * additional_lots
        
        self.lots += additional_lots
        self.avg_entry_price = (total_cost_old + total_cost_new) / self.lots
    
    def reduce_lots(self, reduce_lots: int) -> bool:
        """
        Reduce lots from position.
        
        Args:
            reduce_lots: Number of lots to reduce
            
        Returns:
            True if successful, False if insufficient lots
        """
        if reduce_lots > self.lots:
            return False
        
        self.lots -= reduce_lots
        return True
    
    def is_empty(self) -> bool:
        """Check if position is empty (no lots held)."""
        return self.lots == 0
    
    def to_dict(self) -> dict:
        """Convert to dictionary representation."""
        return {
            'symbol': self.symbol,
            'direction': self.direction.value,
            'lots': self.lots,
            'avg_entry_price': self.avg_entry_price,
            'multiplier': self.multiplier,
            'initial_margin_per_lot': self.initial_margin_per_lot,
            'maintenance_margin_per_lot': self.maintenance_margin_per_lot,
            'metadata': self.metadata
        }
