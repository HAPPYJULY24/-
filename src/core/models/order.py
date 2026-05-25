"""
Order Request Model
Defines order request structure for RiskManager validation pipeline.
"""

from dataclasses import dataclass
from typing import Literal, Optional
from datetime import datetime


@dataclass
class OrderRequest:
    """
    Order request for risk validation.
    
    Attributes:
        symbol: Asset symbol (e.g., 'FCPO', 'FKLI')
        volume: Number of lots to trade
        direction: 1 for LONG, -1 for SHORT
        order_type: 'MARKET' or 'LIMIT'
        price: Current market price or limit price
        timestamp: Request timestamp
        atr: Average True Range for position sizing
        adx: ADX value for regime filtering
    """
    symbol: str
    volume: int
    direction: Literal[1, -1]
    order_type: Literal['MARKET', 'LIMIT'] = 'MARKET'
    price: float = 0.0
    timestamp: Optional[datetime] = None
    atr: float = 0.0
    adx: float = 0.0
    is_exit: bool = False
    
    def __post_init__(self):
        """Validate order request fields."""
        if self.volume <= 0:
            raise ValueError(f"Volume must be positive, got {self.volume}")
        
        if self.direction not in [1, -1]:
            raise ValueError(f"Direction must be 1 (LONG) or -1 (SHORT), got {self.direction}")
        
        if self.price < 0:
            raise ValueError(f"Price cannot be negative, got {self.price}")
    
    @property
    def direction_str(self) -> str:
        """Get human-readable direction."""
        return "LONG" if self.direction == 1 else "SHORT"
    
    def to_dict(self) -> dict:
        """Convert to dictionary for logging."""
        return {
            'symbol': self.symbol,
            'volume': self.volume,
            'direction': self.direction_str,
            'order_type': self.order_type,
            'price': self.price,
            'timestamp': str(self.timestamp) if self.timestamp else None,
            'atr': self.atr,
            'adx': self.adx,
            'is_exit': self.is_exit
        }


@dataclass
class OrderResponse:
    """
    Response from risk validation.
    
    Attributes:
        approved: Whether order is approved
        adjusted_volume: Approved volume (may be reduced from requested)
        reason: Rejection or adjustment reason
        details: Additional details about the decision
    """
    approved: bool
    adjusted_volume: int = 0
    reason: str = ""
    details: dict = None
    
    def __post_init__(self):
        """Initialize details dict if None."""
        if self.details is None:
            self.details = {}
    
    @staticmethod
    def approve(volume: int, reason: str = "Approved", details: dict = None) -> 'OrderResponse':
        """Create approved response."""
        return OrderResponse(approved=True, adjusted_volume=volume, reason=reason, details=details or {})
    
    @staticmethod
    def reject(reason: str, details: dict = None) -> 'OrderResponse':
        """Create rejected response."""
        return OrderResponse(approved=False, adjusted_volume=0, reason=reason, details=details or {})
    
    @staticmethod
    def adjust(original_volume: int, adjusted_volume: int, reason: str, details: dict = None) -> 'OrderResponse':
        """Create adjusted response."""
        return OrderResponse(
            approved=True, 
            adjusted_volume=adjusted_volume, 
            reason=reason,
            details=details or {'original_volume': original_volume}
        )
    
    def to_dict(self) -> dict:
        """Convert to dictionary for logging."""
        return {
            'approved': self.approved,
            'adjusted_volume': self.adjusted_volume,
            'reason': self.reason,
            'details': self.details
        }
