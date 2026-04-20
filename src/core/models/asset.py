"""
Asset Configuration Model
Defines trading instrument parameters and presets for Bursa Malaysia Futures.
"""

from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class AssetConfig:
    """
    Configuration for a trading asset/instrument.
    
    Attributes:
        symbol: Asset symbol (e.g., 'FCPO', 'FKLI')
        name: Full name of the asset
        multiplier: Contract multiplier (points to currency)
        tick_size: Minimum price movement
        initial_margin: Initial margin requirement per contract
        maintenance_margin: Maintenance margin per contract (default: 80% of initial)
        currency: Trading currency (default: 'MYR')
        exchange: Exchange name (default: 'Bursa Malaysia')
    """
    symbol: str
    name: str
    multiplier: float
    tick_size: float
    initial_margin: float
    maintenance_margin: Optional[float] = None
    currency: str = "MYR"
    exchange: str = "Bursa Malaysia"
    metadata: Dict = field(default_factory=dict)
    
    def __post_init__(self):
        """Set maintenance margin to 80% of initial if not specified."""
        if self.maintenance_margin is None:
            self.maintenance_margin = self.initial_margin * 0.8
    
    def calculate_contract_value(self, price: float) -> float:
        """
        Calculate the notional value of one contract.
        
        Args:
            price: Current market price
            
        Returns:
            Contract value in currency units
        """
        return price * self.multiplier
    
    def calculate_tick_value(self) -> float:
        """
        Calculate the monetary value of one tick movement.
        
        Returns:
            Tick value in currency units
        """
        return self.tick_size * self.multiplier
    
    def to_dict(self) -> Dict:
        """Convert to dictionary representation."""
        return {
            'symbol': self.symbol,
            'name': self.name,
            'multiplier': self.multiplier,
            'tick_size': self.tick_size,
            'initial_margin': self.initial_margin,
            'maintenance_margin': self.maintenance_margin,
            'currency': self.currency,
            'exchange': self.exchange,
            'metadata': self.metadata
        }


# ==================== Bursa Malaysia Futures Presets ====================

FCPO_CONFIG = AssetConfig(
    symbol="FCPO",
    name="Crude Palm Oil Futures",
    multiplier=25.0,
    tick_size=1.0,
    initial_margin=5000.0,
    maintenance_margin=4000.0,
    currency="MYR",
    exchange="Bursa Malaysia",
    metadata={
        'sector': 'Commodities',
        'trading_hours': '10:30-12:30, 14:30-18:00, 21:00-23:30',
        'contract_size': '25 metric tons',
        'settlement': 'Cash Settlement'
    }
)

FKLI_CONFIG = AssetConfig(
    symbol="FKLI",
    name="FTSE Bursa Malaysia KLCI Futures",
    multiplier=50.0,
    tick_size=0.5,
    initial_margin=4000.0,
    maintenance_margin=3200.0,
    currency="MYR",
    exchange="Bursa Malaysia",
    metadata={
        'sector': 'Equity Index',
        'trading_hours': '08:45-12:45, 14:30-17:15',
        'contract_size': 'RM50 per index point',
        'settlement': 'Cash Settlement'
    }
)


# Registry for quick access
ASSET_REGISTRY: Dict[str, AssetConfig] = {
    'FCPO': FCPO_CONFIG,
    'FKLI': FKLI_CONFIG,
}


def get_asset_config(symbol: str) -> AssetConfig:
    """
    Retrieve asset configuration by symbol.
    
    Args:
        symbol: Asset symbol (e.g., 'FCPO', 'FKLI')
        
    Returns:
        AssetConfig instance
        
    Raises:
        KeyError: If symbol not found in registry
    """
    if symbol not in ASSET_REGISTRY:
        raise KeyError(f"Asset '{symbol}' not found in registry. Available: {list(ASSET_REGISTRY.keys())}")
    return ASSET_REGISTRY[symbol]


def register_asset(config: AssetConfig):
    """
    Register a new asset configuration.
    
    Args:
        config: AssetConfig instance to register
    """
    ASSET_REGISTRY[config.symbol] = config
