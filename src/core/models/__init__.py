"""
Core Models Package
Exports all model classes for easy import.
"""

from .asset import (
    AssetConfig,
    FCPO_CONFIG,
    FKLI_CONFIG,
    ASSET_REGISTRY,
    get_asset_config,
    register_asset
)

from .trade import (
    Trade,
    Position,
    TradeDirection,
    TradeStatus
)

from .order import (
    OrderRequest,
    OrderResponse
)

__all__ = [
    # Asset models
    'AssetConfig',
    'FCPO_CONFIG',
    'FKLI_CONFIG',
    'ASSET_REGISTRY',
    'get_asset_config',
    'register_asset',
    # Trade models
    'Trade',
    'Position',
    'TradeDirection',
    'TradeStatus',
    # Order models
    'OrderRequest',
    'OrderResponse',
]
