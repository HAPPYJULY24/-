"""
Quant Bridge - Unified Facade Entry Point
==========================================

This module provides a clean, unified API for the Quant Data Bridge system.
All core components are exposed through this single entry point.

Usage:
    from src.quant_bridge import BacktestEngine, AssetConfig, OrderRequest, Position

Instead of:
    from src.core.backtest_engine_refactored import BacktestEngine
    from src.core.models.asset import AssetConfig
    from src.core.models.order import OrderRequest
    from src.core.models.trade import Position
"""

# Backtest Engine
from src.core.backtest_engine_refactored import BacktestEngine

# Asset Configuration
from src.core.models.asset import (
    AssetConfig,
    get_asset_config,
    register_asset,
    FCPO_CONFIG,
    FKLI_CONFIG,
    ASSET_REGISTRY
)

# Order Models
from src.core.models.order import (
    OrderRequest,
    OrderResponse
)

# Trade Models
from src.core.models.trade import (
    Position,
    Trade,
    TradeDirection,
    TradeStatus
)

# Vectorized and Event-Driven Engines (for advanced usage)
from src.core.engines.bt_vectorized import VectorizedBacktest
from src.core.engines.bt_event_driven import EventDrivenBacktest
from src.core.engines.alpha_engine import AlphaEngine


# Data Fetchers (for direct adapter access)
from src.core.fetchers.base_adapter import BaseAdapter
from src.core.fetchers.yf_adapter import YFinanceAdapter
from src.core.fetchers.tv_adapter import TradingViewAdapter
from src.core.fetchers.ccxt_adapter import CCXTAdapter

# DataFetcher Facade (backward compatibility for legacy UI)
from src.quant_bridge.data_fetcher_facade import DataFetcher


# Risk Manager (Interceptor Pattern)
try:
    from logic.risk_manager_interceptor import RiskManager
except ImportError:
    # Fallback to legacy if new one doesn't exist
    from logic.risk_manager import RiskManager


__all__ = [
    # === Core Backtest Engine ===
    'BacktestEngine',
    'VectorizedBacktest',
    'EventDrivenBacktest',
    'AlphaEngine',

    
    # === Asset Configuration ===
    'AssetConfig',
    'get_asset_config',
    'register_asset',
    'FCPO_CONFIG',
    'FKLI_CONFIG',
    'ASSET_REGISTRY',
    
    # === Order Models ===
    'OrderRequest',
    'OrderResponse',
    
    # === Trade Models ===
    'Position',
    'Trade',
    'TradeDirection',
    'TradeStatus',
    
    # === Data Fetchers ===
    'BaseAdapter',
    'YFinanceAdapter',
    'TradingViewAdapter',
    'CCXTAdapter',
    'DataFetcher',  # Backward-compatible facade

    
    # === Risk Management ===
    'RiskManager',
]


# Version information
__version__ = '2.0.0-refactored'
__author__ = 'Quant Data Bridge Team'
