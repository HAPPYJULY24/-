# Quant Bridge - Quick Start Guide

## Installation

No installation needed. Just ensure you're in the project directory.

## Basic Usage

### Method 1: Unified Facade (Recommended)

```python
from src.quant_bridge import (
    BacktestEngine,
    AssetConfig,
    get_asset_config,
    OrderRequest,
    Position
)

# Create backtest engine
engine = BacktestEngine()

# Get asset configuration
fcpo_config = get_asset_config('FCPO')
print(f"FCPO Multiplier: {fcpo_config.multiplier}")

# Run backtest
result = engine.run_backtest(
    df=data,
    multiplier=fcpo_config.multiplier,
    commission=5,
    slippage=1,
    initial_capital=100000,
    upper_bound=1.5,
    lower_bound=-1.5,
    initial_margin=fcpo_config.margin_per_lot
)
```

### Method 2: Direct Import (Advanced)

```python
from src.core.engines.bt_vectorized import VectorizedBacktest
from src.core.engines.bt_event_driven import EventDrivenBacktest
from src.core.models.order import OrderRequest
from logic.risk_manager_interceptor import RiskManager

# Use event-driven with RiskManager
engine = EventDrivenBacktest(signal_emitter=None)
result = engine.run(
    df=data,
    asset_symbol='FCPO',
    RiskManagerClass=RiskManager,
    multiplier=25,
    ...
)
```

## Asset Configuration

### Predefined Assets

```python
from src.quant_bridge import FCPO_CONFIG, FKLI_CONFIG

print(FCPO_CONFIG.multiplier)  # 25
print(FKLI_CONFIG.multiplier)  # 50
```

### Register New Asset

```python
from src.quant_bridge import AssetConfig, register_asset

sgx_config = AssetConfig(
    symbol="SGXFE",
    name="SGX Iron Ore Futures",
    exchange="SGX",
    multiplier=100,
    margin_per_lot=8000,
    tick_size=0.5
)

register_asset("SGXFE", sgx_config)
```

## Data Fetching

```python
from src.quant_bridge import YFinanceAdapter, TradingViewAdapter

# Yahoo Finance
yf = YFinanceAdapter()
data = yf.fetch(code="AAPL", timeframe="1d", start_date="2024-01-01", end_date="2024-12-31")

# TradingView (Bursa Malaysia)
tv = TradingViewAdapter()
data = tv.fetch(code="FCPO1!", timeframe="15", start_date="2024-01-01", end_date="2024-12-31")
```

## Risk Management

### Legacy Mode (Puppet)

```python
from src.quant_bridge import RiskManager

rm = RiskManager(
    initial_capital=100000,
    multiplier=25,
    risk_params={'use_adx': True, 'adx_threshold': 20}
)

# Calculate position size
lots = rm.calculate_lots(atr=12.5)
```

### Interceptor Pattern (New)

```python
from src.quant_bridge import RiskManager, OrderRequest

rm = RiskManager(initial_capital=100000, multiplier=25)

# Create order request
order = OrderRequest(
    symbol='FCPO',
    volume=10,
    direction=1,
    price=2850.0,
    atr=12.5,
    adx=25.0
)

# Validate order
response = rm.validate_order(order)

if response.approved:
    print(f"✅ Order approved: {response.adjusted_volume} lots")
else:
    print(f"🚫 Order rejected: {response.reason}")
```

## Complete Example

```python
from src.quant_bridge import (
    BacktestEngine,
    get_asset_config,
    YFinanceAdapter
)
import pandas as pd

# 1. Fetch data
fetcher = YFinanceAdapter()
df = fetcher.fetch(
    code="1155.KL",  # Maybank
    timeframe="1d",
    start_date="2023-01-01",
    end_date="2024-01-01"
)

# 2. Add factor (simple moving average crossover)
df['sma_fast'] = df['close'].rolling(10).mean()
df['sma_slow'] = df['close'].rolling(30).mean()
df['factor'] = df['sma_fast'] - df['sma_slow']

# 3. Get asset config
asset = get_asset_config('FCPO')  # Use FCPO as proxy

# 4. Run backtest
engine = BacktestEngine()
result = engine.run_backtest(
    df=df,
    multiplier=asset.multiplier,
    commission=5,
    slippage=1,
    initial_capital=100000,
    upper_bound=0,  # Buy when fast > slow
    lower_bound=0,  # Sell when fast < slow
    initial_margin=asset.margin_per_lot,
    execution_mode='Close',
    risk_target=2.0  # 2% risk per trade
)

# 5. Display results
print("=== Backtest Results ===")
for key, value in result['metrics'].items():
    print(f"{key}: {value}")
```

## Migration from Legacy Code

### Before (Old)

```python
from core.backtest_engine import BacktestEngine  # ❌ Legacy
from core.data_fetcher import DataFetcher        # ❌ Legacy
```

### After (New)

```python
from src.quant_bridge import BacktestEngine      # ✅ Unified
from src.quant_bridge import YFinanceAdapter     # ✅ Unified
```

## API Reference

See `src/quant_bridge/__init__.py` for full list of exports.
