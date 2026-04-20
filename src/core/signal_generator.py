import pandas as pd
from abc import ABC, abstractmethod

class BaseSignalGenerator(ABC):
    """
    Base interface for all signal generators.
    Transforms raw factor data into standard trading signals (-1, 0, 1).
    Stateless: Evaluates bar-by-bar without retaining position memory.
    """
    
    @abstractmethod
    def generate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        """
        Produce a Series of signals (1: LONG, -1: SHORT, 0: FLAT)
        based on the provided DataFrame.
        """
        pass

class MeanReversionGenerator(BaseSignalGenerator):
    """
    Mean Reversion Logic:
    - Long (1) when factor < lower_bound (Oversold)
    - Short (-1) when factor > upper_bound (Overbought)
    - Flat (0) when factor is between lower_bound and upper_bound (or crosses 0 for exit)
    """
    def generate(self, df: pd.DataFrame, upper_bound: float = 2.0, lower_bound: float = -2.0, **kwargs) -> pd.Series:
        signals = pd.Series(0, index=df.index)
        
        # Ensure 'factor' exists
        factor = df.get('factor', pd.Series(0.0, index=df.index))
        
        # Generate signals natively
        signals.loc[factor < lower_bound] = 1
        signals.loc[factor > upper_bound] = -1
        
        # Exit conditions: in mean reversion, hitting 0 means revert to mean (neutral)
        # Note: Event-driven engine will decide to close if it holds LONG and sees factor>=0,
        # but the engine itself will now look at `signal`.
        # To make it perfectly align with existing logic:
        # Existing logic: if LONG and factor>=0 -> close. if SHORT and factor<=0 -> close.
        # This implies the signal itself shouldn't tell the engine to explicitly close unless we want to map "Close" to a specific enum.
        # However, a cleaner pipeline is: 
        # Engine State: FLAT -> sees 1 -> ENTER LONG.
        # Engine State: LONG -> sees 0 -> close? or sees opposite?
        # To maintain exact compatibility with the old "factor >= 0 closes LONG", 
        # we can define an explicitly "EXIT" signal, e.g., 2 for close LONG, -2 for close SHORT?
        # NO. User requested: 1=LONG, -1=SHORT, 0=FLAT/NEUTRAL.
        # Engine rule:
        # If State=LONG, and signal=0 -> Close.
        # Let's map "factor that dictates close" to signal=0.
        
        # So we map factor logic -> signal space.
        # If factor > upper: -1
        # If factor < lower: 1
        # otherwise 0.
        return signals

class MomentumBreakoutGenerator(BaseSignalGenerator):
    """
    Momentum/Breakout Logic:
    - Long (1) when factor > upper_bound (Bullish Breakout)
    - Short (-1) when factor < lower_bound (Bearish Breakdown)
    - Flat (0) when factor is between bounds
    """
    def generate(self, df: pd.DataFrame, upper_bound: float = 2.0, lower_bound: float = -2.0, **kwargs) -> pd.Series:
        signals = pd.Series(0, index=df.index)
        factor = df.get('factor', pd.Series(0.0, index=df.index))
        
        signals.loc[factor > upper_bound] = 1
        signals.loc[factor < lower_bound] = -1
        return signals

class SignalFactory:
    """
    Factory to resolve strategy names to Generator instances.
    """
    
    _registry = {
        'Mean Reversion': MeanReversionGenerator,
        'Momentum Breakout': MomentumBreakoutGenerator
    }
    
    @classmethod
    def create(cls, strategy_name: str) -> BaseSignalGenerator:
        generator_class = cls._registry.get(strategy_name, MeanReversionGenerator)
        return generator_class()

    @classmethod
    def get_available_strategies(cls) -> list[str]:
        return list(cls._registry.keys())
