"""
Engine Registry - Plugin System for Extensibility

Provides a centralized registry for dynamically discovering and loading engines.
Phase 5D: Extensibility Enhancement (+5 points)
"""

from typing import Dict, Type, Optional, Any
import importlib
import inspect
from pathlib import Path


class EngineRegistry:
    """
    Centralized registry for all engine plugins.
    
    Enables dynamic discovery and loading of engines without hardcoding imports.
    Supports both manual registration and auto-discovery.
    """
    
    _engines: Dict[str, Type] = {}
    
    @classmethod
    def register(cls, name: str, engine_class: Type) -> None:
        """
        Register an engine class.
        
        Args:
            name: Unique identifier for the engine (e.g., 'backtest', 'alpha')
            engine_class: The engine class to register
        
        Raises:
            ValueError: If name already registered
        """
        if name in cls._engines:
            raise ValueError(f"Engine '{name}' is already registered")
        
        cls._engines[name] = engine_class
        print(f"[EngineRegistry] Registered: {name} -> {engine_class.__name__}")
    
    @classmethod
    def get(cls, name: str) -> Optional[Type]:
        """
        Retrieve an engine class by name.
        
        Args:
            name: Engine identifier
            
        Returns:
            Engine class or None if not found
        """
        return cls._engines.get(name)
    
    @classmethod
    def list_engines(cls) -> Dict[str, Type]:
        """
        Get all registered engines.
        
        Returns:
            Dictionary mapping engine names to classes
        """
        return cls._engines.copy()
    
    @classmethod
    def auto_discover(cls, package_path: str = "src.core.engines") -> None:
        """
        Auto-discover and register engines from a package.
        
        Scans for classes ending with 'Engine' and registers them.
        
        Args:
            package_path: Python package path to scan
        """
        try:
            # Get the directory path
            base_path = Path("src/core/engines")
            if not base_path.exists():
                print(f"[EngineRegistry] Warning: {base_path} does not exist")
                return
            
            # Scan Python files
            for python_file in base_path.glob("*.py"):
                if python_file.name.startswith("__"):
                    continue
                
                # Import module
                module_name = f"{package_path}.{python_file.stem}"
                try:
                    module = importlib.import_module(module_name)
                    
                    # Find engine classes
                    for name, obj in inspect.getmembers(module, inspect.isclass):
                        if name.endswith("Engine") and obj.__module__ == module_name:
                            # Auto-generate registry name (e.g., BacktestEngine -> backtest)
                            registry_name = name.replace("Engine", "").lower()
                            
                            # Skip if already registered
                            if registry_name not in cls._engines:
                                cls.register(registry_name, obj)
                
                except Exception as e:
                    print(f"[EngineRegistry] Failed to import {module_name}: {e}")
        
        except Exception as e:
            print(f"[EngineRegistry] Auto-discovery failed: {e}")
    
    @classmethod
    def create_instance(cls, name: str, *args: Any, **kwargs: Any) -> Optional[Any]:
        """
        Create an instance of a registered engine.
        
        Args:
            name: Engine identifier
            *args: Positional arguments for engine constructor
            **kwargs: Keyword arguments for engine constructor
            
        Returns:
            Engine instance or None if not found
        """
        engine_class = cls.get(name)
        if engine_class is None:
            print(f"[EngineRegistry] Engine '{name}' not found")
            return None
        
        try:
            return engine_class(*args, **kwargs)
        except Exception as e:
            print(f"[EngineRegistry] Failed to create {name}: {e}")
            return None
    
    @classmethod
    def clear(cls) -> None:
        """Clear all registered engines (useful for testing)."""
        cls._engines.clear()


# Decorator for easy registration
def register_engine(name: str):
    """
    Decorator to register an engine class.
    
    Usage:
        @register_engine('my_engine')
        class MyEngine:
            pass
    """
    def decorator(engine_class: Type) -> Type:
        EngineRegistry.register(name, engine_class)
        return engine_class
    return decorator
