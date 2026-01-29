"""
Configuration Manager - Secure credential storage for Quant Data Bridge
Handles reading/writing of user settings with encryption.
"""

import os
import json
import base64
from pathlib import Path


class ConfigManager:
    """
    Manages application configuration with secure credential storage.
    
    Configuration is stored in: <user_home>/.quant_data_bridge/config.json
    Credentials are base64 encoded (basic obfuscation, not military-grade encryption).
    """
    
    def __init__(self):
        # 使用用户主目录存储配置（便携式）
        self.config_dir = Path.home() / ".quant_data_bridge"
        self.config_file = self.config_dir / "config.json"
        
        # 确保配置目录存在
        self.config_dir.mkdir(exist_ok=True)
        
        # 默认配置
        self.default_config = {
            "tradingview": {
                "username": "",
                "password": "",
                "enabled": False
            }
        }
    
    def _encode_password(self, password: str) -> str:
        """使用 Base64 编码密码（基础混淆）"""
        if not password:
            return ""
        return base64.b64encode(password.encode('utf-8')).decode('utf-8')
    
    def _decode_password(self, encoded: str) -> str:
        """解码 Base64 密码"""
        if not encoded:
            return ""
        try:
            return base64.b64decode(encoded.encode('utf-8')).decode('utf-8')
        except Exception:
            return ""
    
    def load_config(self) -> dict:
        """
        加载配置文件
        
        Returns:
            dict: 配置字典
        """
        if not self.config_file.exists():
            return self.default_config.copy()
        
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # 解码密码
            if 'tradingview' in config and 'password' in config['tradingview']:
                config['tradingview']['password'] = self._decode_password(
                    config['tradingview']['password']
                )
            
            return config
        except Exception as e:
            print(f"[WARNING] Failed to load config: {e}")
            return self.default_config.copy()
    
    def save_config(self, config: dict) -> bool:
        """
        保存配置文件
        
        Args:
            config: 配置字典
        
        Returns:
            bool: 是否保存成功
        """
        try:
            # 编码密码
            config_to_save = config.copy()
            if 'tradingview' in config_to_save and 'password' in config_to_save['tradingview']:
                config_to_save['tradingview']['password'] = self._encode_password(
                    config_to_save['tradingview']['password']
                )
            
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(config_to_save, f, indent=4, ensure_ascii=False)
            
            print(f"[INFO] Config saved to {self.config_file}")
            return True
        except Exception as e:
            print(f"[ERROR] Failed to save config: {e}")
            return False
    
    def get_tradingview_credentials(self) -> tuple:
        """
        获取 TradingView 凭证
        
        Returns:
            tuple: (username, password, enabled)
        """
        config = self.load_config()
        tv_config = config.get('tradingview', {})
        
        return (
            tv_config.get('username', ''),
            tv_config.get('password', ''),
            tv_config.get('enabled', False)
        )
    
    def save_tradingview_credentials(self, username: str, password: str, enabled: bool = True) -> bool:
        """
        保存 TradingView 凭证
        
        Args:
            username: TradingView 用户名/邮箱
            password: TradingView 密码
            enabled: 是否启用认证
        
        Returns:
            bool: 是否保存成功
        """
        config = self.load_config()
        config['tradingview'] = {
            'username': username,
            'password': password,
            'enabled': enabled
        }
        return self.save_config(config)
    
    def clear_tradingview_credentials(self) -> bool:
        """
        清除 TradingView 凭证
        
        Returns:
            bool: 是否清除成功
        """
        config = self.load_config()
        config['tradingview'] = {
            'username': '',
            'password': '',
            'enabled': False
        }
        return self.save_config(config)
    
    def get_config_file_path(self) -> str:
        """获取配置文件路径（用于显示给用户）"""
        return str(self.config_file)
