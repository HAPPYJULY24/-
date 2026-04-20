"""
Localization Module - 国际化模块

Provides bilingual support (Chinese/English) for the application.
Handles language loading, switching, and translation key lookup.
"""

import json
from pathlib import Path
from typing import Dict, Optional


class Localization:
    """国际化管理器"""
    
    _instance = None
    _current_language = "zh_CN"  # 默认中文
    _translations: Dict[str, Dict] = {}
    _config_file = Path("config/settings.json")
    _lang_dir = Path("config/languages")
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load_config()
            cls._instance._load_all_languages()
        return cls._instance
    
    def _load_config(self):
        """加载配置文件"""
        try:
            if self._config_file.exists():
                with open(self._config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    self._current_language = config.get('language', 'zh_CN')
        except Exception as e:
            print(f"[Localization] Failed to load config: {e}")
            self._current_language = "zh_CN"
    
    def _save_config(self):
        """保存配置文件"""
        try:
            self._config_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self._config_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'language': self._current_language
                }, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"[Localization] Failed to save config: {e}")
    
    def _load_all_languages(self):
        """加载所有语言文件"""
        self._lang_dir.mkdir(parents=True, exist_ok=True)
        
        for lang_file in self._lang_dir.glob("*.json"):
            lang_code = lang_file.stem
            try:
                with open(lang_file, 'r', encoding='utf-8') as f:
                    self._translations[lang_code] = json.load(f)
                print(f"[Localization] Loaded language: {lang_code}")
            except Exception as e:
                print(f"[Localization] Failed to load {lang_code}: {e}")
    
    def set_language(self, lang_code: str):
        """设置当前语言"""
        if lang_code in self._translations:
            self._current_language = lang_code
            self._save_config()
            self._configure_chart_fonts(lang_code)
            print(f"[Localization] Language set to: {lang_code}")
            return True
        else:
            print(f"[Localization] Language not found: {lang_code}")
            return False
    
    def _configure_chart_fonts(self, lang_code: str):
        """
        根据语言配置图表字体（matplotlib, pyqtgraph等）
        
        Args:
            lang_code: 语言代码
        """
        try:
            import matplotlib.pyplot as plt
            
            if lang_code == "zh_CN":
                # 中文字体配置
                plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'DejaVu Sans']
                plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示问题
                print("[Localization] Configured Chinese fonts for matplotlib charts")
            else:
                # 英文默认字体
                plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'Arial']
                plt.rcParams['axes.unicode_minus'] = True
                print("[Localization] Configured default fonts for matplotlib charts")
                
        except ImportError:
            # matplotlib未安装，跳过
            pass
    
    def get_current_language(self) -> str:
        """获取当前语言代码"""
        return self._current_language
    
    def get_available_languages(self) -> Dict[str, str]:
        """获取可用语言列表"""
        return {
            "zh_CN": "简体中文",
            "en_US": "English"
        }
    
    def translate(self, key: str, **kwargs) -> str:
        """
        获取翻译文本，支持嵌套键和格式化参数
        
        Args:
            key: 翻译键，支持点号分隔的嵌套键 (e.g., 'alignment.title')
            **kwargs: 格式化参数
        
        Returns:
            翻译后的文本，如果找不到则返回键本身
        """
        # 获取当前语言的翻译字典
        translations = self._translations.get(self._current_language, {})
        
        # 使用点号分割键，逐层查找
        keys = key.split('.')
        value = translations
        
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
                if value is None:
                    # 键不存在，返回原键
                    return key
            else:
                # 中间层不是字典，返回原键
                return key
        
        # 如果最终值不是字符串，返回原键
        if not isinstance(value, str):
            return key
        
        # 格式化字符串（如果有参数）
        if kwargs:
            try:
                # 支持 {count}, {error} 等格式化
                value = value.format(**kwargs)
            except KeyError:
                # 格式化失败，返回原字符串
                pass
        
        return value


# 全局实例
_localization = Localization()


def tr(key: str, **kwargs) -> str:
    """
    便捷翻译函数
    
    Usage:
        from logic.localization import tr
        
        title = tr("window.title")
        message = tr("status.files_found", count=5)
    """
    return _localization.translate(key, **kwargs)


def set_language(lang_code: str) -> bool:
    """设置语言"""
    return _localization.set_language(lang_code)


def get_current_language() -> str:
    """获取当前语言"""
    return _localization.get_current_language()


def get_available_languages() -> Dict[str, str]:
    """获取可用语言"""
    return _localization.get_available_languages()
