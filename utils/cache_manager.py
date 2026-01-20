"""
Cache Manager - Utility for managing Master DB and temporary files.
"""

import os
import shutil
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
import pandas as pd


class CacheManager:
    """缓存和临时文件管理器"""
    
    STORE_DIR = "data/store"
    EXPORTED_DIR = "exported_data"
    
    @staticmethod
    def get_master_db_info() -> Tuple[List[Dict], int, float]:
        """
        获取Master DB统计信息
        
        Returns:
            Tuple of (file_list, total_files, total_size_mb)
            - file_list: 文件信息列表 [{code, timeframe, rows, last_date, size_mb, filepath}, ...]
            - total_files: 总文件数
            - total_size_mb: 总大小(MB)
        """
        store_dir = CacheManager.STORE_DIR
        
        if not os.path.exists(store_dir):
            return [], 0, 0.0
        
        file_list = []
        total_size = 0
        
        for filename in os.listdir(store_dir):
            if filename.endswith('.parquet'):
                filepath = os.path.join(store_dir, filename)
                
                try:
                    # 获取文件大小
                    file_size = os.path.getsize(filepath)
                    total_size += file_size
                    
                    # 读取parquet文件获取数据信息
                    df = pd.read_parquet(filepath)
                    
                    # 解析文件名：{code}_{timeframe}.parquet
                    name_without_ext = filename.replace('.parquet', '')
                    parts = name_without_ext.rsplit('_', 1)
                    
                    if len(parts) == 2:
                        code, timeframe = parts
                    else:
                        code = name_without_ext
                        timeframe = "Unknown"
                    
                    # 获取最新日期
                    df['Date'] = pd.to_datetime(df['Date'])
                    last_date = df['Date'].max()
                    last_date_str = last_date.strftime('%Y-%m-%d') if pd.notna(last_date) else "N/A"
                    
                    # 数据行数
                    row_count = len(df)
                    
                    file_list.append({
                        'code': code,
                        'timeframe': timeframe,
                        'rows': row_count,
                        'last_date': last_date_str,
                        'size_mb': file_size / (1024 * 1024),  # Convert to MB
                        'filepath': filepath
                    })
                    
                except Exception as e:
                    print(f"[WARNING] Failed to read {filename}: {str(e)}")
                    # 即使读取失败，也添加基本信息
                    file_list.append({
                        'code': filename.replace('.parquet', ''),
                        'timeframe': 'N/A',
                        'rows': 0,
                        'last_date': 'N/A',
                        'size_mb': file_size / (1024 * 1024),
                        'filepath': filepath
                    })
        
        total_size_mb = total_size / (1024 * 1024)
        return file_list, len(file_list), total_size_mb
    
    @staticmethod
    def get_exported_data_info() -> Tuple[int, float]:
        """
        获取导出数据目录信息
        
        Returns:
            Tuple of (file_count, total_size_mb)
        """
        exported_dir = CacheManager.EXPORTED_DIR
        
        if not os.path.exists(exported_dir):
            return 0, 0.0
        
        total_size = 0
        file_count = 0
        
        for filename in os.listdir(exported_dir):
            filepath = os.path.join(exported_dir, filename)
            if os.path.isfile(filepath):
                total_size += os.path.getsize(filepath)
                file_count += 1
        
        total_size_mb = total_size / (1024 * 1024)
        return file_count, total_size_mb
    
    @staticmethod
    def delete_master_db_file(filepath: str) -> bool:
        """
        删除指定的Master DB文件
        
        Args:
            filepath: 文件的完整路径
        
        Returns:
            成功返回True，失败返回False
        """
        try:
            if os.path.exists(filepath):
                os.remove(filepath)
                print(f"[INFO] Deleted Master DB file: {filepath}")
                return True
            else:
                print(f"[WARNING] File not found: {filepath}")
                return False
        except Exception as e:
            print(f"[ERROR] Failed to delete {filepath}: {str(e)}")
            return False
    
    @staticmethod
    def clear_all_master_db() -> Tuple[bool, str]:
        """
        清空所有Master DB文件（危险操作！）
        
        Returns:
            Tuple of (success, message)
        """
        store_dir = CacheManager.STORE_DIR
        
        if not os.path.exists(store_dir):
            return True, "Master DB目录不存在，无需清理"
        
        try:
            deleted_count = 0
            for filename in os.listdir(store_dir):
                if filename.endswith('.parquet'):
                    filepath = os.path.join(store_dir, filename)
                    os.remove(filepath)
                    deleted_count += 1
            
            message = f"成功删除 {deleted_count} 个Master DB文件"
            print(f"[INFO] {message}")
            return True, message
        
        except Exception as e:
            error_msg = f"清理失败：{str(e)}"
            print(f"[ERROR] {error_msg}")
            return False, error_msg
    
    @staticmethod
    def export_parquet_to_csv(parquet_filepath: str, output_dir: str = None) -> Tuple[bool, str]:
        """
        将Parquet文件导出为CSV
        
        Args:
            parquet_filepath: Parquet文件路径
            output_dir: 输出目录（默认为exported_data）
        
        Returns:
            Tuple of (success, message_or_filepath)
        """
        if output_dir is None:
            output_dir = CacheManager.EXPORTED_DIR
        
        try:
            # 读取parquet
            df = pd.read_parquet(parquet_filepath)
            
            # 生成CSV文件名
            basename = os.path.basename(parquet_filepath)
            csv_filename = basename.replace('.parquet', '.csv')
            
            # 确保输出目录存在
            os.makedirs(output_dir, exist_ok=True)
            csv_filepath = os.path.join(output_dir, csv_filename)
            
            # 导出CSV
            df.to_csv(csv_filepath, index=False, encoding='utf-8-sig')
            
            print(f"[INFO] Exported to CSV: {csv_filepath}")
            return True, csv_filepath
        
        except Exception as e:
            error_msg = f"导出失败：{str(e)}"
            print(f"[ERROR] {error_msg}")
            return False, error_msg
    
    @staticmethod
    def export_all_to_csv(output_dir: str = None, progress_callback=None) -> Tuple[int, int, List[str]]:
        """
        批量导出所有Master DB为CSV
        
        Args:
            output_dir: 输出目录（默认为exported_data）
            progress_callback: 进度回调函数 callback(current, total, filename)
        
        Returns:
            Tuple of (success_count, fail_count, error_messages)
        """
        if output_dir is None:
            output_dir = CacheManager.EXPORTED_DIR
        
        # 获取所有Master DB文件
        file_list, _, _ = CacheManager.get_master_db_info()
        
        if not file_list:
            return 0, 0, ["No Master DB files found"]
        
        success_count = 0
        fail_count = 0
        errors = []
        total = len(file_list)
        
        for idx, file_info in enumerate(file_list, 1):
            filepath = file_info['filepath']
            filename = os.path.basename(filepath)
            
            # 调用进度回调
            if progress_callback:
                progress_callback(idx, total, filename)
            
            try:
                success, result = CacheManager.export_parquet_to_csv(filepath, output_dir)
                if success:
                    success_count += 1
                else:
                    fail_count += 1
                    errors.append(f"{filename}: {result}")
            except Exception as e:
                fail_count += 1
                errors.append(f"{filename}: {str(e)}")
        
        return success_count, fail_count, errors
    
    @staticmethod
    def cleanup_build_artifacts() -> Tuple[bool, str]:
        """
        清理构建产物（app/, build/, dist/）
        
        Returns:
            Tuple of (success, message)
        """
        dirs_to_clean = ['app', 'build', 'dist']
        cleaned = []
        
        try:
            for dir_name in dirs_to_clean:
                if os.path.exists(dir_name):
                    shutil.rmtree(dir_name)
                    cleaned.append(dir_name)
                    print(f"[INFO] Deleted build directory: {dir_name}/")
            
            if cleaned:
                message = f"已清理构建目录：{', '.join(cleaned)}"
            else:
                message = "没有需要清理的构建目录"
            
            return True, message
        
        except Exception as e:
            error_msg = f"清理失败：{str(e)}"
            print(f"[ERROR] {error_msg}")
            return False, error_msg
    
    @staticmethod
    def cleanup_old_logs(days: int = 7) -> Tuple[bool, str]:
        """
        清理N天前的日志文件
        
        Args:
            days: 保留最近N天的日志
        
        Returns:
            Tuple of (success, message)
        """
        cutoff = datetime.now() - timedelta(days=days)
        deleted = []
        
        try:
            for filename in os.listdir('.'):
                if filename.endswith('.log'):
                    filepath = os.path.join('.', filename)
                    mtime = datetime.fromtimestamp(os.path.getmtime(filepath))
                    
                    if mtime < cutoff:
                        os.remove(filepath)
                        deleted.append(filename)
                        print(f"[INFO] Deleted old log: {filename}")
            
            if deleted:
                message = f"已删除 {len(deleted)} 个旧日志文件"
            else:
                message = f"没有超过 {days} 天的日志需要清理"
            
            return True, message
        
        except Exception as e:
            error_msg = f"清理日志失败：{str(e)}"
            print(f"[ERROR] {error_msg}")
            return False, error_msg
    
    @staticmethod
    def open_directory_in_explorer(directory: str) -> bool:
        """
        在系统文件管理器中打开目录
        
        Args:
            directory: 要打开的目录路径
        
        Returns:
            成功返回True，失败返回False
        """
        import platform
        import subprocess
        
        try:
            # 确保目录存在
            if not os.path.exists(directory):
                os.makedirs(directory, exist_ok=True)
            
            # 获取绝对路径
            abs_path = os.path.abspath(directory)
            
            # 根据操作系统选择命令
            system = platform.system()
            
            if system == 'Windows':
                os.startfile(abs_path)
            elif system == 'Darwin':  # macOS
                subprocess.run(['open', abs_path])
            else:  # Linux
                subprocess.run(['xdg-open', abs_path])
            
            print(f"[INFO] Opened directory in explorer: {abs_path}")
            return True
        
        except Exception as e:
            print(f"[ERROR] Failed to open directory: {str(e)}")
            return False
    
    @staticmethod
    def check_disk_space(path=".") -> Tuple[float, float, float, float]:
        """
        检查磁盘剩余空间
        
        Args:
            path: 要检查的路径（默认当前目录）
        
        Returns:
            Tuple of (total_gb, used_gb, free_gb, percent_used)
        """
        import shutil
        
        try:
            stat = shutil.disk_usage(path)
            total = stat.total / (1024**3)  # Convert to GB
            used = stat.used / (1024**3)
            free = stat.free / (1024**3)
            percent = (used / total) * 100
            
            return (total, used, free, percent)
        except Exception as e:
            print(f"[ERROR] Failed to check disk space: {str(e)}")
            return (0.0, 0.0, 0.0, 0.0)
    
    @staticmethod
    def is_disk_space_low(threshold_gb=1.0, path=".") -> Tuple[bool, float, str]:
        """
        检查磁盘空间是否不足
        
        Args:
            threshold_gb: 阈值（GB）
            path: 要检查的路径
        
        Returns:
            Tuple of (is_low, free_gb, message)
        """
        _, _, free, _ = CacheManager.check_disk_space(path)
        is_low = free < threshold_gb
        
        if is_low:
            msg = f"磁盘空间不足！剩余 {free:.2f} GB"
        else:
            msg = f"磁盘空间充足，剩余 {free:.2f} GB"
        
        return (is_low, free, msg)
    
    @staticmethod
    def load_settings() -> Dict:
        """
        加载配置设置
        
        Returns:
            配置字典
        """
        import json
        
        config_file = "config/settings.json"
        default_settings = {
            "auto_cleanup_logs": True,
            "cleanup_days": 7,
            "last_cleanup": None,
            "disk_warning_threshold_gb": 1.0
        }
        
        try:
            if os.path.exists(config_file):
                with open(config_file, 'r', encoding='utf-8') as f:
                    settings = json.load(f)
                    # 合并默认设置（处理新增的配置项）
                    for key, value in default_settings.items():
                        if key not in settings:
                            settings[key] = value
                    return settings
            else:
                # 创建默认配置文件
                os.makedirs("config", exist_ok=True)
                with open(config_file, 'w', encoding='utf-8') as f:
                    json.dump(default_settings, f, indent=2, ensure_ascii=False)
                return default_settings
        except Exception as e:
            print(f"[ERROR] Failed to load settings: {str(e)}")
            return default_settings
    
    @staticmethod
    def save_settings(settings: Dict) -> bool:
        """
        保存配置设置
        
        Args:
            settings: 配置字典
        
        Returns:
            成功返回True
        """
        import json
        
        config_file = "config/settings.json"
        
        try:
            os.makedirs("config", exist_ok=True)
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(settings, f, indent=2, ensure_ascii=False)
            print(f"[INFO] Settings saved to {config_file}")
            return True
        except Exception as e:
            print(f"[ERROR] Failed to save settings: {str(e)}")
            return False
    
    @staticmethod
    def should_auto_cleanup() -> bool:
        """
        检查是否应该执行自动清理
        
        Returns:
            如果应该清理返回True
        """
        settings = CacheManager.load_settings()
        
        if not settings.get('auto_cleanup_logs', False):
            return False
        
        last_cleanup = settings.get('last_cleanup')
        if last_cleanup is None:
            return True
        
        # 检查距离上次清理是否超过1天
        from datetime import datetime
        try:
            last_date = datetime.fromisoformat(last_cleanup)
            days_since = (datetime.now() - last_date).days
            return days_since >= 1  # 每天最多清理一次
        except:
            return True
    
    @staticmethod
    def perform_auto_cleanup() -> Tuple[bool, str]:
        """
        执行自动清理操作
        
        Returns:
            Tuple of (success, message)
        """
        settings = CacheManager.load_settings()
        cleanup_days = settings.get('cleanup_days', 7)
        
        # 执行清理
        success, message = CacheManager.cleanup_old_logs(cleanup_days)
        
        if success:
            # 更新最后清理时间
            from datetime import datetime
            settings['last_cleanup'] = datetime.now().isoformat()
            CacheManager.save_settings(settings)
        
        return success, message
