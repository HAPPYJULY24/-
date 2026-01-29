"""
数据处理模块 - 用于对齐和合并多个期货品种的数据

主要功能：
1. 数据重采样 (确保统一时间粒度)
2. 数据对齐与合并 (处理不同交易时间)
3. 生成 Ready-to-Use 数据集用于回测
"""

import os
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Optional, Tuple


class DataProcessor:
    """数据处理器 - 负责多品种数据的对齐与合并"""
    
    def __init__(self, store_dir: str = "data/store", output_dir: str = "data/processed"):
        """
        初始化数据处理器
        
        Args:
            store_dir: 原始数据存储目录
            output_dir: 处理后数据输出目录
        """
        self.store_dir = Path(store_dir)
        self.output_dir = Path(output_dir)
        
        # 确保输出目录存在
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"[DataProcessor] 初始化完成")
        print(f"[DataProcessor] 数据源目录: {self.store_dir}")
        print(f"[DataProcessor] 输出目录: {self.output_dir}")
    
    def align_datasets(
        self, 
        base_symbol: str = 'FCPO1!', 
        target_symbol: str = 'ZL1!',
        timeframe: str = '15m',
        output_filename: Optional[str] = None
    ) -> pd.DataFrame:
        """
        对齐并合并两个期货品种的数据
        
        Args:
            base_symbol: 基准品种代码 (如 FCPO1!)
            target_symbol: 目标品种代码 (如 ZL1!)
            timeframe: 时间粒度 (如 15m)
            output_filename: 输出文件名 (默认自动生成)
        
        Returns:
            合并后的 DataFrame
        
        Raises:
            FileNotFoundError: 如果数据文件不存在
            ValueError: 如果数据格式不正确
        """
        print(f"\n{'='*60}")
        print(f"[DataProcessor] 开始数据对齐处理")
        print(f"[DataProcessor] 基准品种: {base_symbol}")
        print(f"[DataProcessor] 目标品种: {target_symbol}")
        print(f"[DataProcessor] 时间粒度: {timeframe}")
        print(f"{'='*60}\n")
        
        # 1. 读取数据文件
        base_df = self._load_data(base_symbol, timeframe)
        target_df = self._load_data(target_symbol, timeframe)
        
        print(f"[DataProcessor] ✅ 数据加载完成")
        print(f"  - {base_symbol}: {len(base_df)} 行, 时间范围 {base_df['Date'].min()} ~ {base_df['Date'].max()}")
        print(f"  - {target_symbol}: {len(target_df)} 行, 时间范围 {target_df['Date'].min()} ~ {target_df['Date'].max()}")
        
        # 2. 重采样 (确保统一时间粒度)
        base_df = self._resample_data(base_df, timeframe, f"{base_symbol}_")
        target_df = self._resample_data(target_df, timeframe, f"{target_symbol}_")
        
        # 3. 合并数据 (Outer Join 保留所有时间点)
        print(f"\n[DataProcessor] 📊 开始合并数据 (Outer Join)...")
        merged_df = pd.merge(
            base_df, 
            target_df, 
            left_index=True, 
            right_index=True, 
            how='outer',
            suffixes=('', '_drop')  # 避免列名冲突
        )
        
        # 删除重复的 Date 列（如果有）
        merged_df = merged_df[[col for col in merged_df.columns if not col.endswith('_drop')]]
        
        print(f"[DataProcessor] ✅ 合并完成: {len(merged_df)} 行")
        
        # 4. 添加 overlap 标记列
        merged_df = self._add_overlap_flag(merged_df, base_symbol, target_symbol)
        
        # 5. 重置索引，确保 Date 为列
        merged_df.reset_index(inplace=True)
        if 'index' in merged_df.columns:
            merged_df.rename(columns={'index': 'Date'}, inplace=True)
        
        # 6. 数据统计
        self._print_statistics(merged_df, base_symbol, target_symbol)
        
        # 7. 保存文件
        if output_filename is None:
            output_filename = f"merged_{base_symbol.replace('!', '')}_{target_symbol.replace('!', '')}_{timeframe}.parquet"
        
        output_path = self.output_dir / output_filename
        merged_df.to_parquet(output_path, index=False)
        
        print(f"\n[DataProcessor] 💾 数据已保存: {output_path}")
        print(f"[DataProcessor] 文件大小: {output_path.stat().st_size / 1024 / 1024:.2f} MB")
        print(f"\n{'='*60}")
        
        return merged_df
    
    def _load_data(self, symbol: str, timeframe: str) -> pd.DataFrame:
        """
        加载 Parquet 数据文件
        
        Args:
            symbol: 期货代码
            timeframe: 时间粒度
        
        Returns:
            DataFrame with Date column
        """
        # 构造文件名 (Master DB 格式: symbol_timeframe.parquet)
        filename = f"{symbol}_{timeframe}.parquet"
        filepath = self.store_dir / filename
        
        if not filepath.exists():
            raise FileNotFoundError(
                f"数据文件不存在: {filepath}\n\n"
                f"请先下载数据：\n"
                f"1. 在主界面选择 'Bursa期货 (TV)'\n"
                f"2. 输入代码: {symbol}\n"
                f"3. 选择时间粒度: {timeframe}\n"
                f"4. 点击下载"
            )
        
        print(f"[DataProcessor] 📖 读取文件: {filepath.name}")
        df = pd.read_parquet(filepath)
        
        # 确保 Date 列存在
        if 'Date' not in df.columns:
            if df.index.name == 'Date' or isinstance(df.index, pd.DatetimeIndex):
                df.reset_index(inplace=True)
                if 'index' in df.columns:
                    df.rename(columns={'index': 'Date'}, inplace=True)
            else:
                raise ValueError(f"数据文件缺少 'Date' 列: {filepath}")
        
        # 确保 Date 为 datetime 类型
        df['Date'] = pd.to_datetime(df['Date'])
        
        return df
    
    def _resample_data(self, df: pd.DataFrame, timeframe: str, prefix: str = "") -> pd.DataFrame:
        """
        重采样数据到指定时间粒度
        
        Args:
            df: 原始 DataFrame
            timeframe: 目标时间粒度 (如 '15m', '1h', '1d')
            prefix: 列名前缀 (用于区分不同品种)
        
        Returns:
            重采样后的 DataFrame (Date 作为 index)
        """
        print(f"[DataProcessor] 🔄 重采样数据到 {timeframe}...")
        
        # 设置 Date 为索引
        df_resampled = df.set_index('Date')
        
        # 映射时间粒度
        freq_map = {
            '1m': '1min',
            '5m': '5min',
            '15m': '15min',
            '30m': '30min',
            '1h': '1H',
            '4h': '4H',
            '1d': '1D',
            '1w': '1W',
            '1M': '1ME'  # Month end
        }
        
        freq = freq_map.get(timeframe, timeframe)
        
        # OHLCV 重采样规则
        agg_dict = {
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum'
        }
        
        # 只保留存在的列
        agg_dict = {k: v for k, v in agg_dict.items() if k in df_resampled.columns}
        
        # 执行重采样
        df_resampled = df_resampled.resample(freq).agg(agg_dict)
        
       # 删除全为 NaN 的行 (没有数据的时间段)
        df_resampled = df_resampled.dropna(how='all')
        
        # 添加列名前缀
        if prefix:
            df_resampled.columns = [f"{prefix}{col}" for col in df_resampled.columns]
        
        print(f"[DataProcessor]   → 重采样后: {len(df_resampled)} 行")
        
        return df_resampled
    
    def _add_overlap_flag(self, df: pd.DataFrame, base_symbol: str, target_symbol: str) -> pd.DataFrame:
        """
        添加 overlap 标记列，标示两个品种都有交易的时间段
        
        Args:
            df: 合并后的 DataFrame
            base_symbol: 基准品种代码
            target_symbol: 目标品种代码
        
        Returns:
            添加了 is_overlap 列的 DataFrame
        """
        print(f"[DataProcessor] 🏷️  添加 overlap 标记...")
        
        # 检查两个品种的 Close 列是否都有数据
        base_col = f"{base_symbol}_Close"
        target_col = f"{target_symbol}_Close"
        
        if base_col in df.columns and target_col in df.columns:
            df['is_overlap'] = df[base_col].notna() & df[target_col].notna()
            overlap_count = df['is_overlap'].sum()
            print(f"[DataProcessor]   → 重叠时间段: {overlap_count} 行 ({overlap_count/len(df)*100:.1f}%)")
        else:
            print(f"[DataProcessor]   ⚠️  未找到 Close 列，跳过 overlap 标记")
            df['is_overlap'] = False
        
        return df
    
    def _print_statistics(self, df: pd.DataFrame, base_symbol: str, target_symbol: str):
        """打印数据统计信息"""
        print(f"\n[DataProcessor] 📈 数据统计:")
        print(f"  - 总行数: {len(df)}")
        print(f"  - 时间范围: {df['Date'].min()} ~ {df['Date'].max()}")
        print(f"  - 时间跨度: {(df['Date'].max() - df['Date'].min()).days} 天")
        
        # 计算各品种的数据完整性
        base_close_col = f"{base_symbol}_Close"
        target_close_col = f"{target_symbol}_Close"
        
        if base_close_col in df.columns:
            base_coverage = df[base_close_col].notna().sum() / len(df) * 100
            print(f"  - {base_symbol} 数据覆盖率: {base_coverage:.1f}%")
        
        if target_close_col in df.columns:
            target_coverage = df[target_close_col].notna().sum() / len(df) * 100
            print(f"  - {target_symbol} 数据覆盖率: {target_coverage:.1f}%")
        
        if 'is_overlap' in df.columns:
            overlap_pct = df['is_overlap'].sum() / len(df) * 100
            print(f"  - 重叠时间段占比: {overlap_pct:.1f}%")
    
    # ========== 🆕 Generic Alignment Method for GUI ==========
    
    def align_custom_files(
        self,
        file_path_a: str,
        file_path_b: str,
        output_filename: Optional[str] = None,
        apply_ffill: bool = True,
        ffill_asset: str = 'B'  # 'A', 'B', or 'both'
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        通用文件对齐方法 - 支持任意两个 Parquet 文件的对齐 (GUI 版本)
        
        **Killer Fixes:**
        1. 时区处理：自动检测并统一转换为 UTC
        2. 动态列名：从文件名提取 symbol 并重命名列
        3. 前向填充：可选的 ffill() 处理不同交易时间
        
        Args:
            file_path_a: Asset A 文件路径 (Base)
            file_path_b: Asset B 文件路径 (Reference)
            output_filename: 输出文件名 (默认自动生成)
            apply_ffill: 是否应用前向填充
            ffill_asset: 对哪个资产应用填充 ('A', 'B', or 'both')
        
        Returns:
            Tuple[完整 DataFrame, 预览 DataFrame (前50+后50行)]
        """
        print(f"\n{'='*70}")
        print(f"[DataProcessor] 🔄 Generic Alignment - GUI Mode")
        print(f"{'='*70}")
        print(f"[Asset A (Base)]:      {Path(file_path_a).name}")
        print(f"[Asset B (Reference)]: {Path(file_path_b).name}")
        print(f"{'='*70}\n")
        
        # 1. 提取 Symbol 名称从文件名
        symbol_a = self._extract_symbol_from_filename(file_path_a)
        symbol_b = self._extract_symbol_from_filename(file_path_b)
        
        print(f"[DataProcessor] 📝 提取的 Symbol:")
        print(f"  - Asset A: {symbol_a}")
        print(f"  - Asset B: {symbol_b}\n")
        
        # 2. 加载数据文件 (直接从路径)
        df_a = self._load_parquet_file(file_path_a, symbol_a)
        df_b = self._load_parquet_file(file_path_b, symbol_b)
        
        print(f"[DataProcessor] ✅ 文件加载完成")
        print(f"  - {symbol_a}: {len(df_a)} 行")
        print(f"  - {symbol_b}: {len(df_b)} 行\n")
        
        # 3. 🔥 Killer Fix 1: 时区处理
        df_a = self._fix_timezone(df_a, symbol_a)
        df_b = self._fix_timezone(df_b, symbol_b)
        
        # 4. 🔥 Killer Fix 2: 动态列名重命名
        df_a = self._rename_columns_with_prefix(df_a, symbol_a)
        df_b = self._rename_columns_with_prefix(df_b, symbol_b)
        
        # 5. 合并数据 (Outer Join)
        print(f"[DataProcessor] 📊 合并数据 (Outer Join)...\n")
        
        # 使用 concat 而不是 merge，因为 Date 已经是 index
        merged_df = pd.concat([df_a, df_b], axis=1, join='outer')
        
        print(f"[DataProcessor] ✅ 合并完成: {len(merged_df)} 行\n")
        
        # 6. 🔥 Killer Fix 3: 前向填充 (Forward Fill)
        if apply_ffill:
            merged_df = self._apply_forward_fill(merged_df, symbol_a, symbol_b, ffill_asset)
        
        # 7. 添加 overlap 标记
        merged_df = self._add_generic_overlap_flag(merged_df, symbol_a, symbol_b)
        
        # 8. 重置索引，将 Date 转为列
        merged_df.reset_index(inplace=True)
        if 'index' in merged_df.columns:
            merged_df.rename(columns={'index': 'Date'}, inplace=True)
        
        # 9. 统计信息
        self._print_generic_statistics(merged_df, symbol_a, symbol_b)
        
        # 10. 保存文件
        if output_filename is None:
            output_filename = f"aligned_{symbol_a.replace('!', '')}_{symbol_b.replace('!', '')}.parquet"
        
        output_path = self.output_dir / output_filename
        merged_df.to_parquet(output_path, index=False)
        
        print(f"\n[DataProcessor] 💾 数据已保存: {output_path}")
        print(f"[DataProcessor] 文件大小: {output_path.stat().st_size / 1024 / 1024:.2f} MB\n")
        print(f"{'='*70}\n")
        
        # 11. 生成预览 DataFrame (前50 + 后50行)
        preview_df = self._generate_preview(merged_df)
        
        return merged_df, preview_df
    
    def _extract_symbol_from_filename(self, filepath: str) -> str:
        """
        从文件名提取 Symbol
        例如: FCPO1!_15m.parquet -> FCPO1!
        """
        filename = Path(filepath).stem  # 去掉扩展名
        # 假设格式是 {symbol}_{timeframe}
        parts = filename.rsplit('_', 1)  # 从右边分割一次
        return parts[0] if parts else filename
    
    def _load_parquet_file(self, filepath: str, symbol: str) -> pd.DataFrame:
        """
        加载单个 Parquet 文件并返回 DataFrame
        """
        filepath = Path(filepath)
        
        if not filepath.exists():
            raise FileNotFoundError(f"文件不存在: {filepath}")
        
        print(f"[DataProcessor] 📖 读取: {filepath.name}")
        df = pd.read_parquet(filepath)
        
        # 确保有 Date 列或索引
        if 'Date' not in df.columns:
            if df.index.name == 'Date' or isinstance(df.index, pd.DatetimeIndex):
                df.reset_index(inplace=True)
                if 'index' in df.columns:
                    df.rename(columns={'index': 'Date'}, inplace=True)
            else:
                raise ValueError(f"数据文件缺少 'Date' 列或索引: {filepath}")
        
        # 设置 Date 为索引
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
        
        return df
    
    def _fix_timezone(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """
        🔥 Killer Fix 1: 时区处理
        
        检查索引时区，如果有时区则转换为 UTC，如果没有则发出警告
        """
        print(f"[Timezone Fix] 检查 {symbol} 的时区...")
        
        if df.index.tz is not None:
            # 有时区 - 转换为 UTC
            original_tz = df.index.tz
            print(f"  ✅ 检测到时区: {original_tz} → 转换为 UTC")
            df.index = df.index.tz_convert('UTC')
        else:
            # 没有时区 (naive datetime)
            print(f"  ⚠️  警告: {symbol} 的时间戳为 naive (无时区)")
            print(f"     假设为本地时间，不进行时区转换")
            print(f"     建议: 确保所有数据源使用统一时区\n")
        
        return df
    
    def _rename_columns_with_prefix(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """
        🔥 Killer Fix 2: 动态列名重命名
        
        将标准列名 (Open, High, Low, Close, Volume) 重命名为 {symbol}_Open 等
        """
        print(f"[Column Rename] 为 {symbol} 添加前缀...")
        
        rename_map = {}
        for col in df.columns:
            if col in ['Open', 'High', 'Low', 'Close', 'Volume']:
                rename_map[col] = f"{symbol}_{col}"
        
        df.rename(columns=rename_map, inplace=True)
        
        print(f"  ✅ 重命名列: {list(rename_map.values())}\n")
        
        return df
    
    def _apply_forward_fill(
        self, 
        df: pd.DataFrame, 
        symbol_a: str, 
        symbol_b: str, 
        ffill_asset: str
    ) -> pd.DataFrame:
        """
        🔥 Killer Fix 3: 前向填充 (Forward Fill)
        
        对指定资产的列应用 ffill() 以填补交易时间差异
        """
        print(f"[Forward Fill] 应用前向填充 (asset: {ffill_asset})...")
        
        if ffill_asset == 'A' or ffill_asset == 'both':
            cols_a = [col for col in df.columns if col.startswith(f"{symbol_a}_")]
            if cols_a:
                df[cols_a] = df[cols_a].ffill()
                print(f"  ✅ 填充 Asset A ({symbol_a}): {len(cols_a)} 列")
        
        if ffill_asset == 'B' or ffill_asset == 'both':
            cols_b = [col for col in df.columns if col.startswith(f"{symbol_b}_")]
            if cols_b:
                df[cols_b] = df[cols_b].ffill()
                print(f"  ✅ 填充 Asset B ({symbol_b}): {len(cols_b)} 列")
        
        print()
        return df
    
    def _add_generic_overlap_flag(
        self, 
        df: pd.DataFrame, 
        symbol_a: str, 
        symbol_b: str
    ) -> pd.DataFrame:
        """添加 overlap 标记列 (通用版本)"""
        close_a = f"{symbol_a}_Close"
        close_b = f"{symbol_b}_Close"
        
        if close_a in df.columns and close_b in df.columns:
            df['is_overlap'] = df[close_a].notna() & df[close_b].notna()
            overlap_count = df['is_overlap'].sum()
            print(f"[Overlap] 重叠时间段: {overlap_count} / {len(df)} ({overlap_count/len(df)*100:.1f}%)\n")
        else:
            df['is_overlap'] = False
        
        return df
    
    def _print_generic_statistics(
        self, 
        df: pd.DataFrame, 
        symbol_a: str, 
        symbol_b: str
    ):
        """打印统计信息 (通用版本)"""
        print(f"[DataProcessor] 📈 数据统计:")
        print(f"  - 总行数: {len(df)}")
        
        if 'Date' in df.columns:
            print(f"  - 时间范围: {df['Date'].min()} ~ {df['Date'].max()}")
            print(f"  - 时间跨度: {(df['Date'].max() - df['Date'].min()).days} 天")
        
        # 计算覆盖率
        close_a = f"{symbol_a}_Close"
        close_b = f"{symbol_b}_Close"
        
        if close_a in df.columns:
            coverage_a = df[close_a].notna().sum() / len(df) * 100
            print(f"  - {symbol_a} 覆盖率: {coverage_a:.1f}%")
        
        if close_b in df.columns:
            coverage_b = df[close_b].notna().sum() / len(df) * 100
            print(f"  - {symbol_b} 覆盖率: {coverage_b:.1f}%")
        
        if 'is_overlap' in df.columns:
            overlap_pct = df['is_overlap'].sum() / len(df) * 100
            print(f"  - 重叠时间段: {overlap_pct:.1f}%")
    
    def _generate_preview(self, df: pd.DataFrame, n_head: int = 50, n_tail: int = 50) -> pd.DataFrame:
        """
        生成预览 DataFrame (前 n_head 行 + 后 n_tail 行)
        
        用于 GUI 显示，避免加载整个大数据集
        """
        print(f"\n[Preview] 生成预览数据 (前{n_head} + 后{n_tail}行)...")
        
        if len(df) <= (n_head + n_tail):
            # 数据量小，返回全部
            preview_df = df.copy()
        else:
            # 拼接头尾
            head = df.head(n_head).copy()
            tail = df.tail(n_tail).copy()
            preview_df = pd.concat([head, tail])
        
        print(f"  ✅ 预览数据: {len(preview_df)} 行\n")
        
        return preview_df

