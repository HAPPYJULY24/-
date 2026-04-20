"""
µò░µì«σñäτÉåµ¿íσ¥ù - τö¿Σ║Äσ»╣Θ╜ÉσÆîσÉêσ╣╢σñÜΣ╕¬µ£ƒΦ┤ºσôüτºìτÜäµò░µì«

Σ╕╗ΦªüσèƒΦâ╜∩╝Ü
1. µò░µì«ΘçìΘççµá╖ (τí«Σ┐¥τ╗ƒΣ╕Çµù╢Θù┤τ▓Æσ║ª)
2. µò░µì«σ»╣Θ╜ÉΣ╕ÄσÉêσ╣╢ (σñäτÉåΣ╕ìσÉîΣ║ñµÿôµù╢Θù┤)
3. τöƒµêÉ Ready-to-Use µò░µì«Θ¢åτö¿Σ║Äσ¢₧µ╡ï
"""

import os
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Tuple


class DataProcessor:
    """µò░µì«σñäτÉåσÖ¿ - Φ┤ƒΦ┤úσñÜσôüτºìµò░µì«τÜäσ»╣Θ╜ÉΣ╕ÄσÉêσ╣╢"""
    
    def __init__(self, store_dir: str = "data/store", output_dir: str = "data/processed"):
        """
        σê¥σºïσîûµò░µì«σñäτÉåσÖ¿
        
        Args:
            store_dir: σÄƒσºïµò░µì«σ¡ÿσé¿τ¢«σ╜ò
            output_dir: σñäτÉåσÉÄµò░µì«Φ╛ôσç║τ¢«σ╜ò
        """
        self.store_dir = Path(store_dir)
        self.output_dir = Path(output_dir)
        
        # τí«Σ┐¥Φ╛ôσç║τ¢«σ╜òσ¡ÿσ£¿
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"[DataProcessor] σê¥σºïσîûσ«îµêÉ")
        print(f"[DataProcessor] µò░µì«µ║Éτ¢«σ╜ò: {self.store_dir}")
        print(f"[DataProcessor] Φ╛ôσç║τ¢«σ╜ò: {self.output_dir}")
    
    def align_datasets(
        self, 
        base_symbol: str = 'FCPO1!', 
        target_symbol: str = 'ZL1!',
        timeframe: str = '15m',
        output_filename: Optional[str] = None
    ) -> pd.DataFrame:
        """
        σ»╣Θ╜Éσ╣╢σÉêσ╣╢Σ╕ñΣ╕¬µ£ƒΦ┤ºσôüτºìτÜäµò░µì«
        
        Args:
            base_symbol: σƒ║σçåσôüτºìΣ╗úτáü (σªé FCPO1!)
            target_symbol: τ¢«µáçσôüτºìΣ╗úτáü (σªé ZL1!)
            timeframe: µù╢Θù┤τ▓Æσ║ª (σªé 15m)
            output_filename: Φ╛ôσç║µûçΣ╗╢σÉì (Θ╗ÿΦ«ñΦç¬σè¿τöƒµêÉ)
        
        Returns:
            σÉêσ╣╢σÉÄτÜä DataFrame
        
        Raises:
            FileNotFoundError: σªéµ₧£µò░µì«µûçΣ╗╢Σ╕ìσ¡ÿσ£¿
            ValueError: σªéµ₧£µò░µì«µá╝σ╝ÅΣ╕ìµ¡úτí«
        """
        print(f"\n{'='*60}")
        print(f"[DataProcessor] σ╝Çσºïµò░µì«σ»╣Θ╜ÉσñäτÉå")
        print(f"[DataProcessor] σƒ║σçåσôüτºì: {base_symbol}")
        print(f"[DataProcessor] τ¢«µáçσôüτºì: {target_symbol}")
        print(f"[DataProcessor] µù╢Θù┤τ▓Æσ║ª: {timeframe}")
        print(f"{'='*60}\n")
        
        # 1. Φ»╗σÅûµò░µì«µûçΣ╗╢
        base_df = self._load_data(base_symbol, timeframe)
        target_df = self._load_data(target_symbol, timeframe)
        
        print(f"[DataProcessor] Γ£à µò░µì«σèáΦ╜╜σ«îµêÉ")
        print(f"  - {base_symbol}: {len(base_df)} Φíî, µù╢Θù┤Φîâσ¢┤ {base_df['Date'].min()} ~ {base_df['Date'].max()}")
        print(f"  - {target_symbol}: {len(target_df)} Φíî, µù╢Θù┤Φîâσ¢┤ {target_df['Date'].min()} ~ {target_df['Date'].max()}")
        
        # 2. ΘçìΘççµá╖ (τí«Σ┐¥τ╗ƒΣ╕Çµù╢Θù┤τ▓Æσ║ª)
        base_df = self._resample_data(base_df, timeframe, f"{base_symbol}_")
        target_df = self._resample_data(target_df, timeframe, f"{target_symbol}_")
        
        # 3. σÉêσ╣╢µò░µì« (Outer Join Σ┐¥τòÖµëÇµ£ëµù╢Θù┤τé╣)
        print(f"\n[DataProcessor] ≡ƒôè σ╝ÇσºïσÉêσ╣╢µò░µì« (Outer Join)...")
        merged_df = pd.merge(
            base_df, 
            target_df, 
            left_index=True, 
            right_index=True, 
            how='outer',
            suffixes=('', '_drop')  # Θü┐σàìσêùσÉìσå▓τ¬ü
        )
        
        # σêáΘÖñΘçìσñìτÜä Date σêù∩╝êσªéµ₧£µ£ë∩╝ë
        merged_df = merged_df[[col for col in merged_df.columns if not col.endswith('_drop')]]
        
        print(f"[DataProcessor] Γ£à σÉêσ╣╢σ«îµêÉ: {len(merged_df)} Φíî")
        
        # 4. µ╖╗σèá overlap µáçΦ«░σêù
        merged_df = self._add_overlap_flag(merged_df, base_symbol, target_symbol)
        
        # 5. Θçìτ╜«τ┤óσ╝ò∩╝îτí«Σ┐¥ Date Σ╕║σêù
        merged_df.reset_index(inplace=True)
        if 'index' in merged_df.columns:
            merged_df.rename(columns={'index': 'Date'}, inplace=True)
        
        # 6. µò░µì«τ╗ƒΦ«í
        self._print_statistics(merged_df, base_symbol, target_symbol)
        
        # 7. Σ┐¥σ¡ÿµûçΣ╗╢
        if output_filename is None:
            output_filename = f"merged_{base_symbol.replace('!', '')}_{target_symbol.replace('!', '')}_{timeframe}.parquet"
        
        output_path = self.output_dir / output_filename
        merged_df.to_parquet(output_path, index=False)
        
        print(f"\n[DataProcessor] ≡ƒÆ╛ µò░µì«σ╖▓Σ┐¥σ¡ÿ: {output_path}")
        print(f"[DataProcessor] µûçΣ╗╢σñºσ░Å: {output_path.stat().st_size / 1024 / 1024:.2f} MB")
        print(f"\n{'='*60}")
        
        return merged_df
    
    def _load_data(self, symbol: str, timeframe: str) -> pd.DataFrame:
        """
        σèáΦ╜╜ Parquet µò░µì«µûçΣ╗╢
        
        Args:
            symbol: µ£ƒΦ┤ºΣ╗úτáü
            timeframe: µù╢Θù┤τ▓Æσ║ª
        
        Returns:
            DataFrame with Date column
        """
        # µ₧äΘÇáµûçΣ╗╢σÉì (Master DB µá╝σ╝Å: symbol_timeframe.parquet)
        filename = f"{symbol}_{timeframe}.parquet"
        
        # Support recursive search for nested folder structures (e.g. data/store/FUTURES/...)
        matches = list(self.store_dir.rglob(filename))
        filepath = matches[0] if matches else self.store_dir / filename
        
        if not filepath.exists():
            raise FileNotFoundError(
                f"µò░µì«µûçΣ╗╢Σ╕ìσ¡ÿσ£¿: {filename}\n\n"
                f"Φ»╖σàêΣ╕ïΦ╜╜µò░µì«∩╝Ü\n"
                f"1. σ£¿Σ╕╗τòîΘ¥óΘÇëµï⌐ 'Bursaµ£ƒΦ┤º (TV)'\n"
                f"2. Φ╛ôσàÑΣ╗úτáü: {symbol}\n"
                f"3. ΘÇëµï⌐µù╢Θù┤τ▓Æσ║ª: {timeframe}\n"
                f"4. τé╣σç╗Σ╕ïΦ╜╜"
            )
        
        print(f"[DataProcessor] ≡ƒôû Φ»╗σÅûµûçΣ╗╢: {filepath.name}")
        df = pd.read_parquet(filepath)
        
        # τí«Σ┐¥ Date σêùσ¡ÿσ£¿
        if 'Date' not in df.columns:
            if df.index.name == 'Date' or isinstance(df.index, pd.DatetimeIndex):
                df.reset_index(inplace=True)
                if 'index' in df.columns:
                    df.rename(columns={'index': 'Date'}, inplace=True)
            else:
                raise ValueError(f"µò░µì«µûçΣ╗╢τ╝║σ░æ 'Date' σêù: {filepath}")
        
        # τí«Σ┐¥ Date Σ╕║ datetime τ▒╗σ₧ï
        df['Date'] = pd.to_datetime(df['Date'])
        
        return df
    
    def _resample_data(self, df: pd.DataFrame, timeframe: str, prefix: str = "") -> pd.DataFrame:
        """
        ΘçìΘççµá╖µò░µì«σê░µîçσ«Üµù╢Θù┤τ▓Æσ║ª
        
        Args:
            df: σÄƒσºï DataFrame
            timeframe: τ¢«µáçµù╢Θù┤τ▓Æσ║ª (σªé '15m', '1h', '1d')
            prefix: σêùσÉìσëìτ╝Ç (τö¿Σ║Äσî║σêåΣ╕ìσÉîσôüτºì)
        
        Returns:
            ΘçìΘççµá╖σÉÄτÜä DataFrame (Date Σ╜£Σ╕║ index)
        """
        print(f"[DataProcessor] ≡ƒöä ΘçìΘççµá╖µò░µì«σê░ {timeframe}...")
        
        # Φ«╛τ╜« Date Σ╕║τ┤óσ╝ò
        df_resampled = df.set_index('Date')
        
        # µÿáσ░äµù╢Θù┤τ▓Æσ║ª
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
        
        # OHLCV ΘçìΘççµá╖ΦºäσêÖ
        agg_dict = {
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum'
        }
        
        # σÅ¬Σ┐¥τòÖσ¡ÿσ£¿τÜäσêù
        agg_dict = {k: v for k, v in agg_dict.items() if k in df_resampled.columns}
        
        # µëºΦíîΘçìΘççµá╖
        df_resampled = df_resampled.resample(freq).agg(agg_dict)
        
       # σêáΘÖñσà¿Σ╕║ NaN τÜäΦíî (µ▓íµ£ëµò░µì«τÜäµù╢Θù┤µ«╡)
        df_resampled = df_resampled.dropna(how='all')
        
        # µ╖╗σèáσêùσÉìσëìτ╝Ç
        if prefix:
            df_resampled.columns = [f"{prefix}{col}" for col in df_resampled.columns]
        
        print(f"[DataProcessor]   ΓåÆ ΘçìΘççµá╖σÉÄ: {len(df_resampled)} Φíî")
        
        return df_resampled

    def clean_non_trading_days(self, df: pd.DataFrame, timeframe: str = '1d') -> pd.DataFrame:
        """
        [Non-Trading Day Cleaner]
        Remove rows where Volume == 0 for Daily timeframe (1d).
        Rationale: Eliminate public holidays or non-trading days to prevent skewed analysis.
        
        Args:
            df: Input DataFrame
            timeframe: Data timeframe (default '1d')
            
        Returns:
            Cleaned DataFrame
        """
        # Only apply for Daily timeframe (1d) to avoid deleting valid intraday 0-volume candles
        if timeframe != '1d':
            return df
            
        if 'Volume' not in df.columns:
            print("[INFO] 'Volume' column not found. Skipping non-trading day cleaning.")
            return df
            
        initial_count = len(df)
        
        # Filter: Keep rows where Volume > 0
        df_cleaned = df[df['Volume'] > 0].copy()
        
        removed_count = initial_count - len(df_cleaned)
        
        if removed_count > 0:
             print(f"[INFO] Removed {removed_count} holiday/non-trading rows (Volume=0).")
        
        return df_cleaned
    
    def _add_overlap_flag(self, df: pd.DataFrame, base_symbol: str, target_symbol: str) -> pd.DataFrame:
        """
        µ╖╗σèá overlap µáçΦ«░σêù∩╝îµáçτñ║Σ╕ñΣ╕¬σôüτºìΘâ╜µ£ëΣ║ñµÿôτÜäµù╢Θù┤µ«╡
        
        Args:
            df: σÉêσ╣╢σÉÄτÜä DataFrame
            base_symbol: σƒ║σçåσôüτºìΣ╗úτáü
            target_symbol: τ¢«µáçσôüτºìΣ╗úτáü
        
        Returns:
            µ╖╗σèáΣ║å is_overlap σêùτÜä DataFrame
        """
        print(f"[DataProcessor] ≡ƒÅ╖∩╕Å  µ╖╗σèá overlap µáçΦ«░...")
        
        # µúÇµƒÑΣ╕ñΣ╕¬σôüτºìτÜä Close σêùµÿ»σÉªΘâ╜µ£ëµò░µì«
        base_col = f"{base_symbol}_Close"
        target_col = f"{target_symbol}_Close"
        
        if base_col in df.columns and target_col in df.columns:
            df['is_overlap'] = df[base_col].notna() & df[target_col].notna()
            overlap_count = df['is_overlap'].sum()
            print(f"[DataProcessor]   ΓåÆ ΘçìσÅáµù╢Θù┤µ«╡: {overlap_count} Φíî ({overlap_count/len(df)*100:.1f}%)")
        else:
            print(f"[DataProcessor]   ΓÜá∩╕Å  µ£¬µë╛σê░ Close σêù∩╝îΦ╖│Φ┐ç overlap µáçΦ«░")
            df['is_overlap'] = False
        
        return df
    
    def _print_statistics(self, df: pd.DataFrame, base_symbol: str, target_symbol: str):
        """µëôσì░µò░µì«τ╗ƒΦ«íΣ┐íµü»"""
        print(f"\n[DataProcessor] ≡ƒôê µò░µì«τ╗ƒΦ«í:")
        print(f"  - µÇ╗Φíîµò░: {len(df)}")
        print(f"  - µù╢Θù┤Φîâσ¢┤: {df['Date'].min()} ~ {df['Date'].max()}")
        print(f"  - µù╢Θù┤Φ╖¿σ║ª: {(df['Date'].max() - df['Date'].min()).days} σñ⌐")
        
        # Φ«íτ«ùσÉäσôüτºìτÜäµò░µì«σ«îµò┤µÇº
        base_close_col = f"{base_symbol}_Close"
        target_close_col = f"{target_symbol}_Close"
        
        if base_close_col in df.columns:
            base_coverage = df[base_close_col].notna().sum() / len(df) * 100
            print(f"  - {base_symbol} µò░µì«Φªåτ¢ûτÄç: {base_coverage:.1f}%")
        
        if target_close_col in df.columns:
            target_coverage = df[target_close_col].notna().sum() / len(df) * 100
            print(f"  - {target_symbol} µò░µì«Φªåτ¢ûτÄç: {target_coverage:.1f}%")
        
        if 'is_overlap' in df.columns:
            overlap_pct = df['is_overlap'].sum() / len(df) * 100
            print(f"  - ΘçìσÅáµù╢Θù┤µ«╡σìáµ»ö: {overlap_pct:.1f}%")
    
    # ========== ≡ƒåò Generic Alignment Method for GUI ==========
    
    def align_custom_files(
        self,
        file_path_a: str,
        file_path_b: str,
        output_filename: Optional[str] = None,
        apply_ffill: bool = True,
        ffill_asset: str = 'B',  # 'A', 'B', or 'both'
        only_overlap: bool = False
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        ΘÇÜτö¿µûçΣ╗╢σ»╣Θ╜Éµû╣µ│ò - µö»µîüΣ╗╗µäÅΣ╕ñΣ╕¬ Parquet µûçΣ╗╢τÜäσ»╣Θ╜É (GUI τëêµ£¼)
        
        **Killer Fixes:**
        1. µù╢σî║σñäτÉå∩╝ÜΦç¬σè¿µúÇµ╡ïσ╣╢τ╗ƒΣ╕ÇΦ╜¼µìóΣ╕║ UTC
        2. σè¿µÇüσêùσÉì∩╝ÜΣ╗ÄµûçΣ╗╢σÉìµÅÉσÅû symbol σ╣╢Θçìσæ╜σÉìσêù
        3. σëìσÉæσí½σàà∩╝ÜσÅ»ΘÇëτÜä ffill() σñäτÉåΣ╕ìσÉîΣ║ñµÿôµù╢Θù┤
        
        Args:
            file_path_a: Asset A µûçΣ╗╢Φ╖»σ╛ä (Base)
            file_path_b: Asset B µûçΣ╗╢Φ╖»σ╛ä (Reference)
            output_filename: Φ╛ôσç║µûçΣ╗╢σÉì (Θ╗ÿΦ«ñΦç¬σè¿τöƒµêÉ)
            apply_ffill: µÿ»σÉªσ║öτö¿σëìσÉæσí½σàà
            ffill_asset: σ»╣σô¬Σ╕¬Φ╡äΣ║ºσ║öτö¿σí½σàà ('A', 'B', or 'both')
        
        Returns:
            Tuple[σ«îµò┤ DataFrame, ΘóäΦºê DataFrame (σëì50+σÉÄ50Φíî)]
        """
        print(f"\n{'='*70}")
        print(f"[DataProcessor] ≡ƒöä Generic Alignment - GUI Mode")
        print(f"{'='*70}")
        print(f"[Asset A (Base)]:      {Path(file_path_a).name}")
        print(f"[Asset B (Reference)]: {Path(file_path_b).name}")
        print(f"{'='*70}\n")
        
        # 1. 提取 Symbol 名称从文件名
        symbol_a = self._extract_symbol_from_filename(file_path_a)
        symbol_b = self._extract_symbol_from_filename(file_path_b)
        
        print(f"[DataProcessor] 📥 提取的 Symbol:")
        print(f"  - Asset A: {symbol_a}")
        print(f"  - Asset B: {symbol_b}\n")
        
        # 2. 加载数据文件 (直接从路径)
        df_a = self._load_parquet_file(file_path_a, symbol_a)
        df_b = self._load_parquet_file(file_path_b, symbol_b)
        
        # 🛡️ 防呆检测: 已对齐文件不能作为输入
        if self._detect_already_aligned(df_a, file_path_a):
            raise ValueError(
                f"选择的文件 '{Path(file_path_a).name}' 已经是经过对齐处理的数据文件。\n\n"
                f"请选择原始（未对齐的）数据文件作为输入。\n"
                f"已对齐的文件通常保存在 Align_data 目录中。"
            )
        if self._detect_already_aligned(df_b, file_path_b):
            raise ValueError(
                f"选择的文件 '{Path(file_path_b).name}' 已经是经过对齐处理的数据文件。\n\n"
                f"请选择原始（未对齐的）数据文件作为输入。\n"
                f"已对齐的文件通常保存在 Align_data 目录中。"
            )
        
        print(f"[DataProcessor] ✅ 文件加载完成")
        print(f"  - {symbol_a}: {len(df_a)} 行")
        print(f"  - {symbol_b}: {len(df_b)} 行\n")
        
        # 3. ≡ƒöÑ Killer Fix 1: µù╢σî║σñäτÉå
        df_a = self._fix_timezone(df_a, symbol_a)
        df_b = self._fix_timezone(df_b, symbol_b)

        # 4. ≡ƒöÑ Killer Fix 0: Clean Non-Trading Days (Volume=0 for 1d)
        # Extract timeframe from filename (e.g. 1155.KL_1d.parquet -> 1d)
        try:
             timeframe_a = file_path_a.rsplit('_', 1)[-1].replace('.parquet', '')
             timeframe_b = file_path_b.rsplit('_', 1)[-1].replace('.parquet', '')
        except:
             timeframe_a = '1d' # Default fallback
             timeframe_b = '1d'
        
        df_a = self.clean_non_trading_days(df_a, timeframe_a)
        df_b = self.clean_non_trading_days(df_b, timeframe_b)
        
        # 5. ≡ƒöÑ Killer Fix 2: σè¿µÇüσêùσÉìΘçìσæ╜σÉì
        df_a = self._rename_columns_with_prefix(df_a, symbol_a)
        df_b = self._rename_columns_with_prefix(df_b, symbol_b)
        
        # 6. σÉêσ╣╢µò░µì« (Outer Join)
        print(f"[DataProcessor] ≡ƒôè σÉêσ╣╢µò░µì« (Outer Join)...\n")
        
        # Σ╜┐τö¿ concat ΦÇîΣ╕ìµÿ» merge∩╝îσ¢áΣ╕║ Date σ╖▓τ╗Åµÿ» index
        merged_df = pd.concat([df_a, df_b], axis=1, join='outer')
        
        print(f"[DataProcessor] Γ£à σÉêσ╣╢σ«îµêÉ: {len(merged_df)} Φíî\n")
        
        # 6. ≡ƒöÑ Killer Fix 3: σëìσÉæσí½σàà (Forward Fill)
        if apply_ffill:
            merged_df = self._apply_forward_fill(merged_df, symbol_a, symbol_b, ffill_asset)
        
        # 7. 添加 overlap 标记
        merged_df = self._add_generic_overlap_flag(merged_df, symbol_a, symbol_b)
        
        # 7.5 Optional Overlap Filtering
        if only_overlap and 'is_overlap' in merged_df.columns:
            original_len = len(merged_df)
            merged_df = merged_df[merged_df['is_overlap'] == True].copy()
            deleted_rows = original_len - len(merged_df)
            print(f"[DataProcessor] ✂️ 启用纯净重叠模式 (only_overlap=True)")
            print(f"  - 已剔除 {deleted_rows} 行非重叠数据 (保留: {len(merged_df)} 行)\n")
        
        # 8. Θçìτ╜«τ┤óσ╝ò∩╝îσ░å Date Φ╜¼Σ╕║σêù
        merged_df.reset_index(inplace=True)
        if 'index' in merged_df.columns:
            merged_df.rename(columns={'index': 'Date'}, inplace=True)
        
        # 9. τ╗ƒΦ«íΣ┐íµü»
        self._print_generic_statistics(merged_df, symbol_a, symbol_b)
        
        # 10. Σ┐¥σ¡ÿµûçΣ╗╢
        if output_filename is None:
            output_filename = f"aligned_{symbol_a.replace('!', '')}_{symbol_b.replace('!', '')}.parquet"
        
        output_path = self.output_dir / output_filename
        merged_df.to_parquet(output_path, index=False)
        
        print(f"\n[DataProcessor] ≡ƒÆ╛ µò░µì«σ╖▓Σ┐¥σ¡ÿ: {output_path}")
        print(f"[DataProcessor] µûçΣ╗╢σñºσ░Å: {output_path.stat().st_size / 1024 / 1024:.2f} MB\n")
        print(f"{'='*70}\n")
        
        # 11. τöƒµêÉΘóäΦºê DataFrame (σëì50 + σÉÄ50Φíî)
        preview_df = self._generate_preview(merged_df)
        
        return merged_df, preview_df
    
    def _extract_symbol_from_filename(self, filepath: str) -> str:
        """
        Σ╗ÄµûçΣ╗╢σÉìµÅÉσÅû Symbol
        Σ╛ïσªé: FCPO1!_15m.parquet -> FCPO1!
        """
        filename = Path(filepath).stem  # σÄ╗µÄëµë⌐σ▒òσÉì
        # σüçΦ«╛µá╝σ╝Åµÿ» {symbol}_{timeframe}
        parts = filename.rsplit('_', 1)  # Σ╗ÄσÅ│Φ╛╣σêåσë▓Σ╕Çµ¼í
        return parts[0] if parts else filename
    
    def _load_parquet_file(self, filepath: str, symbol: str) -> pd.DataFrame:
        """
        σèáΦ╜╜σìòΣ╕¬ Parquet µûçΣ╗╢σ╣╢Φ┐öσ¢₧ DataFrame
        """
        filepath = Path(filepath)
        
        if not filepath.exists():
            raise FileNotFoundError(f"µûçΣ╗╢Σ╕ìσ¡ÿσ£¿: {filepath}")
        
        print(f"[DataProcessor] ≡ƒôû Φ»╗σÅû: {filepath.name}")
        df = pd.read_parquet(filepath)
        
        # τí«Σ┐¥µ£ë Date σêùµêûτ┤óσ╝ò
        if 'Date' not in df.columns:
            if df.index.name == 'Date' or isinstance(df.index, pd.DatetimeIndex):
                df.reset_index(inplace=True)
                if 'index' in df.columns:
                    df.rename(columns={'index': 'Date'}, inplace=True)
            else:
                raise ValueError(f"µò░µì«µûçΣ╗╢τ╝║σ░æ 'Date' σêùµêûτ┤óσ╝ò: {filepath}")
        
        # Φ«╛τ╜« Date Σ╕║τ┤óσ╝ò
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
        
        return df
    
    def _fix_timezone(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """
        ≡ƒöÑ Killer Fix 1: µù╢σî║σñäτÉå
        
        µúÇµƒÑτ┤óσ╝òµù╢σî║∩╝îσªéµ₧£µ£ëµù╢σî║σêÖΦ╜¼µìóΣ╕║ UTC∩╝îσªéµ₧£µ▓íµ£ëσêÖσÅæσç║Φ¡ªσæè
        """
        print(f"[Timezone Fix] µúÇµƒÑ {symbol} τÜäµù╢σî║...")
        
        if df.index.tz is not None:
            # µ£ëµù╢σî║ - Φ╜¼µìóΣ╕║ UTC
            original_tz = df.index.tz
            print(f"  Γ£à µúÇµ╡ïσê░µù╢σî║: {original_tz} ΓåÆ Φ╜¼µìóΣ╕║ UTC")
            df.index = df.index.tz_convert('UTC')
        else:
            # µ▓íµ£ëµù╢σî║ (naive datetime)
            print(f"  ΓÜá∩╕Å  Φ¡ªσæè: {symbol} τÜäµù╢Θù┤µê│Σ╕║ naive (µùáµù╢σî║)")
            print(f"     σüçΦ«╛Σ╕║µ£¼σ£░µù╢Θù┤∩╝îΣ╕ìΦ┐¢Φíîµù╢σî║Φ╜¼µìó")
            print(f"     σ╗║Φ««: τí«Σ┐¥µëÇµ£ëµò░µì«µ║ÉΣ╜┐τö¿τ╗ƒΣ╕Çµù╢σî║\n")
        
        return df
    
    def _rename_columns_with_prefix(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """
        ≡ƒöÑ Killer Fix 2: σè¿µÇüσêùσÉìΘçìσæ╜σÉì
        
        σ░åµáçσçåσêùσÉì (Open, High, Low, Close, Volume) Θçìσæ╜σÉìΣ╕║ {symbol}_Open τ¡ë
        """
        print(f"[Column Rename] 为 {symbol} 添加前缀...")
        
        rename_map = {}
        for col in df.columns:
            # 跳过特殊列，且防止双重前缀 (如果列名已包含 symbol_ 前缀则跳过)
            if col not in ['Date', 'is_overlap'] and not col.startswith(f"{symbol}_"):
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
        ≡ƒöÑ Killer Fix 3: σëìσÉæσí½σàà (Forward Fill)
        
        σ»╣µîçσ«ÜΦ╡äΣ║ºτÜäσêùσ║öτö¿ ffill() Σ╗Ñσí½ΦíÑΣ║ñµÿôµù╢Θù┤σ╖«σ╝é
        """
        print(f"[Forward Fill] σ║öτö¿σëìσÉæσí½σàà (asset: {ffill_asset})...")
        
        if ffill_asset == 'A' or ffill_asset == 'both':
            cols_a = [col for col in df.columns if col.startswith(f"{symbol_a}_")]
            if cols_a:
                df[cols_a] = df[cols_a].ffill()
                print(f"  Γ£à σí½σàà Asset A ({symbol_a}): {len(cols_a)} σêù")
        
        if ffill_asset == 'B' or ffill_asset == 'both':
            cols_b = [col for col in df.columns if col.startswith(f"{symbol_b}_")]
            if cols_b:
                df[cols_b] = df[cols_b].ffill()
                print(f"  Γ£à σí½σàà Asset B ({symbol_b}): {len(cols_b)} σêù")
        
        print()
        return df
    
    def _add_generic_overlap_flag(
        self, 
        df: pd.DataFrame, 
        symbol_a: str, 
        symbol_b: str
    ) -> pd.DataFrame:
        """µ╖╗σèá overlap µáçΦ«░σêù (ΘÇÜτö¿τëêµ£¼)"""
        close_a = f"{symbol_a}_Close"
        close_b = f"{symbol_b}_Close"
        
        if close_a in df.columns and close_b in df.columns:
            df['is_overlap'] = df[close_a].notna() & df[close_b].notna()
            overlap_count = df['is_overlap'].sum()
            print(f"[Overlap] ΘçìσÅáµù╢Θù┤µ«╡: {overlap_count} / {len(df)} ({overlap_count/len(df)*100:.1f}%)\n")
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
        print(f"[DataProcessor] 📊 数据统计:")
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
            # 🛡️ 防御: 如果存在重复列名导致 .sum() 返回 Series 而非标量
            overlap_val = df['is_overlap'].sum()
            if isinstance(overlap_val, pd.Series):
                overlap_val = overlap_val.iloc[0]
            overlap_pct = float(overlap_val) / len(df) * 100
            print(f"  - 重叠时间段: {overlap_pct:.1f}%")
    
    def _generate_preview(self, df: pd.DataFrame, n_head: int = 50, n_tail: int = 50) -> pd.DataFrame:
        """
        τöƒµêÉΘóäΦºê DataFrame (σëì n_head Φíî + σÉÄ n_tail Φíî)
        
        τö¿Σ║Ä GUI µÿ╛τñ║∩╝îΘü┐σàìσèáΦ╜╜µò┤Σ╕¬σñºµò░µì«Θ¢å
        """
        print(f"\n[Preview] τöƒµêÉΘóäΦºêµò░µì« (σëì{n_head} + σÉÄ{n_tail}Φíî)...")
        
        if len(df) <= (n_head + n_tail):
            # µò░µì«ΘçÅσ░Å∩╝îΦ┐öσ¢₧σà¿Θâ¿
            preview_df = df.copy()
        else:
            # µï╝µÄÑσñ┤σ░╛
            head = df.head(n_head).copy()
            tail = df.tail(n_tail).copy()
            preview_df = pd.concat([head, tail])
        
        print(f"  Γ£à ΘóäΦºêµò░µì«: {len(preview_df)} Φíî\n")
        
        return preview_df

    # ========== 🛡️ 防呆检测: 已对齐文件 ==========
    
    def _detect_already_aligned(self, df: pd.DataFrame, filepath: str) -> bool:
        """
        检测 DataFrame 是否为已经对齐过的数据文件
        
        检测标志:
        1. 存在 is_overlap 列 (对齐输出特有)
        2. 存在多个 _Close 列 (已有前缀，说明是对齐后的合并数据)
        3. 文件名以 aligned_ 或 Aligned_ 或 merged_ 开头
        
        Args:
            df: 加载后的 DataFrame
            filepath: 原文件路径
        
        Returns:
            True 表示该文件是已对齐数据，不应再次作为输入
        """
        # 检测标志1: 存在 is_overlap 列
        if 'is_overlap' in df.columns:
            print(f"[Detection] ⚠️ 检测到 is_overlap 列 → 已对齐文件")
            return True
        
        # 检测标志2: 存在多个 _Close 列 (已有前缀)
        close_cols = [c for c in df.columns if c.endswith('_Close')]
        if len(close_cols) >= 2:
            print(f"[Detection] ⚠️ 检测到 {len(close_cols)} 个 _Close 列 → 已对齐文件")
            return True
        
        # 检测标志3: 文件名以 aligned_ 或 merged_ 开头
        fname = Path(filepath).stem.lower()
        if fname.startswith('aligned') or fname.startswith('merged'):
            print(f"[Detection] ⚠️ 文件名以 aligned/merged 开头 → 已对齐文件")
            return True
        
        return False

    # ========== 🔬 多数据流对齐 (Multi-Data Alignment) ==========
    
    def align_multi_files(
        self,
        file_paths: List[str],
        anchor_index: int = 0,
        apply_ffill: bool = True,
        only_overlap: bool = False,
        output_filename: Optional[str] = None
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        多数据流对齐 — 支持 2~5 个数据源
        
        以 Anchor Asset (基准资产) 的交易时间为准，
        其余所有非基准资产自动执行 ffill() 前向填充，
        确保在每个可交易 K 线上都有最新的参考数据。
        
        Args:
            file_paths: 文件路径列表 (2~5个)
            anchor_index: 基准资产在列表中的索引 (默认0=第一个文件)
            apply_ffill: 是否对非基准资产执行前向填充
            only_overlap: 是否仅保留所有资产都有数据的时间段
            output_filename: 输出文件名 (默认自动生成)
        
        Returns:
            Tuple[完整 DataFrame, 预览 DataFrame]
        
        Raises:
            ValueError: 数据源数量不在 2~5 范围，或文件已经是对齐数据
        """
        print(f"\n{'='*70}")
        print(f"[DataProcessor] 🔬 Multi-Data Alignment ({len(file_paths)} sources)")
        print(f"{'='*70}")
        
        # 1. 验证输入数量
        if len(file_paths) < 2 or len(file_paths) > 5:
            raise ValueError(f"数据源数量必须在 2~5 之间，当前: {len(file_paths)}")
        
        if anchor_index < 0 or anchor_index >= len(file_paths):
            raise ValueError(f"基准资产索引无效: {anchor_index}，有效范围: 0~{len(file_paths)-1}")
        
        # 2. 加载所有文件 + 检测已对齐文件
        dfs = []
        symbols = []
        
        for i, fp in enumerate(file_paths):
            label = chr(65 + i)  # A, B, C, D, E
            is_anchor = (i == anchor_index)
            anchor_tag = " ⚓ (Anchor)" if is_anchor else ""
            print(f"[Asset {label}{anchor_tag}]: {Path(fp).name}")
            
            symbol = self._extract_symbol_from_filename(fp)
            df = self._load_parquet_file(fp, symbol)
            
            # 🛡️ 防呆检测
            if self._detect_already_aligned(df, fp):
                raise ValueError(
                    f"选择的文件 '{Path(fp).name}' 已经是经过对齐处理的数据文件。\n\n"
                    f"请选择原始（未对齐的）数据文件作为输入。"
                )
            
            # 时区修正
            df = self._fix_timezone(df, symbol)
            
            # 清理非交易日 (仅对日线)
            try:
                timeframe = fp.rsplit('_', 1)[-1].replace('.parquet', '')
            except:
                timeframe = '1d'
            df = self.clean_non_trading_days(df, timeframe)
            
            # 列名加前缀
            df = self._rename_columns_with_prefix(df, symbol)
            
            dfs.append(df)
            symbols.append(symbol)
        
        print(f"\n[DataProcessor] ✅ 所有文件加载完成 ({len(dfs)} 个)\n")
        
        # 3. 多文件 Outer Join (pd.concat)
        print(f"[DataProcessor] 📑 合并数据 (Outer Join)...")
        merged_df = pd.concat(dfs, axis=1, join='outer')
        print(f"[DataProcessor] ✅ 合并完成: {len(merged_df)} 行\n")
        
        # 4. 对非 Anchor 资产执行 ffill (基准资产概念)
        anchor_symbol = symbols[anchor_index]
        if apply_ffill:
            print(f"[Forward Fill] 基准资产: {anchor_symbol} (⚓ Anchor)")
            print(f"[Forward Fill] 对所有非基准资产执行前向填充...")
            for i, symbol in enumerate(symbols):
                if i != anchor_index:
                    cols = [c for c in merged_df.columns if c.startswith(f"{symbol}_")]
                    if cols:
                        merged_df[cols] = merged_df[cols].ffill()
                        print(f"  ✅ 填充 {symbol}: {len(cols)} 列")
            print()
        
        # 5. 多源 overlap 标记 (所有 Close 列都有数据 = True)
        close_cols = [f"{s}_Close" for s in symbols if f"{s}_Close" in merged_df.columns]
        if close_cols:
            merged_df['is_overlap'] = merged_df[close_cols].notna().all(axis=1)
            overlap_count = int(merged_df['is_overlap'].sum())
            print(f"[Overlap] 重叠时间段: {overlap_count} / {len(merged_df)} ({overlap_count/len(merged_df)*100:.1f}%)\n")
        else:
            merged_df['is_overlap'] = False
        
        # 6. 可选: 仅保留重叠时间段
        if only_overlap and 'is_overlap' in merged_df.columns:
            original_len = len(merged_df)
            merged_df = merged_df[merged_df['is_overlap'] == True].copy()
            deleted_rows = original_len - len(merged_df)
            print(f"[DataProcessor] ✂️ 启用纯净重叠模式 (only_overlap=True)")
            print(f"  - 已剔除 {deleted_rows} 行非重叠数据 (保留: {len(merged_df)} 行)\n")
        
        # 7. 重置索引，将 Date 转为列
        merged_df.reset_index(inplace=True)
        if 'index' in merged_df.columns:
            merged_df.rename(columns={'index': 'Date'}, inplace=True)
        
        # 8. 统计信息
        self._print_multi_statistics(merged_df, symbols, anchor_index)
        
        # 9. 保存文件
        if output_filename is None:
            symbol_names = '_'.join([s.replace('!', '') for s in symbols])
            output_filename = f"aligned_{symbol_names}.parquet"
        
        output_path = self.output_dir / output_filename
        merged_df.to_parquet(output_path, index=False)
        
        print(f"\n[DataProcessor] 💾 数据已保存: {output_path}")
        print(f"[DataProcessor] 文件大小: {output_path.stat().st_size / 1024 / 1024:.2f} MB\n")
        print(f"{'='*70}\n")
        
        # 10. 生成预览
        preview_df = self._generate_preview(merged_df)
        
        return merged_df, preview_df
    
    def _print_multi_statistics(
        self,
        df: pd.DataFrame,
        symbols: List[str],
        anchor_index: int
    ):
        """
        打印多数据源统计信息
        
        Args:
            df: 合并后的 DataFrame
            symbols: 所有 symbol 列表
            anchor_index: 基准资产索引
        """
        print(f"[DataProcessor] 📊 多数据源统计:")
        print(f"  - 总行数: {len(df)}")
        print(f"  - 数据源数量: {len(symbols)}")
        print(f"  - 基准资产 (Anchor): {symbols[anchor_index]}")
        
        if 'Date' in df.columns:
            print(f"  - 时间范围: {df['Date'].min()} ~ {df['Date'].max()}")
            print(f"  - 时间跨度: {(df['Date'].max() - df['Date'].min()).days} 天")
        
        # 每个 symbol 的覆盖率
        for i, symbol in enumerate(symbols):
            close_col = f"{symbol}_Close"
            anchor_tag = " ⚓" if i == anchor_index else ""
            if close_col in df.columns:
                coverage = df[close_col].notna().sum() / len(df) * 100
                print(f"  - {symbol}{anchor_tag} 覆盖率: {coverage:.1f}%")
        
        if 'is_overlap' in df.columns:
            overlap_val = df['is_overlap'].sum()
            if isinstance(overlap_val, pd.Series):
                overlap_val = overlap_val.iloc[0]
            overlap_pct = float(overlap_val) / len(df) * 100
            print(f"  - 全部重叠时间段: {overlap_pct:.1f}%")
