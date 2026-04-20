"""
Data Alignment Studio Dialog - 数据对齐工作室对话框

支持 2~5 个数据源 (A, B, C, D, E) 的灵活对齐。
引入 Anchor Asset (基准资产) 概念，以基准资产的交易时间为准，
其余所有参考资产自动执行前向填充 (ffill)。
实现互斥文件选择（已选文件在其他下拉框中置灰禁用）。

v3.0 - Multi-Data Alignment + Anchor Asset + 防呆检测
"""

from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QComboBox,
    QPushButton, QTableWidget, QTableWidgetItem, QMessageBox,
    QProgressDialog, QCheckBox, QGroupBox, QRadioButton, QButtonGroup,
    QFileDialog
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QColor, QStandardItemModel
from pathlib import Path
import pandas as pd

# Import localization
from logic.localization import tr


class AlignmentWorker(QThread):
    """
    异步对齐线程 — 支持双文件对齐和多文件对齐
    
    mode='dual': 调用 align_custom_files (兼容旧版 A+B 对齐)
    mode='multi': 调用 align_multi_files (新版 2~5 数据源对齐)
    """
    finished = pyqtSignal(pd.DataFrame, pd.DataFrame)  # full_df, preview_df
    error = pyqtSignal(str)
    
    def __init__(self, processor, mode='dual',
                 # dual mode 参数
                 file_a=None, file_b=None, apply_ffill=True, ffill_asset='B',
                 only_overlap=False,
                 # multi mode 参数
                 file_paths=None, anchor_index=0):
        super().__init__()
        self.processor = processor
        self.mode = mode
        # dual
        self.file_a = file_a
        self.file_b = file_b
        self.apply_ffill = apply_ffill
        self.ffill_asset = ffill_asset
        self.only_overlap = only_overlap
        # multi
        self.file_paths = file_paths or []
        self.anchor_index = anchor_index
    
    def run(self):
        try:
            if self.mode == 'multi':
                full_df, preview_df = self.processor.align_multi_files(
                    file_paths=self.file_paths,
                    anchor_index=self.anchor_index,
                    apply_ffill=self.apply_ffill,
                    only_overlap=self.only_overlap
                )
            else:
                full_df, preview_df = self.processor.align_custom_files(
                    file_path_a=self.file_a,
                    file_path_b=self.file_b,
                    apply_ffill=self.apply_ffill,
                    ffill_asset=self.ffill_asset,
                    only_overlap=self.only_overlap
                )
            self.finished.emit(full_df, preview_df)
        except Exception as e:
            import traceback
            error_msg = f"{str(e)}\n\n{traceback.format_exc()}"
            self.error.emit(error_msg)


class AlignmentDialog(QDialog):
    """
    数据对齐工作室对话框 - GUI 界面
    
    支持功能:
    - 双文件对齐 (A + B)
    - 多数据流对齐 (A + B + C/D/E, 需勾选启用)
    - 基准资产 (Anchor Asset) 选择
    - 互斥文件选择 (已选文件置灰)
    - 已对齐文件防呆检测
    """
    
    # 数据组标签
    ASSET_LABELS = ['A', 'B', 'C', 'D', 'E']
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle(tr("alignment.title"))
        self.setMinimumSize(1000, 750)
        
        # 数据目录
        self.store_dir = Path("DataCenter/RawData")
        self.output_dir = Path("DataCenter/RawData/Align_data")
        
        # Lazy import after directory setup
        from src.core.data_processor import DataProcessor
        self.processor = DataProcessor(
            store_dir=str(self.store_dir),
            output_dir=str(self.output_dir)
        )
        
        # 可用文件
        self.available_files = []
        
        # 对齐结果
        self.full_df = None
        self.preview_df = None
        
        # 互斥锁标志 (防止回调循环)
        self._updating_combos = False
        
        # 初始化 UI
        self._init_ui()
        
        # 扫描可用文件
        self._scan_parquet_files()
    
    def _init_ui(self):
        """初始化界面"""
        layout = QVBoxLayout()
        
        # === 1. 文件选择区域 ===
        selection_group = QGroupBox(tr("alignment.select_files"))
        selection_layout = QVBoxLayout()
        
        # --- Asset A (必选) ---
        asset_a_layout = QHBoxLayout()
        self.label_asset_a = QLabel("⚓ " + tr("alignment.asset_a"))
        self.label_asset_a.setToolTip(tr("alignment.anchor_tooltip"))
        self.label_asset_a.setStyleSheet("font-weight: bold; color: #FF9800;")
        asset_a_layout.addWidget(self.label_asset_a)
        self.combo_asset_a = QComboBox()
        self.combo_asset_a.setMinimumWidth(400)
        self.combo_asset_a.currentIndexChanged.connect(lambda: self._on_combo_changed(0))
        asset_a_layout.addWidget(self.combo_asset_a)
        asset_a_layout.addStretch()
        selection_layout.addLayout(asset_a_layout)
        
        # --- Asset B (必选) ---
        asset_b_layout = QHBoxLayout()
        self.label_asset_b = QLabel("   " + tr("alignment.asset_b"))
        asset_b_layout.addWidget(self.label_asset_b)
        self.combo_asset_b = QComboBox()
        self.combo_asset_b.setMinimumWidth(400)
        self.combo_asset_b.currentIndexChanged.connect(lambda: self._on_combo_changed(1))
        asset_b_layout.addWidget(self.combo_asset_b)
        asset_b_layout.addStretch()
        selection_layout.addLayout(asset_b_layout)
        
        # --- Multi-Data Alignment 启用复选框 ---
        self.multi_align_checkbox = QCheckBox(tr("alignment.multi_align_checkbox"))
        self.multi_align_checkbox.setChecked(False)
        self.multi_align_checkbox.setStyleSheet("margin-top: 8px; font-weight: bold; color: #2196F3;")
        self.multi_align_checkbox.toggled.connect(self._on_multi_align_toggled)
        selection_layout.addWidget(self.multi_align_checkbox)
        
        # --- Asset C/D/E (可选, 默认隐藏) ---
        self.multi_asset_widgets = {}  # 存储 C/D/E 的 layout 和 combo
        self.combo_assets_cde = []  # [combo_c, combo_d, combo_e]
        
        for idx, label in enumerate(['C', 'D', 'E']):
            asset_layout = QHBoxLayout()
            lbl = QLabel(f"   Asset {label} (Ref):")
            asset_layout.addWidget(lbl)
            
            combo = QComboBox()
            combo.setMinimumWidth(400)
            combo_idx = idx + 2  # C=2, D=3, E=4
            combo.currentIndexChanged.connect(lambda checked, ci=combo_idx: self._on_combo_changed(ci))
            asset_layout.addWidget(combo)
            asset_layout.addStretch()
            
            # 创建一个容器 widget 来控制显示/隐藏
            from PyQt6.QtWidgets import QWidget
            container = QWidget()
            container.setLayout(asset_layout)
            container.setVisible(False)  # 默认隐藏
            selection_layout.addWidget(container)
            
            self.multi_asset_widgets[label] = {
                'container': container,
                'label': lbl,
                'combo': combo
            }
            self.combo_assets_cde.append(combo)
        
        # --- Anchor Asset 选择 ---
        self.anchor_container = QWidget()
        anchor_layout = QHBoxLayout()
        anchor_layout.setContentsMargins(0, 4, 0, 0)
        anchor_layout.addWidget(QLabel(tr("alignment.anchor_label")))
        
        self.anchor_group = QButtonGroup(self)
        self.anchor_radios = {}  # {'A': QRadioButton, 'B': QRadioButton, ...}
        
        for i, label in enumerate(self.ASSET_LABELS):
            radio = QRadioButton(f"Asset {label}")
            self.anchor_group.addButton(radio, i)
            anchor_layout.addWidget(radio)
            self.anchor_radios[label] = radio
            # C/D/E 默认禁用
            if label in ['C', 'D', 'E']:
                radio.setEnabled(False)
                radio.setVisible(False)
        
        # 默认 A 为 Anchor
        self.anchor_radios['A'].setChecked(True)
        
        anchor_layout.addStretch()
        self.anchor_container.setLayout(anchor_layout)
        selection_layout.addWidget(self.anchor_container)
        
        from PyQt6.QtWidgets import QWidget
        self.anchor_container = self.anchor_container  # 已经赋值
        
        selection_group.setLayout(selection_layout)
        layout.addWidget(selection_group)
        
        # === 2. 对齐选项 ===
        options_group = QGroupBox(tr("alignment.options"))
        options_layout = QVBoxLayout()
        
        # Forward Fill 选项 (简化: 勾选 = 对所有非 Anchor 资产 ffill)
        ffill_layout = QHBoxLayout()
        self.ffill_checkbox = QCheckBox(tr("alignment.forward_fill"))
        self.ffill_checkbox.setChecked(True)
        self.ffill_checkbox.setToolTip(tr("alignment.anchor_tooltip"))
        ffill_layout.addWidget(self.ffill_checkbox)
        ffill_layout.addStretch()
        options_layout.addLayout(ffill_layout)
        
        # 仅保留重叠 选项
        overlap_layout = QHBoxLayout()
        self.overlap_checkbox = QCheckBox("仅保留重叠时间段 (纯净模式)")
        self.overlap_checkbox.setChecked(False)
        self.overlap_checkbox.setToolTip("开启后，将彻底剔除因单边行情或节假日未开盘造成的非时间重叠行 (is_overlap=False)")
        overlap_layout.addWidget(self.overlap_checkbox)
        overlap_layout.addStretch()
        options_layout.addLayout(overlap_layout)
        
        options_group.setLayout(options_layout)
        layout.addWidget(options_group)
        
        # === 3. 操作按钮 ===
        button_layout = QHBoxLayout()
        button_layout.addStretch()
        
        self.refresh_btn = QPushButton(tr("alignment.buttons.refresh"))
        self.refresh_btn.clicked.connect(self._scan_parquet_files)
        button_layout.addWidget(self.refresh_btn)
        
        self.export_btn = QPushButton(tr("alignment.buttons.export"))
        self.export_btn.setEnabled(False)
        self.export_btn.setStyleSheet("""
            QPushButton {
                background-color: #2196F3;
                color: white;
                font-weight: bold;
                padding: 8px 16px;
                border-radius: 4px;
            }
            QPushButton:hover {
                background-color: #1976D2;
            }
            QPushButton:disabled {
                background-color: #cccccc;
            }
        """)
        self.export_btn.clicked.connect(self._export_result)
        button_layout.addWidget(self.export_btn)
        
        # Save to Master DB Button
        self.save_db_btn = QPushButton("💾 保存至数据中心")
        self.save_db_btn.setEnabled(False)
        self.save_db_btn.setStyleSheet("""
            QPushButton {
                background-color: #607D8B;
                color: white;
                font-weight: bold;
                padding: 8px 16px;
                border-radius: 4px;
            }
            QPushButton:hover {
                background-color: #546E7A;
            }
            QPushButton:disabled {
                background-color: #cccccc;
            }
        """)
        self.save_db_btn.clicked.connect(self._on_save_to_db)
        button_layout.addWidget(self.save_db_btn)
        
        self.align_btn = QPushButton(tr("alignment.buttons.execute"))
        self.align_btn.setStyleSheet("""
            QPushButton {
                background-color: #4CAF50;
                color: white;
                font-weight: bold;
                padding: 8px 16px;
                border-radius: 4px;
            }
            QPushButton:hover {
                background-color: #45a049;
            }
            QPushButton:disabled {
                background-color: #cccccc;
            }
        """)
        self.align_btn.clicked.connect(self._start_alignment)
        button_layout.addWidget(self.align_btn)
        
        layout.addLayout(button_layout)
        
        # === 4. 预览表格 ===
        preview_label = QLabel(tr("alignment.preview.title"))
        preview_label.setStyleSheet("font-weight: bold; font-size: 14px; margin-top: 10px;")
        layout.addWidget(preview_label)
        
        self.preview_table = QTableWidget()
        self.preview_table.setAlternatingRowColors(True)
        self.preview_table.setStyleSheet("""
            QTableWidget {
                gridline-color: #ddd;
                font-size: 11px;
            }
            QTableWidget::item {
                padding: 4px;
            }
        """)
        layout.addWidget(self.preview_table)
        
        # === 5. 状态栏 ===
        self.status_label = QLabel(tr("alignment.status.ready"))
        self.status_label.setStyleSheet("color: #666; padding: 8px; background-color: #f5f5f5; border-radius: 4px;")
        layout.addWidget(self.status_label)
        
        self.setLayout(layout)
    
    # ========== 文件扫描 ==========
    
    def _scan_parquet_files(self):
        """扫描 DataCenter/RawData 目录下的所有 Parquet 文件"""
        try:
            if not self.store_dir.exists():
                self.store_dir.mkdir(parents=True, exist_ok=True)
            
            # 获取 .parquet 文件 (Recursive scan)
            self.available_files = list(self.store_dir.rglob("*.parquet"))
            
            # 所有 combo (A, B, C, D, E)
            all_combos = self._get_all_combos()
            
            # 清空并重新填充
            self._updating_combos = True  # 防止回调循环
            for combo in all_combos:
                combo.clear()
            
            if not self.available_files:
                for combo in all_combos:
                    combo.addItem(tr("alignment.placeholders.no_files"))
                self.align_btn.setEnabled(False)
                self.status_label.setText(tr("alignment.status.no_files"))
                self.status_label.setStyleSheet("color: #ff9800; padding: 8px; background-color: #fff3e0; border-radius: 4px;")
                self._updating_combos = False
                return
            
            # 添加文件列表到 A 和 B
            for file in self.available_files:
                try:
                    display_name = str(file.relative_to(self.store_dir))
                except ValueError:
                    display_name = file.name
                
                self.combo_asset_a.addItem(display_name, str(file))
                self.combo_asset_b.addItem(display_name, str(file))
            
            # C/D/E 添加占位项 + 文件列表
            for combo in self.combo_assets_cde:
                combo.addItem(tr("alignment.optional_placeholder"), None)  # 占位项
                for file in self.available_files:
                    try:
                        display_name = str(file.relative_to(self.store_dir))
                    except ValueError:
                        display_name = file.name
                    combo.addItem(display_name, str(file))
            
            # 如果至少有两个文件，默认选择不同的文件
            if len(self.available_files) >= 2:
                self.combo_asset_b.setCurrentIndex(1)
            
            self._updating_combos = False
            
            self.align_btn.setEnabled(True)
            self.status_label.setText(tr("alignment.status.files_scanned", count=len(self.available_files)))
            self.status_label.setStyleSheet("color: #4CAF50; padding: 8px; background-color: #f1f8f4; border-radius: 4px;")
            
            # 刷新互斥状态
            self._update_combo_exclusion()
            
        except Exception as e:
            QMessageBox.warning(self, tr("alignment.messages.scan_error_title"), tr("alignment.messages.scan_error_message", error=str(e)))
    
    # ========== 互斥逻辑 (Mutual Exclusion) ==========
    
    def _get_all_combos(self):
        """获取所有 combo 的列表 (A, B + 可能的 C, D, E)"""
        return [self.combo_asset_a, self.combo_asset_b] + self.combo_assets_cde
    
    def _get_active_combos(self):
        """获取当前激活的 combos (A+B 始终, C/D/E 仅在多数据模式)"""
        combos = [self.combo_asset_a, self.combo_asset_b]
        if self.multi_align_checkbox.isChecked():
            combos.extend(self.combo_assets_cde)
        return combos
    
    def _on_combo_changed(self, combo_index):
        """当任意 combo 选择变更时，更新所有其他 combo 的可用性"""
        if self._updating_combos:
            return  # 防止回调循环
        self._update_combo_exclusion()
    
    def _update_combo_exclusion(self):
        """
        互斥逻辑核心: 当一个文件被 combo A 选中后,
        在 combo B/C/D/E 中该文件应被置灰(Disabled)。
        """
        self._updating_combos = True
        
        active_combos = self._get_active_combos()
        
        # 1. 收集所有已选文件 -> {file_path: combo_index}
        selected_files = {}
        for i, combo in enumerate(active_combos):
            data = combo.currentData()
            if data:  # 排除占位项 (None)
                selected_files[data] = i
        
        # 2. 对每个 combo，禁用已被其他 combo 选中的项
        for i, combo in enumerate(active_combos):
            model = combo.model()
            current_data = combo.currentData()
            
            for row in range(combo.count()):
                item_data = combo.itemData(row)
                if item_data is None:
                    # 占位项始终可选
                    if isinstance(model, QStandardItemModel):
                        model.item(row).setEnabled(True)
                    continue
                    
                # 如果这个文件被其他 combo 选中且不是当前 combo 自己
                if item_data in selected_files and selected_files[item_data] != i:
                    # 置灰禁用
                    if isinstance(model, QStandardItemModel):
                        model.item(row).setEnabled(False)
                else:
                    # 可选
                    if isinstance(model, QStandardItemModel):
                        model.item(row).setEnabled(True)
        
        self._updating_combos = False
    
    # ========== Multi-Data 切换 ==========
    
    def _on_multi_align_toggled(self, checked):
        """当 'Enable Multi-Data Alignment' 勾选/取消时"""
        # 显示/隐藏 C, D, E 的容器
        for label in ['C', 'D', 'E']:
            self.multi_asset_widgets[label]['container'].setVisible(checked)
            self.anchor_radios[label].setVisible(checked)
        
        if not checked:
            # 取消多数据模式时:
            # 1. 重置 C/D/E 为占位项
            self._updating_combos = True
            for combo in self.combo_assets_cde:
                combo.setCurrentIndex(0)  # 回到 "(可选 - 不使用)"
            self._updating_combos = False
            
            # 2. 禁用 C/D/E 的 Anchor 选项
            for label in ['C', 'D', 'E']:
                self.anchor_radios[label].setEnabled(False)
            
            # 3. 如果当前 Anchor 是 C/D/E，回退到 A
            current_anchor_id = self.anchor_group.checkedId()
            if current_anchor_id >= 2:
                self.anchor_radios['A'].setChecked(True)
        else:
            # 启用多数据模式: 更新 C/D/E 的 Anchor 可用性
            self._update_anchor_availability()
        
        # 刷新互斥状态
        self._update_combo_exclusion()
        
        # 更新 Anchor 标签
        self._update_anchor_labels()
    
    def _update_anchor_availability(self):
        """根据 C/D/E 是否选了文件来启用/禁用对应的 Anchor RadioButton"""
        for idx, label in enumerate(['C', 'D', 'E']):
            combo = self.combo_assets_cde[idx]
            has_selection = combo.currentData() is not None
            self.anchor_radios[label].setEnabled(has_selection)
            
            # 如果取消了文件选择但 Anchor 还指向它，回退到 A
            if not has_selection and self.anchor_radios[label].isChecked():
                self.anchor_radios['A'].setChecked(True)
    
    def _update_anchor_labels(self):
        """更新 Asset 标签，Anchor 显示 ⚓ 标记"""
        anchor_id = self.anchor_group.checkedId()
        anchor_label = self.ASSET_LABELS[anchor_id] if 0 <= anchor_id < len(self.ASSET_LABELS) else 'A'
        
        # Asset A 标签
        if anchor_label == 'A':
            self.label_asset_a.setText("⚓ " + tr("alignment.asset_a"))
            self.label_asset_a.setStyleSheet("font-weight: bold; color: #FF9800;")
        else:
            self.label_asset_a.setText("   " + tr("alignment.asset_a"))
            self.label_asset_a.setStyleSheet("")
        
        # Asset B 标签
        if anchor_label == 'B':
            self.label_asset_b.setText("⚓ " + tr("alignment.asset_b"))
            self.label_asset_b.setStyleSheet("font-weight: bold; color: #FF9800;")
        else:
            self.label_asset_b.setText("   " + tr("alignment.asset_b"))
            self.label_asset_b.setStyleSheet("")
        
        # C/D/E 标签
        for idx, label in enumerate(['C', 'D', 'E']):
            lbl_widget = self.multi_asset_widgets[label]['label']
            if anchor_label == label:
                lbl_widget.setText(f"⚓ Asset {label} (Ref):")
                lbl_widget.setStyleSheet("font-weight: bold; color: #FF9800;")
            else:
                lbl_widget.setText(f"   Asset {label} (Ref):")
                lbl_widget.setStyleSheet("")
    
    # ========== 对齐执行 ==========
    
    def _start_alignment(self):
        """开始执行对齐"""
        # 获取必选文件 A + B
        file_a = self.combo_asset_a.currentData()
        file_b = self.combo_asset_b.currentData()
        
        if not file_a or not file_b:
            QMessageBox.warning(self, tr("alignment.messages.select_error_title"),
                                tr("alignment.select_ab_required"))
            return
        
        if file_a == file_b:
            QMessageBox.warning(self, tr("alignment.messages.select_error_title"),
                                tr("alignment.messages.select_different_files"))
            return
        
        # 获取对齐选项
        apply_ffill = self.ffill_checkbox.isChecked()
        only_overlap = self.overlap_checkbox.isChecked()
        
        # 收集所有激活的文件路径
        file_paths = [file_a, file_b]
        
        is_multi = self.multi_align_checkbox.isChecked()
        if is_multi:
            for combo in self.combo_assets_cde:
                data = combo.currentData()
                if data:  # 非占位项
                    file_paths.append(data)
            
            # 检查是否有重复文件
            if len(file_paths) != len(set(file_paths)):
                QMessageBox.warning(self, tr("alignment.messages.select_error_title"),
                                    tr("alignment.messages.select_different_files"))
                return
        
        # 确定 Anchor index
        anchor_id = self.anchor_group.checkedId()
        
        # 映射 anchor_id 到实际 file_paths 索引
        # anchor_id: 0=A, 1=B, 2=C, 3=D, 4=E
        # file_paths: [A, B, 可能的C, 可能的D, 可能的E]
        # 需要计算 anchor 在 file_paths 中的实际位置
        if anchor_id <= 1:
            anchor_index = anchor_id
        else:
            # C/D/E 需要找到它在 file_paths 中的位置
            # C = combo_assets_cde[0], D = combo_assets_cde[1], E = combo_assets_cde[2]
            cde_combo_idx = anchor_id - 2  # 0=C, 1=D, 2=E
            anchor_file = self.combo_assets_cde[cde_combo_idx].currentData()
            if anchor_file and anchor_file in file_paths:
                anchor_index = file_paths.index(anchor_file)
            else:
                anchor_index = 0  # 回退到 A
        
        # 禁用按钮
        self.align_btn.setEnabled(False)
        self.export_btn.setEnabled(False)
        self.save_db_btn.setEnabled(False)
        self.align_btn.setText(tr("alignment.buttons.executing"))
        self.status_label.setText(tr("alignment.status.aligning"))
        self.status_label.setStyleSheet("color: #2196F3; padding: 8px; background-color: #e3f2fd; border-radius: 4px;")
        
        # 决定使用哪种模式
        if len(file_paths) == 2 and not is_multi:
            # 双文件模式 (向后兼容)
            # 确定 ffill_asset: Anchor 是 A 则填充 B, Anchor 是 B 则填充 A, 否则 both
            if anchor_id == 0:
                ffill_asset = 'B'
            elif anchor_id == 1:
                ffill_asset = 'A'
            else:
                ffill_asset = 'both'
            
            self.worker = AlignmentWorker(
                processor=self.processor,
                mode='dual',
                file_a=file_a,
                file_b=file_b,
                apply_ffill=apply_ffill,
                ffill_asset=ffill_asset,
                only_overlap=only_overlap
            )
        else:
            # 多文件模式
            self.worker = AlignmentWorker(
                processor=self.processor,
                mode='multi',
                file_paths=file_paths,
                anchor_index=anchor_index,
                apply_ffill=apply_ffill,
                only_overlap=only_overlap
            )
        
        self.worker.finished.connect(self._on_alignment_finished)
        self.worker.error.connect(self._on_alignment_error)
        self.worker.start()
    
    def _on_alignment_finished(self, full_df, preview_df):
        """对齐完成回调"""
        self.full_df = full_df
        self.preview_df = preview_df
        
        # 更新预览表格
        self._display_preview(preview_df)
        
        # 恢复按钮
        self.align_btn.setEnabled(True)
        self.align_btn.setText(tr("alignment.buttons.execute"))
        self.export_btn.setEnabled(True)
        self.save_db_btn.setEnabled(True)
        
        # 更新状态
        self.status_label.setText(f"✅ 对齐完成：总行数: {len(full_df)} | 预览: {len(preview_df)} 行")
        self.status_label.setStyleSheet("color: #4CAF50; padding: 8px; background-color: #f1f8f4; border-radius: 4px;")
        
        QMessageBox.information(
            self,
            tr("alignment.messages.success_title"),
            f"数据对齐完成！\n\n"
            f"总行数: {len(full_df)}\n\n"
            f"现在可以点击 '导出结果' 按钮保存数据至本地。"
        )
    
    def _on_alignment_error(self, error_msg):
        """对齐错误回调 — 区分友好提示和系统错误"""
        self.align_btn.setEnabled(True)
        self.align_btn.setText(tr("alignment.buttons.execute"))
        
        self.status_label.setText("❌ 对齐失败 - 请查看错误信息")
        self.status_label.setStyleSheet("color: #f44336; padding: 8px; background-color: #ffebee; border-radius: 4px;")
        
        # 🛡️ 检测是否为「已对齐文件」的友好提示
        if "已经是经过对齐处理的数据文件" in error_msg:
            # 提取第一行作为主消息
            friendly_msg = error_msg.split('\n\n')[0]
            QMessageBox.warning(
                self,
                tr("alignment.already_aligned_title"),
                friendly_msg
            )
        else:
            QMessageBox.critical(
                self,
                tr("alignment.messages.error_title"),
                f"数据对齐失败：\n\n{error_msg}"
            )
    
    # ========== 导出与保存 ==========
    
    def _export_result(self):
        """导出对齐后的数据"""
        if self.full_df is None:
            return
            
        try:
            default_name = "aligned_data.parquet"
            
            file_path, filter_selected = QFileDialog.getSaveFileName(
                self,
                tr("alignment.messages.save_dialog_title"),
                str(Path.home() / "Desktop" / default_name),
                "Parquet Files (*.parquet);;CSV Files (*.csv)"
            )
            
            if not file_path:
                return
            
            if file_path.endswith('.csv'):
                self.full_df.to_csv(file_path, index=False)
            else:
                self.full_df.to_parquet(file_path, index=False)
                
            QMessageBox.information(
                self,
                tr("alignment.messages.export_success_title"),
                f"文件已保存至：\n{file_path}"
            )
            
        except Exception as e:
            QMessageBox.critical(
                self,
                tr("alignment.messages.export_error_title"),
                f"保存文件时出错:\n\n{str(e)}"
            )

    def _on_save_to_db(self):
        """Save aligned data to Master DB"""
        if self.full_df is None:
            return

        from PyQt6.QtWidgets import QInputDialog
        
        name, ok = QInputDialog.getText(
            self, 
            "保存至数据中心",
            "请输入数据集名称 (例如: Aligned_FCPO_ZL1_15m):",
            text="Aligned_Data"
        )
        
        if not ok or not name.strip():
            return
            
        safe_name = "".join([c for c in name if c.isalnum() or c in ('-', '_')]).strip()
        if not safe_name:
            QMessageBox.warning(self, "无效名称", "文件名包含无效字符！")
            return
            
        try:
            aligned_dir = self.output_dir
            aligned_dir.mkdir(parents=True, exist_ok=True)
            
            save_path = aligned_dir / f"{safe_name}.parquet"
            self.full_df.to_parquet(save_path)
            
            QMessageBox.information(
                self,
                "保存成功",
                f"数据已成功保存至 Master DB！\n\n"
                f"路径: {save_path}\n"
                f"您可以在 '数据管理中心' (Data Manager) 中查看。"
            )
            
        except Exception as e:
            QMessageBox.critical(self, "保存失败", f"无法保存到 Master DB:\n{str(e)}")
    
    # ========== 预览表格 ==========

    def _display_preview(self, df):
        """表格中更新预览数据"""
        if df is None or df.empty:
            return
        
        self.preview_table.setRowCount(len(df))
        self.preview_table.setColumnCount(len(df.columns))
        self.preview_table.setHorizontalHeaderLabels(df.columns.tolist())
        
        for i, row in enumerate(df.itertuples(index=False)):
            for j, value in enumerate(row):
                if pd.isna(value):
                    item = QTableWidgetItem("NaN")
                    item.setBackground(QColor(255, 200, 200))
                    item.setForeground(QColor(150, 150, 150))
                elif isinstance(value, (int, float)):
                    item = QTableWidgetItem(f"{value:.4f}" if isinstance(value, float) else str(value))
                else:
                    item = QTableWidgetItem(str(value))
                
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.preview_table.setItem(i, j, item)
        
        self.preview_table.resizeColumnsToContents()
