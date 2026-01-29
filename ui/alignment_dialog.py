"""
Data Alignment Studio Dialog - 数据对齐实验室

交互式对齐工具，允许用户选择任意两个 Parquet 文件进行对齐，
并实时预览结果以验证时区和列名处理的正确性。
"""

from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QComboBox,
    QPushButton, QTableWidget, QTableWidgetItem, QMessageBox,
    QProgressDialog, QCheckBox, QGroupBox, QRadioButton, QButtonGroup,
    QFileDialog
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QColor
from pathlib import Path
import pandas as pd


class AlignmentWorker(QThread):
    """后台对齐线程"""
    finished = pyqtSignal(pd.DataFrame, pd.DataFrame)  # full_df, preview_df
    error = pyqtSignal(str)
    
    def __init__(self, processor, file_a, file_b, apply_ffill, ffill_asset):
        super().__init__()
        self.processor = processor
        self.file_a = file_a
        self.file_b = file_b
        self.apply_ffill = apply_ffill
        self.ffill_asset = ffill_asset
    
    def run(self):
        try:
            full_df, preview_df = self.processor.align_custom_files(
                file_path_a=self.file_a,
                file_path_b=self.file_b,
                apply_ffill=self.apply_ffill,
                ffill_asset=self.ffill_asset
            )
            self.finished.emit(full_df, preview_df)
        except Exception as e:
            import traceback
            error_msg = f"{str(e)}\n\n{traceback.format_exc()}"
            self.error.emit(error_msg)


class AlignmentDialog(QDialog):
    """数据对齐实验室 - GUI 对话框"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("🔬 Data Alignment Studio - 数据对齐实验室")
        self.setMinimumSize(1000, 700)
        
        # 数据处理器
        from core.data_processor import DataProcessor
        self.processor = DataProcessor(
            store_dir="data/store",
            output_dir="data/processed"
        )
        
        # 存储路径
        self.store_dir = Path("data/store")
        self.available_files = []
        
        # 结果数据
        self.full_df = None
        self.preview_df = None
        
        # 初始化 UI
        self._init_ui()
        
        # 扫描可用文件
        self._scan_parquet_files()
    
    def _init_ui(self):
        """初始化界面"""
        layout = QVBoxLayout()
        
        # === 1. 文件选择区域 ===
        selection_group = QGroupBox("📁 选择要对齐的数据文件")
        selection_layout = QVBoxLayout()
        
        # Asset A
        asset_a_layout = QHBoxLayout()
        asset_a_layout.addWidget(QLabel("Asset A (Base):"))
        self.combo_asset_a = QComboBox()
        self.combo_asset_a.setMinimumWidth(400)
        asset_a_layout.addWidget(self.combo_asset_a)
        asset_a_layout.addStretch()
        selection_layout.addLayout(asset_a_layout)
        
        # Asset B
        asset_b_layout = QHBoxLayout()
        asset_b_layout.addWidget(QLabel("Asset B (Reference):"))
        self.combo_asset_b = QComboBox()
        self.combo_asset_b.setMinimumWidth(400)
        asset_b_layout.addWidget(self.combo_asset_b)
        asset_b_layout.addStretch()
        selection_layout.addLayout(asset_b_layout)
        
        selection_group.setLayout(selection_layout)
        layout.addWidget(selection_group)
        
        # === 2. 对齐选项 ===
        options_group = QGroupBox("⚙️ 对齐选项")
        options_layout = QVBoxLayout()
        
        # Forward Fill 选项
        ffill_layout = QHBoxLayout()
        self.ffill_checkbox = QCheckBox("应用前向填充 (Forward Fill)")
        self.ffill_checkbox.setChecked(True)
        self.ffill_checkbox.setToolTip("填补不同交易时间的数据缺口")
        ffill_layout.addWidget(self.ffill_checkbox)
        
        ffill_layout.addWidget(QLabel("  填充对象:"))
        self.ffill_group = QButtonGroup(self)
        self.ffill_asset_a = QRadioButton("Asset A")
        self.ffill_asset_b = QRadioButton("Asset B")
        self.ffill_both = QRadioButton("Both")
        self.ffill_asset_b.setChecked(True)  # 默认填充 B
        
        self.ffill_group.addButton(self.ffill_asset_a, 0)
        self.ffill_group.addButton(self.ffill_asset_b, 1)
        self.ffill_group.addButton(self.ffill_both, 2)
        
        ffill_layout.addWidget(self.ffill_asset_a)
        ffill_layout.addWidget(self.ffill_asset_b)
        ffill_layout.addWidget(self.ffill_both)
        ffill_layout.addStretch()
        
        options_layout.addLayout(ffill_layout)
        options_group.setLayout(options_layout)
        layout.addWidget(options_group)
        
        # === 3. 控制按钮 ===
        button_layout = QHBoxLayout()
        button_layout.addStretch()
        
        self.refresh_btn = QPushButton("🔄 刷新文件列表")
        self.refresh_btn.clicked.connect(self._scan_parquet_files)
        button_layout.addWidget(self.refresh_btn)
        
        self.export_btn = QPushButton("💾 导出结果")
        self.export_btn.setEnabled(False)  # 初始禁用
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
        
        self.align_btn = QPushButton("🚀 开始对齐")
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
        preview_label = QLabel("📊 数据预览 (前50行 + 后50行):")
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
        self.status_label = QLabel("准备就绪 - 请选择两个文件进行对齐")
        self.status_label.setStyleSheet("color: #666; padding: 8px; background-color: #f5f5f5; border-radius: 4px;")
        layout.addWidget(self.status_label)
        
        self.setLayout(layout)
    
    def _scan_parquet_files(self):
        """扫描 data/store 目录下的所有 Parquet 文件"""
        try:
            if not self.store_dir.exists():
                self.store_dir.mkdir(parents=True, exist_ok=True)
            
            # 查找所有 .parquet 文件
            self.available_files = list(self.store_dir.glob("*.parquet"))
            
            # 更新下拉框
            self.combo_asset_a.clear()
            self.combo_asset_b.clear()
            
            if not self.available_files:
                self.combo_asset_a.addItem("(无可用文件 - 请先下载数据)")
                self.combo_asset_b.addItem("(无可用文件 - 请先下载数据)")
                self.align_btn.setEnabled(False)
                self.status_label.setText("⚠️ 未找到 Parquet 文件 - 请先在主界面下载数据")
                self.status_label.setStyleSheet("color: #ff9800; padding: 8px; background-color: #fff3e0; border-radius: 4px;")
                return
            
            # 填充文件名
            for file in self.available_files:
                display_name = file.name
                self.combo_asset_a.addItem(display_name, str(file))
                self.combo_asset_b.addItem(display_name, str(file))
            
            # 如果有至少2个文件，自动选择不同的文件
            if len(self.available_files) >= 2:
                self.combo_asset_b.setCurrentIndex(1)
            
            self.align_btn.setEnabled(True)
            self.status_label.setText(f"✅ 找到 {len(self.available_files)} 个数据文件")
            self.status_label.setStyleSheet("color: #4CAF50; padding: 8px; background-color: #f1f8f4; border-radius: 4px;")
            
        except Exception as e:
            QMessageBox.warning(self, "扫描错误", f"扫描文件时出错:\n{str(e)}")
    
    def _start_alignment(self):
        """开始对齐处理"""
        # 获取选择的文件
        file_a = self.combo_asset_a.currentData()
        file_b = self.combo_asset_b.currentData()
        
        if not file_a or not file_b:
            QMessageBox.warning(self, "选择错误", "请选择两个文件进行对齐")
            return
        
        if file_a == file_b:
            QMessageBox.warning(self, "选择错误", "请选择两个不同的文件")
            return
        
        # 获取对齐选项
        apply_ffill = self.ffill_checkbox.isChecked()
        
        if self.ffill_asset_a.isChecked():
            ffill_asset = 'A'
        elif self.ffill_asset_b.isChecked():
            ffill_asset = 'B'
        else:
            ffill_asset = 'both'
        
        # 禁用按钮
        self.align_btn.setEnabled(False)
        self.export_btn.setEnabled(False)  # 禁用导出
        self.align_btn.setText("⏳ 对齐中...")
        self.status_label.setText("🔄 正在对齐数据，请稍候...")
        self.status_label.setStyleSheet("color: #2196F3; padding: 8px; background-color: #e3f2fd; border-radius: 4px;")
        
        # 启动后台线程
        self.worker = AlignmentWorker(
            processor=self.processor,
            file_a=file_a,
            file_b=file_b,
            apply_ffill=apply_ffill,
            ffill_asset=ffill_asset
        )
        
        self.worker.finished.connect(self._on_alignment_finished)
        self.worker.error.connect(self._on_alignment_error)
        self.worker.start()
    
    def _on_alignment_finished(self, full_df, preview_df):
        """对齐完成回调"""
        self.full_df = full_df
        self.preview_df = preview_df
        
        # 显示预览
        self._display_preview(preview_df)
        
        # 恢复按钮
        self.align_btn.setEnabled(True)
        self.align_btn.setText("🚀 开始对齐")
        # 启用导出按钮
        self.export_btn.setEnabled(True)
        
        # 更新状态
        self.status_label.setText(f"✅ 对齐完成！总行数: {len(full_df)} | 预览: {len(preview_df)} 行")
        self.status_label.setStyleSheet("color: #4CAF50; padding: 8px; background-color: #f1f8f4; border-radius: 4px;")
        
        QMessageBox.information(
            self,
            "对齐成功",
            f"数据对齐完成！\n\n"
            f"总行数: {len(full_df)}\n\n"
            f"您现在可以点击 '导出结果' 按钮将数据保存到指定位置。"
        )
    
    def _on_alignment_error(self, error_msg):
        """对齐错误回调"""
        self.align_btn.setEnabled(True)
        self.align_btn.setText("🚀 开始对齐")
        
        self.status_label.setText("❌ 对齐失败 - 请查看错误信息")
        self.status_label.setStyleSheet("color: #f44336; padding: 8px; background-color: #ffebee; border-radius: 4px;")
        
        QMessageBox.critical(
            self,
            "对齐错误",
            f"数据对齐失败:\n\n{error_msg}"
        )
    
    def _export_result(self):
        """导出对齐后的数据"""
        if self.full_df is None:
            return
            
        try:
            # 默认文件名
            default_name = "aligned_data.parquet"
            # 如果知道资产名称，可以使用更具体的名称
            # 这里简单起见使用默认名
            
            file_path, filter_selected = QFileDialog.getSaveFileName(
                self,
                "保存对齐结果",
                str(Path.home() / "Desktop" / default_name),  # 默认保存到桌面
                "Parquet Files (*.parquet);;CSV Files (*.csv)"
            )
            
            if not file_path:
                return
            
            # 保存文件
            if file_path.endswith('.csv'):
                self.full_df.to_csv(file_path, index=False)
            else:
                self.full_df.to_parquet(file_path, index=False)
                
            QMessageBox.information(
                self,
                "导出成功",
                f"文件已保存至:\n{file_path}"
            )
            
        except Exception as e:
            QMessageBox.critical(
                self,
                "导出错误",
                f"保存文件时出错:\n\n{str(e)}"
            )

    def _display_preview(self, df):
        """在表格中显示预览数据"""
        if df is None or df.empty:
            return
        
        # 设置表格尺寸
        self.preview_table.setRowCount(len(df))
        self.preview_table.setColumnCount(len(df.columns))
        self.preview_table.setHorizontalHeaderLabels(df.columns.tolist())
        
        # 填充数据
        for i, row in enumerate(df.itertuples(index=False)):
            for j, value in enumerate(row):
                # 格式化显示
                if pd.isna(value):
                    item = QTableWidgetItem("NaN")
                    item.setBackground(QColor(255, 200, 200))  # 浅红色高亮 NaN
                    item.setForeground(QColor(150, 150, 150))  # 灰色文字
                elif isinstance(value, (int, float)):
                    item = QTableWidgetItem(f"{value:.4f}" if isinstance(value, float) else str(value))
                else:
                    item = QTableWidgetItem(str(value))
                
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.preview_table.setItem(i, j, item)
        
        # 自动调整列宽
        self.preview_table.resizeColumnsToContents()
