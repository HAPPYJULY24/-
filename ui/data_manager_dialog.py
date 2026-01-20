"""
Data Manager Dialog - UI for managing Master DB and cached data.
"""

from PyQt6.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QPushButton,
                             QLabel, QTableWidget, QTableWidgetItem, QHeaderView,
                             QMessageBox, QInputDialog, QAbstractItemView)
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QFont
import os

from utils.cache_manager import CacheManager


class DataManagerDialog(QDialog):
    """数据管理中心对话框"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("数据管理中心 (Master DB Manager)")
        self.setMinimumSize(900, 600)
        self._init_ui()
        self.refresh_data()
    
    def _init_ui(self):
        """初始化界面"""
        layout = QVBoxLayout()
        layout.setSpacing(15)
        layout.setContentsMargins(20, 20, 20, 20)
        
        # Title
        title = QLabel("📊 数据管理中心")
        title_font = QFont()
        title_font.setPointSize(14)
        title_font.setBold(True)
        title.setFont(title_font)
        layout.addWidget(title)
        
        # Master DB路径显示
        path_layout = QHBoxLayout()
        
        self.path_label = QLabel(f"📁 Master DB位置: {os.path.abspath(CacheManager.STORE_DIR)}")
        self.path_label.setStyleSheet("color: #555; font-size: 11px;")
        path_layout.addWidget(self.path_label)
        
        open_folder_btn = QPushButton("打开文件夹")
        open_folder_btn.clicked.connect(self._on_open_master_db_folder)
        open_folder_btn.setMaximumWidth(120)
        path_layout.addWidget(open_folder_btn)
        
        layout.addLayout(path_layout)
        
        # 统计信息
        self.stats_label = QLabel()
        self.stats_label.setStyleSheet("font-size: 11px; color: #666;")
        layout.addWidget(self.stats_label)
        
        # 🆕 磁盘空间警告标签
        self.disk_warning_label = QLabel()
        self.disk_warning_label.setStyleSheet("font-size: 11px; font-weight: bold;")
        self.disk_warning_label.hide()  # 默认隐藏
        layout.addWidget(self.disk_warning_label)
        
        # Master DB文件列表
        list_label = QLabel("📂 已缓存的Master DB文件：")
        list_label_font = QFont()
        list_label_font.setBold(True)
        list_label.setFont(list_label_font)
        layout.addWidget(list_label)
        
        # 表格
        self.table = QTableWidget()
        self.table.setColumnCount(6)
        self.table.setHorizontalHeaderLabels([
            "代码", "时间粒度", "数据条数", "最新日期", "文件大小(MB)", "文件路径"
        ])
        
        # 表格样式
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(5, QHeaderView.ResizeMode.Stretch)
        
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SelectionMode.MultiSelection)
        self.table.setAlternatingRowColors(True)
        
        # 🆕 连接双击事件用于预览
        self.table.doubleClicked.connect(self._on_preview_double_click)
        
        layout.addWidget(self.table)
        
        # 操作按钮行
        button_layout = QHBoxLayout()
        
        refresh_btn = QPushButton("🔄 刷新列表")
        refresh_btn.clicked.connect(self.refresh_data)
        button_layout.addWidget(refresh_btn)
        
        # 🆕 预览按钮
        preview_btn = QPushButton("👁️ 预览数据")
        preview_btn.clicked.connect(self._on_preview_selected)
        preview_btn.setToolTip("查看选中文件的前10行数据")
        button_layout.addWidget(preview_btn)
        
        export_btn = QPushButton("📤 导出选中为CSV")
        export_btn.clicked.connect(self._on_export_selected)
        button_layout.addWidget(export_btn)
        
        # 🆕 批量导出按钮
        export_all_btn = QPushButton("📦 批量导出全部")
        export_all_btn.clicked.connect(self._on_export_all)
        export_all_btn.setToolTip("将所有Master DB导出为CSV")
        button_layout.addWidget(export_all_btn)
        
        delete_btn = QPushButton("🗑️ 删除选中")
        delete_btn.clicked.connect(self._on_delete_selected)
        delete_btn.setStyleSheet("background-color: #ff6b6b; color: white;")
        button_layout.addWidget(delete_btn)
        
        clear_all_btn = QPushButton("⚠️ 清空全部Master DB")
        clear_all_btn.clicked.connect(self._on_clear_all)
        clear_all_btn.setStyleSheet("background-color: #dc3545; color: white;")
        button_layout.addWidget(clear_all_btn)
        
        button_layout.addStretch()
        layout.addLayout(button_layout)
        
        # Exported data目录信息
        exported_layout = QHBoxLayout()
        
        self.exported_label = QLabel()
        self.exported_label.setStyleSheet("font-size: 11px; color: #666;")
        exported_layout.addWidget(self.exported_label)
        
        open_exported_btn = QPushButton("打开导出目录")
        open_exported_btn.clicked.connect(self._on_open_exported_folder)
        open_exported_btn.setMaximumWidth(120)
        exported_layout.addWidget(open_exported_btn)
        
        layout.addLayout(exported_layout)
        
        # Close button
        close_btn = QPushButton("关闭")
        close_btn.clicked.connect(self.accept)
        close_btn.setMaximumWidth(100)
        button_close_layout = QHBoxLayout()
        button_close_layout.addStretch()
        button_close_layout.addWidget(close_btn)
        layout.addLayout(button_close_layout)
        
        self.setLayout(layout)
    
    def refresh_data(self):
        """刷新Master DB列表和统计信息"""
        # 获取Master DB信息
        file_list, total_files, total_size_mb = CacheManager.get_master_db_info()
        
        # 更新统计信息
        self.stats_label.setText(
            f"共 {total_files} 个Master DB文件，总大小: {total_size_mb:.2f} MB"
        )
        
        # 清空表格
        self.table.setRowCount(0)
        
        # 填充表格
        for file_info in file_list:
            row_position = self.table.rowCount()
            self.table.insertRow(row_position)
            
            self.table.setItem(row_position, 0, QTableWidgetItem(file_info['code']))
            self.table.setItem(row_position, 1, QTableWidgetItem(file_info['timeframe']))
            self.table.setItem(row_position, 2, QTableWidgetItem(f"{file_info['rows']:,}"))
            self.table.setItem(row_position, 3, QTableWidgetItem(file_info['last_date']))
            self.table.setItem(row_position, 4, QTableWidgetItem(f"{file_info['size_mb']:.2f}"))
            self.table.setItem(row_position, 5, QTableWidgetItem(file_info['filepath']))
        
        # 获取导出目录信息
        exported_count, exported_size_mb = CacheManager.get_exported_data_info()
        self.exported_label.setText(
            f"💾 导出的数据文件: exported_data/ ({exported_count} 个文件, {exported_size_mb:.2f} MB)"
        )
        
        # 🆕 检查磁盘空间
        settings = CacheManager.load_settings()
        threshold = settings.get('disk_warning_threshold_gb', 1.0)
        is_low, free_gb, msg = CacheManager.is_disk_space_low(threshold_gb=threshold)
        
        if is_low:
            self.disk_warning_label.setText(f"⚠️  {msg}")
            self.disk_warning_label.setStyleSheet("color: #dc3545; font-weight: bold; font-size: 11px;")
            self.disk_warning_label.show()
        else:
            self.disk_warning_label.hide()
    
    def _on_open_master_db_folder(self):
        """打开Master DB文件夹"""
        success = CacheManager.open_directory_in_explorer(CacheManager.STORE_DIR)
        if not success:
            QMessageBox.warning(self, "错误", "无法打开文件夹！")
    
    def _on_open_exported_folder(self):
        """打开导出数据文件夹"""
        success = CacheManager.open_directory_in_explorer(CacheManager.EXPORTED_DIR)
        if not success:
            QMessageBox.warning(self, "错误", "无法打开文件夹！")
    
    def _on_export_selected(self):
        """导出选中的文件为CSV"""
        selected_rows = self.table.selectionModel().selectedRows()
        
        if not selected_rows:
            QMessageBox.information(self, "提示", "请先选择要导出的文件！")
            return
        
        success_count = 0
        fail_count = 0
        
        for index in selected_rows:
            row = index.row()
            filepath = self.table.item(row, 5).text()
            
            success, result = CacheManager.export_parquet_to_csv(filepath)
            if success:
                success_count += 1
            else:
                fail_count += 1
        
        # 刷新导出目录信息
        exported_count, exported_size_mb = CacheManager.get_exported_data_info()
        self.exported_label.setText(
            f"💾 导出的数据文件: exported_data/ ({exported_count} 个文件, {exported_size_mb:.2f} MB)"
        )
        
        if fail_count == 0:
            QMessageBox.information(
                self, 
                "导出成功", 
                f"成功导出 {success_count} 个文件到 exported_data/ 目录"
            )
        else:
            QMessageBox.warning(
                self,
                "导出完成",
                f"成功: {success_count} 个\n失败: {fail_count} 个"
            )
    
    def _on_delete_selected(self):
        """删除选中的文件"""
        selected_rows = self.table.selectionModel().selectedRows()
        
        if not selected_rows:
            QMessageBox.information(self, "提示", "请先选择要删除的文件！")
            return
        
        # 二次确认
        reply = QMessageBox.question(
            self,
            "确认删除",
            f"确定要删除选中的 {len(selected_rows)} 个Master DB文件吗？\n\n"
            "⚠️ 删除后将丢失增量更新的优势，下次需要全量下载！",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if reply != QMessageBox.StandardButton.Yes:
            return
        
        success_count = 0
        fail_count = 0
        
        for index in selected_rows:
            row = index.row()
            filepath = self.table.item(row, 5).text()
            
            if CacheManager.delete_master_db_file(filepath):
                success_count += 1
            else:
                fail_count += 1
        
        # 刷新列表
        self.refresh_data()
        
        if fail_count == 0:
            QMessageBox.information(
                self,
                "删除成功",
                f"成功删除 {success_count} 个文件"
            )
        else:
            QMessageBox.warning(
                self,
                "删除完成",
                f"成功: {success_count} 个\n失败: {fail_count} 个"
            )
    
    def _on_clear_all(self):
        """清空所有Master DB（三次确认）"""
        # 第一次确认
        reply1 = QMessageBox.warning(
            self,
            "⚠️ 危险操作",
            "确定要清空所有Master DB文件吗？\n\n"
            "这将删除所有缓存的历史数据！",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if reply1 != QMessageBox.StandardButton.Yes:
            return
        
        # 第二次确认（输入确认）
        text, ok = QInputDialog.getText(
            self,
            "二次确认",
            "请输入 'DELETE ALL' 来确认删除所有数据："
        )
        
        if not ok or text != "DELETE ALL":
            QMessageBox.information(self, "已取消", "操作已取消")
            return
        
        # 执行清空
        success, message = CacheManager.clear_all_master_db()
        
        if success:
            QMessageBox.information(self, "清理完成", message)
            self.refresh_data()
        else:
            QMessageBox.critical(self, "清理失败", message)
    
    def _on_preview_selected(self):
        """预览选中的文件"""
        selected_rows = self.table.selectionModel().selectedRows()
        
        if not selected_rows:
            QMessageBox.information(self, "提示", "请先选择要预览的文件！")
            return
        
        # 只预览第一个选中的文件
        row = selected_rows[0].row()
        filepath = self.table.item(row, 5).text()
        
        try:
            from .data_preview_dialog import DataPreviewDialog
            dialog = DataPreviewDialog(filepath, max_rows=10, parent=self)
            dialog.exec()
        except Exception as e:
            QMessageBox.critical(
                self,
                "预览失败",
                f"无法预览文件：\n\n{str(e)}"
            )
    
    def _on_preview_double_click(self, index):
        """双击表格行预览数据"""
        row = index.row()
        filepath = self.table.item(row, 5).text()
        
        try:
            from .data_preview_dialog import DataPreviewDialog
            dialog = DataPreviewDialog(filepath, max_rows=10, parent=self)
            dialog.exec()
        except Exception as e:
            QMessageBox.critical(
                self,
                "预览失败",
                f"无法预览文件：\n\n{str(e)}"
            )
    
    def _on_export_all(self):
        """批量导出所有Master DB为CSV"""
        # 获取文件数量
        file_list, total_files, _ = CacheManager.get_master_db_info()
        
        if total_files == 0:
            QMessageBox.information(self, "提示", "没有Master DB文件可导出！")
            return
        
        # 🆕 让用户选择导出目录
        from PyQt6.QtWidgets import QFileDialog
        import os
        
        default_dir = os.path.abspath(CacheManager.EXPORTED_DIR)
        
        export_dir = QFileDialog.getExistingDirectory(
            self,
            "选择导出目录",
            default_dir,
            QFileDialog.Option.ShowDirsOnly
        )
        
        # 用户取消选择
        if not export_dir:
            return
        
        # 确认导出
        reply = QMessageBox.question(
            self,
            "批量导出确认",
            f"确定要将所有 {total_files} 个Master DB文件导出为CSV吗？\n\n"
            f"导出位置：{export_dir}\n\n"
            f"这可能需要一些时间...",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if reply != QMessageBox.StandardButton.Yes:
            return
        
        # 创建进度对话框
        from PyQt6.QtWidgets import QProgressDialog
        from PyQt6.QtCore import Qt
        
        progress = QProgressDialog(
            "正在导出...",
            "取消",
            0,
            total_files,
            self
        )
        progress.setWindowTitle("批量导出进度")
        progress.setWindowModality(Qt.WindowModality.WindowModal)
        progress.setMinimumDuration(0)
        
        # 进度回调
        def update_progress(current, total, filename):
            if progress.wasCanceled():
                raise Exception("用户取消了导出操作")
            progress.setValue(current)
            progress.setLabelText(f"正在导出 ({current}/{total}): {filename}")
        
        try:
            # 🆕 使用用户选择的目录执行批量导出
            success_count, fail_count, errors = CacheManager.export_all_to_csv(
                output_dir=export_dir,  # 传递用户选择的目录
                progress_callback=update_progress
            )
            
            progress.close()
            
            # 刷新导出目录信息（如果导出到默认目录）
            if os.path.abspath(export_dir) == os.path.abspath(CacheManager.EXPORTED_DIR):
                exported_count, exported_size_mb = CacheManager.get_exported_data_info()
                self.exported_label.setText(
                    f"💾 导出的数据文件: exported_data/ ({exported_count} 个文件, {exported_size_mb:.2f} MB)"
                )
            
            # 显示结果
            if fail_count == 0:
                QMessageBox.information(
                    self,
                    "导出完成",
                    f"成功导出 {success_count} 个文件到：\n{export_dir}"
                )
            else:
                error_details = "\n".join(errors[:5])  # 最多显示5个错误
                if len(errors) > 5:
                    error_details += f"\n... 以及其他 {len(errors)-5} 个错误"
                
                QMessageBox.warning(
                    self,
                    "导出完成（部分失败）",
                    f"成功: {success_count} 个\n"
                    f"失败: {fail_count} 个\n\n"
                    f"导出位置：{export_dir}\n\n"
                    f"错误详情:\n{error_details}"
                )
        
        except Exception as e:
            progress.close()
            QMessageBox.critical(
                self,
                "导出失败",
                f"批量导出过程中发生错误：\n\n{str(e)}"
            )
