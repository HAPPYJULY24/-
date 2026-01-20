"""
Data Preview Dialog - Display first N rows of a Master DB file.
"""

from PyQt6.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QPushButton,
                             QLabel, QTableWidget, QTableWidgetItem, QHeaderView)
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QFont
import pandas as pd


class DataPreviewDialog(QDialog):
    """数据预览对话框（显示Master DB文件的前N行）"""
    
    def __init__(self, filepath, max_rows=10, parent=None):
        """
        Initialize data preview dialog
        
        Args:
            filepath: Path to the parquet file
            max_rows: Maximum number of rows to display (default: 10)
            parent: Parent widget
        """
        super().__init__(parent)
        self.filepath = filepath
        self.max_rows = max_rows
        
        # Extract filename
        import os
        self.filename = os.path.basename(filepath)
        
        self.setWindowTitle(f"数据预览 - {self.filename}")
        self.setMinimumSize(800, 500)
        
        self._init_ui()
        self._load_data()
    
    def _init_ui(self):
        """Initialize UI"""
        layout = QVBoxLayout()
        layout.setSpacing(10)
        layout.setContentsMargins(15, 15, 15, 15)
        
        # Title
        title = QLabel(f"📄 {self.filename}")
        title_font = QFont()
        title_font.setPointSize(12)
        title_font.setBold(True)
        title.setFont(title_font)
        layout.addWidget(title)
        
        # Info label
        self.info_label = QLabel()
        self.info_label.setStyleSheet("color: #666; font-size: 11px;")
        layout.addWidget(self.info_label)
        
        # Table
        self.table = QTableWidget()
        self.table.setAlternatingRowColors(True)
        self.table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        layout.addWidget(self.table)
        
        # Close button
        button_layout = QHBoxLayout()
        button_layout.addStretch()
        
        close_btn = QPushButton("关闭")
        close_btn.clicked.connect(self.accept)
        close_btn.setMinimumWidth(100)
        button_layout.addWidget(close_btn)
        
        layout.addLayout(button_layout)
        
        self.setLayout(layout)
    
    def _load_data(self):
        """Load and display data from parquet file"""
        try:
            # Read parquet file
            df = pd.read_parquet(self.filepath)
            total_rows = len(df)
            
            # Get first N rows
            preview_df = df.head(self.max_rows)
            
            # Update info label
            if total_rows > self.max_rows:
                self.info_label.setText(
                    f"显示前 {self.max_rows} 行（共 {total_rows:,} 行，"
                    f"{len(df.columns)} 列）"
                )
            else:
                self.info_label.setText(
                    f"共 {total_rows:,} 行，{len(df.columns)} 列"
                )
            
            # Set up table
            self.table.setRowCount(len(preview_df))
            self.table.setColumnCount(len(preview_df.columns))
            self.table.setHorizontalHeaderLabels(preview_df.columns.tolist())
            
            # Fill table
            for row_idx, row in preview_df.iterrows():
                for col_idx, value in enumerate(row):
                    # Format value
                    if pd.isna(value):
                        display_value = "N/A"
                    elif isinstance(value, float):
                        display_value = f"{value:.4f}"
                    else:
                        display_value = str(value)
                    
                    item = QTableWidgetItem(display_value)
                    self.table.setItem(row_idx, col_idx, item)
            
            # Auto-resize columns
            self.table.horizontalHeader().setSectionResizeMode(
                QHeaderView.ResizeMode.ResizeToContents
            )
            
        except Exception as e:
            from PyQt6.QtWidgets import QMessageBox
            QMessageBox.critical(
                self,
                "读取失败",
                f"无法读取文件：\n\n{str(e)}"
            )
            self.accept()  # Close dialog
