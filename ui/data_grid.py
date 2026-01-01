"""
Data Grid - Table widget for displaying first and last 5 rows of DataFrame.
"""

from PyQt6.QtWidgets import QTableWidget, QTableWidgetItem, QHeaderView
from PyQt6.QtGui import QColor
from PyQt6.QtCore import Qt
import pandas as pd
import math


class DataGrid(QTableWidget):
    """
    Custom table widget for displaying DataFrame preview with highlighting.
    Shows first 5 rows, separator, and last 5 rows.
    """
    
    def __init__(self):
        super().__init__()
        self._init_ui()
    
    def _init_ui(self):
        """Initialize table styling."""
        # Set alternating row colors
        self.setAlternatingRowColors(True)
        
        # Set column resize mode
        header = self.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        
        # Set selection behavior
        self.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        
        # Minimal styling - let dark theme handle colors
        self.setStyleSheet("""
            QTableWidget {
                border: 1px solid #555;
                gridline-color: #444;
            }
            QTableWidget::item {
                padding: 5px;
            }
            QHeaderView::section {
                padding: 8px;
                border: 1px solid #555;
                font-weight: bold;
            }
        """)
    
    def display_dataframe(self, df: pd.DataFrame):
        """
        Display first 5 and last 5 rows of DataFrame with highlighting.
        
        Args:
            df: DataFrame to display
        """
        print(f"[DEBUG] DataGrid.display_dataframe called with {len(df)} rows")
        
        if df.empty:
            self.setRowCount(0)
            self.setColumnCount(0)
            return
        
        # Make a copy and reset index to avoid index conflicts
        df_copy = df.copy().reset_index(drop=True)
        
        # Determine how many rows to show
        total_rows = len(df_copy)
        
        if total_rows <= 10:
            # Show all rows
            display_df = df_copy.copy()
            show_separator = False
        else:
            # Show first 5 + separator + last 5
            first_5 = df_copy.head(5).copy()
            last_5 = df_copy.tail(5).copy()
            
            # Create separator row
            separator_data = {col: '...' for col in df_copy.columns}
            separator_df = pd.DataFrame([separator_data])
            
            # Reset indices before concatenation
            first_5.reset_index(drop=True, inplace=True)
            separator_df.reset_index(drop=True, inplace=True)
            last_5.reset_index(drop=True, inplace=True)
            
            display_df = pd.concat([first_5, separator_df, last_5], ignore_index=True)
            show_separator = True
        
        print(f"[DEBUG] Display DataFrame shape: {display_df.shape}, show_separator: {show_separator}")
        
        # Set table dimensions
        num_rows = len(display_df)
        num_cols = len(display_df.columns)
        
        self.setRowCount(num_rows)
        self.setColumnCount(num_cols)
        
        # Set column headers
        self.setHorizontalHeaderLabels(display_df.columns.tolist())
        
        # Populate table
        for row_idx in range(num_rows):
            # Check if this is the separator row
            is_separator = show_separator and row_idx == 5
            
            # Get original row data for highlighting logic
            if not is_separator:
                if show_separator:
                    if row_idx < 5:
                        # First 5 rows: row_idx maps directly
                        orig_row_idx = row_idx
                    else:
                        # After separator (row 6-10): map to last 5 rows of original data
                        # row_idx=6 -> orig_row_idx=239 (244-5)
                        # row_idx=10 -> orig_row_idx=243 (244-1)
                        orig_row_idx = total_rows - (num_rows - row_idx)
                else:
                    orig_row_idx = row_idx
                
                # Use df_copy instead of df to avoid index issues
                orig_row = df_copy.iloc[orig_row_idx]
            
            for col_idx, col_name in enumerate(display_df.columns):
                value = display_df.iloc[row_idx][col_name]
                
                # Create table item
                item = QTableWidgetItem(str(value))
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                
                # Apply highlighting rules (skip separator row)
                if not is_separator:
                    should_highlight = False
                    
                    # Check if Volume == 0
                    if col_name == 'Volume' and value == 0:
                        should_highlight = True
                    
                    # Check if Close is NaN
                    if col_name == 'Close':
                        try:
                            if pd.isna(orig_row['Close']) or math.isnan(float(value)):
                                should_highlight = True
                        except (ValueError, TypeError):
                            pass
                    
                    # Apply red background if needed
                    if should_highlight:
                        item.setBackground(QColor('#ffcccc'))
                
                # Set separator row styling
                if is_separator:
                    item.setBackground(QColor('#f0f0f0'))
                    font = item.font()
                    font.setBold(True)
                    item.setFont(font)
                
                self.setItem(row_idx, col_idx, item)
        
        # Adjust row heights
        self.resizeRowsToContents()
