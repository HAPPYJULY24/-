"""
Data Grid - Table widget for displaying first and last 5 rows of DataFrame.
"""

from PyQt6.QtWidgets import QTableWidget, QTableWidgetItem, QHeaderView
from PyQt6.QtGui import QColor
from PyQt6.QtCore import Qt
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
    
    def display_records(self, columns, rows):
        """Display first 5 and last 5 rows from plain-Python rows."""
        total_rows = len(rows)
        print(f"[DEBUG] DataGrid.display_records called with {total_rows} rows")
        if total_rows == 0:
            self.setRowCount(0)
            self.setColumnCount(0)
            return

        show_separator = total_rows > 10
        
        row_payloads = []
        if show_separator:
            first_rows = rows[:5]
            last_rows = rows[-5:]
            row_payloads.extend(first_rows)
            row_payloads.append({col: '...' for col in columns})
            row_payloads.extend(last_rows)
        else:
            row_payloads = rows
        
        print(f"[DEBUG] Display rows: {len(row_payloads)}, columns: {len(columns)}, show_separator: {show_separator}")
        
        # Set table dimensions
        num_rows = len(row_payloads)
        num_cols = len(columns)
        
        self.setRowCount(num_rows)
        self.setColumnCount(num_cols)
        
        # Set column headers
        self.setHorizontalHeaderLabels(columns)
        
        # Populate table
        for row_idx in range(num_rows):
            # Check if this is the separator row
            is_separator = show_separator and row_idx == 5
            
            row_data = row_payloads[row_idx]
            close_value = row_data.get('Close', "")
            close_is_invalid = False
            try:
                close_str = str(close_value).lower()
                close_is_invalid = close_str in ['nan', 'none', 'na', '']
                if not close_is_invalid:
                    close_is_invalid = math.isnan(float(close_value))
            except (ValueError, TypeError):
                close_is_invalid = close_is_invalid or close_value is None

            for col_idx, col_name in enumerate(columns):
                value = row_data.get(col_name, "")
                
                # Create table item
                item = QTableWidgetItem(str(value))
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                
                # Apply highlighting rules (skip separator row)
                if not is_separator:
                    should_highlight = False
                    
                    # Highlight volume only when zero volume accompanies invalid close.
                    if col_name == 'Volume' and (value == 0 or value == '0') and close_is_invalid:
                        should_highlight = True
                    
                    # Check if Close is NaN
                    if col_name == 'Close':
                        try:
                            # Use string-based or float-based checks on the cell value itself
                            val_str = str(value).lower()
                            if val_str in ['nan', 'none', 'na'] or math.isnan(float(value)):
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

    def display_dataframe(self, df):
        """
        Backward-compatible wrapper. Prefer `display_records`.
        """
        try:
            columns = list(df.columns)
            rows = df.where(df.notna(), None).to_dict(orient='records')
            self.display_records(columns, rows)
        except Exception:
            self.setRowCount(0)
            self.setColumnCount(0)
