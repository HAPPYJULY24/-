
from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel,
                             QPushButton, QComboBox, QGroupBox, QFormLayout,
                             QSplitter, QTableWidget, QTableWidgetItem,
                             QHeaderView, QMessageBox, QTextEdit, QFrame,
                             QDoubleSpinBox, QSpinBox, QCheckBox, QSizePolicy,
                             QScrollArea)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QFont, QColor
import pandas as pd
import pyqtgraph as pg
import numpy as np
import json
import copy
from pathlib import Path

from src.quant_bridge import BacktestEngine
from logic.risk_manager_interceptor import RiskManager as RMInterceptor, RiskConfig
from utils.cache_manager import CacheManager
from ui.widgets.kpi_card import KPICard


# ─────────────────────────────────────────────────────────
# WORKER
# ─────────────────────────────────────────────────────────

class RiskWorker(QThread):
    """
    Dual-Track or Triple-Track Audit Worker.
    - override_dna=None  → 2-track (Base vs Original DNA)
    - override_dna=dict  → 3-track (Base vs Original DNA vs Overridden DNA)
    """
    finished = pyqtSignal(dict)
    error    = pyqtSignal(str)

    def __init__(self, dna_path: str, signal_path: str, override_dna: dict = None):
        super().__init__()
        self.dna_path     = dna_path
        self.signal_path  = signal_path
        self.override_dna = override_dna   # None → 2-track, dict → 3-track

    # ------------------------------------------------------------------
    def run(self):
        try:
            df = pd.read_parquet(self.signal_path)

            with open(self.dna_path, 'r', encoding='utf-8') as f:
                dna = json.load(f)

            # ── Shared params from original DNA ─────────────────────
            upper_bound    = float(dna["optimized_decision_parameters"].get("entry_threshold", 0.0))
            lower_bound    = float(dna["optimized_decision_parameters"].get("exit_threshold", 0.0))
            execution_mode = dna["optimized_decision_parameters"].get("execution_mode", 'Close')
            allow_overnight= bool(dna["execution_constraints"].get("allow_overnight", True))
            allow_lunch    = bool(dna["execution_constraints"].get("allow_lunch", True))
            multiplier     = float(dna["friction_costs"].get("multiplier", 25.0))
            commission     = float(dna["friction_costs"].get("commission_per_lot", 15.0))
            slippage       = float(dna["friction_costs"].get("slippage_ticks", 1.0))
            initial_capital= float(dna["environment"].get("initial_capital", 100000.0))
            initial_margin = float(dna["backtest_risk_settings"].get("initial_margin", 5000.0))

            # Generate signals once (shared across all tracks)
            from src.core.signal_generator import SignalFactory
            df['signal'] = SignalFactory.create('Mean Reversion').generate(
                df, upper_bound=upper_bound, lower_bound=lower_bound)

            # Read max_position_size from DNA so BASE track uses the same lot cap
            max_lots = int(dna["backtest_risk_settings"].get("max_position_size", 20))

            # Read SL/TP from DNA for Track 2 (Original)
            dna_sl_pct = float(dna.get("backtest_risk_settings", {}).get("stop_loss_value", 0.0))
            dna_tp_pct = float(dna.get("backtest_risk_settings", {}).get("take_profit_value", 0.0))

            common_kwargs = dict(
                multiplier=multiplier, commission=commission, slippage=slippage,
                initial_capital=initial_capital, initial_margin=initial_margin,
                maintenance_margin_rate=0.8,
                allow_lunch=allow_lunch, allow_overnight=allow_overnight,
                execution_mode=execution_mode,
                risk_params={'max_lots': max_lots,
                             'sl_pct': dna_sl_pct,
                             'tp_pct': dna_tp_pct}
            )

            # BASE: same max_lots as DNA, but margin=0 & ADX off → pure alpha, no capital constraints
            dummy_cfg = RiskConfig(
                initial_capital=initial_capital, initial_margin=0.0,
                risk_target_pct=999.0, max_position_size=max_lots,
                multiplier=multiplier, adx_filter_enabled=False)
            engine1 = BacktestEngine()
            base_kwargs = dict(common_kwargs)
            base_kwargs['risk_params'] = {'max_lots': max_lots}  # BASE: no SL/TP
            base_results = engine1.event_driven.run(
                df=df.copy(), asset_symbol="BASE",
                RiskManagerClass=lambda *a, **kw: RMInterceptor(dummy_cfg),
                **base_kwargs)

            # ── Track 2: Original DNA Run ────────────────────────────
            auth_cfg = RiskConfig.from_dna(self.dna_path)
            engine2  = BacktestEngine()
            orig_results = engine2.event_driven.run(
                df=df.copy(), asset_symbol="ORIGINAL",
                RiskManagerClass=lambda *a, **kw: RMInterceptor(auth_cfg),
                **common_kwargs)

            result = {'base': base_results, 'original': orig_results}

            # ── Track 3: Overridden DNA (only when override active) ──
            if self.override_dna is not None:
                ov = self.override_dna
                override_cfg = RiskConfig(
                    initial_capital = float(ov.get("initial_capital", initial_capital)),
                    initial_margin  = float(ov.get("initial_margin", initial_margin)),
                    risk_target_pct = float(ov.get("risk_target_pct", 1.0)),
                    max_position_size = int(ov.get("max_position_size", 20)),
                    multiplier      = multiplier,   # never override alpha params
                    adx_filter_enabled = bool(ov.get("adx_filter_enabled", False))
                )
                # Override capital & margin in engine kwargs if user changed them
                override_kwargs = dict(common_kwargs)
                override_kwargs['initial_capital'] = float(ov.get("initial_capital", initial_capital))
                override_kwargs['initial_margin']  = float(ov.get("initial_margin", initial_margin))

                # Override allow_overnight / allow_lunch from constraints
                override_kwargs['allow_overnight'] = bool(ov.get("allow_overnight", allow_overnight))
                override_kwargs['allow_lunch']     = bool(ov.get("allow_lunch", allow_lunch))

                # Override SL/TP in risk_params (critical: must create a NEW dict)
                override_kwargs['risk_params'] = {
                    'max_lots': int(ov.get("max_position_size", max_lots)),
                    'sl_pct':   float(ov.get("stop_loss_value", 0.0)),
                    'tp_pct':   float(ov.get("take_profit_value", 0.0)),
                }

                engine3 = BacktestEngine()
                override_results = engine3.event_driven.run(
                    df=df.copy(), asset_symbol="OVERRIDE",
                    RiskManagerClass=lambda *a, **kw: RMInterceptor(override_cfg),
                    **override_kwargs)
                result['override'] = override_results

            self.finished.emit(result)

        except Exception as e:
            import traceback
            self.error.emit(str(e) + "\n" + traceback.format_exc())


# ─────────────────────────────────────────────────────────
# RISK TAB
# ─────────────────────────────────────────────────────────

class RiskTab(QWidget):
    """Risk Sentinel – DNA-Driven Dual/Triple Track Audit Dashboard."""

    def __init__(self):
        super().__init__()
        self._current_dna = None          # loaded DNA dict
        self.current_results = None
        self.init_ui()

    # ===========================================================
    # UI BUILD
    # ===========================================================
    def init_ui(self):
        main_layout = QHBoxLayout(self)
        main_layout.setContentsMargins(10, 10, 10, 10)

        # ── LEFT PANEL ──────────────────────────────────────────
        left_scroll = QScrollArea()
        left_scroll.setWidgetResizable(True)
        left_scroll.setFixedWidth(340)
        left_scroll.setFrameShape(QFrame.Shape.NoFrame)

        left_container = QWidget()
        left_layout    = QVBoxLayout(left_container)
        left_layout.setContentsMargins(0, 0, 8, 0)
        left_layout.setSpacing(8)

        # Title
        title = QLabel("🛡️ Risk Sentinel (DNA Driven)")
        title.setStyleSheet("font-size: 16px; font-weight: bold; color: #4CAF50;")
        left_layout.addWidget(title)

        # ─ Data Center selector ────────────────────────────────
        sel_group  = QGroupBox("Select Strategy from Data Center")
        sel_layout = QVBoxLayout()
        self.folder_combo = QComboBox()
        self.folder_combo.currentIndexChanged.connect(self._on_folder_changed)
        sel_layout.addWidget(self.folder_combo)
        refresh_btn = QPushButton("⟳ Refresh Data Center")
        refresh_btn.clicked.connect(self.refresh_files)
        sel_layout.addWidget(refresh_btn)
        sel_group.setLayout(sel_layout)
        left_layout.addWidget(sel_group)

        # ─ Read-Only DNA summary (alpha + cost params) ─────────
        ro_group  = QGroupBox("📄 Alpha DNA  (Read-Only)")
        ro_layout = QVBoxLayout()
        self.dna_summary = QTextEdit()
        self.dna_summary.setReadOnly(True)
        self.dna_summary.setFixedHeight(180)
        self.dna_summary.setPlaceholderText("Select a strategy folder…")
        self.dna_summary.setStyleSheet(
            "background:#1A1A2E; color:#A5D6A7; font-family:monospace; font-size:11px;")
        ro_layout.addWidget(self.dna_summary)
        ro_group.setLayout(ro_layout)
        left_layout.addWidget(ro_group)

        # ─ Override toggle ─────────────────────────────────────
        self.override_chk = QCheckBox("🎮 Enable Risk Override (Playground)")
        self.override_chk.setChecked(False)
        self.override_chk.stateChanged.connect(self._on_override_toggled)
        left_layout.addWidget(self.override_chk)

        # ─ Playground inputs (disabled by default) ─────────────
        self.pg_group = QGroupBox("⚙️ Override Parameters")
        pg_form       = QFormLayout()
        pg_form.setSpacing(4)

        def dbl(lo, hi, val, step=1.0, suffix=""):
            sb = QDoubleSpinBox(); sb.setRange(lo, hi)
            sb.setValue(val); sb.setSingleStep(step)
            if suffix: sb.setSuffix(suffix)
            return sb

        def spin(lo, hi, val):
            sb = QSpinBox(); sb.setRange(lo, hi); sb.setValue(val); return sb

        self.pg_capital       = dbl(1000, 10_000_000, 100_000, 1000)
        self.pg_margin        = dbl(0, 100_000, 5000, 100)
        self.pg_risk_target   = dbl(0, 100, 1.0, 0.1, "%")
        self.pg_max_position  = spin(1, 9999, 20)
        self.pg_sl_value      = dbl(0, 10, 1.0, 0.1, "%")
        self.pg_tp_value      = dbl(0, 10, 0.0, 0.1, "%")
        self.pg_leverage      = dbl(1, 50, 1.0, 0.5, "x")
        self.pg_adx           = QCheckBox()
        self.pg_overnight     = QCheckBox(); self.pg_overnight.setChecked(True)
        self.pg_lunch         = QCheckBox(); self.pg_lunch.setChecked(True)

        pg_form.addRow("Initial Capital:",     self.pg_capital)
        pg_form.addRow("Initial Margin:",      self.pg_margin)
        pg_form.addRow("Risk Target %:",       self.pg_risk_target)
        pg_form.addRow("Max Lots:",            self.pg_max_position)
        pg_form.addRow("Stop Loss %:",         self.pg_sl_value)
        pg_form.addRow("Take Profit %:",       self.pg_tp_value)
        pg_form.addRow("Leverage Limit:",      self.pg_leverage)
        pg_form.addRow("ADX Filter:",          self.pg_adx)
        pg_form.addRow("Allow Overnight:",     self.pg_overnight)
        pg_form.addRow("Allow Lunch:",         self.pg_lunch)

        self.pg_group.setLayout(pg_form)
        self._set_playground_enabled(False)
        left_layout.addWidget(self.pg_group)

        left_layout.addStretch()

        # ─ Run button ──────────────────────────────────────────
        self.run_btn = QPushButton("🚀 Run Audit")
        self.run_btn.setMinimumHeight(46)
        self.run_btn.setStyleSheet(
            "background:#2196F3; color:white; font-weight:bold; font-size:13px; border-radius:4px;")
        self.run_btn.clicked.connect(self._run_audit)
        left_layout.addWidget(self.run_btn)

        left_scroll.setWidget(left_container)
        main_layout.addWidget(left_scroll)

        # ── RIGHT PANEL ─────────────────────────────────────────
        right_panel  = QWidget()
        right_layout = QVBoxLayout(right_panel)
        right_layout.setContentsMargins(0, 0, 0, 0)

        # Top bar: export button (right-aligned)
        top_bar = QHBoxLayout()
        top_bar.addStretch()
        self.export_btn = QPushButton("💾 Export Audit Log")
        self.export_btn.setEnabled(False)
        self.export_btn.setStyleSheet(
            "background:#37474F; color:#B0BEC5; padding:4px 10px; border-radius:4px;")
        self.export_btn.clicked.connect(self._export_audit_log)
        top_bar.addWidget(self.export_btn)
        right_layout.addLayout(top_bar)

        # KPI row
        kpi_layout = QHBoxLayout()
        self.card_calmar = KPICard("Calmar Ratio")
        self.card_mdd    = KPICard("Max DD Duration", tooltip_text="Longest drawdown period")
        self.card_recv   = KPICard("Recovery Factor")
        self.card_block  = KPICard("Signals Blocked", is_interactive=True,
                                   tooltip_text="Click for breakdown")
        for c in (self.card_calmar, self.card_mdd, self.card_recv, self.card_block):
            kpi_layout.addWidget(c)
        right_layout.addLayout(kpi_layout)

        # Splitter: chart + table
        splitter = QSplitter(Qt.Orientation.Vertical)

        chart_container = QWidget()
        cl = QVBoxLayout(chart_container)
        cl.setContentsMargins(0, 0, 0, 0)

        self.plot_widget = pg.PlotWidget(
            axisItems={'bottom': pg.DateAxisItem(orientation='bottom')})
        self.plot_widget.setBackground('#1E1E1E')
        self.plot_widget.setTitle("Risk Audit Verification", color='#FFF', size='12pt')
        self.plot_widget.showGrid(x=True, y=True, alpha=0.3)
        self.plot_widget.addLegend()
        p1 = self.plot_widget.getPlotItem()
        p1.setLabel('left', "Equity (RM)")
        p1.getAxis('left').enableAutoSIPrefix(False)
        self.plot_widget.setContentsMargins(10, 10, 40, 10)
        cl.addWidget(self.plot_widget)
        splitter.addWidget(chart_container)

        # Comparison table (dynamically 2 or 3 col)
        self.comp_table = QTableWidget()
        self.comp_table.verticalHeader().setVisible(False)
        self.comp_table.setAlternatingRowColors(True)
        self.comp_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        splitter.addWidget(self.comp_table)

        splitter.setSizes([500, 220])
        right_layout.addWidget(splitter)

        main_layout.addWidget(right_panel)

        self.refresh_files()

    # ===========================================================
    # HELPERS
    # ===========================================================

    def _set_playground_enabled(self, enabled: bool):
        for w in (self.pg_capital, self.pg_margin, self.pg_risk_target,
                  self.pg_max_position, self.pg_sl_value, self.pg_tp_value,
                  self.pg_leverage, self.pg_adx, self.pg_overnight, self.pg_lunch):
            w.setEnabled(enabled)
        self.pg_group.setStyleSheet(
            "" if enabled else
            "QGroupBox { color: #888; } QLabel { color: #888; }")

    def _on_override_toggled(self, state):
        enabled = (state == Qt.CheckState.Checked.value)
        self._set_playground_enabled(enabled)
        self.run_btn.setText(
            "🚀 Run 3-Track Audit" if enabled else "🚀 Run Audit")

    # ------------------------------------------------------------------
    def refresh_files(self):
        storage_dir = CacheManager.get_backtest_storage_dir()
        storage_dir.mkdir(parents=True, exist_ok=True)
        folders = [f for f in storage_dir.iterdir()
                   if f.is_dir() and list(f.glob("*.json"))]
        self.folder_combo.clear()
        if folders:
            self.folder_combo.addItems([f.name for f in folders])
        else:
            self.folder_combo.addItem("No DNA found in Data Center")
            self.run_btn.setEnabled(False)
            self.dna_summary.clear()

    # ------------------------------------------------------------------
    def _on_folder_changed(self):
        folder_name = self.folder_combo.currentText()
        if not folder_name or folder_name == "No DNA found in Data Center":
            self.run_btn.setEnabled(False)
            return

        dc_path    = CacheManager.get_backtest_storage_dir() / folder_name
        json_files = list(dc_path.glob("*.json"))

        if not json_files:
            self.run_btn.setEnabled(False)
            return

        try:
            with open(json_files[0], 'r', encoding='utf-8') as f:
                dna = json.load(f)
            self._current_dna = dna

            # ── Read-only summary ──────────────────────────────
            op = dna.get("optimized_decision_parameters", {})
            env = dna.get("environment", {})
            fc  = dna.get("friction_costs", {})
            brs = dna.get("backtest_risk_settings", {})
            ec  = dna.get("execution_constraints", {})
            idn = dna.get("identification", {})

            lines = [
                f"ID:            {idn.get('strategy_id','?')}",
                f"Universe:      {env.get('universe','?')}",
                f"Timeframe:     {env.get('timeframe','?')}",
                "─" * 35,
                f"Entry Thresh:  {op.get('entry_threshold', '?')}",
                f"Exit Thresh:   {op.get('exit_threshold', '?')}",
                f"Exec Mode:     {op.get('execution_mode', '?')}",
                f"Order Type:    {op.get('order_type', '?')}",
                "─" * 35,
                f"Multiplier:    {fc.get('multiplier', '?')}",
                f"Commission:    {fc.get('commission_per_lot', '?')}",
                f"Slippage:      {fc.get('slippage_ticks', '?')} ticks",
                "─" * 35,
                f"Capital:       {env.get('initial_capital', 0):,.0f}",
            ]
            self.dna_summary.setText("\n".join(lines))

            # ── Pre-fill playground with original DNA values ───
            self.pg_capital.setValue(float(env.get("initial_capital", 100000)))
            self.pg_margin.setValue(float(brs.get("initial_margin", 5000)))
            self.pg_risk_target.setValue(float(brs.get("risk_target_pct", 1.0)))
            self.pg_max_position.setValue(int(brs.get("max_position_size", 20)))
            self.pg_sl_value.setValue(float(brs.get("stop_loss_value", 0.0)))
            self.pg_tp_value.setValue(float(brs.get("take_profit_value", 0.0)))
            self.pg_leverage.setValue(float(brs.get("leverage_limit", 1.0)))
            self.pg_adx.setChecked(bool(ec.get("adx_filter_enabled", False)))
            self.pg_overnight.setChecked(bool(ec.get("allow_overnight", True)))
            self.pg_lunch.setChecked(bool(ec.get("allow_lunch", True)))

            self.run_btn.setEnabled(True)

        except Exception as e:
            self.dna_summary.setText(f"Error loading DNA: {e}")
            self.run_btn.setEnabled(False)

    # ===========================================================
    # RUN
    # ===========================================================
    def _run_audit(self):
        folder_name = self.folder_combo.currentText()
        if not folder_name or folder_name == "No DNA found in Data Center":
            return

        dc_path    = CacheManager.get_backtest_storage_dir() / folder_name
        json_files = list(dc_path.glob("*.json"))
        if not json_files:
            QMessageBox.critical(self, "Error", "No DNA json found.")
            return

        dna_path = str(json_files[0])

        # Resolve signal parquet
        dna      = self._current_dna or {}
        raw_u    = dna.get("environment", {}).get("universe", "")
        if isinstance(raw_u, list):
            raw_u = ", ".join(raw_u)
        universe  = raw_u.strip("[]").strip()
        timeframe = dna.get("environment", {}).get("timeframe", "unknown")

        signals_dir    = Path("DataCenter/Alpha_data")
        possible_paths = [
            signals_dir / f"{universe}_{timeframe}.parquet",
            signals_dir / f"{universe}.parquet",
        ]
        signal_path = None
        for p in possible_paths:
            if p.exists():
                signal_path = str(p)
                break
        if not signal_path and signals_dir.exists():
            candidates = sorted(signals_dir.rglob(f"{universe}*.parquet"))
            if candidates:
                signal_path = str(candidates[0])

        if not signal_path:
            QMessageBox.critical(self, "File Not Found",
                f"Cannot locate signal data for universe '{universe}'.\n"
                f"Searched:\n" + "\n".join(f"- {p}" for p in possible_paths))
            return

        # Collect override dict when enabled
        override_dna = None
        if self.override_chk.isChecked():
            override_dna = {
                "initial_capital":  self.pg_capital.value(),
                "initial_margin":   self.pg_margin.value(),
                "risk_target_pct":  self.pg_risk_target.value(),
                "max_position_size":self.pg_max_position.value(),
                "stop_loss_value":  self.pg_sl_value.value(),
                "take_profit_value":self.pg_tp_value.value(),
                "leverage_limit":   self.pg_leverage.value(),
                "adx_filter_enabled": self.pg_adx.isChecked(),
                "allow_overnight":  self.pg_overnight.isChecked(),
                "allow_lunch":      self.pg_lunch.isChecked(),
            }

        self.run_btn.setEnabled(False)
        self.run_btn.setText("⏳ Auditing…")

        self.worker = RiskWorker(dna_path, signal_path, override_dna)
        self.worker.finished.connect(self._on_finished)
        self.worker.error.connect(self._on_error)
        self.worker.start()

    # ===========================================================
    # CALLBACKS
    # ===========================================================
    def _on_finished(self, results):
        self.run_btn.setEnabled(True)
        
        # Robust Cleanup: Wait for the thread to fully exit its C++ loop before destroying
        if hasattr(self, 'worker') and self.worker:
            self.worker.wait()
            self.worker.deleteLater()
            self.worker = None

        has_override = "override" in results
        self.run_btn.setText("🚀 Run 3-Track Audit" if has_override else "🚀 Run Audit")

        self.current_results = results
        self.export_btn.setEnabled(True)
        self.export_btn.setStyleSheet(
            "background:#546E7A; color:white; padding:4px 10px; border-radius:4px;")

        base_m = results["base"].get("metrics", {})
        orig_m = results["original"].get("metrics", {})
        over_m = results.get("override", {}).get("metrics", {})

        self._update_kpis(orig_m, results["original"].get("audit_log", []))
        self._update_table(base_m, orig_m, over_m if has_override else None)
        self._plot_chart(
            results["base"].get("equity_curve"),
            results["original"].get("equity_curve"),
            results.get("override", {}).get("equity_curve") if has_override else None,
        )

    def _on_error(self, msg):
        self.run_btn.setEnabled(True)
        self.run_btn.setText(
            "🚀 Run 3-Track Audit" if self.override_chk.isChecked() else "🚀 Run Audit")
        QMessageBox.critical(self, "Audit Error", msg)

    # ===========================================================
    # CHART
    # ===========================================================
    def _plot_chart(self, base_df, orig_df, override_df=None):
        self.plot_widget.clear()
        self.plot_widget.addLegend()

        def _plot(df, pen, name):
            if df is None or (hasattr(df, 'empty') and df.empty):
                return
            idx = df.index
            try:
                ts = idx.astype('int64') // 10**9
            except Exception:
                ts = np.arange(len(idx))
            eq = df['equity'].values if 'equity' in df.columns else np.zeros(len(df))
            self.plot_widget.plot(ts.values, eq, pen=pen, name=name)

        _plot(base_df,     pg.mkPen('#888888', width=2, style=Qt.PenStyle.DashLine),
              'Base (Gross)')
        _plot(orig_df,     pg.mkPen('#2196F3', width=2),
              'Original DNA')
        if override_df is not None:
            _plot(override_df, pg.mkPen('#FF9800', width=2),
                  'Overridden DNA')

    # ===========================================================
    # KPI CARDS
    # ===========================================================
    def _update_kpis(self, metrics, audit_log=None):
        self.card_calmar.update_value(f"{metrics.get('Calmar Ratio', 0):.2f}", 'calmar')
        self.card_recv.update_value(f"{metrics.get('Recovery Factor', 0):.2f}", 'recovery_factor')
        mdd_dur = metrics.get('Max DD Duration', 0)
        self.card_mdd.update_value(f"{mdd_dur} Days", 'mdd_duration')

        breakdown   = {'Reject: Margin/ADX': 0, 'Adjust: Max Pos': 0}
        total_blocked = 0
        if audit_log is not None and isinstance(audit_log, pd.DataFrame) and not audit_log.empty:
            counts = audit_log['Type'].value_counts()
            breakdown['Reject: Margin/ADX'] = int(counts.get('Order_Rejected', 0))
            breakdown['Adjust: Max Pos']    = int(counts.get('Order_Adjusted', 0))
            total_blocked = breakdown['Reject: Margin/ADX'] + breakdown['Adjust: Max Pos']

        self.card_block.update_value(total_blocked)
        self.card_block.set_breakdown(breakdown)

    # ===========================================================
    # COMPARISON TABLE (dynamic 2 or 3 columns)
    # ===========================================================
    def _update_table(self, base, orig, override=None):
        metrics = [
            ("Net Profit",        "Net Profit"),
            ("Max Drawdown (%)",  "Max Drawdown (%)"),
            ("Sharpe Ratio",      "Sharpe Ratio"),
            ("Calmar Ratio",      "Calmar Ratio"),
            ("Win Rate (%)",      "Win Rate (%)"),
            ("Total Trades",      "Total Trades"),
            ("Profit Factor",     "Profit Factor"),
        ]

        has_override = override is not None
        col_headers  = ["Metric", "Base (Gross)", "Original DNA"]
        if has_override:
            col_headers.append("Overridden DNA")

        self.comp_table.setColumnCount(len(col_headers))
        self.comp_table.setHorizontalHeaderLabels(col_headers)
        self.comp_table.setRowCount(len(metrics))

        def fmt(v):
            if isinstance(v, float): return f"{v:,.2f}"
            return str(v)

        def color_item(val, ref, name):
            item = QTableWidgetItem(fmt(val))
            if isinstance(val, (int, float)) and isinstance(ref, (int, float)):
                better = (val > ref and "Drawdown" not in name) or \
                         ("Drawdown" in name and abs(val) < abs(ref))
                if better:
                    item.setForeground(QColor("#4CAF50"))
                elif val != ref:
                    item.setForeground(QColor("#FF5252"))
            return item

        for i, (name, key) in enumerate(metrics):
            b_val  = base.get(key, 0)
            o_val  = orig.get(key, 0)
            ov_val = override.get(key, 0) if has_override else None

            self.comp_table.setItem(i, 0, QTableWidgetItem(name))
            self.comp_table.setItem(i, 1, QTableWidgetItem(fmt(b_val)))
            self.comp_table.setItem(i, 2, color_item(o_val,  b_val, name))
            if has_override:
                self.comp_table.setItem(i, 3, color_item(ov_val, o_val, name))

    # ===========================================================
    # EXPORT AUDIT LOG — Folder-based, one CSV per track
    # ===========================================================
    def _export_audit_log(self):
        if not self.current_results:
            QMessageBox.warning(self, "No Data", "Run an audit first.")
            return

        from ui.export_audit_dialog import ExportAuditDialog
        from utils.cache_manager import CacheManager
        from PyQt6.QtWidgets import QFileDialog
        from pathlib import Path
        import shutil

        has_override = "override" in self.current_results
        dialog = ExportAuditDialog(has_override=has_override, parent=self)
        if dialog.exec() != dialog.DialogCode.Accepted:
            return

        export_data = dialog.get_export_data()
        folder_name = export_data["folder_name"]
        save_mode   = export_data["save_mode"]

        try:
            local_dir_path = None
            if save_mode in ("local", "both"):
                local_dir_path = QFileDialog.getExistingDirectory(
                    self, "Select Local Export Directory", "",
                    QFileDialog.Option.ShowDirsOnly)
                if not local_dir_path:
                    return

            def _write_track_csv(dest_dir, track_key, label):
                res = self.current_results.get(track_key, {})

                # Trade log
                trades = res.get("trade_log") or res.get("trades")
                if trades is None:
                    return
                if isinstance(trades, list):
                    trades = pd.DataFrame(trades)
                if hasattr(trades, "empty") and trades.empty:
                    return
                trades = trades.reset_index(drop=True)

                # Risk intercept log -- keep ONLY Order_Approved (1:1 with trades)
                audit_raw = res.get("audit_log", [])
                if isinstance(audit_raw, list) and audit_raw:
                    audit_all = pd.DataFrame(audit_raw)
                elif isinstance(audit_raw, pd.DataFrame) and not audit_raw.empty:
                    audit_all = audit_raw.copy()
                else:
                    audit_all = pd.DataFrame()

                if not audit_all.empty and "Type" in audit_all.columns:
                    approved = (audit_all[audit_all["Type"] == "Order_Approved"]
                                .reset_index(drop=True))
                    intercept_df = pd.DataFrame({
                        "risk_decision":      approved["Type"].values      if "Type"      in approved.columns else [],
                        "risk_direction":     approved["Direction"].values  if "Direction" in approved.columns else [],
                        "risk_approved_lots": approved["Volume"].values     if "Volume"   in approved.columns else [],
                        "risk_reason":        approved["Reason"].values     if "Reason"   in approved.columns else [],
                    }).reset_index(drop=True)
                else:
                    intercept_df = pd.DataFrame()

                if not intercept_df.empty:
                    n = max(len(trades), len(intercept_df))
                    trades       = trades.reindex(range(n))
                    intercept_df = intercept_df.reindex(range(n))
                    sep = pd.DataFrame({"risk_SEPARATOR": [""] * n})
                    combined = pd.concat([trades, sep, intercept_df], axis=1)
                else:
                    combined = trades

                combined.to_csv(dest_dir / f"{label}_trade_log.csv",
                                index=False, encoding="utf-8-sig")

            paths_created = []
            dc_base = None

            if save_mode in ("data_center", "both"):
                dc_base = CacheManager.get_risk_storage_dir() / folder_name
                dc_base.mkdir(parents=True, exist_ok=True)
                _write_track_csv(dc_base, "base",     "BASE")
                _write_track_csv(dc_base, "original", "ORIGINAL")
                if has_override:
                    _write_track_csv(dc_base, "override", "OVERRIDE")
                paths_created.append(f"Data Center:\n{dc_base}")

            if save_mode in ("local", "both"):
                local_base = Path(local_dir_path) / folder_name
                if save_mode == "both" and dc_base and dc_base.exists():
                    shutil.copytree(str(dc_base), str(local_base), dirs_exist_ok=True)
                else:
                    local_base.mkdir(parents=True, exist_ok=True)
                    _write_track_csv(local_base, "base",     "BASE")
                    _write_track_csv(local_base, "original", "ORIGINAL")
                    if has_override:
                        _write_track_csv(local_base, "override", "OVERRIDE")
                paths_created.append(f"Local Export:\n{local_base}")

            tracks = ["BASE", "ORIGINAL"] + (["OVERRIDE"] if has_override else [])
            files_str = ", ".join(f"{t}_trade_log.csv" for t in tracks)
            QMessageBox.information(self, "Export Successful",
                f"Audit log exported!\n\nFiles: {files_str}\n\n"
                + "\n\n".join(paths_created))

        except Exception as e:
            QMessageBox.critical(self, "Export Error", str(e))
