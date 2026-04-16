from __future__ import annotations

import base64
import json
import queue
import smtplib
import threading
import time
import tkinter as tk
import xml.etree.ElementTree as ET
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from tkinter import filedialog, messagebox, scrolledtext, ttk
from tkinter.filedialog import askopenfilename, asksaveasfilename
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# ── Optional heavy deps ────────────────────────────────────────────────────────
try:
    import matplotlib
    matplotlib.use("TkAgg")
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
    from matplotlib.figure import Figure
    import matplotlib.dates as mdates
    MPL_OK = True
except Exception:
    MPL_OK = False

try:
    from influxdb_client import InfluxDBClient
    from influxdb_client.client.flux_table import FluxStructureEncoder
    INFLUX_OK = True
except Exception:
    INFLUX_OK = False

try:
    from twilio.rest import Client as TwilioClient
    TWILIO_OK = True
except Exception:
    TWILIO_OK = False

try:
    import win32com.client as _win32com
    WIN32_OK = True
except Exception:
    WIN32_OK = False

# ── Timezone shim (Python 3.8 compat) ─────────────────────────────────────────
try:
    from zoneinfo import ZoneInfo
    def _tz(name: str): return ZoneInfo(name)
except ImportError:
    try:
        import pytz
        def _tz(name: str): return pytz.timezone(name)
    except ImportError:
        import datetime as _dt
        _AEST = _dt.timezone(_dt.timedelta(hours=10))
        def _tz(name: str): return _AEST

LOCAL_TZ    = _tz("Australia/Brisbane")
CONFIG_FILE = Path("influx_alert_config.json")
XML_DIR     = Path("alert_records")
LOG_QUEUE: queue.Queue[Tuple[str, str]] = queue.Queue()
DATA_QUEUE: queue.Queue[dict]           = queue.Queue()   # cycle data → GUI
APP_VERSION = "1"

COLORS = {
    "bg"      : "#1e2130",
    "bg_mid"  : "#252a3d",
    "bg_light": "#2a2f45",
    "acc"     : "#4a9eff",
    "green"   : "#4ddd88",
    "red"     : "#ff6b7a",
    "orange"  : "#ffaa44",
    "purple"  : "#b48eff",
    "text"    : "#e8eaf0",
    "muted"   : "#9aa0b8",
    "dim"     : "#555e7a",
    "toolbar" : "#16192b",
}

DIM_PALETTE = ["#4a9eff", "#4ddd88", "#ffaa44", "#b48eff", "#ff6b7a", "#ff9f7f", "#7fdfff"]


# ══════════════════════════════════════════════════════════════════════════════
# Data-classes
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class InfluxSettings:
    url            : str       = "http://localhost:8086"
    token          : str       = ""
    org            : str       = ""
    bucket         : str       = ""
    project        : str       = ""
    fields         : List[str] = field(default_factory=lambda: ["velx","vely","velz"])
    range_window   : str       = "20m"
    aggregate_every: str       = "1s"
    use_aggregation: bool      = True
    max_raw_points : int       = 50000
    timeout_seconds: int       = 30


@dataclass
class ThresholdRule:
    name            : str   = ""
    sensor_filter   : str   = "All"
    dimension       : str   = "All"
    operator        : str   = ">="
    value           : float = 0.0
    alert_level     : str   = "Warning"
    cooldown_minutes: int   = 30
    enabled         : bool  = True
    _last_alerted   : float = field(default=0.0, compare=False, repr=False)


@dataclass
class EmailSettings:
    smtp_server      : str  = "smtp-mail.outlook.com"
    smtp_port        : int  = 587
    username         : str  = ""
    password         : str  = ""
    from_addr        : str  = ""
    to_addrs         : str  = ""
    use_tls          : bool = True
    use_win32        : bool = False  # True → send via Outlook COM (win32com)
    win32_from_account: str = ""    # Outlook account/email to send from (blank = default)
    logo_path        : str  = ""
    subject_template: str  = "[{alert_level}] Structural Alert – {project} – {timestamp}"
    body_template   : str  = (
        "STRUCTURAL MONITORING ALERT\n"
        "============================\n\n"
        "Project  : {project}\n"
        "Time     : {timestamp}\n"
        "Cycle    : {cycle}\n\n"
        "Threshold violations detected:\n\n"
        "{violations_table}\n\n"
        "Highest level : {max_alert_level}\n"
        "Violations    : {num_violations}\n\n"
        "---\nInfluxDB Alert Monitor v{version}"
    )


@dataclass
class SmsSettings:
    enabled      : bool = False
    account_sid  : str  = ""
    auth_token   : str  = ""
    from_number  : str  = ""
    to_numbers   : str  = ""          # comma-separated
    body_template: str  = (
        "[{alert_level}] {project} alert at {timestamp}. "
        "{num_violations} violation(s). "
        "Max rule: {top_rule} sensor={top_sensor} val={top_value:.4f}"
    )


@dataclass
class MonitorSettings:
    interval_seconds   : int       = 60
    selected_sensors   : List[str] = field(default_factory=list)
    selected_dimensions: List[str] = field(default_factory=lambda: ["velx","vely","velz"])


@dataclass
class AppConfig:
    influx    : InfluxSettings  = field(default_factory=InfluxSettings)
    email     : EmailSettings   = field(default_factory=EmailSettings)
    sms       : SmsSettings     = field(default_factory=SmsSettings)
    monitor   : MonitorSettings = field(default_factory=MonitorSettings)
    thresholds: List[ThresholdRule] = field(default_factory=list)

    def save(self, path: Path = CONFIG_FILE) -> None:
        data = {
            "influx"    : asdict(self.influx),
            "email"     : asdict(self.email),
            "sms"       : asdict(self.sms),
            "monitor"   : asdict(self.monitor),
            "thresholds": [
                {k: v for k, v in asdict(t).items() if not k.startswith("_")}
                for t in self.thresholds
            ],
        }
        path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

    @classmethod
    def load(cls, path: Path = CONFIG_FILE) -> "AppConfig":
        if not path.exists():
            return cls()
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            cfg = cls()
            for key, target in [("influx",cfg.influx),("email",cfg.email),
                                  ("sms",cfg.sms),("monitor",cfg.monitor)]:
                if key in raw:
                    for k, v in raw[key].items():
                        if hasattr(target, k):
                            setattr(target, k, v)
            if "thresholds" in raw:
                cfg.thresholds = []
                for t in raw["thresholds"]:
                    try:
                        cfg.thresholds.append(
                            ThresholdRule(**{k: v for k, v in t.items() if not k.startswith("_")})
                        )
                    except TypeError:
                        pass
            return cfg
        except Exception:
            return cls()


# ══════════════════════════════════════════════════════════════════════════════
# InfluxDB helpers
# ══════════════════════════════════════════════════════════════════════════════

def build_flux_query(bucket, project, fields, range_window,
                     aggregate_every, use_aggregation, max_raw_points) -> str:
    f_filter  = " or ".join(f'r["_field"] == "{f}"' for f in fields)
    keep_cols = ", ".join(f'"{f}"' for f in fields)
    
    # Apply aggregateWindow BEFORE pivot (while _value column exists)
    agg = ""
    if use_aggregation:
        agg = (f'  |> aggregateWindow(every: {aggregate_every}, fn: max, createEmpty: false)\n')
               
    raw = (f'  |> sort(columns: ["_time"], desc: false)\n'
       f'  |> limit(n: {max_raw_points})\n') if not use_aggregation else ""
            
    return (f'from(bucket: "{bucket}")\n'
        f'  |> range(start: -{range_window})\n'
        f'  |> filter(fn: (r) => r["_measurement"] == "{project}")\n'
        f'  |> filter(fn: (r) => {f_filter})\n'
        f'{agg}'
        f'  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")\n'
        f'  |> keep(columns: ["_time", "device_name", {keep_cols}])\n'
        f'{raw}')



def fetch_frame(query_api, query: str) -> pd.DataFrame:
    try:
        frame = query_api.query_data_frame(query)
    except Exception:
        frame = query_api.query_data_frame(query)
    if isinstance(frame, list):
        frame = pd.concat(frame, ignore_index=True) if frame else pd.DataFrame()
    if frame.empty:
        return frame
    frame = frame.drop(columns=[c for c in frame.columns if c.startswith("result") or c.startswith("table")], errors="ignore")
    frame["_time"] = pd.to_datetime(frame["_time"], errors="coerce", utc=True).dt.tz_convert(LOCAL_TZ)
    return frame.dropna(subset=["_time"]).sort_values(["device_name","_time"]).reset_index(drop=True)


def fetch_sensors_influx(query_api, bucket, project) -> List[str]:
    sensors = []
    for tag in ("device_name","device","sensor"):
        q = (f'import "influxdata/influxdb/schema"\n'
             f'schema.tagValues(bucket: "{bucket}", tag: "{tag}", '
             f'predicate: (r) => r._measurement == "{project}")')
        try:
            result = query_api.query(q)
            for tbl in result:
                for rec in tbl.records:
                    v = rec.values.get("_value")
                    if v:
                        sensors.append(v)
        except Exception:
            pass
    return sorted(set(sensors))


# ══════════════════════════════════════════════════════════════════════════════
# XML record helpers
# ══════════════════════════════════════════════════════════════════════════════

def xml_path(project: str) -> Path:
    XML_DIR.mkdir(exist_ok=True)
    return XML_DIR / f"alerts_{project or 'default'}.xml"


def append_xml_alerts(project: str, cycle: int, violations: List[dict]) -> Path:
    path = xml_path(project)
    if path.exists():
        try:
            tree = ET.parse(str(path))
            root = tree.getroot()
        except ET.ParseError:
            root = ET.Element("alerts", project=project)
    else:
        root = ET.Element("alerts", project=project)

    ts = datetime.now(LOCAL_TZ).isoformat()
    ev = ET.SubElement(root, "event", timestamp=ts, cycle=str(cycle))
    for v in violations:
        ET.SubElement(ev, "violation",
                      rule        = str(v.get("rule","")),
                      sensor      = str(v.get("sensor","")),
                      dimension   = str(v.get("dimension","")),
                      max_value   = f"{v.get('max_value',0):.6f}",
                      threshold   = f"{v.get('threshold',0):.6f}",
                      operator    = str(v.get("operator",">=")),
                      alert_level = str(v.get("alert_level","Warning")))
    tree_out = ET.ElementTree(root)
    ET.indent(tree_out, space="  ")
    tree_out.write(str(path), encoding="utf-8", xml_declaration=True)
    return path


def load_xml_alerts(path: str) -> List[dict]:
    """Return flat list of violation dicts from an XML file."""
    records = []
    try:
        tree = ET.parse(path)
        root = tree.getroot()
        for ev in root.findall("event"):
            ts    = ev.attrib.get("timestamp","")
            cycle = ev.attrib.get("cycle","")
            for v in ev.findall("violation"):
                records.append({
                    "timestamp"  : ts,
                    "cycle"      : cycle,
                    "rule"       : v.attrib.get("rule",""),
                    "sensor"     : v.attrib.get("sensor",""),
                    "dimension"  : v.attrib.get("dimension",""),
                    "max_value"  : float(v.attrib.get("max_value",0)),
                    "threshold"  : float(v.attrib.get("threshold",0)),
                    "operator"   : v.attrib.get("operator",">="),
                    "alert_level": v.attrib.get("alert_level","Warning"),
                })
    except Exception:
        pass
    return records


# ══════════════════════════════════════════════════════════════════════════════
# Email helpers
# ══════════════════════════════════════════════════════════════════════════════

def _violations_table(violations: List[dict]) -> str:
    """Plain-text table (kept for SMS / win32 plain-text body)."""
    pending = [v for v in violations if v.get("violation_type") == "pending"]
    new = [v for v in violations if v.get("violation_type") != "pending"]
    
    cw = [20,20,10,12,6,12,10]
    hdr = (f"{'Rule':<{cw[0]}}  {'Sensor':<{cw[1]}}  {'Dim':<{cw[2]}}"
           f"  {'Max Value':>{cw[3]}}  {'Op':^{cw[4]}}  {'Threshold':>{cw[5]}}  Level")
    sep  = "─"*(sum(cw)+14)
    
    rows = []
    
    # Show new violations first
    if new:
        rows.append("NEW VIOLATIONS:\n" + hdr)
        rows.append(sep)
        for v in new:
            rows.append(f"{v['rule']:<{cw[0]}}  {v['sensor']:<{cw[1]}}  {v['dimension']:<{cw[2]}}"
                        f"  {v['max_value']:>{cw[3]}.5f}  {v['operator']:^{cw[4]}}"
                        f"  {v['threshold']:>{cw[5]}.5f}  {v['alert_level']}")
    
    # Then show pending violations
    if pending:
        if new:
            rows.append("")
        rows.append("PENDING VIOLATIONS (from cooldown period):\n" + hdr)
        rows.append(sep)
        for v in pending:
            rows.append(f"{v['rule']:<{cw[0]}}  {v['sensor']:<{cw[1]}}  {v['dimension']:<{cw[2]}}"
                        f"  {v['max_value']:>{cw[3]}.5f}  {v['operator']:^{cw[4]}}"
                        f"  {v['threshold']:>{cw[5]}.5f}  {v['alert_level']}")
    
    return "\n".join(rows)


# ── Chart generation ──────────────────────────────────────────────────────────

def _build_violation_chart_png(violations: List[dict]) -> Optional[bytes]:
    """
    Render a bar chart of max values vs thresholds for every violation.
    Returns raw PNG bytes, or None if matplotlib is unavailable.
    """
    if not violations:
        return None
    if not MPL_OK:
        print("✗ matplotlib not available for chart generation")
        return None
    try:
        import matplotlib.pyplot as plt
        from matplotlib.lines import Line2D
        from matplotlib.patches import Patch

        labels   = [f"{v['sensor']}\n{v['dimension']}" for v in violations]
        max_vals = [v["max_value"]   for v in violations]
        thresh   = [v["threshold"]   for v in violations]
        levels   = [v["alert_level"] for v in violations]
        n        = len(violations)

        fig, ax = plt.subplots(figsize=(max(10, n * 1.8), 6.0), dpi=150)
        fig.patch.set_facecolor("#ffffff")  # White background
        ax.set_facecolor("#f9f9f9")         # Light gray plot area

        bar_colors = ["#ff6b7a" if lv == "Critical" else "#ffaa44" for lv in levels]
        xs = range(n)

        bars = ax.bar(xs, max_vals, color=bar_colors, width=0.55,
                      zorder=3, edgecolor="#ffffff", linewidth=0.8)

        # Threshold markers
        for i, t in enumerate(thresh):
            ax.plot([i - 0.4, i + 0.4], [t, t],
                    color="#0066cc", linewidth=2.0, zorder=4,
                    solid_capstyle="round")

        # Value labels on bars
        for bar, mv in zip(bars, max_vals):
            # Use absolute max for scaling offset safely
            global_max_abs = max([abs(m) for m in max_vals]) if max_vals else 1.0
            offset = global_max_abs * 0.02
            
            # Position labels above for positive bars, properly below for negative bars
            y_pos = bar.get_height() + offset if mv >= 0 else bar.get_height() - offset
            va_align = "bottom" if mv >= 0 else "top"
            
            ax.text(bar.get_x() + bar.get_width() / 2,
                    y_pos,
                    f"{mv:.4f}",
                    ha="center", va=va_align,
                    color="#1a1a1a", fontsize=9.5, fontweight="bold")

        # Legend proxy
        legend_elements = [
            Patch(facecolor="#ff6b7a", label="Critical violation"),
            Patch(facecolor="#ffaa44", label="Warning violation"),
            Line2D([0], [0], color="#0066cc", linewidth=2, label="Threshold"),
        ]
        ax.legend(handles=legend_elements, loc="upper right",
                  fontsize=8, facecolor="#ffffff",
                  labelcolor="#1a1a1a", edgecolor="#dddddd")

        ax.set_xticks(list(xs))
        ax.set_xticklabels(labels, fontsize=10, color="#333333")
        ax.set_ylabel("Max Absolute Value", color="#333333", fontsize=11)
        ax.set_title("Threshold Violations — Values vs Limits",
                     color="#1a1a1a", fontsize=14, fontweight="bold", pad=15)
        ax.tick_params(axis="y", colors="#333333", labelsize=10)
        ax.tick_params(axis="x", colors="#333333", labelsize=10)
        for spine in ax.spines.values():
            spine.set_edgecolor("#dddddd")
        ax.yaxis.grid(True, color="#e8e8e8", linewidth=0.6, zorder=0)
        ax.set_axisbelow(True)

        fig.tight_layout(pad=1.2)

        import io
        buf = io.BytesIO()
        fig.savefig(buf, format="png", bbox_inches="tight",
                    facecolor=fig.get_facecolor())
        plt.close(fig)
        buf.seek(0)
        data = buf.read()
        print(f"✓ Chart generated: {len(data)} bytes")
        return data
    except Exception as e:
        print(f"✗ Error generating chart: {e}")
        import traceback
        traceback.print_exc()
        return None


# ── HTML email body ───────────────────────────────────────────────────────────

def _build_html_email(project: str, timestamp: str, cycle: int,
                      violations: List[dict], version: str,
                      max_level: str, logo_path: Optional[str] = None,
                      use_file_urls: bool = False, chart_path: Optional[str] = None) -> str:
    """Return a beautifully styled HTML email body with modern design.
    
    Args:
        use_file_urls: Deprecated - kept for compatibility but not used.
                       Both SMTP and Outlook COM now use cid: references.
    """
    print(f"🔧 _build_html_email called:")
    print(f"   logo_path={logo_path}, use_file_urls={use_file_urls}, chart_path={chart_path}")

    level_color  = "#ff6b7a" if max_level == "Critical" else "#ffaa44"
    level_bg     = "#ff6b7a" if max_level == "Critical" else "#ffaa44"
    level_light  = "#3a1a1e" if max_level == "Critical" else "#2e2010"
    level_icon   = "🚨" if max_level == "Critical" else "⚠️"
    level_icon2  = "●" if max_level == "Critical" else "▲"

    # Logo block - ALWAYS use cid: references for embedded images
    # (Both SMTP MIME and Outlook COM support cid:)
    logo_html = ""
    logo_file_path = None
    if logo_path:
        # Resolve logo path
        lp = Path(logo_path)
        if not lp.is_absolute():
            lp = Path.cwd() / lp
            if not lp.is_file():
                lp = Path(__file__).parent / logo_path
        if lp.is_file():
            logo_file_path = str(lp.resolve())
            # Use cid: reference for compatibility with both SMTP and Outlook COM
            img_src = "cid:logo"
            print(f"   ✓ Logo URL (cid mode): {img_src}")
            logo_html = f'<img src="{img_src}" style="max-height:60px;margin-bottom:0;" alt="Logo">'
        else:
            print(f"   ✗ Logo file not found: {lp}")

    # Separate violations
    pending_viols = [v for v in violations if v.get("violation_type") == "pending"]
    new_viols = [v for v in violations if v.get("violation_type") != "pending"]
    
    num_crit = sum(1 for v in violations if v["alert_level"] == "Critical")
    num_warn = len(violations) - num_crit
    
    # Prepare chart image reference - ALWAYS use cid: for compatibility
    chart_img_src = "cid:alert_chart"
    print(f"   ✓ Chart URL (cid mode): {chart_img_src}")
    
    chart_html = ""
    if chart_path or not use_file_urls:
        # Show chart section if path is available (Outlook) or we're using MIME (SMTP)
        chart_html = f"""
      <!-- ── Chart ── -->
      <tr>
        <td style="padding:24px 32px;">
          <p style="color:#333333;font-size:12px;text-transform:uppercase;letter-spacing:1px;
                    margin:0 0 12px 0;font-weight:700;">📈 Violation Visualization</p>
          <div style="background:#f9f9f9;border:1px solid #dddddd;border-radius:10px;overflow:hidden;text-align:center;padding:20px;">
            <img src="{chart_img_src}"
                 style="width:100%;max-width:600px;display:block;margin:0 auto;border-radius:8px;"
                 alt="Threshold violation chart">
          </div>
        </td>
      </tr>"""
    
    # Build violation rows with better styling
    rows_html = ""
    
    # New violations section
    if new_viols:
        rows_html += """
        <tr style="background:#f5f5f5;border-bottom:2px solid #4ddd88;">
          <td colspan="7" style="padding:12px 16px;color:#4ddd88;font-size:13px;font-weight:700;
                                 text-transform:uppercase;letter-spacing:1px;">
            ✓ NEW VIOLATIONS ({}) 
          </td>
        </tr>""".format(len(new_viols))
        for v in new_viols:
            row_color = "#fff5f5" if v["alert_level"] == "Critical" else "#fffaf5"
            badge_col = "#ff6b7a" if v["alert_level"] == "Critical" else "#ffaa44"
            text_color = "#333333"
            exceed_pct = ""
            try:
                pct = ((v["max_value"] - v["threshold"]) / abs(v["threshold"])) * 100
                exceed_pct = f'<br><span style="color:#ff6b7a;font-size:10px;font-weight:bold;">↑ {pct:.1f}%</span>'
            except ZeroDivisionError:
                pass
            rows_html += f"""
        <tr style="background:{row_color};border-bottom:1px solid #dddddd;">
          <td style="padding:11px 14px;color:{text_color};font-size:12px;font-weight:600;">{v['rule']}</td>
          <td style="padding:11px 14px;color:{text_color};font-size:12px;">{v['sensor']}</td>
          <td style="padding:11px 14px;color:{text_color};font-size:12px;text-align:center;font-family:monospace;">{v['dimension'].upper()}</td>
          <td style="padding:11px 14px;color:#4ddd88;font-size:12px;text-align:right;font-weight:700;">
            {v['max_value']:.4f}{exceed_pct}
          </td>
          <td style="padding:11px 14px;color:{text_color};font-size:12px;text-align:center;font-weight:bold;">{v['operator']}</td>
          <td style="padding:11px 14px;color:#ff9900;font-size:12px;text-align:right;font-weight:600;">{v['threshold']:.4f}</td>
          <td style="padding:11px 14px;text-align:center;">
            <span style="background:{badge_col};color:#fff;font-size:10px;font-weight:bold;
                         padding:4px 10px;border-radius:12px;text-transform:uppercase;">{v['alert_level']}</span>
          </td>
        </tr>"""
    
    # Pending violations section
    if pending_viols:
        rows_html += """
        <tr style="background:#f5f5f5;border-bottom:2px solid #b48eff;">
          <td colspan="7" style="padding:12px 16px;color:#b48eff;font-size:13px;font-weight:700;
                                 text-transform:uppercase;letter-spacing:1px;">
            ⏱ PENDING ({}) - Detected during cooldown
          </td>
        </tr>""".format(len(pending_viols))
        for v in pending_viols:
            row_color = "#fff5f5" if v["alert_level"] == "Critical" else "#fffaf5"
            badge_col = "#ff6b7a" if v["alert_level"] == "Critical" else "#ffaa44"
            text_color = "#333333"
            exceed_pct = ""
            try:
                pct = ((v["max_value"] - v["threshold"]) / abs(v["threshold"])) * 100
                exceed_pct = f'<br><span style="color:#ff6b7a;font-size:10px;font-weight:bold;">↑ {pct:.1f}%</span>'
            except ZeroDivisionError:
                pass
            rows_html += f"""
        <tr style="background:{row_color};border-bottom:1px solid #dddddd;opacity:0.9;">
          <td style="padding:11px 14px;color:{text_color};font-size:12px;font-weight:600;">{v['rule']}</td>
          <td style="padding:11px 14px;color:{text_color};font-size:12px;">{v['sensor']}</td>
          <td style="padding:11px 14px;color:{text_color};font-size:12px;text-align:center;font-family:monospace;">{v['dimension'].upper()}</td>
          <td style="padding:11px 14px;color:#4ddd88;font-size:12px;text-align:right;font-weight:700;">
            {v['max_value']:.4f}{exceed_pct}
          </td>
          <td style="padding:11px 14px;color:{text_color};font-size:12px;text-align:center;font-weight:bold;">{v['operator']}</td>
          <td style="padding:11px 14px;color:#ff9900;font-size:12px;text-align:right;font-weight:600;">{v['threshold']:.4f}</td>
          <td style="padding:11px 14px;text-align:center;">
            <span style="background:{badge_col};color:#fff;font-size:10px;font-weight:bold;
                         padding:4px 10px;border-radius:12px;text-transform:uppercase;">{v['alert_level']}</span>
          </td>
        </tr>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1.0">
  <title>🚨 Structural Monitoring Alert</title>
  <style>
    body {{ margin:0; padding:0; background:#ffffff; font-family:'Segoe UI','Roboto',sans-serif; }}
    a {{ color:#0066cc; text-decoration:none; }}
    a:hover {{ text-decoration:underline; }}
  </style>
</head>
<body>
<table width="100%" cellpadding="0" cellspacing="0" style="background:#ffffff;padding:32px 0;">
  <tr><td align="center">
    <table width="680" cellpadding="0" cellspacing="0" style="max-width:680px;background:#ffffff;
           border-radius:16px;overflow:hidden;border-left:4px solid {level_bg};box-shadow:0 2px 12px rgba(0,0,0,0.1);">

      <!-- ── Top Accent Bar ── -->
      <tr>
        <td style="background:linear-gradient(90deg, {level_bg}, {level_bg}cc);height:6px;padding:0;"></td>
      </tr>

      <!-- ── Header with Logo ── -->
      <tr>
        <td style="padding:28px 32px;background:#f9f9f9;border-bottom:1px solid #e8e8e8;">
          <table width="100%" cellpadding="0" cellspacing="0">
            <tr>
              <td width="60" valign="top">{logo_html}</td>
              <td style="padding-left:16px;">
                <div style="margin:0;padding:0;">
                  <span style="font-size:28px;font-weight:800;color:#1a1a1a;letter-spacing:-0.5px;display:block;margin-bottom:4px;">
                    {level_icon} STRUCTURAL ALERT
                  </span>
                  <span style="font-size:13px;color:#555555;display:block;line-height:1.6;">
                    {project} • {timestamp} • Cycle #{cycle}
                  </span>
                </div>
              </td>
              <td align="right" valign="top">
                <span style="font-size:11px;color:#666666;font-family:monospace;background:#eeeeee;
                           padding:8px 12px;border-radius:6px;display:inline-block;">-</span>
              </td>
            </tr>
          </table>
        </td>
      </tr>

      <!-- ── Alert Summary Cards ── -->
      <tr>
        <td style="padding:24px 32px;">
          <table width="100%" cellpadding="0" cellspacing="0">
            <tr>
              <td width="32%" style="padding-right:16px;">
                <div style="background:#f5f5f5;border:2px solid {level_bg};border-radius:12px;padding:16px;text-align:center;">
                  <div style="font-size:32px;font-weight:800;color:{level_color};">{len(violations)}</div>
                  <div style="font-size:11px;color:#555555;text-transform:uppercase;letter-spacing:0.8px;margin-top:6px;font-weight:600;">Total Violations</div>
                </div>
              </td>
              <td width="32%" style="padding:0 8px;">
                <div style="background:#fff5f5;border:2px solid #ff6b7a;border-radius:12px;padding:16px;text-align:center;">
                  <div style="font-size:32px;font-weight:800;color:#ff6b7a;">{num_crit}</div>
                  <div style="font-size:11px;color:#555555;text-transform:uppercase;letter-spacing:0.8px;margin-top:6px;font-weight:600;">Critical</div>
                </div>
              </td>
              <td width="32%" style="padding-left:16px;">
                <div style="background:#fffaf5;border:2px solid #ffaa44;border-radius:12px;padding:16px;text-align:center;">
                  <div style="font-size:32px;font-weight:800;color:#ffaa44;">{num_warn}</div>
                  <div style="font-size:11px;color:#555555;text-transform:uppercase;letter-spacing:0.8px;margin-top:6px;font-weight:600;">Warnings</div>
                </div>
              </td>
            </tr>
          </table>
        </td>
      </tr>

      <!-- ── Alert Level Banner ── -->
      <tr>
        <td style="padding:16px 32px;">
          <div style="background:linear-gradient(90deg, {level_bg}, {level_bg}dd);border-radius:10px;padding:16px;
                      border-left:4px solid {level_color};color:#fff;">
            <span style="font-size:15px;font-weight:700;">{level_icon2} {max_level.upper()} LEVEL ALERT</span>
            <br>
            <span style="font-size:12px;margin-top:6px;display:block;opacity:0.95;">
              Threshold violations detected at {timestamp}
            </span>
          </div>
        </td>
      </tr>

      <!-- ── Violations Table ── -->
      <tr>
        <td style="padding:24px 32px;">
          <p style="color:#333333;font-size:12px;text-transform:uppercase;letter-spacing:1px;
                    margin:0 0 12px 0;font-weight:700;">📊 Detailed Violation Report</p>
          <table width="100%" cellpadding="0" cellspacing="0"
                 style="border-collapse:collapse;border-radius:10px;overflow:hidden;
                        border:1px solid #dddddd;background:#ffffff;">
            <thead>
              <tr style="background:#f5f5f5;">
                <th style="padding:14px 16px;color:#333333;font-size:11px;font-weight:700;text-align:left;
                           text-transform:uppercase;letter-spacing:0.8px;border-bottom:2px solid #dddddd;">Rule</th>
                <th style="padding:14px 16px;color:#333333;font-size:11px;font-weight:700;text-align:left;
                           text-transform:uppercase;letter-spacing:0.8px;border-bottom:2px solid #dddddd;">Sensor</th>
                <th style="padding:14px 16px;color:#333333;font-size:11px;font-weight:700;text-align:center;
                           text-transform:uppercase;letter-spacing:0.8px;border-bottom:2px solid #dddddd;">Dimension</th>
                <th style="padding:14px 16px;color:#333333;font-size:11px;font-weight:700;text-align:right;
                           text-transform:uppercase;letter-spacing:0.8px;border-bottom:2px solid #dddddd;">Max Value</th>
                <th style="padding:14px 16px;color:#333333;font-size:11px;font-weight:700;text-align:center;
                           text-transform:uppercase;letter-spacing:0.8px;border-bottom:2px solid #dddddd;">Operator</th>
                <th style="padding:14px 16px;color:#333333;font-size:11px;font-weight:700;text-align:right;
                           text-transform:uppercase;letter-spacing:0.8px;border-bottom:2px solid #dddddd;">Threshold</th>
                <th style="padding:14px 16px;color:#333333;font-size:11px;font-weight:700;text-align:center;
                           text-transform:uppercase;letter-spacing:0.8px;border-bottom:2px solid #dddddd;">Severity</th>
              </tr>
            </thead>
            <tbody>
              {rows_html}
            </tbody>
          </table>
        </td>
      </tr>

      {chart_html}

      <!-- ── Quick Action & Info ── -->
      <tr>
        <td style="padding:24px 32px;background:#f9f9f9;border-top:1px solid #dddddd;">
          <table width="100%" cellpadding="0" cellspacing="0">
            <tr>
              <td>
                <div style="background:#fff5f5;padding:16px;border-radius:8px;border-left:4px solid #ff6b7a;">
                  <span style="color:#d84444;font-size:13px;font-weight:700;display:block;margin-bottom:6px;">⚡ Recommended Action</span>
                  <span style="color:#555555;font-size:12px;">.</span>
                </div>
              </td>
            </tr>
          </table>
        </td>
      </tr>

      <!-- ── Footer ── -->
      <tr>
        <td style="padding:20px 32px;background:#eeeeee;border-top:1px solid #dddddd;text-align:center;">
          <p style="margin:0;color:#666666;font-size:11px;line-height:1.6;">
            <strong>Structural Monitoring System</strong> • Alert Monitor<br>
            <span style="color:#999999;">Automated alert at {timestamp} • Do not reply to this message</span>
          </p>
        </td>
      </tr>

    </table>
  </td></tr>
</table>
</body>
</html>"""
    return html


def compose_email(cfg: EmailSettings, influx: InfluxSettings,
                  violations: List[dict], cycle: int) -> Tuple[str, str]:
    ts        = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
    max_level = "Critical" if any(v["alert_level"]=="Critical" for v in violations) else "Warning"
    top = max(violations, key=lambda v: v["max_value"]) if violations else {}
    ctx = dict(project=influx.project, timestamp=ts, cycle=cycle,
               violations_table=_violations_table(violations),
               max_alert_level=max_level, alert_level=max_level,
               num_violations=len(violations), version=APP_VERSION,
               top_rule=top.get("rule",""), top_sensor=top.get("sensor",""),
               top_value=top.get("max_value",0.0))
    return cfg.subject_template.format(**ctx), cfg.body_template.format(**ctx)


def _send_email_smtp(cfg: EmailSettings, subject: str, body: str,
                     logo_path: Optional[str] = None,
                     violations: Optional[List[dict]] = None,
                     influx_project: str = "",
                     timestamp: str = "",
                     cycle: int = 0,
                     max_level: str = "Warning",
                     version: str = APP_VERSION) -> None:
    """Send alert email via SMTP with a rich HTML body and embedded chart."""
    msg           = MIMEMultipart("related")
    msg["From"]   = cfg.from_addr
    msg["To"]     = cfg.to_addrs
    msg["Subject"]= subject
    recipients    = [a.strip() for a in cfg.to_addrs.split(",") if a.strip()]

    alt = MIMEMultipart("alternative")
    msg.attach(alt)

    # ── Build HTML body ───────────────────────────────────────────────────────
    html_body = _build_html_email(
        project    = influx_project,
        timestamp  = timestamp or datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S %Z"),
        cycle      = cycle,
        violations = violations or [],
        version    = version,
        max_level  = max_level,
        logo_path  = logo_path,
    )

    alt.attach(MIMEText(body,      "plain", "utf-8"))
    alt.attach(MIMEText(html_body, "html",  "utf-8"))

    # ── Embed logo ────────────────────────────────────────────────────────────
    if logo_path:
        # Resolve logo path - try relative first, then absolute
        lp = Path(logo_path)
        if not lp.is_absolute():
            lp = Path.cwd() / lp
            if not lp.is_file():
                lp = Path(__file__).parent / logo_path
        if lp.is_file():
            try:
                with open(str(lp), "rb") as f:
                    logo_data = f.read()
                # Determine logo type from extension
                ext = lp.suffix.lower().lstrip(".")
                if ext not in ("png", "jpg", "jpeg", "gif"):
                    ext = "png"
                logo_img = MIMEImage(logo_data, _subtype=ext)
                logo_img.add_header("Content-ID", "<logo>")
                logo_img.add_header("Content-Disposition", "inline", filename=lp.name)
                msg.attach(logo_img)
                print(f"✓ Logo attached: {lp.name}")
            except Exception as e:
                print(f"✗ Failed to attach logo: {e}")
        else:
            print(f"✗ Logo not found: {lp}")

    # ── Embed chart ───────────────────────────────────────────────────────────
    chart_png = _build_violation_chart_png(violations or [])
    if chart_png:
        try:
            chart_img = MIMEImage(chart_png, _subtype="png")
            chart_img.add_header("Content-ID", "<alert_chart>")
            chart_img.add_header("Content-Disposition", "inline", filename="alert_chart.png")
            msg.attach(chart_img)
            print(f"✓ Chart attached: {len(chart_png)} bytes")
        except Exception as e:
            print(f"✗ Failed to attach chart: {e}")
    else:
        print("✗ Chart not generated")

    with smtplib.SMTP(cfg.smtp_server, cfg.smtp_port, timeout=30) as srv:
        srv.ehlo()
        if cfg.use_tls:
            srv.starttls(); srv.ehlo()
        srv.login(cfg.username, cfg.password)
        srv.sendmail(cfg.from_addr, recipients, msg.as_string())


def _send_email_win32(cfg: EmailSettings, subject: str, body: str,
                     logo_path: Optional[str] = None,
                     violations: Optional[List[dict]] = None,
                     influx_project: str = "",
                     timestamp: str = "",
                     cycle: int = 0,
                     max_level: str = "Warning",
                     version: str = APP_VERSION) -> None:
    """Send via the signed-in Outlook desktop app using win32com.client COM.

    Requires: pip install pywin32
    No SMTP credentials needed — uses whichever Outlook account is configured.
    Set cfg.win32_from_account to a specific email address to choose the
    sending account when multiple accounts are configured in Outlook.
    """
    if not WIN32_OK:
        raise RuntimeError(
            "pywin32 not installed.  Run:  pip install pywin32\n"
            "Then restart the application."
        )
    import win32com.client as win32  # noqa: F401
    import os
    import tempfile
    try:
        ol = win32.Dispatch("Outlook.Application")
    except Exception:
        try:
            ol = win32.DispatchEx("Outlook.Application")
        except Exception as e:
            try:
                import time
                os.startfile("outlook")
                time.sleep(3)
                ol = win32.Dispatch("Outlook.Application")
            except Exception as e2:
                raise RuntimeError(
                    f"COM Dispatch failed: {e2}\n"
                    "This usually means Outlook is running with different privileges "
                    "(e.g., as Administrator) than Python. Please run both normally."
                )
    mail = ol.CreateItem(0)          # 0 = olMailItem
    mail.Subject = subject
    mail.Body    = body
    print(f"📧 Email created: Subject={subject}")

    # Generate and save chart first if violations provided
    chart_file_path = None
    temp_png = None
    if violations:
        print(f"📊 Generating chart for {len(violations)} violation(s)...")
        try:
            chart_png = _build_violation_chart_png(violations)
            if chart_png:
                # Save chart to temp file for Outlook and HTML reference
                temp_png = tempfile.NamedTemporaryFile(delete=False, suffix=".png")
                temp_png.write(chart_png)
                temp_png.close()
                chart_file_path = temp_png.name  
                print(f"✓ Chart file: {chart_file_path} ({len(chart_png)} bytes)")
            else:
                print(f"✗ Chart generation returned None")
        except Exception as e:
            print(f"✗ Chart generation error: {e}")
            import traceback
            traceback.print_exc()

    # Build HTML body with violations - now always uses cid: references
    if violations:
        print(f"🎨 Building HTML with violations...")
        html_body = _build_html_email(
            project=influx_project,
            timestamp=timestamp or datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S %Z"),
            cycle=cycle,
            violations=violations,
            version=version,
            max_level=max_level,
            logo_path=logo_path,
            use_file_urls=False,  # Always use cid: now
            chart_path=chart_file_path,
        )
        print(f"✓ HTML body created ({len(html_body)} chars)")
        print(f"  Contains 'cid:logo'? {'cid:logo' in html_body}")
        print(f"  Contains 'cid:alert_chart'? {'cid:alert_chart' in html_body}")
        mail.HTMLBody = html_body
        print(f"✓ HTMLBody set")

    # Select sending account when multiple Outlook accounts are configured
    from_acct = (cfg.win32_from_account or "").strip()
    if from_acct:
        accounts = ol.Session.Accounts
        for i in range(accounts.Count):
            acct = accounts.Item(i + 1)
            if acct.SmtpAddress.lower() == from_acct.lower():
                mail._oleobj_.Invoke(
                    0xF034,  # PR_SENTREPRESENTINGNAME
                    0, 8, True, acct
                )
                break

    # Attach logo with inline disposition
    if logo_path:
        # Resolve logo path - try relative first, then absolute
        lp = Path(logo_path)
        if not lp.is_absolute():
            lp = Path.cwd() / lp
            if not lp.is_file():
                lp = Path(__file__).parent / logo_path
        if lp.is_file():
            try:
                attachment = mail.Attachments.Add(str(lp.resolve()))
                # Try to set inline disposition for Outlook COM
                # PR_ATTACH_MIME_DISPOSITION = 0x37101F00
                try:
                    pa = attachment.PropertyAccessor
                    pa.SetProperty("http://schemas.microsoft.com/mapi/proptag/0x3712001F", "logo")
                    pa.SetProperty("http://schemas.microsoft.com/mapi/proptag/0x37101F00", 1)
                    print(f"✓ Logo attached (inline): {lp.name}")
                except:
                    print(f"✓ Logo attached: {lp.name} (inline property not set)")
            except Exception as e:
                print(f"✗ Failed to attach logo: {e}")
        else:
            print(f"✗ Logo file not found: {lp}")
    
    # Attach chart if it was generated with inline disposition
    if chart_file_path and Path(chart_file_path).is_file():
        try:
            attachment = mail.Attachments.Add(chart_file_path)
            # Try to set inline disposition
            try:
                pa = attachment.PropertyAccessor
                pa.SetProperty("http://schemas.microsoft.com/mapi/proptag/0x3712001F", "alert_chart")
                pa.SetProperty("http://schemas.microsoft.com/mapi/proptag/0x37101F00", 1)
                print(f"✓ Chart attached (inline): {chart_file_path}")
            except:
                print(f"✓ Chart attached: {chart_file_path} (inline property not set)")
        except Exception as e:
            print(f"✗ Failed to attach chart: {e}")
        
        # Schedule cleanup after email is sent
        def cleanup_temp():
            import time
            time.sleep(2)
            try:
                os.remove(chart_file_path)
                print(f"✓ Cleaned up temp chart")
            except:
                pass
        import threading
        threading.Timer(5.0, cleanup_temp).start()
    
    for addr in [a.strip() for a in cfg.to_addrs.split(",") if a.strip()]:
        mail.Recipients.Add(addr)
    mail.Recipients.ResolveAll()
    
    # Log before sending
    print(f"📨 Ready to send:")
    print(f"   Recipients: {[a.strip() for a in cfg.to_addrs.split(',') if a.strip()]}")
    print(f"   Attachments: {mail.Attachments.Count}")
    print(f"   HTMLBody length: {len(mail.HTMLBody) if mail.HTMLBody else 0}")
    print(f"   Sending email...")
    
    try:
        mail.Send()
        print(f"✓ Email sent successfully")
    except Exception as e:
        print(f"✗ Failed to send email: {e}")
        import traceback
        traceback.print_exc()


def send_email(cfg: EmailSettings, subject: str, body: str,
               logo_path: Optional[str] = None,
               violations: Optional[List[dict]] = None,
               influx_project: str = "",
               timestamp: str = "",
               cycle: int = 0,
               max_level: str = "Warning",
               version: str = APP_VERSION) -> None:
    """Dispatcher: routes to win32 Outlook COM or SMTP based on cfg.use_win32."""
    if cfg.use_win32:
        _send_email_win32(cfg, subject, body, logo_path,
                         violations=violations,
                         influx_project=influx_project,
                         timestamp=timestamp,
                         cycle=cycle,
                         max_level=max_level,
                         version=version)
    else:
        _send_email_smtp(cfg, subject, body, logo_path,
                         violations=violations,
                         influx_project=influx_project,
                         timestamp=timestamp,
                         cycle=cycle,
                         max_level=max_level,
                         version=version)


# ══════════════════════════════════════════════════════════════════════════════
# SMS helpers
# ══════════════════════════════════════════════════════════════════════════════

def send_sms(cfg: SmsSettings, influx: InfluxSettings,
             violations: List[dict], cycle: int) -> None:
    if not TWILIO_OK:
        raise RuntimeError("twilio package not installed. Run: pip install twilio")
    ts        = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
    max_level = "Critical" if any(v["alert_level"]=="Critical" for v in violations) else "Warning"
    top       = max(violations, key=lambda v: v["max_value"]) if violations else {}
    body = cfg.body_template.format(
        alert_level=max_level, project=influx.project, timestamp=ts,
        num_violations=len(violations),
        top_rule=top.get("rule",""), top_sensor=top.get("sensor",""),
        top_value=top.get("max_value",0.0),
    )
    client = TwilioClient(cfg.account_sid, cfg.auth_token)
    for num in [n.strip() for n in cfg.to_numbers.split(",") if n.strip()]:
        client.messages.create(body=body, from_=cfg.from_number, to=num)


# ══════════════════════════════════════════════════════════════════════════════
# Alert Engine
# ══════════════════════════════════════════════════════════════════════════════

class AlertEngine:
    def __init__(self, config: AppConfig, on_alert_fn, on_data_fn):
        self.config     = config
        self._on_alert  = on_alert_fn
        self._on_data   = on_data_fn
        self._stop      = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._cycle     = 0
        self._pending_violations: Dict[str, List[dict]] = {}  # rule_name -> list of violations during cooldown

    def start(self):
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name="AlertEngine")
        self._thread.start()

    def stop(self):
        self._stop.set()

    def _log(self, msg: str, tag: str = "info"):
        LOG_QUEUE.put((msg, tag))

    def _run(self):
        self._log("Monitor started.", "ok")
        while not self._stop.is_set():
            self._cycle += 1
            self._log(f"── Cycle {self._cycle}  [{datetime.now(LOCAL_TZ).strftime('%H:%M:%S')}] ──────────", "section")
            try:
                self._do_cycle()
            except Exception as exc:
                self._log(f"Cycle error: {exc}", "error")
            interval = self.config.monitor.interval_seconds
            self._log(f"Next check in {interval}s.")
            self._stop.wait(interval)
        self._log("Monitor stopped.", "ok")

    def _do_cycle(self):
        if not INFLUX_OK:
            self._log("influxdb-client not available.", "error"); return
        cfg = self.config; inf = cfg.influx
        self._log(f"Querying {inf.url}  bucket={inf.bucket}  project={inf.project}")
        client = InfluxDBClient(url=inf.url, token=inf.token, org=inf.org,
                                timeout=inf.timeout_seconds*1000)
        try:
            query_api = client.query_api()
            frame = fetch_frame(query_api, build_flux_query(
                inf.bucket, inf.project, inf.fields,
                inf.range_window, inf.aggregate_every,
                inf.use_aggregation, inf.max_raw_points,
            ))
        finally:
            client.close()

        if frame.empty:
            self._log("No data returned.", "warning"); return

        dims = [c for c in frame.columns if c not in {"_time","device_name"}]
        self._log(f"Fetched {len(frame)} rows from {frame['device_name'].nunique()} sensor(s).")

        # ── Build per-sensor/dim max values ──────────────────────────────────
        max_vals: Dict[str,Dict[str,float]] = {}
        for sensor in frame["device_name"].unique():
            sub = frame[frame["device_name"]==sensor]
            max_vals[sensor] = {}
            for dim in dims:
                if dim in sub.columns:
                    s_dim = sub[dim].dropna()
                    if not s_dim.empty:
                        idx = s_dim.abs().idxmax()
                        val = float(s_dim.loc[idx])
                        max_vals[sensor][dim] = val

        cycle_ts = datetime.now(LOCAL_TZ).isoformat()
        self._on_data({"ts": cycle_ts, "max_vals": max_vals, "cycle": self._cycle})

        # ── Check thresholds ─────────────────────────────────────────────────
        violations = []; now = time.time()
        for rule in cfg.thresholds:
            if not rule.enabled: continue
            
            sensors = list(frame["device_name"].unique())
            if rule.sensor_filter != "All":
                sensors = [s for s in sensors if s == rule.sensor_filter]
            check_dims = dims if rule.dimension=="All" else [d for d in dims if d==rule.dimension]
            
            rule_violations = []
            for sensor in sensors:
                for dim in check_dims:
                    mv = max_vals.get(sensor,{}).get(dim)
                    if mv is None: continue
                    mv_abs = abs(mv)
                    hit = ((rule.operator==">=" and mv_abs>=rule.value) or
                           (rule.operator==">"  and mv_abs>rule.value)  or
                           (rule.operator=="==" and abs(mv_abs-rule.value)<1e-9))
                    if hit:
                        self._log(f"  [{rule.alert_level.upper()}] {sensor}/{dim}  "
                                  f"max={mv:.5f} {rule.operator} {rule.value:.5f}",
                                  "warning" if rule.alert_level=="Warning" else "error")
                        rule_violations.append({"rule":rule.name,"sensor":sensor,"dimension":dim,
                                           "max_value":mv,"threshold":rule.value,
                                           "operator":rule.operator,"alert_level":rule.alert_level,
                                           "violation_type":"new"})
            
            # Check if we're in cooldown period
            in_cooldown = now - rule._last_alerted < rule.cooldown_minutes*60
            
            if rule_violations:
                if in_cooldown:
                    # In cooldown: store as pending (will send on cooldown expiry)
                    rem = int((rule.cooldown_minutes*60-(now-rule._last_alerted))/60)
                    self._log(f"  [PENDING] '{rule.name}' – cooldown {rem}min left. "
                              f"{len(rule_violations)} violation(s) stored.", "warning")
                    if rule.name not in self._pending_violations:
                        self._pending_violations[rule.name] = []
                    # Mark current violations as pending
                    for v in rule_violations:
                        v["violation_type"] = "pending"
                        # Only keep the worst pending violation per sensor/dimension
                        found = False
                        for p in self._pending_violations[rule.name]:
                            if p["sensor"] == v["sensor"] and p["dimension"] == v["dimension"]:
                                found = True
                                if abs(v["max_value"]) > abs(p["max_value"]):
                                    p["max_value"] = v["max_value"]
                                break
                        if not found:
                            self._pending_violations[rule.name].append(v)
                else:
                    # Not in cooldown: send immediately
                    # Include both pending violations (accumulated during last cooldown) and current ones
                    if rule.name in self._pending_violations:
                        # Mark pending violations with "pending" type
                        pending_viols = self._pending_violations[rule.name]
                        num_pending = len(pending_viols)
                        all_violations = pending_viols + rule_violations
                        self._log(f"  [ALERT] '{rule.name}' cooldown expired. "
                                  f"{num_pending} pending + {len(rule_violations)} new = {len(all_violations)} total violations.",
                                  "warning")
                        del self._pending_violations[rule.name]
                    else:
                        all_violations = rule_violations
                        self._log(f"  [ALERT] '{rule.name}' triggered. {len(rule_violations)} violation(s).", "warning")
                    violations.extend(all_violations)
                    rule._last_alerted = now
            else:
                # No violations for this rule in current cycle
                # But check if cooldown just expired - send pending violations
                if not in_cooldown and rule.name in self._pending_violations:
                    pending_viols = self._pending_violations[rule.name]
                    num_pending = len(pending_viols)
                    self._log(f"  [ALERT] '{rule.name}' cooldown expired. "
                              f"{num_pending} pending violations detected during cooldown. Sending email...",
                              "warning")
                    violations.extend(pending_viols)
                    del self._pending_violations[rule.name]
                    rule._last_alerted = now
        
        if self._cycle == 1:
            self._log("Cycle 1 complete (baseline sync). Alerts suppressed for old data.", "ok")
            if violations:
                for rule in cfg.thresholds:
                    if rule.name in {v["rule"] for v in violations}:
                        rule._last_alerted = now
                self._pending_violations.clear()
        elif violations:
            self._on_alert(violations, self._cycle)
        else:
            self._log("All sensors within thresholds.", "ok")


# ══════════════════════════════════════════════════════════════════════════════
# Scrollable frame helper
# ══════════════════════════════════════════════════════════════════════════════

def make_scrollable(parent) -> tk.Frame:
    """Return an inner Frame inside a canvas+scrollbar pair packed into parent."""
    canvas = tk.Canvas(parent, bg=COLORS["bg"], highlightthickness=0)
    vsb    = ttk.Scrollbar(parent, orient="vertical", command=canvas.yview)
    canvas.configure(yscrollcommand=vsb.set)
    vsb.pack(side="right", fill="y")
    canvas.pack(side="left", fill="both", expand=True)
    inner  = tk.Frame(canvas, bg=COLORS["bg"])
    win_id = canvas.create_window((0,0), window=inner, anchor="nw")

    def _on_configure(e):
        canvas.configure(scrollregion=canvas.bbox("all"))
        canvas.itemconfig(win_id, width=canvas.winfo_width())

    inner.bind("<Configure>", _on_configure)
    canvas.bind("<Configure>", lambda e: canvas.itemconfig(win_id, width=e.width))

    def _mousewheel(e):
        canvas.yview_scroll(int(-1*(e.delta/120)), "units")

    canvas.bind_all("<MouseWheel>", _mousewheel)
    return inner


# ══════════════════════════════════════════════════════════════════════════════
# Main Application
# ══════════════════════════════════════════════════════════════════════════════

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(f"InfluxDB Alert Monitor  v{APP_VERSION}")
        self.geometry("1100x780")
        self.minsize(900, 640)
        self.protocol("WM_DELETE_WINDOW", self._on_close)

        self.cfg           = AppConfig.load()
        self._engine       : Optional[AlertEngine] = None
        self._running      = False
        self._influx_client: Optional[object] = None

        # Graph state  { sensor -> { dim -> list[float] } }
        self._graph_history      : Dict[str, Dict[str,List[float]]] = {}
        self._graph_ts           : List[str] = []           # iso timestamps
        self._graph_canvases     : Dict[str, object] = {}   # kept for compat (unused)
        self._graph_axes         : Dict[str, Dict[str,object]] = {}  # kept for compat
        self._graph_nb           : Optional[object] = None  # sentinel: layout built?
        self._graphs_tab_frame   : Optional[tk.Frame] = None
        self._graph_axes_by_dim  : Dict[str, object] = {}   # dim -> Axes
        self._graph_single_canvas: Optional[object] = None  # single FigureCanvasTkAgg

        # Alert records for history/pie
        self._alert_records : List[dict] = []
        
        # Log and graph clearing timer (1 hour = 3600 seconds)
        self._last_clear_time = time.time()
        self._clear_interval = 3600  # 1 hour

        self._apply_theme()
        self._build_ui()
        self._load_cfg_to_ui()
        self._pump_log()
        self._pump_data()

    # ── Theme ─────────────────────────────────────────────────────────────────

    def _apply_theme(self):
        s = ttk.Style(self)
        s.theme_use("clam")
        BG, FG, ACC = COLORS["bg"], COLORS["text"], COLORS["acc"]
        s.configure(".", background=BG, foreground=FG, fieldbackground=COLORS["bg_light"],
                    troughcolor=COLORS["bg_light"], bordercolor="#3a4060", darkcolor=BG, lightcolor=BG)
        s.configure("TNotebook", background=BG, tabmargins=[0,4,0,0])
        s.configure("TNotebook.Tab", background=COLORS["bg_light"], foreground=COLORS["muted"],
                    padding=[14,6], font=("Segoe UI",9))
        s.map("TNotebook.Tab", background=[("selected",BG)], foreground=[("selected",ACC)])
        s.configure("TLabelframe", background=BG, foreground=COLORS["muted"], bordercolor="#3a4060")
        s.configure("TLabelframe.Label", background=BG, foreground=COLORS["muted"], font=("Segoe UI",9))
        s.configure("TButton", background=COLORS["bg_light"], foreground=FG, padding=[8,4],
                    font=("Segoe UI",9), relief="flat", borderwidth=0)
        s.map("TButton", background=[("active","#3a4870"),("pressed",ACC)])
        s.configure("Accent.TButton", background=ACC, foreground="#fff", font=("Segoe UI",9,"bold"))
        s.map("Accent.TButton", background=[("active","#2d7de8")])
        s.configure("Green.TButton", background=COLORS["green"], foreground="#0a1a10",
                    font=("Segoe UI",9,"bold"))
        s.map("Green.TButton", background=[("active","#3acc77")])
        s.configure("Stop.TButton", background="#cc3344", foreground="#fff", font=("Segoe UI",9,"bold"))
        s.map("Stop.TButton", background=[("active","#aa2233")])
        s.configure("TEntry", fieldbackground=COLORS["bg_light"], foreground=FG,
                    insertcolor=ACC, bordercolor="#3a4060")
        s.configure("TCombobox", fieldbackground=COLORS["bg_light"], foreground=FG,
                    selectbackground=COLORS["bg_light"], selectforeground=FG)
        s.configure("Treeview", background=COLORS["bg_light"], foreground=FG,
                    fieldbackground=COLORS["bg_light"], rowheight=24, font=("Segoe UI",9))
        s.configure("Treeview.Heading", background="#16192b", foreground=COLORS["muted"],
                    font=("Segoe UI",9,"bold"), relief="flat")
        s.map("Treeview", background=[("selected","#3a4870")], foreground=[("selected","#fff")])
        s.configure("TCheckbutton", background=BG, foreground=FG)
        s.configure("TScrollbar", background=COLORS["bg_light"], troughcolor=BG,
                    arrowcolor=COLORS["muted"], bordercolor=BG)
        self.configure(bg=BG)

    # ── UI ────────────────────────────────────────────────────────────────────

    def _build_ui(self):
        # Toolbar
        tb = tk.Frame(self, bg=COLORS["toolbar"], pady=6)
        tb.pack(fill="x")
        self._start_btn = ttk.Button(tb, text="▶  Start Monitoring",
                                     command=self._toggle, style="Accent.TButton", width=20)
        self._start_btn.pack(side="left", padx=(10,4))
        ttk.Button(tb, text="💾  Save",    command=self._save_cfg, width=10).pack(side="left", padx=2)
        ttk.Button(tb, text="🔌  Test DB", command=self._test_connection, width=12).pack(side="left", padx=2)
        self._status_var = tk.StringVar(value="⏹  Stopped")
        tk.Label(tb, textvariable=self._status_var, bg=COLORS["toolbar"], fg=COLORS["muted"],
                 font=("Segoe UI",10,"bold")).pack(side="right", padx=14)

        # Main notebook
        self._nb = ttk.Notebook(self)
        self._nb.pack(fill="both", expand=True, padx=8, pady=(6,8))
        self._tab_connection(self._nb)
        self._tab_monitoring(self._nb)
        self._tab_thresholds(self._nb)
        self._tab_graphs(self._nb)
        self._tab_alert_history(self._nb)
        self._tab_email(self._nb)
        self._tab_sms(self._nb)
        self._tab_xml_replay(self._nb)
        self._tab_log(self._nb)

    # ══════════════════════════════════════════════════════════════════════════
    # Tab: Connection
    # ══════════════════════════════════════════════════════════════════════════

    def _tab_connection(self, nb):
        outer = ttk.Frame(nb, padding=0)
        nb.add(outer, text="  🔌  Connection  ")
        inner = make_scrollable(outer)
        f = tk.Frame(inner, bg=COLORS["bg"], padx=16, pady=12)
        f.pack(fill="x")

        self._cv: Dict[str,tk.StringVar] = {}

        # ── Credentials ───────────────────────────────────────────────────────
        cred = ttk.LabelFrame(f, text="InfluxDB Credentials", padding=12)
        cred.pack(fill="x", pady=(0,10))
        for row,(lbl,key,default,secret) in enumerate([
            ("InfluxDB URL", "url",   "http://localhost:8086", False),
            ("Auth Token",   "token", "",                      True ),
            ("Organisation", "org",   "",                      False),
        ]):
            tk.Label(cred, text=lbl+":", bg=COLORS["bg"], fg=COLORS["muted"],
                     font=("Segoe UI",9), anchor="w").grid(row=row,column=0,sticky="w",pady=5,padx=(0,12))
            var = tk.StringVar(value=default)
            self._cv[key] = var
            ttk.Entry(cred, textvariable=var, width=60, show="●" if secret else "").grid(
                row=row, column=1, sticky="ew", pady=5)
        cred.columnconfigure(1, weight=1)

        # Credential buttons
        cbr = ttk.Frame(cred)
        cbr.grid(row=3, column=0, columnspan=2, sticky="w", pady=(6,0))
        ttk.Button(cbr, text="⚡  Default Values",  command=self._apply_defaults,
                   style="Accent.TButton").pack(side="left")
        self._connect_btn = ttk.Button(cbr, text="🔌  Connect & Load",
                                        command=self._connect_influx, width=18)
        self._connect_btn.pack(side="left", padx=8)
        self._conn_status = tk.Label(cbr, text="● Not connected", bg=COLORS["bg"],
                                      fg=COLORS["red"], font=("Segoe UI",9,"bold"))
        self._conn_status.pack(side="left", padx=6)

        # ── Bucket / Project ──────────────────────────────────────────────────
        bp = ttk.LabelFrame(f, text="Bucket & Project", padding=12)
        bp.pack(fill="x", pady=(0,10))
        for row,(lbl,key) in enumerate([("Bucket","bucket"),("Project (measurement)","project")]):
            tk.Label(bp, text=lbl+":", bg=COLORS["bg"], fg=COLORS["muted"],
                     font=("Segoe UI",9), anchor="w").grid(row=row,column=0,sticky="w",pady=5,padx=(0,12))
            var = tk.StringVar()
            self._cv[key] = var
            combo = ttk.Combobox(bp, textvariable=var, state="readonly", width=42, font=("Segoe UI",9))
            combo.grid(row=row, column=1, sticky="ew", pady=5)
            if key=="bucket":
                self._bucket_combo = combo
                combo.bind("<<ComboboxSelected>>", self._on_bucket_changed)
            else:
                self._project_combo = combo
                combo.bind("<<ComboboxSelected>>", self._on_project_changed)
        bp.columnconfigure(1, weight=1)

        # ── Fields / Range ────────────────────────────────────────────────────
        fr = ttk.LabelFrame(f, text="Fields & Query Window", padding=12)
        fr.pack(fill="x", pady=(0,10))
        tk.Label(fr, text="Fields (comma-sep):", bg=COLORS["bg"], fg=COLORS["muted"],
                 font=("Segoe UI",9)).grid(row=0,column=0,sticky="w",pady=5,padx=(0,12))
        self._fields_var = tk.StringVar(value="velx,vely,velz")
        ttk.Entry(fr, textvariable=self._fields_var, width=42).grid(row=0,column=1,sticky="ew",pady=5)
        tk.Label(fr, text="← auto-filled after Connect", bg=COLORS["bg"], fg=COLORS["dim"],
                 font=("Segoe UI",8)).grid(row=0,column=2,sticky="w",padx=6)

        tk.Label(fr, text="Range Window:", bg=COLORS["bg"], fg=COLORS["muted"],
                 font=("Segoe UI",9)).grid(row=1,column=0,sticky="w",pady=5,padx=(0,12))
        rf2 = ttk.Frame(fr)
        rf2.grid(row=1, column=1, sticky="w")
        self._range_var = tk.StringVar(value="20m")
        ttk.Entry(rf2, textvariable=self._range_var, width=10).pack(side="left")
        tk.Label(rf2, text="  e.g. 5m, 20m, 1h", bg=COLORS["bg"], fg=COLORS["dim"],
                 font=("Segoe UI",8)).pack(side="left")
        fr.columnconfigure(1, weight=1)

        # ── Aggregation ───────────────────────────────────────────────────────
        agg = ttk.LabelFrame(f, text="Aggregation", padding=10)
        agg.pack(fill="x", pady=(0,6))
        self._use_agg_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(agg, text="Use aggregateWindow (recommended)",
                        variable=self._use_agg_var).grid(row=0,column=0,columnspan=6,sticky="w",pady=(0,6))
        for col,(lbl,var_name,default,w) in enumerate([
            ("Interval:",  "_agg_every_var", "1s",    10),
            ("Max raw:",   "_max_raw_var",   "50000", 10),
            ("Timeout(s):","_timeout_var",   "30",    6),
        ]):
            tk.Label(agg, text=lbl, bg=COLORS["bg"], fg=COLORS["muted"],
                     font=("Segoe UI",9)).grid(row=1,column=col*2,sticky="w",padx=(16 if col else 0,6))
            var = tk.StringVar(value=default)
            setattr(self, var_name, var)
            ttk.Entry(agg, textvariable=var, width=w).grid(row=1,column=col*2+1,sticky="w")

    # ══════════════════════════════════════════════════════════════════════════
    # Tab: Monitoring
    # ══════════════════════════════════════════════════════════════════════════

    def _tab_monitoring(self, nb):
        outer = ttk.Frame(nb, padding=0)
        nb.add(outer, text="  ⏱  Monitoring  ")
        inner = make_scrollable(outer)
        f = tk.Frame(inner, bg=COLORS["bg"], padx=16, pady=12)
        f.pack(fill="x")

        ivf = ttk.LabelFrame(f, text="Poll Interval", padding=10)
        ivf.pack(fill="x", pady=(0,12))
        tk.Label(ivf, text="Check every:", bg=COLORS["bg"], fg=COLORS["muted"],
                 font=("Segoe UI",9)).pack(side="left")
        self._interval_var = tk.StringVar(value="60")
        ttk.Entry(ivf, textvariable=self._interval_var, width=8).pack(side="left", padx=6)
        tk.Label(ivf, text="seconds", bg=COLORS["bg"], fg=COLORS["muted"],
                 font=("Segoe UI",9)).pack(side="left")
        tk.Label(ivf, text="  Quick:", bg=COLORS["bg"], fg=COLORS["dim"],
                 font=("Segoe UI",9)).pack(side="left", padx=(20,6))
        for lbl,val in [("30s","30"),("1m","60"),("5m","300"),("15m","900"),("30m","1800"),("1h","3600")]:
            ttk.Button(ivf, text=lbl, width=5,
                       command=lambda v=val: self._interval_var.set(v)).pack(side="left", padx=1)

        sf = ttk.LabelFrame(f, text="Sensor Filter  (leave empty = All sensors)", padding=10)
        sf.pack(fill="both", expand=True, pady=(0,12))
        br = ttk.Frame(sf)
        br.pack(fill="x", pady=(0,6))
        ttk.Button(br, text="⬇  Load Sensors from InfluxDB",
                   command=self._load_sensors).pack(side="left")
        ttk.Button(br, text="Select All",
                   command=lambda: self._sensor_lb.select_set(0,"end")).pack(side="left",padx=4)
        ttk.Button(br, text="Clear All",
                   command=lambda: self._sensor_lb.select_clear(0,"end")).pack(side="left")
        lbf = tk.Frame(sf, bg=COLORS["bg_light"])
        lbf.pack(fill="both", expand=True)
        sb = tk.Scrollbar(lbf)
        sb.pack(side="right", fill="y")
        self._sensor_lb = tk.Listbox(lbf, selectmode="multiple", yscrollcommand=sb.set,
                                      bg=COLORS["bg_light"], fg=COLORS["text"],
                                      selectbackground="#3a4870", font=("Segoe UI",9),
                                      height=8, bd=0, highlightthickness=0)
        self._sensor_lb.pack(fill="both", expand=True)
        sb.config(command=self._sensor_lb.yview)

        df = ttk.LabelFrame(f, text="Dimensions to Monitor", padding=10)
        df.pack(fill="x")
        self._dim_vars: Dict[str,tk.BooleanVar] = {}
        for dim in ["velx","vely","velz"]:
            v = tk.BooleanVar(value=True)
            self._dim_vars[dim] = v
            ttk.Checkbutton(df, text=dim, variable=v).pack(side="left", padx=14)

    # ══════════════════════════════════════════════════════════════════════════
    # Tab: Thresholds
    # ══════════════════════════════════════════════════════════════════════════

    def _tab_thresholds(self, nb):
        f = ttk.Frame(nb, padding=12)
        nb.add(f, text="  🎯  Thresholds  ")
        tk.Label(f, text=(
            "Each rule checks max absolute value per sensor/dimension each cycle. "
            "A threshold line is drawn on the live graph automatically."
        ), bg=COLORS["bg"], fg=COLORS["dim"], font=("Segoe UI",8),
           wraplength=900, justify="left").pack(fill="x", pady=(0,8))

        br = ttk.Frame(f)
        br.pack(fill="x", pady=(0,8))
        ttk.Button(br, text="➕  Add Rule",       command=self._add_rule, style="Accent.TButton").pack(side="left")
        ttk.Button(br, text="✏  Edit Selected",   command=self._edit_rule).pack(side="left",padx=4)
        ttk.Button(br, text="🗑  Remove Selected", command=self._remove_rule).pack(side="left")
        ttk.Button(br, text="Enable All",  command=lambda: self._all_enabled(True)).pack(side="right",padx=4)
        ttk.Button(br, text="Disable All", command=lambda: self._all_enabled(False)).pack(side="right")

        cols = ("Name","Sensor","Dimension","Op","Value","Level","Cooldown","En")
        self._tree = ttk.Treeview(f, columns=cols, show="headings", height=18)
        for col,w,an in zip(cols,[180,140,90,40,90,80,80,40],
                            ["w","w","center","center","center","center","center","center"]):
            self._tree.heading(col,text=col)
            self._tree.column(col,width=w,anchor=an,minwidth=w)
        vsb = ttk.Scrollbar(f, orient="vertical", command=self._tree.yview)
        self._tree.configure(yscrollcommand=vsb.set)
        self._tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")
        self._tree.tag_configure("critical", foreground=COLORS["red"])
        self._tree.tag_configure("warning",  foreground=COLORS["orange"])
        self._tree.tag_configure("disabled", foreground=COLORS["dim"])
        self._tree.bind("<Double-1>", lambda _: self._edit_rule())

    # ══════════════════════════════════════════════════════════════════════════
    # Tab: Live Graphs
    # ══════════════════════════════════════════════════════════════════════════

    def _tab_graphs(self, nb):
        f = ttk.Frame(nb)
        nb.add(f, text="  📊  Live Graphs  ")
        self._graphs_tab_frame = f

        banner = tk.Frame(f, bg=COLORS["bg_mid"], pady=6)
        banner.pack(fill="x")
        tk.Label(banner, text="Connect to InfluxDB and start monitoring to see live graphs.",
                 bg=COLORS["bg_mid"], fg=COLORS["muted"], font=("Segoe UI",9)).pack()
        self._graph_banner = banner

        # Placeholder for inner sensor notebook (built on first data)
        self._graph_host = tk.Frame(f, bg=COLORS["bg"])
        self._graph_host.pack(fill="both", expand=True)

    # ── Graph layout helpers ──────────────────────────────────────────────────

    def _ensure_graph_layout(self, dims: List[str]) -> None:
        """
        Build (once) a fixed per-dimension figure: one subplot per dim,
        arranged 2 columns wide, all sensors overlaid per subplot.
        Matches the Grafana-style PPV threshold panel layout.
        """
        if self._graph_nb is not None:
            return
        if not dims:
            return
        self._graph_banner.pack_forget()

        n     = len(dims)
        ncols = 2
        nrows = (n + ncols - 1) // ncols

        fig = Figure(figsize=(14, 4 * nrows), dpi=96, facecolor=COLORS["bg"])
        self._graph_axes_by_dim: Dict[str, object] = {}

        _dim_labels = {
            "velx": "X-direction",
            "vely": "Y-direction",
            "velz": "Z-direction",
        }
        for i, dim in enumerate(dims):
            ax = fig.add_subplot(nrows, ncols, i + 1)
            ax.set_facecolor(COLORS["bg_light"])
            ax.tick_params(colors=COLORS["muted"], labelsize=8)
            ax.set_ylabel("Peak Particle Velocity\n(PPV, mm/s)",
                          color=COLORS["muted"], fontsize=8)
            for spine in ax.spines.values():
                spine.set_edgecolor("#3a4060")
            label = _dim_labels.get(dim, dim)
            ax.set_title(f"Particle Velocity Thresholds,  {label}",
                         color=COLORS["text"], fontsize=9, pad=4)
            self._graph_axes_by_dim[dim] = ax

        fig.tight_layout(pad=3.0, h_pad=4.0, w_pad=3.0)

        canvas = FigureCanvasTkAgg(fig, master=self._graph_host)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True, side="top")

        try:
            tb_frame = tk.Frame(self._graph_host, bg=COLORS["bg"])
            tb_frame.pack(fill="x", side="bottom")
            NavigationToolbar2Tk(canvas, tb_frame)
        except Exception:
            pass

        # Reuse _graph_nb as "layout initialised" sentinel
        self._graph_nb     = True           # type: ignore[assignment]
        self._graph_single_canvas = canvas

    def _update_graphs(self, ts: str, max_vals: Dict[str, Dict[str, float]]):
        """
        Redraw all per-dimension subplots.

        Layout (Grafana-style):
          • One subplot per velocity dimension (velx / vely / velz), 2 cols wide.
          • All sensors overlaid on each subplot as vertical stem markers.
          • Threshold bands filled with green / orange / red zones.
          • Threshold lines drawn as dashed horizontals.
          • Real-time timestamps on the X-axis.
        """
        if not MPL_OK:
            return

        dims = [v for v, b in self._dim_vars.items() if b.get()]
        if not dims:
            return

        # ── Accumulate history ────────────────────────────────────────────────
        self._graph_ts.append(ts)
        MAX_HIST = 200
        if len(self._graph_ts) > MAX_HIST:
            self._graph_ts = self._graph_ts[-MAX_HIST:]

        for sensor, dvals in max_vals.items():
            if sensor not in self._graph_history:
                self._graph_history[sensor] = {d: [] for d in dims}
            for dim in dims:
                self._graph_history[sensor].setdefault(dim, []).append(
                    dvals.get(dim, 0.0))
                if len(self._graph_history[sensor][dim]) > MAX_HIST:
                    self._graph_history[sensor][dim] = \
                        self._graph_history[sensor][dim][-MAX_HIST:]

        # ── Build layout once ─────────────────────────────────────────────────
        self._ensure_graph_layout(dims)

        # ── Parse x-axis timestamps ───────────────────────────────────────────
        try:
            xs = [datetime.fromisoformat(t) for t in self._graph_ts]
        except Exception:
            xs = list(range(len(self._graph_ts)))
        use_dates = xs and not isinstance(xs[0], int)

        sensors = sorted(self._graph_history.keys())

        _dim_labels = {
            "velx": "X-direction",
            "vely": "Y-direction",
            "velz": "Z-direction",
        }

        # ── Redraw each subplot ───────────────────────────────────────────────
        for dim_i, dim in enumerate(dims):
            ax = self._graph_axes_by_dim.get(dim)
            if ax is None:
                continue
            ax.cla()

            # Axes styling
            ax.set_facecolor(COLORS["bg_light"])
            ax.tick_params(colors=COLORS["muted"], labelsize=8)
            ax.set_ylabel("Peak Particle Velocity\n(PPV, mm/s)",
                          color=COLORS["muted"], fontsize=8)
            for spine in ax.spines.values():
                spine.set_edgecolor("#3a4060")
            label = _dim_labels.get(dim, dim)
            ax.set_title(f"Particle Velocity Thresholds,  {label}",
                         color=COLORS["text"], fontsize=9, pad=4)

            # ── Threshold bands (green → orange → red zones) ──────────────
            rules_for_dim = [
                r for r in self.cfg.thresholds
                if r.enabled and r.dimension in ("All", dim)
            ]
            thresh_vals = sorted(set(abs(r.value) for r in rules_for_dim))

            # Determine y-range for band shading
            all_ys: List[float] = []
            for sensor in sensors:
                all_ys.extend(self._graph_history.get(sensor, {}).get(dim, []))
            data_max  = max((abs(v) for v in all_ys), default=0.0)
            band_ceil = max(thresh_vals[-1] * 1.4 if thresh_vals else 12.0,
                            data_max * 1.2, 1.0)

            # Zone fill colours (dark green / dark orange / dark red)
            zone_fills  = ["#0d2b17", "#2e1a00", "#2e0008"]
            zone_alphas = [0.55,      0.55,       0.55     ]
            prev = 0.0
            for zi, tv in enumerate(thresh_vals):
                fc = zone_fills[min(zi, len(zone_fills) - 1)]
                fa = zone_alphas[min(zi, len(zone_alphas) - 1)]
                ax.axhspan(prev, tv, alpha=fa, color=fc, zorder=0)
                prev = tv
            # Red above highest threshold
            ax.axhspan(prev, band_ceil, alpha=0.55, color="#2e0008", zorder=0)
            ax.set_ylim(0, band_ceil)

            # ── Threshold lines ───────────────────────────────────────────
            for rule in rules_for_dim:
                line_color = (COLORS["red"]
                              if rule.alert_level == "Critical"
                              else COLORS["orange"])
                ax.axhline(abs(rule.value), color=line_color,
                           linestyle="--", linewidth=1.5, alpha=0.95,
                           label=f"{rule.name}  ({abs(rule.value):.4g})",
                           zorder=3)

            # ── Sensor stem plots ─────────────────────────────────────────
            for si, sensor in enumerate(sensors):
                ys_full = self._graph_history.get(sensor, {}).get(dim, [])
                if not ys_full:
                    continue
                n_pts  = min(len(xs), len(ys_full))
                x_plot = xs[-n_pts:]
                y_plot = ys_full[-n_pts:]
                col    = DIM_PALETTE[si % len(DIM_PALETTE)]
                last_v = y_plot[-1] if y_plot else 0.0
                lbl    = f"{dim} {sensor}   Last: {last_v:.5g}"
                try:
                    markerline, stemlines, baseline = ax.stem(
                        x_plot, y_plot,
                        linefmt=col,
                        markerfmt="o",
                        basefmt=" ",
                        label=lbl,
                    )
                    plt.setp(stemlines,  linewidth=1.0, alpha=0.80, color=col)
                    plt.setp(markerline, markersize=3,
                             markerfacecolor=col, markeredgecolor=col)
                except Exception:
                    ax.plot(x_plot, y_plot, color=col,
                            linewidth=1.0, label=lbl, zorder=4)

            # ── X-axis date formatting ────────────────────────────────────
            if use_dates:
                try:
                    ax.xaxis.set_major_formatter(
                        mdates.DateFormatter("%m/%d %H:%M"))
                    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
                except Exception:
                    pass
            ax.tick_params(axis="x", rotation=15, labelsize=7,
                           colors=COLORS["muted"])

            ax.legend(loc="upper left", fontsize=7,
                      facecolor=COLORS["bg_mid"],
                      labelcolor=COLORS["text"],
                      edgecolor="#3a4060")

        # ── Redraw canvas ─────────────────────────────────────────────────────
        try:
            self._graph_single_canvas.figure.tight_layout(
                pad=3.0, h_pad=4.0, w_pad=3.0)
            self._graph_single_canvas.draw_idle()
        except Exception:
            pass

    # ══════════════════════════════════════════════════════════════════════════
    # Tab: Alert History + Pie Chart
    # ══════════════════════════════════════════════════════════════════════════

    def _tab_alert_history(self, nb):
        f = ttk.Frame(nb)
        nb.add(f, text="  🔔  Alert History  ")

        paned = tk.PanedWindow(f, orient="horizontal", bg=COLORS["bg"],
                               sashwidth=6, sashrelief="flat")
        paned.pack(fill="both", expand=True)

        # Left: table
        left = tk.Frame(paned, bg=COLORS["bg"])
        paned.add(left, minsize=400)

        br = ttk.Frame(left)
        br.pack(fill="x", padx=8, pady=6)
        tk.Label(br, text="Triggered Alerts", bg=COLORS["bg"], fg=COLORS["acc"],
                 font=("Segoe UI",11,"bold")).pack(side="left")
        ttk.Button(br, text="Clear", command=self._clear_alert_history).pack(side="right")

        cols = ("Time","Rule","Sensor","Dim","Value","Level")
        self._hist_tree = ttk.Treeview(left, columns=cols, show="headings", height=20)
        for col,w in zip(cols,[140,140,100,60,80,70]):
            self._hist_tree.heading(col,text=col)
            self._hist_tree.column(col,width=w,minwidth=w)
        hsb = ttk.Scrollbar(left, orient="horizontal", command=self._hist_tree.xview)
        vsb = ttk.Scrollbar(left, orient="vertical",   command=self._hist_tree.yview)
        self._hist_tree.configure(xscrollcommand=hsb.set, yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y")
        self._hist_tree.pack(fill="both", expand=True, padx=8)
        hsb.pack(fill="x", padx=8)
        self._hist_tree.tag_configure("critical", foreground=COLORS["red"])
        self._hist_tree.tag_configure("warning",  foreground=COLORS["orange"])

        # Right: pie chart
        right = tk.Frame(paned, bg=COLORS["bg"])
        paned.add(right, minsize=300)

        tk.Label(right, text="Daily Alert Summary", bg=COLORS["bg"], fg=COLORS["acc"],
                 font=("Segoe UI",11,"bold")).pack(pady=(10,4))

        if MPL_OK:
            self._pie_fig = Figure(figsize=(4,4), dpi=96, facecolor=COLORS["bg"])
            self._pie_ax  = self._pie_fig.add_subplot(111)
            self._pie_ax.set_facecolor(COLORS["bg"])
            self._pie_canvas = FigureCanvasTkAgg(self._pie_fig, master=right)
            self._pie_canvas.draw()
            self._pie_canvas.get_tk_widget().pack(fill="both", expand=True, padx=8, pady=8)
        else:
            tk.Label(right, text="matplotlib not available", bg=COLORS["bg"],
                     fg=COLORS["muted"], font=("Segoe UI",9)).pack()

    def _add_alert_record(self, violations: List[dict], cycle: int):
        ts = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
        for v in violations:
            rec = {**v, "timestamp": ts, "cycle": cycle}
            self._alert_records.append(rec)
            tag = "critical" if v["alert_level"]=="Critical" else "warning"
            self._hist_tree.insert("","end", tags=(tag,), values=(
                ts, v["rule"], v["sensor"], v["dimension"],
                f"{v['max_value']:.5f}", v["alert_level"],
            ))
            self._hist_tree.yview_moveto(1.0)
        self._update_pie()

    def _update_pie(self):
        if not MPL_OK:
            return
        today = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d")
        today_recs = [r for r in self._alert_records if r.get("timestamp","").startswith(today)]
        w_count = sum(1 for r in today_recs if r["alert_level"]=="Warning")
        c_count = sum(1 for r in today_recs if r["alert_level"]=="Critical")
        ax = self._pie_ax
        ax.cla()
        ax.set_facecolor(COLORS["bg"])
        if w_count+c_count == 0:
            ax.text(0.5,0.5,"No alerts today",ha="center",va="center",
                    fontsize=11,color=COLORS["muted"],transform=ax.transAxes)
        else:
            sizes  = []
            labels = []
            colors = []
            if w_count: sizes.append(w_count); labels.append(f"Warning\n{w_count}"); colors.append(COLORS["orange"])
            if c_count: sizes.append(c_count); labels.append(f"Critical\n{c_count}"); colors.append(COLORS["red"])
            wedges,_ = ax.pie(sizes, colors=colors, startangle=90,
                              wedgeprops=dict(edgecolor=COLORS["bg"],linewidth=2))
            ax.legend(wedges, labels, loc="lower center", fontsize=9,
                      facecolor=COLORS["bg_mid"], labelcolor=COLORS["text"],
                      edgecolor="#3a4060", ncol=2)
            ax.set_title(f"Today's Alerts  ({today})",
                         color=COLORS["muted"], fontsize=9, pad=10)
        self._pie_fig.set_facecolor(COLORS["bg"])
        try:
            self._pie_canvas.draw_idle()
        except Exception:
            pass

    def _clear_alert_history(self):
        if messagebox.askyesno("Clear","Clear all alert history?"):
            self._alert_records.clear()
            for item in self._hist_tree.get_children():
                self._hist_tree.delete(item)
            self._update_pie()

    # ══════════════════════════════════════════════════════════════════════════
    # Tab: Email
    # ══════════════════════════════════════════════════════════════════════════

    def _tab_email(self, nb):
        outer = ttk.Frame(nb, padding=0)
        nb.add(outer, text="  📧  Email  ")
        inner = make_scrollable(outer)
        f = tk.Frame(inner, bg=COLORS["bg"], padx=16, pady=12)
        f.pack(fill="x")

        # ── Send method selector ───────────────────────────────────────────────
        method_f = ttk.LabelFrame(f, text="Send Method", padding=10)
        method_f.pack(fill="x", pady=(0,10))
        self._win32_var = tk.BooleanVar(value=False)
        win32_banner = (
            "Use Outlook WIN32 (desktop app, no credentials needed)"
            if WIN32_OK else
            "Use Outlook WIN32  ⚠ pywin32 not installed — run: pip install pywin32"
        )
        self._win32_chk = ttk.Checkbutton(
            method_f, text=win32_banner, variable=self._win32_var,
            command=self._on_win32_toggle
        )
        self._win32_chk.grid(row=0, column=0, columnspan=3, sticky="w", pady=(0,4))
        tk.Label(
            method_f,
            text="When checked: sends via Outlook desktop app (COM). SMTP fields below are ignored.",
            bg=COLORS["bg"], fg=COLORS["dim"], font=("Segoe UI",8),
        ).grid(row=1, column=0, columnspan=3, sticky="w", pady=(0,6))
        # From account (for multi-account Outlook)
        tk.Label(method_f, text="From Account (Outlook):",
                 bg=COLORS["bg"], fg=COLORS["muted"],
                 font=("Segoe UI",9)).grid(row=2, column=0, sticky="w", padx=(0,8), pady=2)
        self._win32_from_var = tk.StringVar()
        self._win32_from_entry = ttk.Entry(method_f, textvariable=self._win32_from_var, width=36)
        self._win32_from_entry.grid(row=2, column=1, sticky="w", pady=2)
        tk.Label(method_f,
                 text="e.g. you@company.com  (leave blank to use Outlook default)",
                 bg=COLORS["bg"], fg=COLORS["dim"], font=("Segoe UI",8)
                 ).grid(row=2, column=2, sticky="w", padx=(8,0))
        method_f.columnconfigure(1, weight=1)

        # ── SMTP ──────────────────────────────────────────────────────────────
        smtp = ttk.LabelFrame(f, text="SMTP Settings", padding=12)
        smtp.pack(fill="x", pady=(0,10))
        self._smtp_frame = smtp
        self._ev: Dict[str,tk.StringVar] = {}
        self._smtp_widgets: list = []
        for row,(lbl,key,default,secret,w) in enumerate([
            ("SMTP Server",             "smtp_server","smtp-mail.outlook.com",False,30),
            ("Port",                    "smtp_port",  "587",                  False,8 ),
            ("Username / Email",        "username",   "",                     False,36),
            ("Password / App Password", "password",   "",                     True, 36),
            ("From Address",            "from_addr",  "",                     False,36),
        ]):
            lbl_w = tk.Label(smtp, text=lbl+":", bg=COLORS["bg"], fg=COLORS["muted"],
                     font=("Segoe UI",9), anchor="w")
            lbl_w.grid(row=row,column=0,sticky="w",pady=4,padx=(0,12))
            var = tk.StringVar(value=default)
            self._ev[key] = var
            ent = ttk.Entry(smtp, textvariable=var, width=w, show="●" if secret else "")
            ent.grid(row=row, column=1, sticky="ew", pady=4)
            self._smtp_widgets.extend([lbl_w, ent])
        self._tls_var = tk.BooleanVar(value=True)
        tls_chk = ttk.Checkbutton(smtp, text="Use STARTTLS  (required for Outlook)",
                        variable=self._tls_var)
        tls_chk.grid(row=5,column=0,columnspan=2,sticky="w",pady=4)
        self._smtp_widgets.append(tls_chk)
        smtp.columnconfigure(1, weight=1)

        # ── Recipients ────────────────────────────────────────────────────────
        rec_out = ttk.LabelFrame(f, text="Recipients  (add multiple)", padding=10)
        rec_out.pack(fill="x", pady=(0,10))
        rec_l = ttk.Frame(rec_out); rec_l.pack(side="left", fill="both", expand=True)
        rec_lbf = tk.Frame(rec_l, bg=COLORS["bg_light"])
        rec_lbf.pack(fill="both", expand=True)
        rec_sb = tk.Scrollbar(rec_lbf); rec_sb.pack(side="right",fill="y")
        self._rec_lb = tk.Listbox(rec_lbf, yscrollcommand=rec_sb.set, height=4,
                                   bg=COLORS["bg_light"], fg=COLORS["text"],
                                   selectbackground="#3a4870", font=("Segoe UI",9),
                                   bd=0, highlightthickness=0)
        self._rec_lb.pack(fill="both",expand=True)
        rec_sb.config(command=self._rec_lb.yview)

        rec_r = ttk.Frame(rec_out); rec_r.pack(side="left",fill="y",padx=(10,0))
        self._new_rec_var = tk.StringVar()
        rec_entry = ttk.Entry(rec_r, textvariable=self._new_rec_var, width=34)
        rec_entry.pack(pady=(0,4))
        rec_entry.bind("<Return>", lambda _: self._add_recipient())
        ttk.Button(rec_r, text="➕  Add Address",    command=self._add_recipient).pack(fill="x",pady=2)
        ttk.Button(rec_r, text="🗑  Remove Selected", command=self._remove_recipient).pack(fill="x",pady=2)
        ttk.Button(rec_r, text="Clear All",           command=lambda: self._rec_lb.delete(0,"end")).pack(fill="x",pady=2)

        # ── Logo ──────────────────────────────────────────────────────────────
        logo_f = ttk.LabelFrame(f, text="Email Logo  (optional PNG shown at top of email)", padding=10)
        logo_f.pack(fill="x", pady=(0,10))
        self._logo_var = tk.StringVar()
        logo_row = ttk.Frame(logo_f); logo_row.pack(fill="x")
        ttk.Entry(logo_row, textvariable=self._logo_var, width=52).pack(side="left")
        ttk.Button(logo_row, text="Browse…",
                   command=self._browse_logo).pack(side="left",padx=6)
        ttk.Button(logo_row, text="Clear",
                   command=lambda: self._logo_var.set("")).pack(side="left")
        tk.Label(logo_f, text="logo.png in the same folder will be auto-detected if path is empty.",
                 bg=COLORS["bg"], fg=COLORS["dim"], font=("Segoe UI",8)).pack(anchor="w",pady=(4,0))

        # ── Template ──────────────────────────────────────────────────────────
        tmpl = ttk.LabelFrame(f, text="Email Template", padding=12)
        tmpl.pack(fill="both", expand=True, pady=(0,10))
        tk.Label(tmpl, text="Subject:", bg=COLORS["bg"], fg=COLORS["muted"],
                 font=("Segoe UI",9)).grid(row=0,column=0,sticky="w",pady=(0,6))
        self._subj_var = tk.StringVar()
        ttk.Entry(tmpl, textvariable=self._subj_var, width=74).grid(row=0,column=1,sticky="ew",pady=(0,6))
        tk.Label(tmpl, text="Body:", bg=COLORS["bg"], fg=COLORS["muted"],
                 font=("Segoe UI",9), anchor="nw").grid(row=1,column=0,sticky="nw",pady=(0,4))
        self._body_txt = scrolledtext.ScrolledText(
            tmpl, width=72, height=8, wrap="word",
            bg=COLORS["bg_light"], fg=COLORS["text"], insertbackground=COLORS["acc"],
            font=("Courier New",9), relief="flat")
        self._body_txt.grid(row=1,column=1,sticky="nsew",pady=(0,4))
        tk.Label(tmpl, text=(
            "Placeholders: {project} {timestamp} {cycle} {violations_table} "
            "{max_alert_level} {alert_level} {num_violations} {version} "
            "{top_rule} {top_sensor} {top_value}"),
            bg=COLORS["bg"], fg=COLORS["dim"], font=("Segoe UI",8),
            wraplength=700, justify="left").grid(row=2,column=0,columnspan=2,sticky="w",pady=(0,8))
        tmpl.columnconfigure(1, weight=1)
        tmpl.rowconfigure(1, weight=1)

        # ── Action buttons ────────────────────────────────────────────────────
        abr = ttk.Frame(f); abr.pack(fill="x", pady=(0,6))
        ttk.Button(abr, text="📨  Send Test Email",
                   command=self._send_test, style="Accent.TButton").pack(side="left")
        ttk.Button(abr, text="👁  Preview Demo",
                   command=self._preview_email).pack(side="left",padx=6)
        ttk.Button(abr, text="🚀  Send Demo Email to Recipients",
                   command=self._send_demo, style="Green.TButton").pack(side="left",padx=0)

    # ══════════════════════════════════════════════════════════════════════════
    # Tab: SMS
    # ══════════════════════════════════════════════════════════════════════════

    def _tab_sms(self, nb):
        outer = ttk.Frame(nb, padding=0)
        nb.add(outer, text="  📱  SMS Alerts  ")
        inner = make_scrollable(outer)
        f = tk.Frame(inner, bg=COLORS["bg"], padx=16, pady=12)
        f.pack(fill="x")

        # Status banner
        if not TWILIO_OK:
            tk.Label(f, text="⚠  Twilio not installed.  Run:  pip install twilio",
                     bg="#3a1a00", fg=COLORS["orange"], font=("Segoe UI",9,"bold"),
                     padx=10, pady=6).pack(fill="x", pady=(0,10))

        # Enable toggle
        self._sms_enabled_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(f, text="Enable SMS Alerts", variable=self._sms_enabled_var).pack(anchor="w",pady=(0,10))

        # Twilio credentials
        twilio_f = ttk.LabelFrame(f, text="Twilio Credentials", padding=12)
        twilio_f.pack(fill="x", pady=(0,10))
        self._sv: Dict[str,tk.StringVar] = {}
        for row,(lbl,key,default,secret,w) in enumerate([
            ("Account SID",   "account_sid", "",  False, 40),
            ("Auth Token",    "auth_token",  "",  True,  40),
            ("From Number",   "from_number", "",  False, 20),
        ]):
            tk.Label(twilio_f, text=lbl+":", bg=COLORS["bg"], fg=COLORS["muted"],
                     font=("Segoe UI",9), anchor="w").grid(row=row,column=0,sticky="w",pady=5,padx=(0,12))
            var = tk.StringVar(value=default)
            self._sv[key] = var
            ttk.Entry(twilio_f, textvariable=var, width=w, show="●" if secret else "").grid(
                row=row, column=1, sticky="ew", pady=5)

        tk.Label(twilio_f, text=(
            "From Number must be a Twilio-provisioned number (e.g. +61400000000).\n"
            "Get credentials at: console.twilio.com"),
            bg=COLORS["bg"], fg=COLORS["dim"], font=("Segoe UI",8),
            justify="left").grid(row=3,column=0,columnspan=2,sticky="w",pady=(4,0))
        twilio_f.columnconfigure(1, weight=1)

        # Recipients
        rec_f = ttk.LabelFrame(f, text="SMS Recipients (E.164 format, e.g. +61412345678)", padding=10)
        rec_f.pack(fill="x", pady=(0,10))
        rec_l = ttk.Frame(rec_f); rec_l.pack(side="left", fill="both", expand=True)
        rec_lbf = tk.Frame(rec_l, bg=COLORS["bg_light"])
        rec_lbf.pack(fill="both", expand=True)
        sms_sb = tk.Scrollbar(rec_lbf); sms_sb.pack(side="right",fill="y")
        self._sms_rec_lb = tk.Listbox(rec_lbf, yscrollcommand=sms_sb.set, height=4,
                                       bg=COLORS["bg_light"], fg=COLORS["text"],
                                       selectbackground="#3a4870", font=("Segoe UI",9),
                                       bd=0, highlightthickness=0)
        self._sms_rec_lb.pack(fill="both",expand=True)
        sms_sb.config(command=self._sms_rec_lb.yview)

        rec_r = ttk.Frame(rec_f); rec_r.pack(side="left",fill="y",padx=(10,0))
        self._new_sms_var = tk.StringVar()
        sms_entry = ttk.Entry(rec_r, textvariable=self._new_sms_var, width=22)
        sms_entry.pack(pady=(0,4))
        sms_entry.bind("<Return>", lambda _: self._add_sms_recipient())
        ttk.Button(rec_r, text="➕  Add Number",     command=self._add_sms_recipient).pack(fill="x",pady=2)
        ttk.Button(rec_r, text="🗑  Remove Selected", command=self._remove_sms_recipient).pack(fill="x",pady=2)
        ttk.Button(rec_r, text="Clear All",           command=lambda: self._sms_rec_lb.delete(0,"end")).pack(fill="x",pady=2)

        # Message template
        tmpl = ttk.LabelFrame(f, text="SMS Message Template", padding=12)
        tmpl.pack(fill="x", pady=(0,10))
        tk.Label(tmpl, text="Template:", bg=COLORS["bg"], fg=COLORS["muted"],
                 font=("Segoe UI",9)).pack(anchor="w")
        self._sms_body_txt = scrolledtext.ScrolledText(
            tmpl, width=72, height=4, wrap="word",
            bg=COLORS["bg_light"], fg=COLORS["text"], insertbackground=COLORS["acc"],
            font=("Courier New",9), relief="flat")
        self._sms_body_txt.pack(fill="x", pady=4)
        tk.Label(tmpl, text=(
            "Placeholders: {alert_level} {project} {timestamp} {num_violations} "
            "{top_rule} {top_sensor} {top_value:.4f}  —  Keep under 160 chars for 1 SMS"),
            bg=COLORS["bg"], fg=COLORS["dim"], font=("Segoe UI",8),
            justify="left").pack(anchor="w")

        # Test button
        abr = ttk.Frame(f); abr.pack(fill="x", pady=(0,6))
        ttk.Button(abr, text="📱  Send Test SMS",
                   command=self._send_test_sms, style="Accent.TButton").pack(side="left")
        tk.Label(abr, text=" sends a dummy violation message to all SMS recipients",
                 bg=COLORS["bg"], fg=COLORS["dim"], font=("Segoe UI",8)).pack(side="left",padx=6)

        # Instructions
        inst = ttk.LabelFrame(f, text="Twilio Setup Instructions", padding=12)
        inst.pack(fill="x")
        for line in [
            "1. Sign up at twilio.com and verify your account.",
            "2. Get a Twilio phone number (with SMS capability) from the console.",
            "3. Copy your Account SID and Auth Token from the console dashboard.",
            "4. Add verified recipient numbers (E.164 format: +CountryCodeNumber).",
            "5. For trial accounts, you must verify each recipient number first.",
            "6. Install the library:  pip install twilio",
        ]:
            tk.Label(inst, text=line, bg=COLORS["bg"], fg=COLORS["muted"],
                     font=("Segoe UI",9), anchor="w").pack(anchor="w", pady=1)

    # ══════════════════════════════════════════════════════════════════════════
    # Tab: XML Replay
    # ══════════════════════════════════════════════════════════════════════════

    def _tab_xml_replay(self, nb):
        f = ttk.Frame(nb)
        nb.add(f, text="  📂  XML Replay  ")

        top = ttk.Frame(f, padding=8)
        top.pack(fill="x")
        tk.Label(top, text="Load an XML alert record and replay/plot the history.",
                 bg=COLORS["bg"], fg=COLORS["muted"], font=("Segoe UI",9)).pack(side="left")
        ttk.Button(top, text="📂  Open XML File…",
                   command=self._load_xml_replay, style="Accent.TButton").pack(side="right",padx=4)
        ttk.Button(top, text="📂  Open Latest",
                   command=self._open_latest_xml).pack(side="right",padx=4)

        # Table
        cols = ("Timestamp","Cycle","Rule","Sensor","Dim","Max Value","Threshold","Level")
        self._xml_tree = ttk.Treeview(f, columns=cols, show="headings", height=12)
        for col,w in zip(cols,[140,50,140,120,60,90,90,70]):
            self._xml_tree.heading(col,text=col)
            self._xml_tree.column(col,width=w,minwidth=w)
        xsb = ttk.Scrollbar(f, orient="horizontal", command=self._xml_tree.xview)
        ysb = ttk.Scrollbar(f, orient="vertical",   command=self._xml_tree.yview)
        self._xml_tree.configure(xscrollcommand=xsb.set, yscrollcommand=ysb.set)
        ysb.pack(side="right", fill="y", padx=(0,4))
        self._xml_tree.pack(fill="both", expand=False, padx=4)
        xsb.pack(fill="x", padx=4)
        self._xml_tree.tag_configure("critical", foreground=COLORS["red"])
        self._xml_tree.tag_configure("warning",  foreground=COLORS["orange"])

        # Replay chart
        self._xml_chart_frame = tk.Frame(f, bg=COLORS["bg"])
        self._xml_chart_frame.pack(fill="both", expand=True, padx=4, pady=4)
        if MPL_OK:
            self._xml_fig    = Figure(figsize=(10,3.5), dpi=96, facecolor=COLORS["bg"])
            self._xml_ax     = self._xml_fig.add_subplot(111)
            self._xml_ax.set_facecolor(COLORS["bg_light"])
            self._xml_canvas = FigureCanvasTkAgg(self._xml_fig, master=self._xml_chart_frame)
            self._xml_canvas.draw()
            self._xml_canvas.get_tk_widget().pack(fill="both", expand=True)
        else:
            tk.Label(self._xml_chart_frame, text="matplotlib not available",
                     bg=COLORS["bg"], fg=COLORS["muted"]).pack()

    def _load_xml_replay(self):
        path = askopenfilename(filetypes=[("XML","*.xml"),("All","*.*")])
        if path:
            self._render_xml(path)

    def _open_latest_xml(self):
        project = self.cfg.influx.project or "default"
        path    = xml_path(project)
        if path.exists():
            self._render_xml(str(path))
        else:
            messagebox.showinfo("Not found",
                                f"No XML file found for project '{project}'.\n"
                                f"Expected: {path}")

    def _render_xml(self, path: str):
        recs = load_xml_alerts(path)
        for item in self._xml_tree.get_children():
            self._xml_tree.delete(item)
        for r in recs:
            tag = "critical" if r["alert_level"]=="Critical" else "warning"
            self._xml_tree.insert("","end", tags=(tag,), values=(
                r["timestamp"], r["cycle"], r["rule"], r["sensor"],
                r["dimension"], f"{r['max_value']:.5f}",
                f"{r['threshold']:.5f}", r["alert_level"],
            ))
        self._log_msg(f"Loaded {len(recs)} violations from {path}", "ok")
        if not MPL_OK or not recs:
            return
        # Plot: max values per (sensor, dim) over time
        ax = self._xml_ax
        ax.cla()
        ax.set_facecolor(COLORS["bg_light"])
        ax.tick_params(colors=COLORS["muted"], labelsize=8)
        ax.set_title("Alert Max Values from XML", color=COLORS["muted"], fontsize=9)
        by_key: Dict[str,List] = defaultdict(list)
        for r in recs:
            k = f"{r['sensor']}/{r['dimension']}"
            by_key[k].append((r["timestamp"], r["max_value"]))
        for i,(k,pts) in enumerate(by_key.items()):
            pts.sort(key=lambda x: x[0])
            xs = list(range(len(pts)))
            ys = [p[1] for p in pts]
            col = DIM_PALETTE[i % len(DIM_PALETTE)]
            if xs:
                ml, sl, bl = ax.stem(
                    xs, ys,
                    linefmt=col,
                    markerfmt="o",
                    basefmt=" ",
                    label=k,
                )
                plt.setp(sl, linewidth=1.2, alpha=0.75)
                plt.setp(ml, markersize=4,
                         markerfacecolor=col, markeredgecolor=col)
        ax.legend(loc="upper left", fontsize=7, facecolor=COLORS["bg_mid"],
                  labelcolor=COLORS["text"], edgecolor="#3a4060")
        ax.set_xlabel("Alert index", color=COLORS["muted"], fontsize=8)
        ax.set_ylabel("Max value", color=COLORS["muted"], fontsize=8)
        for spine in ax.spines.values():
            spine.set_edgecolor("#3a4060")
        self._xml_fig.set_facecolor(COLORS["bg"])
        self._xml_fig.tight_layout()
        self._xml_canvas.draw_idle()

    # ══════════════════════════════════════════════════════════════════════════
    # Tab: Log
    # ══════════════════════════════════════════════════════════════════════════

    def _tab_log(self, nb):
        f = ttk.Frame(nb, padding=8)
        nb.add(f, text="  📋  Activity Log  ")
        br = ttk.Frame(f); br.pack(fill="x", pady=(0,6))
        ttk.Button(br, text="Clear",    command=self._clear_log).pack(side="left")
        ttk.Button(br, text="Save Log", command=self._save_log).pack(side="left",padx=6)
        self._log_widget = scrolledtext.ScrolledText(
            f, state="disabled", wrap="word",
            bg="#12151f", fg=COLORS["muted"], font=("Courier New",9), relief="flat")
        self._log_widget.pack(fill="both", expand=True)
        self._log_widget.tag_configure("error",   foreground=COLORS["red"])
        self._log_widget.tag_configure("warning", foreground=COLORS["orange"])
        self._log_widget.tag_configure("ok",      foreground=COLORS["green"])
        self._log_widget.tag_configure("section", foreground=COLORS["acc"])
        self._log_widget.tag_configure("info",    foreground=COLORS["muted"])

    # ══════════════════════════════════════════════════════════════════════════
    # Config I/O
    # ══════════════════════════════════════════════════════════════════════════

    def _load_cfg_to_ui(self):
        c = self.cfg
        for k in ("url","token","org"):
            self._cv[k].set(getattr(c.influx,k,""))
        self._cv["bucket"].set(c.influx.bucket)
        self._cv["project"].set(c.influx.project)
        self._fields_var.set(",".join(c.influx.fields))
        self._range_var.set(c.influx.range_window)
        self._agg_every_var.set(c.influx.aggregate_every)
        self._use_agg_var.set(c.influx.use_aggregation)
        self._max_raw_var.set(str(c.influx.max_raw_points))
        self._timeout_var.set(str(c.influx.timeout_seconds))
        self._interval_var.set(str(c.monitor.interval_seconds))
        for dim, var in self._dim_vars.items():
            var.set(dim in c.monitor.selected_dimensions)
        self._win32_var.set(c.email.use_win32)
        self._win32_from_var.set(c.email.win32_from_account)
        self._on_win32_toggle()   # sync widget state
        for k, v in self._ev.items():
            v.set(str(getattr(c.email,k,"")))
        self._tls_var.set(c.email.use_tls)
        self._logo_var.set(c.email.logo_path)
        self._subj_var.set(c.email.subject_template)
        self._body_txt.delete("1.0","end")
        self._body_txt.insert("1.0", c.email.body_template)
        self._rec_lb.delete(0,"end")
        for addr in [a.strip() for a in c.email.to_addrs.split(",") if a.strip()]:
            self._rec_lb.insert("end",addr)
        # SMS
        self._sms_enabled_var.set(c.sms.enabled)
        for k,v in self._sv.items():
            v.set(str(getattr(c.sms,k,"")))
        self._sms_body_txt.delete("1.0","end")
        self._sms_body_txt.insert("1.0", c.sms.body_template)
        self._sms_rec_lb.delete(0,"end")
        for num in [n.strip() for n in c.sms.to_numbers.split(",") if n.strip()]:
            self._sms_rec_lb.insert("end",num)
        self._refresh_tree()

    def _collect_cfg_from_ui(self):
        c = self.cfg
        import traceback
        
        # Influx settings
        for k, v in self._cv.items():
            try: setattr(c.influx, k, v.get().strip())
            except Exception as e: print(f"collect err {k}: {e}")
        try: c.influx.fields = [x.strip() for x in getattr(self, "_fields_var").get().split(",") if x.strip()]
        except Exception as e: print(f"collect err fields: {e}")
        for attr, var_name in [("range_window", "_range_var"), ("aggregate_every", "_agg_every_var")]:
            try: setattr(c.influx, attr, getattr(self, var_name).get().strip())
            except Exception as e: print(f"collect err {attr}: {e}")
        try: c.influx.use_aggregation = getattr(self, "_use_agg_var").get()
        except Exception as e: print(f"collect err use_agg: {e}")
        try: c.influx.max_raw_points = _sint(getattr(self, "_max_raw_var").get(), 50000)
        except Exception as e: print(f"collect err max_raw: {e}")
        try: c.influx.timeout_seconds = _sint(getattr(self, "_timeout_var").get(), 30)
        except Exception as e: print(f"collect err timeout: {e}")

        # Monitor settings
        try: c.monitor.interval_seconds = _sint(getattr(self, "_interval_var").get(), 60)
        except Exception as e: print(f"collect err interval: {e}")
        try: c.monitor.selected_dimensions = [d for d, v in getattr(self, "_dim_vars", {}).items() if v.get()]
        except Exception as e: print(f"collect err dims: {e}")
        try: c.monitor.selected_sensors = list(getattr(self, "_sensor_lb").get(0, "end"))
        except Exception as e: print(f"collect err sensors: {e}")

        # Email settings
        try: c.email.use_win32 = getattr(self, "_win32_var").get()
        except Exception as e: print(f"collect err win32: {e}")
        try: c.email.win32_from_account = getattr(self, "_win32_from_var").get().strip()
        except Exception as e: print(f"collect err win32_from: {e}")
        try:
            for k, v in getattr(self, "_ev", {}).items():
                raw = v.get().strip()
                setattr(c.email, k, _sint(raw, 587) if k == "smtp_port" else raw)
        except Exception as e: print(f"collect err ev: {e}")
        try: c.email.use_tls = getattr(self, "_tls_var").get()
        except Exception as e: print(f"collect err tls: {e}")
        try: c.email.logo_path = getattr(self, "_logo_var").get().strip()
        except Exception as e: print(f"collect err logo: {e}")
        try: c.email.subject_template = getattr(self, "_subj_var").get()
        except Exception as e: print(f"collect err subj: {e}")
        try: c.email.body_template = getattr(self, "_body_txt").get("1.0", "end-1c")
        except Exception as e: print(f"collect err body: {e}")
        try: c.email.to_addrs = ", ".join(getattr(self, "_rec_lb").get(0, "end"))
        except Exception as e: print(f"collect err addrs: {e}")

        # SMS
        try: c.sms.enabled = getattr(self, "_sms_enabled_var").get()
        except Exception as e: print(f"collect err sms enabled: {e}")
        try:
            for k, v in getattr(self, "_sv", {}).items():
                setattr(c.sms, k, v.get().strip())
        except Exception as e: print(f"collect err sv: {e}")
        try: c.sms.body_template = getattr(self, "_sms_body_txt").get("1.0", "end-1c")
        except Exception as e: print(f"collect err sms body: {e}")
        try: c.sms.to_numbers = ", ".join(getattr(self, "_sms_rec_lb").get(0, "end"))
        except Exception as e: print(f"collect err sms addrs: {e}")




    def _save_cfg(self):
        self._collect_cfg_from_ui()
        self.cfg.save()
        self._log_msg("Configuration saved.", "ok")
        messagebox.showinfo("Saved","Configuration saved to influx_alert_config.json")

    # ══════════════════════════════════════════════════════════════════════════
    # Monitor control
    # ══════════════════════════════════════════════════════════════════════════

    def _toggle(self):
        if self._running: self._stop_monitor()
        else:             self._start_monitor()

    def _start_monitor(self):
        self._collect_cfg_from_ui()
        # Always sync interval directly so the engine sees the correct value
        try:
            self.cfg.monitor.interval_seconds = _sint(self._interval_var.get(), 60)
        except Exception:
            pass
        
        # Start fresh: reset cooldown timers on all rules
        for rule in self.cfg.thresholds:
            rule._last_alerted = 0.0
            
        self._engine = AlertEngine(self.cfg, self._handle_alert, self._handle_data)
        self._engine.start()
        self._running = True
        self._start_btn.config(text="⏹  Stop Monitoring", style="Stop.TButton")
        self._status_var.set("🟢  Running")

    def _stop_monitor(self):
        if self._engine: self._engine.stop()
        self._running = False
        self._start_btn.config(text="▶  Start Monitoring", style="Accent.TButton")
        self._status_var.set("⏹  Stopped")

    def _handle_data(self, payload: dict):
        """Called from background thread — push to queue for GUI update."""
        DATA_QUEUE.put(payload)

    def _handle_alert(self, violations: List[dict], cycle: int):
        """Called from background thread."""
        subject, body = compose_email(self.cfg.email, self.cfg.influx, violations, cycle)
        logo = self.cfg.email.logo_path or ("logo.png" if Path("logo.png").is_file() else "")
        
        # Count new vs pending violations
        new_count = len([v for v in violations if v.get("violation_type") != "pending"])
        pending_count = len([v for v in violations if v.get("violation_type") == "pending"])
        total_count = len(violations)
        
        if pending_count > 0:
            msg = f"ALERT – {new_count} new + {pending_count} pending = {total_count} total violation(s). Sending email…"
        else:
            msg = f"ALERT – {total_count} violation(s). Sending email…"
        LOG_QUEUE.put((msg, "warning"))

        ts        = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
        max_level = "Critical" if any(v["alert_level"]=="Critical" for v in violations) else "Warning"

        # Email
        try:
            send_email(self.cfg.email, subject, body, logo or None,
                       violations=violations,
                       influx_project=self.cfg.influx.project,
                       timestamp=ts,
                       cycle=cycle,
                       max_level=max_level,
                       version=APP_VERSION)
            LOG_QUEUE.put((f"Email sent to: {self.cfg.email.to_addrs}","ok"))
        except Exception as exc:
            LOG_QUEUE.put((f"Email FAILED: {exc}","error"))

        # SMS
        if self.cfg.sms.enabled:
            try:
                send_sms(self.cfg.sms, self.cfg.influx, violations, cycle)
                LOG_QUEUE.put(("SMS sent.","ok"))
            except Exception as exc:
                LOG_QUEUE.put((f"SMS FAILED: {exc}","error"))

        # XML
        try:
            xml_file = append_xml_alerts(self.cfg.influx.project, cycle, violations)
            LOG_QUEUE.put((f"XML record saved: {xml_file}","ok"))
        except Exception as exc:
            LOG_QUEUE.put((f"XML save FAILED: {exc}","error"))

        # GUI update (from main thread via pump)
        DATA_QUEUE.put({"violations": violations, "cycle": cycle})

    # ══════════════════════════════════════════════════════════════════════════
    # Threshold CRUD
    # ══════════════════════════════════════════════════════════════════════════

    def _refresh_tree(self):
        for item in self._tree.get_children():
            self._tree.delete(item)
        for rule in self.cfg.thresholds:
            tag = ("disabled" if not rule.enabled
                   else "critical" if rule.alert_level=="Critical"
                   else "warning")
            self._tree.insert("","end", tags=(tag,), values=(
                rule.name, rule.sensor_filter, rule.dimension, rule.operator,
                f"{rule.value:.5g}", rule.alert_level,
                f"{rule.cooldown_minutes}m", "✓" if rule.enabled else "✗",
            ))

    def _add_rule(self):    RuleDialog(self, None, self._on_rule_saved)
    def _edit_rule(self):
        sel = self._tree.selection()
        if sel:
            RuleDialog(self, self.cfg.thresholds[self._tree.index(sel[0])], self._on_rule_saved, self._tree.index(sel[0]))
    def _remove_rule(self):
        sel = self._tree.selection()
        if not sel: return
        if messagebox.askyesno("Remove",f"Remove {len(sel)} rule(s)?"):
            for idx in sorted([self._tree.index(s) for s in sel],reverse=True):
                del self.cfg.thresholds[idx]
            self._refresh_tree()
    def _on_rule_saved(self, rule: ThresholdRule, idx: Optional[int]):
        if idx is None: self.cfg.thresholds.append(rule)
        else:           self.cfg.thresholds[idx] = rule
        self._refresh_tree()
    def _all_enabled(self, state):
        for rule in self.cfg.thresholds: rule.enabled = state
        self._refresh_tree()

    # ══════════════════════════════════════════════════════════════════════════
    # InfluxDB connect cascade
    # ══════════════════════════════════════════════════════════════════════════

    def _apply_defaults(self):
        self._cv["url"].set("https://api-influx1.sensorstation.com.au")
        self._cv["token"].set(
            "flVRjcBkSwqi4MhzYKWHOzIIOrzoCRJxaqoeWcE73JuF-q95gj0dHwOUd5nWMuE3_JAPod9eagN5yMlXz9flAg=="
        )
        self._cv["org"].set("sensorstation")
        self._conn_status.config(text="● Defaults applied — click Connect & Load",
                                  fg=COLORS["orange"])
        self._log_msg("Default connection values applied.", "warning")

    def _connect_influx(self):
        if not INFLUX_OK:
            messagebox.showerror("Missing","influxdb-client not available.\n"
                                 "Run: pip install influxdb-client"); return
        url   = self._cv["url"].get().strip()
        token = self._cv["token"].get().strip()
        org   = self._cv["org"].get().strip()
        if not (url and token and org):
            messagebox.showerror("Missing","Fill in URL, Token, and Organisation."); return
        self._conn_status.config(text="● Connecting…", fg=COLORS["orange"])
        self.update_idletasks()
        try:
            timeout = _sint(self._timeout_var.get(),30)
            self._influx_client = InfluxDBClient(url=url,token=token,org=org,timeout=timeout*1000)
            buckets = [b.name for b in self._influx_client.buckets_api().find_buckets().buckets]
            self._bucket_combo["values"] = buckets
            if buckets:
                if self._cv["bucket"].get() not in buckets:
                    self._cv["bucket"].set(buckets[0])
                self._on_bucket_changed()
            self._conn_status.config(text="● Connected", fg=COLORS["green"])
            self._log_msg(f"Connected to {url}  —  {len(buckets)} bucket(s) found.", "ok")
        except Exception as exc:
            self._conn_status.config(text="● Failed", fg=COLORS["red"])
            self._log_msg(f"Connection failed: {exc}", "error")
            messagebox.showerror("Connection Failed", str(exc))

    def _on_bucket_changed(self, event=None):
        client = self._influx_client
        if not client: return
        bucket = self._cv["bucket"].get()
        org    = self._cv["org"].get().strip()
        if not bucket: return
        q = f'import "influxdata/influxdb/schema"\nschema.measurements(bucket: "{bucket}")'
        try:
            result       = client.query_api().query(q, org=org)
            measurements = sorted({rec.values.get("_value") for tbl in result
                                    for rec in tbl.records if rec.values.get("_value")})
            self._project_combo["values"] = measurements
            if measurements:
                if self._cv["project"].get() not in measurements:
                    self._cv["project"].set(measurements[0])
                self._on_project_changed()
        except Exception as exc:
            self._log_msg(f"Failed to load measurements: {exc}", "error")

    def _on_project_changed(self, event=None):
        client = self._influx_client
        if not client: return
        bucket  = self._cv["bucket"].get()
        project = self._cv["project"].get()
        org     = self._cv["org"].get().strip()
        if not (bucket and project): return
        sensors = fetch_sensors_influx(client.query_api(), bucket, project)
        self._sensor_lb.delete(0,"end")
        for s in sensors: self._sensor_lb.insert("end",s)
        q = (f'import "influxdata/influxdb/schema"\n'
             f'schema.fieldKeys(bucket: "{bucket}", predicate: (r) => r._measurement == "{project}")')
        try:
            result = client.query_api().query(q, org=org)
            fields = sorted({rec.values.get("_value") for tbl in result
                             for rec in tbl.records if rec.values.get("_value")})
            if fields: self._fields_var.set(",".join(fields))
        except Exception: pass
        self._log_msg(f"Project '{project}': {len(sensors)} sensor(s) loaded.", "ok")

    def _test_connection(self):
        self._collect_cfg_from_ui()
        if not INFLUX_OK:
            messagebox.showerror("Missing","influxdb-client not available."); return
        inf = self.cfg.influx
        try:
            client = InfluxDBClient(url=inf.url,token=inf.token,org=inf.org,timeout=10000)
            h      = client.health(); client.close()
            msg = f"Connected!  Status: {h.status}"
            self._conn_status.config(text="● Connected", fg=COLORS["green"])
            self._log_msg(msg,"ok")
            messagebox.showinfo("Connection OK", msg)
        except Exception as exc:
            self._conn_status.config(text="● Failed", fg=COLORS["red"])
            self._log_msg(f"Connection failed: {exc}","error")
            messagebox.showerror("Connection Failed", str(exc))

    def _load_sensors(self):
        self._collect_cfg_from_ui()
        if not INFLUX_OK:
            messagebox.showerror("Missing","influxdb-client not available."); return
        inf    = self.cfg.influx
        client = self._influx_client
        close  = False
        if client is None:
            try:
                client = InfluxDBClient(url=inf.url,token=inf.token,org=inf.org,timeout=15000)
                close  = True
            except Exception as exc:
                messagebox.showerror("Error",str(exc)); return
        try:
            sensors = fetch_sensors_influx(client.query_api(), inf.bucket, inf.project)
            self._sensor_lb.delete(0,"end")
            for s in sensors: self._sensor_lb.insert("end",s)
            self._log_msg(f"Loaded {len(sensors)} sensor(s).", "ok")
        except Exception as exc:
            messagebox.showerror("Error",f"Failed to load sensors:\n{exc}")
        finally:
            if close: client.close()

    # ══════════════════════════════════════════════════════════════════════════
    # Email actions
    # ══════════════════════════════════════════════════════════════════════════

    def _on_win32_toggle(self):
        """Grey out SMTP widgets when Outlook WIN32 mode is active."""
        state = "disabled" if self._win32_var.get() else "normal"
        for w in self._smtp_widgets:
            try:
                w.configure(state=state)
            except Exception:
                pass

    def _browse_logo(self):
        """Open file dialog for logo selection.
        Using a named method avoids issues with lambda capture inside scrollable canvas."""
        self.update_idletasks()
        path = askopenfilename(
            parent=self,
            title="Select logo image",
            filetypes=[("PNG","*.png"),("Image","*.jpg *.png"),("All","*.*")],
        )
        if path:
            self._logo_var.set(path)

    def _dummy_violations(self) -> List[dict]:

        return [
            {"rule":"High Velocity","sensor":"SN-101","dimension":"velx",
             "max_value":3.8765,"threshold":3.5,"operator":">=","alert_level":"Warning"},
            {"rule":"Critical Spike","sensor":"SN-202","dimension":"velz",
             "max_value":8.1234,"threshold":5.0,"operator":">=","alert_level":"Critical"},
        ]

    def _send_test(self):
        self._collect_cfg_from_ui()
        dummy   = self._dummy_violations()
        subject, body = compose_email(self.cfg.email, self.cfg.influx, dummy, 0)
        logo = self.cfg.email.logo_path or ("logo.png" if Path("logo.png").is_file() else "")
        try:
            send_email(self.cfg.email, subject, body, logo or None)
            self._log_msg("Test email sent successfully.","ok")
            messagebox.showinfo("Sent","Test email sent!")
        except Exception as exc:
            self._log_msg(f"Test email failed: {exc}","error")
            messagebox.showerror("Email Error",str(exc))

    def _preview_email(self):
        self._collect_cfg_from_ui()
        dummy   = self._dummy_violations()
        subject, body = compose_email(self.cfg.email, self.cfg.influx, dummy, 7)
        EmailPreviewDialog(self, subject, body, self.cfg.email.logo_path)

    def _send_demo(self):
        """Send a demo (dummy data) email to all configured recipients."""
        self._collect_cfg_from_ui()
        if not self._rec_lb.size():
            messagebox.showwarning("No recipients","Add at least one recipient first."); return
        dummy   = self._dummy_violations()
        subject, body = compose_email(self.cfg.email, self.cfg.influx, dummy, 99)
        logo = self.cfg.email.logo_path or ("logo.png" if Path("logo.png").is_file() else "")
        try:
            send_email(self.cfg.email, subject, body, logo or None)
            self._log_msg(f"Demo email sent to: {self.cfg.email.to_addrs}","ok")
            messagebox.showinfo("Demo Sent",f"Demo email sent to:\n{self.cfg.email.to_addrs}")
        except Exception as exc:
            self._log_msg(f"Demo email failed: {exc}","error")
            messagebox.showerror("Email Error",str(exc))

    # ══════════════════════════════════════════════════════════════════════════
    # SMS actions
    # ══════════════════════════════════════════════════════════════════════════

    def _send_test_sms(self):
        self._collect_cfg_from_ui()
        dummy = self._dummy_violations()
        try:
            send_sms(self.cfg.sms, self.cfg.influx, dummy, 0)
            self._log_msg("Test SMS sent.","ok")
            messagebox.showinfo("Sent","Test SMS sent!")
        except Exception as exc:
            self._log_msg(f"Test SMS failed: {exc}","error")
            messagebox.showerror("SMS Error",str(exc))

    # ══════════════════════════════════════════════════════════════════════════
    # Recipients helpers
    # ══════════════════════════════════════════════════════════════════════════

    def _add_recipient(self):
        addr = self._new_rec_var.get().strip()
        if not addr:
            # Fallback dialog — works even when focus is trapped in scrollable canvas
            import tkinter.simpledialog as sd
            self.focus_force()
            addr = (sd.askstring("Add Recipient", "Enter email address:", parent=self) or "").strip()
            if not addr:
                return
        if addr in self._rec_lb.get(0,"end"):
            messagebox.showwarning("Duplicate", f"{addr} already in list."); return
        self._rec_lb.insert("end", addr)
        self._new_rec_var.set("")


    def _remove_recipient(self):
        for idx in reversed(self._rec_lb.curselection()):
            self._rec_lb.delete(idx)

    def _add_sms_recipient(self):
        num = self._new_sms_var.get().strip()
        if not num: return
        if num in self._sms_rec_lb.get(0,"end"):
            messagebox.showwarning("Duplicate",f"{num} already in list."); return
        self._sms_rec_lb.insert("end",num)
        self._new_sms_var.set("")

    def _remove_sms_recipient(self):
        for idx in reversed(self._sms_rec_lb.curselection()):
            self._sms_rec_lb.delete(idx)

    # ══════════════════════════════════════════════════════════════════════════
    # Log helpers
    # ══════════════════════════════════════════════════════════════════════════

    def _pump_log(self):
        while not LOG_QUEUE.empty():
            msg, tag = LOG_QUEUE.get_nowait()
            self._log_msg(msg, tag)
        self.after(250, self._pump_log)

    def _pump_data(self):
        while not DATA_QUEUE.empty():
            payload = DATA_QUEUE.get_nowait()
            if "max_vals" in payload:
                self._update_graphs(payload.get("ts",""), payload["max_vals"])
            if "violations" in payload:
                self.after(0, lambda p=payload: self._add_alert_record(
                    p["violations"], p.get("cycle",0)))
        
        # Check if 1 hour has passed to clear logs and graphs
        self._check_and_clear_logs_graphs()
        
        self.after(500, self._pump_data)
    
    def _check_and_clear_logs_graphs(self):
        """Clear logs and graphs every 1 hour, but keep alert records."""
        now = time.time()
        if now - self._last_clear_time >= self._clear_interval:
            # Clear graph history
            self._graph_history.clear()
            self._graph_ts.clear()
            
            # Redraw empty graphs if they exist
            if self._graph_axes_by_dim:
                for ax in self._graph_axes_by_dim.values():
                    ax.clear()
                if self._graph_single_canvas:
                    self._graph_single_canvas.draw()
            
            # Clear log display
            self._clear_log()
            
            # Log the clear event
            self._log_msg("──────────────────────────────────────────────────────────", "section")
            self._log_msg("🔄 1-hour mark reached: Logs and graphs cleared", "ok")
            self._log_msg("✓ Alert records preserved in XML | New graph cycle started", "ok")
            self._log_msg("──────────────────────────────────────────────────────────", "section")
            
            # Reset the timer
            self._last_clear_time = now

    def _log_msg(self, msg: str, tag: str = "info"):
        ts   = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}]  {msg}\n"
        self._log_widget.configure(state="normal")
        self._log_widget.insert("end", line, tag)
        self._log_widget.see("end")
        self._log_widget.configure(state="disabled")

    def _clear_log(self):
        """Clear the log widget display."""
        self._log_widget.configure(state="normal")
        self._log_widget.delete("1.0","end")
        self._log_widget.configure(state="disabled")

    def _save_log(self):
        path = asksaveasfilename(defaultextension=".txt",
                                 filetypes=[("Text","*.txt"),("All","*.*")])
        if path:
            Path(path).write_text(self._log_widget.get("1.0","end"), encoding="utf-8")

    def _on_close(self):
        if self._running:
            self._stop_monitor()
        try:
            self._collect_cfg_from_ui()
        except Exception:
            pass
        try:
            self.cfg.save()
        except Exception:
            pass
        self.destroy()


# ══════════════════════════════════════════════════════════════════════════════
# Dialogs
# ══════════════════════════════════════════════════════════════════════════════

class RuleDialog(tk.Toplevel):
    def __init__(self, parent, rule: Optional[ThresholdRule], callback, idx=None):
        super().__init__(parent)
        self.title("Add Rule" if rule is None else "Edit Rule")
        self.geometry("520x420")
        self.resizable(False,False)
        self.configure(bg=COLORS["bg"])
        self.grab_set()
        self._cb   = callback
        self._idx  = idx
        self._rule = rule or ThresholdRule()
        self._build()

    def _build(self):
        r = self._rule
        rows = [
            ("Rule Name",          "_name",    r.name,             "entry", None),
            ("Sensor Filter",      "_sensor",  r.sensor_filter,    "entry", None),
            ("Dimension",          "_dim",     r.dimension,        "combo", ["All","velx","vely","velz"]),
            ("Operator",           "_op",      r.operator,         "combo", [">=",">","=="]),
            ("Threshold Value",    "_val",     str(r.value),       "entry", None),
            ("Alert Level",        "_level",   r.alert_level,      "combo", ["Warning","Critical"]),
            ("Cooldown (minutes)", "_cooldown",str(r.cooldown_minutes),"entry",None),
        ]
        for i,(lbl,attr,default,kind,opts) in enumerate(rows):
            tk.Label(self, text=lbl+":", bg=COLORS["bg"], fg=COLORS["muted"],
                     font=("Segoe UI",9), anchor="w", width=20).grid(
                row=i, column=0, sticky="w", padx=(16,8), pady=8)
            var = tk.StringVar(value=default)
            setattr(self,attr,var)
            if kind=="combo":
                w = ttk.Combobox(self, textvariable=var, values=opts, state="readonly", width=24)
            else:
                w = ttk.Entry(self, textvariable=var, width=26)
            w.grid(row=i, column=1, sticky="w", padx=(0,16), pady=8)
        self._en = tk.BooleanVar(value=r.enabled)
        ttk.Checkbutton(self, text="Enabled", variable=self._en).grid(
            row=len(rows), column=0, columnspan=2, sticky="w", padx=16, pady=(4,0))
        hint = tk.Label(self,
            text="Checks max absolute value per sensor/dimension each poll cycle.\n"
                 "Threshold line will be drawn on the live graph automatically.",
            bg=COLORS["toolbar"], fg=COLORS["dim"], font=("Segoe UI",8), padx=10, pady=6)
        hint.grid(row=len(rows)+1, column=0, columnspan=2, sticky="ew", padx=12, pady=8)
        bf = ttk.Frame(self)
        bf.grid(row=len(rows)+2, column=0, columnspan=2, pady=12)
        ttk.Button(bf, text="Save",   command=self._save,    style="Accent.TButton", width=14).pack(side="left",padx=6)
        ttk.Button(bf, text="Cancel", command=self.destroy,                           width=14).pack(side="left")

    def _save(self):
        try:   val = float(self._val.get())
        except ValueError: messagebox.showerror("Invalid","Threshold must be a number.",parent=self); return
        try:   cd = int(self._cooldown.get())
        except ValueError: messagebox.showerror("Invalid","Cooldown must be an integer.",parent=self); return
        rule = ThresholdRule(
            name=self._name.get().strip() or "Unnamed",
            sensor_filter=self._sensor.get().strip() or "All",
            dimension=self._dim.get().strip() or "All",
            operator=self._op.get(), value=val,
            alert_level=self._level.get(), cooldown_minutes=cd, enabled=self._en.get(),
        )
        self._cb(rule, self._idx)
        self.destroy()


class EmailPreviewDialog(tk.Toplevel):
    def __init__(self, parent, subject: str, body: str, logo_path: str = ""):
        super().__init__(parent)
        self.title("Email Preview")
        self.geometry("700x600")
        self.configure(bg=COLORS["bg"])

        # Logo preview
        if logo_path and Path(logo_path).is_file() and MPL_OK:
            try:
                from PIL import Image, ImageTk
                img   = Image.open(logo_path)
                img.thumbnail((280,80))
                photo = ImageTk.PhotoImage(img)
                lbl   = tk.Label(self, image=photo, bg=COLORS["bg"])
                lbl.image = photo
                lbl.pack(pady=(12,4))
            except Exception:
                tk.Label(self, text=f"Logo: {Path(logo_path).name}",
                         bg=COLORS["bg"], fg=COLORS["muted"],
                         font=("Segoe UI",8)).pack(pady=(10,0))
        elif logo_path:
            tk.Label(self, text=f"Logo: {Path(logo_path).name}",
                     bg=COLORS["bg"], fg=COLORS["muted"],
                     font=("Segoe UI",8)).pack(pady=(10,0))

        tk.Label(self, text="Subject:", bg=COLORS["bg"], fg=COLORS["muted"],
                 font=("Segoe UI",9,"bold")).pack(anchor="w",padx=14,pady=(10,2))
        tk.Label(self, text=subject, bg=COLORS["toolbar"], fg=COLORS["text"],
                 font=("Segoe UI",10), padx=10, pady=6, anchor="w").pack(fill="x",padx=10)
        tk.Label(self, text="Body:", bg=COLORS["bg"], fg=COLORS["muted"],
                 font=("Segoe UI",9,"bold")).pack(anchor="w",padx=14,pady=(10,2))
        txt = scrolledtext.ScrolledText(
            self, width=80, height=20, wrap="word",
            bg=COLORS["bg_light"], fg=COLORS["text"], font=("Courier New",9), relief="flat")
        txt.pack(fill="both",expand=True,padx=10)
        txt.insert("1.0",body)
        txt.configure(state="disabled")
        ttk.Button(self, text="Close", command=self.destroy).pack(pady=10)


# ══════════════════════════════════════════════════════════════════════════════
# Utility
# ══════════════════════════════════════════════════════════════════════════════

def _sint(value, default: int) -> int:
    try:   return int(value)
    except: return default


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════

def _start_flask_app(main_app):
    from flask import Flask, jsonify
    import logging
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)
    remote_app = Flask(__name__)

    @remote_app.route('/status', methods=['GET'])
    def api_status():
        st = main_app._status_var.get() if main_app else "Stopped"
        return jsonify({"status": st})

    @remote_app.route('/start', methods=['POST', 'GET'])
    def api_start():
        if main_app and not main_app._running:
            main_app.after(0, main_app._toggle)
        return jsonify({"message": "Monitor started or already running"})

    @remote_app.route('/stop', methods=['POST', 'GET'])
    def api_stop():
        if main_app and main_app._running:
            main_app.after(0, main_app._toggle)
        return jsonify({"message": "Monitor stopped or already stopped"})

    import threading
    t = threading.Thread(target=lambda: remote_app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False), daemon=True)
    t.start()

if __name__ == "__main__":
    missing = []
    if not INFLUX_OK:  missing.append("influxdb-client")
    if not MPL_OK:     missing.append("matplotlib")
    if not TWILIO_OK:  missing.append("twilio  (optional, for SMS)")
    if missing:
        print("Optional/required packages not found:")
        for m in missing:
            print(f"  pip install {m}")
    app = App()
    try:
        import importlib.util
        if importlib.util.find_spec("flask"):
            _start_flask_app(app)
            print("Flask remote control running on port 5000")
        else:
            print("Flask not installed. Remote control unavailable.")
    except Exception as e:
        print("Flask remote control error:", e)
    app.mainloop()