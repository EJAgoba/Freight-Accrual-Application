from pathlib import Path
# === Reference file names (next to app.py) ===
CINTAS_LOCATION_TABLE_FILE = "MY LOCATION TABLE.xlsx"
COMPLETE_LOCATION_TABLE_FILE = "Coding_CintasLocation 11.05.25.xlsx"
LOCATION_CODES_CANDIDATES = ["all_location_codes.xlsx"]
# constants.py
APP_TITLE = "Cintas Logistics — Accrual Re-Coding"
# Fresh palette (Tailwind-inspired)
PRIMARY = "#2563eb"     # blue-600
PRIMARY_HOVER = "#1d4ed8"
ACCENT = "#10b981"      # emerald-500
DANGER = "#ef4444"      # red-500
TEXT = "#0f172a"        # slate-900
TEXT_MUTED = "#475569"  # slate-600
BORDER = "#e2e8f0"      # slate-200
SURFACE = "#ffffff"
SURFACE_ALT = "#f8fafc" # slate-50
RING = "#93c5fd"        # blue-300
# Dark mode
D_TEXT = "#e5e7eb"        # slate-200
D_TEXT_MUTED = "#94a3b8"   # slate-400
D_BORDER = "#334155"       # slate-700
D_SURFACE = "#0b1220"      # almost-black blue
D_SURFACE_ALT = "#0f172a"  # slate-900
D_RING = "#3b82f6"         # blue-500
APP_HEADER_HTML = """
<div class="app-header">
<div class="app-title">Accrual Re-Coding Tool</div>
<div class="app-subtitle">Upload A3’s Accrual/Weekly Audit workbook. References auto-load from this folder.</div>
</div>
"""
