from pathlib import Path
# === Reference file names (next to app.py) ===
CINTAS_LOCATION_TABLE_FILE = "MY LOCATION TABLE.xlsx"
COMPLETE_LOCATION_TABLE_FILE = "Coding_CintasLocation 02.06.25.xlsx"
LOCATION_CODES_CANDIDATES = [
   "Location Codes.xlsx",
   "LOCATION_CODES.xlsx",
   "location_codes.xlsx",
   "LocationCodes.xlsx",
   "all_location_codes.xlsx",
]
# === Branding ===
CINTAS_BLUE = "#003DA5"
CINTAS_RED = "#C8102E"
CINTAS_GRAY = "#F4F6F8"
APP_TITLE = "Cintas Logistics — Accrual Re-Coding"
APP_HEADER_HTML = """
<div class="cintas-header">
<h2 style="margin:0;">Cintas Logistics – Accrual Re-Coding Tool</h2>
<div style="opacity:0.85">
   Upload A3’s Accrual/Weekly Audit workbook. The app auto-loads Location Codes,
   MY LOCATION TABLE, and the Complete Coding table from this folder.
</div>
</div>
"""
