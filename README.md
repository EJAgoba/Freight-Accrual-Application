# Cintas Logistics — Accrual Re-Coding Tool
---
## Overview
The **Cintas Accrual Re-Coding Tool** automatically cleans, validates, and codes freight accrual and weekly audit files received from A3.  
It replaces hours of manual work every month by handling data cleanup, code assignment, and accounting alignment in just seconds.
### What the Tool Does
- Cleans and formats messy spreadsheets into standardized audit-ready outputs  
- Detects missing **Location Codes** and fills them automatically  
- Applies **Profit Center**, **Cost Center**, and **Account Number** logic  
- Ensures data aligns with **Cintas Accounting standards**  
- Produces clean, validated Excel and CSV exports ready for submission or upload  
The app now features a **modern interface** with:
- Light/dark mode toggle  
- Gradient hero header with clear branding  
- Rounded buttons and cards  
- Responsive padding to prevent clipping on all displays  
- Separate modes for **Accrual** and **Weekly Audit**  
- Optional **Redwood Accrual** module for pre-processing  
---
## Files the Tool Uses
All reference files must be stored in the same folder as **app.py** (whether locally or on your shared drive).
| File | Purpose | Key Columns |
|------|----------|-------------|
| **MY LOCATION TABLE.xlsx** | Master list of all Cintas locations. | `Loc Code`, `Loc_Address`, `Loc_City`, `Loc_ST`, `Type` |
| **Coding_CintasLocation 02.06.25.xlsx** | Links each `Loc Code` to its Profit and Cost Centers. | `Loc Code`, `Prof_Cntr`, `Cost_Cntr` |
| **Location Codes.xlsx** | Full list of valid 4-character location codes (e.g., `0K35`, `024P`, `067N`). | `Loc Code` |
> These reference files rarely change. Update them only when new locations are added or accounting structures are modified.
---
## User Interface
### 1. Header and Redwood Accrual Section
- **Title:** “Cintas Logistics — Accrual Re-Coding Tool”  
- **Subtitle:** “Upload A3’s Accrual/Weekly Audit workbook. References auto-load from this folder.”  
- Dedicated space for pre-processing raw Redwood data before re-coding  
- Accepts `.txt`, `.csv`, or `.xlsx` files and prepares them for the main pipeline
### 2. Accrual and Weekly Audit Files
- **Mode Selector:** Choose between *Accrual* or *Weekly Audit* workflows  
---
## How the Tool Works (Step-by-Step)
Each major process runs as a self-contained **module** for easier debugging and maintenance.
### Step 1 — Extract Location Codes (`extract_codes.py`)
Finds valid 4-character Cintas codes in the Consignor and Consignee columns. It converts text to lowercase, searches for codes, and fills them if missing.
| Consignee | Extracted Code |
|------------|----------------|
| Cintas 0K35 Receiving Dock | 0K35 |
| Millennium 024P Loading | 024P |
---
### Step 2 — Build Combined Addresses (`address_merge.py`)
Creates a unique matching key for each address by combining the first words of Address, City, and State. This helps match locations even when a code is missing.
| Address | City | State | Combined Address |
|----------|------|-------|------------------|
| 6800 Cintas Blvd | Mason | OH | 6800MASONOH |
---
### Step 3 — Cross-Reference Addresses (`address_crossref.py`)
Compares each shipment’s Combined Address with those in **MY LOCATION TABLE.xlsx**.  
If a match is found, the tool fills in missing Consignor or Consignee Codes (only if blank).
---
### Step 4 — Clean and Format Codes (`clean_codes.py`)
Standardizes codes so they’re uppercase and correctly padded.  
Example: `"24P"` → `"024P"`, `"ok35"` → `"0K35"`.
---
### Step 5 — Map the Location Types (`map_types.py`)
Assigns each location a type based on **MY LOCATION TABLE.xlsx**.
| Type | Meaning |
|------|----------|
| US DC | U.S. Distribution Center |
| CA DC | Canadian Distribution Center |
| LC | Local Cintas Branch or Service Location |
| MFG | Manufacturing Site |
| FAS DC | First Aid & Safety Distribution Center |
| MM | Millennium Mats |
| FL | Fire Location |
| FC | Fire Charging Location |
| Non-Cintas | Supplier or Vendor Site |
---
### Step 6 — Apply Matrix Logic (`matrix_map.py`)
Determines who the shipment should be charged to (origin or destination) based on Consignor Type and Consignee Type.
| Consignor Type | Consignee Type | Matrix Rule | Assigned Code |
|----------------|----------------|--------------|----------------|
| US DC | LC | DESTINATION | Consignee Code |
| LC | MFG | ORIGIN | Consignor Code |
| MFG | CA DC | DESTINATION | Consignee Code |
| Non-Cintas | US DC | SPECIAL | 0G59 |
---
### Step 7 — Enrich with Profit & Cost Centers
Uses the Assigned Location Code to pull matching **Profit Center** and **Cost Center** from *Coding_CintasLocation.xlsx*.
Adds new columns:
- `Profit Center EJ`
- `Cost Center EJ`
---
### Step 8 — Apply Account Number Rules
| Condition | Assigned Account # EJ |
|------------|-----------------------|
| `Profit Center EJ` contains “G59” | 621000 |
| `Consignee Code == Assigned Location Code` | 621000 |
| *All other cases* | 621020 |
---
### Step 9 — Accuracy Check and Deduplication
- Removes duplicate invoices if both **Invoice Number** and **Paid Amount** exist  
- Adds **Automation Accuracy** column:  
 - `1` = Profit Center matches original  
 - `0` = Different  
---
### Step 10 — Export Results
Exports clean, formatted **Excel** and **CSV** files including:  
`Profit Center EJ`, `Cost Center EJ`, `Account # EJ`, `Automation Accuracy`, and `Assigned Location Code`.
---
## Weekly Audit Accounting Summary
When running **Weekly Audit mode**, the app:
1. Reads both **USD** and **CAD** tabs  
2. Calculates totals and expands GST, HST, QST, and Duty if applicable  
3. Keeps **Account #** stored as text for Accounting uploads  
4. Generates an Excel workbook with two sheets — one for each currency  
Each file includes:
- Header = negative of total Paid/Paid Amount  
- Detail rows = grouped by Profit/Cost Center and Account #  
- Account # column preserved as pure text  
---
## Maintenance & Quality Checks
| Check | Goal |
|--------|------|
| Assigned Codes found | 100% |
| Automation Accuracy | ≥ 95% |
| Account # validation | 621000 (internal), 621020 (external) |
---
## Glossary
| Term | Meaning |
|------|----------|
| **Consignor** | Shipment origin |
| **Consignee** | Shipment destination |
| **Loc Code** | 4-character site code (e.g., `0K35`) |
| **Profit Center** | Entity that owns the cost |
| **Cost Center** | Department responsible for the expense |
| **Profit Center EJ, Cost Center EJ, Account # EJ** | Fields generated by the tool |
| **Assigned Location Code** | Final posting location |
| **Automation Accuracy** | 1 = matched, 0 = different |
---
## Tech Architecture (New)
| Module | Purpose |
|---------|----------|
| `theme.py` | Handles light/dark themes, responsive spacing, and modern header design. |
| `references.py` | Loads all Cintas reference files safely. |
| `upload_readers.py` | Reads and detects file type (.xlsx, .csv, .txt). |
| `pipeline.py` | Runs the complete re-coding process end-to-end. |
| `weekly_audit.py` | Builds Accounting Summary for Weekly Audit runs. |
| `exporters.py` | Creates Excel/CSV downloads in consistent formats. |
| `redwood_accrual.py` | Optional module for pre-processing Redwood reports. |
---
## Summary
> **The Accrual Re-Coding Tool** is now a fully automated, modular system built to Cintas accounting standards.  
> It handles 24/7 freight operations with accuracy, speed, and professional UI design — giving logistics and finance teams clean data, faster close cycles, and consistent reporting quality.
