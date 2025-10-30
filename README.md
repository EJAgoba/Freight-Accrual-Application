# Cintas Logistics — Accrual Re-Coding Tool
### Streamlit Application for Automated Freight Accrual Processing  
---
## Overview
The **Accrual Re-Coding Tool** automates how Cintas processes and codes freight files from A3 — both **Accrual** and **Weekly Audit** reports.
It reads spreadsheets, detects missing codes, maps Profit/Cost Centers, applies Account # rules, and exports **clean, audit-ready workbooks**.  
This process — once manual and time-consuming — now completes in seconds.
## Core Features
- Automatically fills missing **Location Codes**
- Applies official **Profit & Cost Center** logic
- Determines **Account #** using built-in business rules
- Generates **Weekly Audit Accounting Summary (USD/CAD)**
- Uses a **modular architecture** — each Python file serves a single purpose
- Exports **clean, coded, audit-ready workbooks** in Excel and CSV
---

All files are modular and must remain in the same folder as `app.py`.
```text
Freight-Accrual-Application/
├── MY LOCATION TABLE.xlsx              # Master list of all Cintas locations
├── Coding_CintasLocation 02.06.25.xlsx # Profit & Cost Center mapping
├── all_location_codes.xlsx              # Valid 4-char Cintas codes (any accepted name)
│
├── README.md                           # Full project documentation
├── requirements.txt                    # Python dependencies list
│
├── app.py                              # Backup/test version
├── app2.py                             # Main Streamlit interface (modern UI, dark/light theme)
│
├── constants.py                        # Branding, reusable constants
├── theme.py                            # CSS theme (light/dark + header + uploaders)
├── references.py                       # Loads and caches Excel reference files
├── upload_readers.py                   # Reads uploads (.xlsx/.csv/.txt) with delimiter detection
├── exporters.py                        # Builds Excel/CSV downloads (xlsxwriter + openpyxl)
├── pipeline.py                         # Central pipeline for Accrual logic
├── weekly_audit.py                     # Builds Weekly Audit Accounting Summary (USD/CAD)
├── redwood_accrual.py                  # Redwood Accrual pre-processing interface
│
├── extract_codes.py                    # Step 1: Extracts 4-character location codes
├── address_merge.py                    # Step 2: Builds Combined Address
├── address_crossref.py                 # Step 3: Cross-references addresses to MY LOCATION TABLE
├── clean_codes.py                      # Step 4: Cleans/pads/normalizes codes
├── map_types.py                        # Step 5: Maps Cintas Type (US DC, LC, MFG, etc.)
├── matrix_map.py                       # Step 6: Applies matrix logic for Assigned Location Code
├── coding_matrix.py                    # Optional: Holds special routing logic & mappings
│
├── location_codes.py                   # Utility helpers for code list validation
├── io_utils.py                         # Shared I/O helpers used across modules
│
├── cintas_logo.png                     # Optional logo for UI/header branding
└── assets/                             # Optional folder for alternate reference file storage
```
## What the Tool Does
- Cleans raw spreadsheets from **A3 Freight** (Accrual + Weekly Audit)
- Extracts **location codes**, even if embedded in text
- Uses address matching when codes are missing
- Classifies site type (US DC, LC, MFG, etc.)
- Applies **matrix logic** to assign chargeable location
- Links to **Profit Center**, **Cost Center**, and **Account #**
- Flags duplicates and automation accuracy
- Outputs **validated Excel/CSV** for upload
---
## Reference Files
These are auto-loaded by `references.py`. All must be in the **same folder** as `app.py`.
| File | Purpose | Key Columns |
|------|----------|-------------|
| **MY LOCATION TABLE.xlsx** | Official Cintas location master | `Loc Code`, `Loc_Address`, `Loc_City`, `Loc_ST`, `Type` |
| **Coding_CintasLocation 02.06.25.xlsx** | Profit & Cost Center mapping | `Loc Code`, `Prof_Cntr`, `Cost_Cntr` |
| **Location Codes.xlsx** *(or any variant)* | Full list of valid 4-char codes (`0K35`, `024P`) | `Loc Code` |
> Accepted variants: `Location Codes.xlsx`, `LOCATION_CODES.xlsx`, `location_codes.xlsx`, `LocationCodes.xlsx`, or `all_location_codes.xlsx`
---
## User Interface
**Theme:**  
- Gradient header with logo and rounded design  
- Upload buttons & radio selectors fully visible in dark mode  
- Responsive top padding (no clipping)
**Sections:**  
1. Redwood Accrual Upload  
2. Accrual vs Weekly Audit toggle  
3. Dynamic uploaders (accepting `.xlsx`, `.csv`, `.txt`)  
4. Auto-processing spinner  
5. Download buttons for XLSX and CSV  
---
## Accrual Pipeline (Step-by-Step)
### 1. Extract Location Codes (`extract_codes.py`)
Finds Cintas codes (e.g., `0K35`) from Consignor and Consignee text.
| Consignee | Extracted Code |
|------------|----------------|
| Cintas K35 | K35 |
| Millennium 037Q | 037Q |
---
### 2. Build Combined Address (`address_merge.py`)
Creates `Combined Address` = Address + City + State → used for matching.
| Address | City | State | Combined Address |
|----------|------|-------|------------------|
| 6800 Cintas Blvd | Mason | OH | 6800MASONOH |
---
### 3. Cross-Reference Addresses (`address_crossref.py`)
Matches `Combined Address` against **MY LOCATION TABLE.xlsx** to fill missing **Consignor Code** or **Consignee Code** (only when blank).

---
### 4. Clean Codes (clean_codes.py)
Standardizes capitalization and padding.
**Example:** 24P → 024P, ok35 → 0K35

### 5. Map Types (`map_types.py`)
Adds `Consignor Type` and `Consignee Type` based on the master table.
| Type | Meaning |
|------|----------|
| US DC | United States Distribution Center |
| CA DC | Canadian Distribution Center |
| LC | Local Cintas Branch / Service Location |
| MFG | Manufacturing |
| FAS DC | First Aid & Safety DC |
| MM | Millennium Mats |
| FL | Fire Location |
| FC | Fire Charging Location |
| Non-Cintas | External Vendor |
---
### 6. Matrix Logic (`matrix_map.py` + `coding_matrix.py`)
Determines which site the shipment is charged to (origin/destination/special).
| Consignor Type | Consignee Type | Rule | Assigned Location |
|----------------|----------------|------|-------------------|
| US DC | LC | DESTINATION | Consignee Code |
| LC | MFG | ORIGIN | Consignor Code |
| MFG | CA DC | DESTINATION | Consignee Code |
| Non-Cintas | US DC | SPECIAL | 0G59 |
---
### 7. Enrich with Profit & Cost Centers
Joins **Assigned Location Code** → `Coding_CintasLocation.xlsx` to pull:
- `Profit Center EJ`
- `Cost Center EJ`
---
### 8. Apply Account # EJ Logic
| Condition | Account # EJ |
|------------|---------------|
| Profit Center EJ contains “G59” | 621000 |
| Consignee Code = Assigned Code | 621000 |
| Else | 621020 |
---
### 9. Accuracy & De-duplication
- Drops duplicate rows by `Invoice Number + Paid Amount`  
- Adds column `Automation Accuracy` →  
 `1` = Profit Center matches, `0` = Different
---
### 10. Export (Excel + CSV)
Final output includes:
| Column | Description |
|---------|-------------|
| Profit Center EJ | Assigned profit center |
| Cost Center EJ | Assigned cost center |
| Account # EJ | Account number based on logic |
| Automation Accuracy | 1 = match / 0 = different |
| Assigned Location Code | Final location assignment |
---
## Weekly Audit → Accounting Summary
When running **Weekly Audit mode**, the app:
1. Reads both **USD** and **CAD** sheets  
2. Calculates header and detail totals  
3. Expands taxes (GST/PST, HST, QST, Duty)  
4. Keeps **Account #** as text (no scientific notation)  
5. Produces Excel with two sheets: USD & CAD  
**Header Rule:** Negative of `Paid / Paid Amount`  
**Detail Rule:** Uses `Total Paid Minus Duty and CAD Tax`
Tax Mappings:
| Tax | Default Account # |
|-----|--------------------|
| GST/PST | 203063 |
| HST | 203064 |
| QST | 203065 |
| Duty | 621010 |
Output filename example:  
`Weekly Audit Batch 2345 Sep-2025-W4 – Accounting Summary (Run 2345).xlsx`
---
## Exports
**Accrual Mode**
- `Accrual Sep-2025.xlsx`
- `Accrual Sep-2025.csv`
**Weekly Audit Mode**
- `Weekly Audit Batch {Batch} {Month-YYYY}-W{Week}.xlsx`
- `Weekly Audit Batch {Batch} {Month-YYYY} – Accounting Summary.xlsx`
---
## Quality Checks
| Metric | Target |
|---------|---------|
| Assigned Location Code found | 100% |
| Automation Accuracy | ≥ 95% |
| Account # Consistency | 621000 (Internal) / 621020 (External) |
---
## Troubleshooting
**“Reference load error: Missing required file”**  
→ Ensure `MY LOCATION TABLE.xlsx`, `Coding_CintasLocation 02.06.25.xlsx`, and your `all_location_codes.xlsx` are beside `app.py`.
**“Location Codes Excel not found … Expected one of …”**  
→ Rename your location codes file to match one of the valid names.
**UI header looks clipped**  
→ The latest `theme.py` adds top padding and spacer; refresh cache or reset zoom.
**Account # column turns scientific in Excel**  
→ Always use the generated **.xlsx** version; it forces text formatting.
---
## Glossary
| Term | Meaning |
|------|----------|
| **Consignor** | Shipment origin |
| **Consignee** | Shipment destination |
| **Loc Code** | 4-character location ID (e.g., `0K35`) |
| **Profit Center** | Entity that owns the cost |
| **Cost Center** | Department incurring expense |
| **Profit Center EJ / Cost Center EJ / Account # EJ** | Assigned by the tool |
| **Assigned Location Code** | Final posting site |
| **Automation Accuracy** | 1 = match, 0 = different |
---
## Module Summary
| File | Purpose |
|------|----------|
| `app.py` | Main Streamlit entrypoint |
| `app2.py` | Backup version |
| `constants.py` | Branding & variables |
| `theme.py` | Full CSS for light/dark UI |
| `references.py` | Loads/caches reference files |
| `upload_readers.py` | Smart file loaders |
| `exporters.py` | Excel/CSV output writer |
| `pipeline.py` | Accrual data pipeline |
| `weekly_audit.py` | Builds Accounting Summary |
| `redwood_accrual.py` | Redwood pre-processing |
| `extract_codes.py` | Step 1: Code extraction |
| `address_merge.py` | Step 2: Combined Address |
| `address_crossref.py` | Step 3: Cross-Reference |
| `clean_codes.py` | Step 4: Normalize Codes |
| `map_types.py` | Step 5: Assign Type |
| `matrix_map.py` | Step 6: Matrix Logic |
| `coding_matrix.py` | Special mapping logic |
| `io_utils.py` | Common IO utilities |
| `location_codes.py` | Code validation helpers |
---
## Security & Privacy
- No data leaves your local environment or Streamlit Cloud workspace.  
- Reference files stay internal to Cintas and should not be shared externally.  
- Always sanitize test data before public demo uploads.
---
## Change Log
**v2.0 (2025)**  
- Modular architecture (each step split into its own file)  
- New theme with dark/light mode  
- Polished header and spacing  
- Weekly Audit builder with accurate USD/CAD accounting  
- Account # column forced as text  
- Redwood Accrual integrated as standalone pre-processor  
**v1.0 (2024)**  
- Single-file Streamlit version (inline logic)
---
**© 2025 Cintas Corporation — Internal Logistics Automation Project**
