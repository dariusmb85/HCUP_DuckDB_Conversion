# HCUP DuckDB/Arrow Conversion & Revisit Pipeline

This repository contains utilities to convert HCUP ASCII/LOC datasets into modern columnar formats (Parquet via Arrow / DuckDB) **and** a modular R pipeline to compute revisit metrics consistent with the HCUP Revisit User Guide (e.g., discharge→next-admit within 30 days), with optional transfer detection and cohort flagging.

> **Highlights**
>
> * Fast IO using Arrow/DuckDB
> * Clean, modular R functions under `R/`
> * Process **either** SID **or** SEDD (choose at runtime)
> * HCUP‑style 30‑day revisit logic, transfer detection, clean‑period/index flags
> * CLI entrypoint for batch runs; interactive examples for exploration

---

## Repository Structure

```
HCUP_DuckDB_Conversion/
├─ R/
│  ├─ utils.R               # helpers: col normalization, year derivation, type safety
│  ├─ types.R               # standardize core types; optional label mappings
│  ├─ io.R                  # list/read Parquet files with SOURCE+FILE+YEAR
│  ├─ revisit.R             # core revisit metrics & LOS‑adjusted gap
│  ├─ transfers.R           # potential inter‑facility transfer detection
│  ├─ flags.R               # clean‑period/index flagging; prior‑events for a procedure
│  ├─ summaries.R           # overall summaries and cross‑year flows
│  └─ pipeline.R            # orchestrates a single‑source (SID/SEDD) run
├─ scripts/
│  └─ run_pipeline.R        # CLI entrypoint: Rscript-friendly wrapper
├─ data/                    # (optional) place raw/extracted data here
│  └─ <STATE>/(SID|SEDD)/*.parquet
├─ results/                 # (optional) summaries/exports written here
└─ README.md
```

> **Expected data layout (for the revisit pipeline)**
>
> Parquet files organized by state and source (case-sensitive):
>
> ```
> <STATE>/SID/*.parquet
> <STATE>/SEDD/*.parquet
> ```
>
> File names should include a 4‑digit year (e.g., `UT_SID_2019.parquet`) so the pipeline can extract `YEAR` from `FILE`.

---

## Requirements

* **R >= 4.2**
* Packages: `arrow`, `duckdb` (if you use DuckDB side), `dplyr`, `purrr`, `stringr`, `ggplot2` (optional for your own plots), `ggalluvial` (optional), `cli`, `readr`
* Sufficient disk/memory for Parquet files

Install core packages:

```r
install.packages(c("arrow", "duckdb", "dplyr", "purrr", "stringr", "ggplot2", "ggalluvial", "cli"))
```

> If you’re on Linux, ensure system dependencies for Arrow are installed (see Apache Arrow docs for your distro).

---

## Quick Start (Interactive R)

1. Source the modules:

```r
source("R/utils.R"); source("R/types.R"); source("R/io.R");
source("R/revisit.R"); source("R/transfers.R"); source("R/flags.R");
source("R/summaries.R"); source("R/pipeline.R")
```

2. Run the pipeline for a single **source** (SID **or** SEDD):

```r
res <- run_revisit_pipeline(
  state  = "UT",
  root   = "UT",         # expects UT/SID/*.parquet or UT/SEDD/*.parquet
  source = "SID",         # choose one: "SID" or "SEDD"
  years  = 2017:2019,      # NULL = all detected years
  dx_code  = "^250",      # optional: clean‑period/index flagger (example: diabetes)
  proc_code= "^36.1"       # optional: prior‑events for a procedure (example: CABG)
)

res$overall_summary     # total visits, 30‑day revisits, % revisit
head(res$cross_year_flows)
head(res$revisit_df)
```

3. (Optional) Write outputs:

```r
dir.create("results", showWarnings = FALSE)
readr::write_csv(res$overall_summary, file = "results/overall_summary.csv")
readr::write_csv(res$cross_year_flows, file = "results/cross_year_flows.csv")
```

---

## Command‑Line Usage (Batch)

Use the CLI wrapper to run without opening R:

```bash
Rscript scripts/run_pipeline.R <STATE> <ROOT_DIR> <SOURCE: SID|SEDD> [YEARS ...]
```

**Example:**

```bash
Rscript scripts/run_pipeline.R UT ./UT SID 2017 2018 2019
```

This prints an overall summary and a preview of cross‑year flows. You can modify the script to save artifacts as needed.

---

## Script Details (R/)

### `utils.R`

* `safe_as_numeric(x)`: as.numeric without warnings.
* `require_cols(df, cols)`: stop with a helpful message if columns are missing.
* `normalize_colnames(df)`: uppercase names for HCUP consistency.
* `add_year_from_file(df)`: extracts 4‑digit year from `FILE` if `YEAR` is absent.
* `nz(x)`: `!is.na(x)` helper.

### `types.R`

* `standardize_core_types(df)`: coerces `VISITLINK`→character; `DAYSTOEVENT`, `LOS`, `YEAR`, `FEMALE`, `RACE`, `PAY1`→numeric.
* `label_demographics(df)`: optional FEMALE/RACE labels.

### `io.R`

* `list_source_files(root, source_label, years = NULL)`: enumerates Parquet files for a state/source; filters by years.
* `read_hcup_files(files, source_label, cols = NULL)`: reads Parquet, normalizes names, adds `SOURCE`, `FILE`, `YEAR`, standardizes types; optional column pruning for memory.

### `revisit.R`

* `compute_revisits(df)`: sorts within `VISITLINK` by `DAYSTOEVENT`, computes:

  * `PREV_DAYSTOEVENT`, `PREV_LOS`, `DAYS_BETWEEN`
  * `GAP_DISCH_TO_NEXT = DAYS_BETWEEN - PREV_LOS`
  * `REVISIT_30DAY = 1` if `GAP_DISCH_TO_NEXT ∈ [0, 30]`, else `0`
* `add_stable_attributes(df)`: first non‑missing `AGE_FIRST`, `SEX_FIRST`, `PAYER_FIRST` per `VISITLINK`.

### `transfers.R`

* `detect_transfers(df)`: tolerant to casing (`DISPUNIFORM`/`DISPuniform`), checks for `ASOURCE`, `DSHOSPID`.
  Flags `IS_TRANSFER_OUT`, `IS_TRANSFER_IN`, and `POTENTIAL_TRANSFER` when hospital changes and timing matches prior LOS.

### `flags.R`

* `flag_clean_period(df, dx_code = "^250", clean_days = 180)`: row‑wise **clean period** and **index event** flags for a diagnosis (regex matched in `DX1`).
* `get_prior_events(df, proc_code = "^36.1")`: subsets events for patients with target procedure (regex in `PR1`) and returns **pre‑procedure** history.

### `summaries.R`

* `summary_overall(df)`: counts total visits, 30‑day revisits, and percent revisit.
* `cross_year_flows(df)`: transitions between `YEAR`s for 30‑day revisits.

### `pipeline.R`

* `run_revisit_pipeline(state, root, source = c("SID","SEDD"), years = NULL, dx_code = NULL, proc_code = NULL, cols = ...)`:

  1. Lists & reads Parquet for the chosen `source` only.
  2. Computes revisit metrics & stable attributes.
  3. Detects potential transfers (if columns exist).
  4. (Optional) Applies `dx_code` clean‑period flag.
  5. (Optional) Extracts pre‑`proc_code` events.
  6. Returns `revisit_df`, `overall_summary`, `cross_year_flows`, `prior_events`.

### `scripts/run_pipeline.R`

* Command‑line wrapper to run the pipeline headless. Adjust it to write outputs as CSV/Parquet.

---

## Converting HCUP ASCII/LOC → Parquet/DuckDB (Context)

Although the conversion scripts vary by state/year and are often bespoke to HCUP layouts, the general flow is:

1. Use LOC specs to create fixed‑width readers (R or Python).
2. Read ASCII into data frames with correct types.
3. Write to Parquet (via `arrow::write_parquet()`) and/or DuckDB tables.
