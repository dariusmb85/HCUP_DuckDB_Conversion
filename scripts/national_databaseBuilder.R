#!/usr/bin/env Rscript

# Convert headerless national HCUP CSVs (NEDS/NASS/NIS) to Parquet & DuckDB,
# pulling the column names from the online FileSpecifications TXT.

suppressPackageStartupMessages({
  library(readr)
  library(arrow)
  library(duckdb)
  library(DBI)
  library(here)
  library(dplyr)
  library(stringr)
})

# --------------------------
# COMMAND-LINE ARGUMENTS
# --------------------------
args <- commandArgs(trailingOnly = TRUE)

if (length(args) < 2) {
  stop("Usage: Rscript hcup_national_csv_to_parquet_duckdb.R <YEARS> <DATA_SOURCE>\n",
       "  YEARS examples: \"2016\" or \"2015q1q3 2015q4 2016\" (space/comma separated)\n",
       "  DATA_SOURCE: NEDS | NASS | NIS\n", call. = FALSE)
}

years <- unlist(strsplit(args[1], "[,\\s]+", perl = TRUE))
data_source <- toupper(args[2])

if (!data_source %in% c("NEDS", "NASS", "NIS")) {
  stop("Invalid data source. Choose from 'NEDS', 'NASS', or 'NIS'.")
}

# --------------------------
# CONFIGURATION (edit these paths/patterns to match your CSV location)
# --------------------------
data_type <- "CORE"  # as used in your CSV filenames
# Where your CSVs live. Can be a local path or http(s) base URL.
# Example local: here("NEDS_csv")
# Example remote HTTP: "https://example.org/hcup_csv"
csv_base <- here("national")  # change if needed

# Filename pattern (no header in CSV). Use {DATA_SOURCE}, {YEAR}, {TYPE}
# Example actual filename this builds: NEDS_2016_CORE.csv
csv_filename <- function(ds, year, type) sprintf("%s_%s_%s.csv", ds, year, type)

# Where to write outputs
out_base <- here('national','NEDS_converted')

# HCUP "missing" sentinels we want to treat as NA when reading CSVs
missing_values <- c(
  "-99", "-88", "-66", "-99.9999999", "-88.8888888", "-66.6666666",
  "-9", "-8", "-6", "-5", "-9999", "-8888", "-6666",
  "-99999999", "-999999999", "-888888888", "-666666666",
  "-999", "-888", "-666"
)

dir.create(out_base, recursive = TRUE, showWarnings = FALSE)

# --------------------------
# HELPERS
# --------------------------

get_column_positions <- function(loc_file) {
  lines <- readLines(loc_file)
  start_idx <- grep("^\\s*(?:\\d+\\s*-\\s*)?\\d+\\s+\\S+", lines, perl = TRUE)
  spec_lines <- lines[start_idx]
  
  regs <- regexec("^\\s*(?:(\\d+)\\s*-\\s*)?(\\d+)\\s+(.*?)\\s*$", spec_lines, perl = TRUE)
  mats <- regmatches(spec_lines, regs)
  
  parsed <- lapply(mats, function(m) {
    if (length(m) >= 4) {
      start <- if (nzchar(m[2])) as.integer(m[2]) else as.integer(m[3])  # if no start, use end
      end   <- as.integer(m[3])                                         # end or single position
      label <- trimws(m[4])
      list(start = start, end = end, label = label)
    } else {
      NULL
    }
  })
  
  parsed <- Filter(Negate(is.null), parsed)
  colspec <- do.call(rbind, lapply(parsed, as.data.frame))
  colspec$label <- make.unique(colspec$label)  # Avoid duplicate col names
  colspec
}

# https://hcup-us.ahrq.gov/db/nation/neds/stats/FileSpecifications_NEDS_2020_Core.TXT
# https://hcup-us.ahrq.gov/db/nation/neds/stats/FileSpecifications_NEDS_2020_CORE.TXT
fetch_metadata <- function(data_source, year) {
  lower_source <- tolower(data_source)
  if(data_type == "CORE"){
    data_type <- "Core"
  }
  meta_url <- paste0("https://hcup-us.ahrq.gov/db/nation/",
                     lower_source, "/stats/FileSpecifications", "_",
                     data_source, "_", year, "_", data_type, ".TXT")
  positions <- get_column_positions(meta_url)
  # readr::read_fwf(meta_url, positions, skip = 20)
  readr::read_fwf(meta_url,
                  col_positions = fwf_positions(
                    start = positions$start,
                    end = positions$end,
                    col_names = positions$label
                  ),
                  skip = 18)
}

# Get ordered column names from the metadata table
get_csv_colnames <- function(meta_tbl) {
  cols <- meta_tbl
  # Prefer the Data element name to enforce correct order if available
  if ("Data element name" %in% names(cols)) {
    cols <- cols %>% arrange(`Data element name`)
  }
  nm_col <- if ("Data element label" %in% names(cols)) {
    "Data element label"
  } else if ("Data element name" %in% names(cols)) {
    # rare fallback if label not present
    "Data element number"
  } else {
    stop("Metadata table lacks both 'Data element label' and 'Data element name'.")
  }
  # Make names unique but otherwise keep as-is (spaces allowed in CSV headers)
  make.unique(as.character(cols[[nm_col]]))
}


# Read headerless CSV with supplied column names, all as character (safer for HCUP).
read_headerless_csv <- function(path_or_url, col_names) {
  # readr supports http(s) out of the box; for S3 use arrow::read_csv_arrow (commented below)
  readr::read_csv(
    file = path_or_url,
    col_names = col_names,
    col_types = cols(.default = col_character()),
    na = missing_values,
    show_col_types = FALSE
  )
  # For S3, you could switch to:
  # arrow::read_csv_arrow(path_or_url, col_names = col_names, null_values = missing_values)
}

# Write Parquet + DuckDB
write_outputs <- function(df, ds, year, type) {
  parquet_path <- file.path(out_base, sprintf("%s_%s_%s.parquet", ds, year, type))
  duckdb_path  <- file.path(out_base, sprintf("%s_%s_%s.duckdb",  ds, year, type))
  
  write_parquet(arrow::arrow_table(df), parquet_path)
  
  con <- dbConnect(duckdb::duckdb(), dbdir = duckdb_path, read_only = FALSE)
  on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)
  dbWriteTable(con, "hcup_data", df, overwrite = TRUE)
}

# Build input CSV path/URL
csv_path_for <- function(base, ds, year, type) {
  fn <- csv_filename(ds, year, type)
  # If base is an HTTP(S) URL, just paste with "/" separator; if local, use file.path
  if (grepl("^https?://", base, ignore.case = TRUE)) {
    sub("/+$", "", base) |> paste(fn, sep = "/")
  } else {
    file.path(base, fn)
  }
}

# --------------------------
# MAIN
# --------------------------
process_one <- function(year) {
  message("\n—— Processing ", data_source, " ", year, " ——")
  # 1) Pull ordered column names from spec
  varnames <- fetch_metadata(data_source, year)
  varnames <- get_csv_colnames(varnames)
  
  # 2) Locate and read CSV (no header)
  csv_in <- csv_path_for(csv_base, data_source, year, data_type)
  message("Reading CSV: ", csv_in)
  df <- read_headerless_csv(csv_in, col_names = varnames)
  
  # 2b) Defensive: if CSV has a different number of columns than varnames
  if (ncol(df) != length(varnames)) {
    warn <- paste0(
      "Column count mismatch for ", year, ": CSV has ", ncol(df),
      " columns; spec provides ", length(varnames), "."
    )
    warning(warn)
    # Align by trunc/pad to the min columns if necessary (keeps process going)
    min_cols <- min(ncol(df), length(varnames))
    df <- df[, seq_len(min_cols), drop = FALSE]
    names(df) <- varnames[seq_len(min_cols)]
  }
  
  # 3) Write outputs
  write_outputs(df, data_source, year, data_type)
  message("✅ Done: ", data_source, " ", year)
}

for (yr in years) {
  process_one(yr)
}
