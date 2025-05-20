# Parse HCUP .asc files into Parquet and DuckDB formats

library(readr)
library(arrow)
library(duckdb)
library(here)

#--------------------------
# COMMAND-LINE ARGUMENTS
#--------------------------
args <- commandArgs(trailingOnly = TRUE)

if (length(args) < 3) {
  stop("Usage: Rscript script.R <STATE> <YEARS> <DATA_SOURCE>")
}

state <- toupper(args[1])  # e.g., OR
years <- as.integer(strsplit(args[2], ":")[[1]])  # e.g., 2020:2021
data_source <- toupper(args[3])  # e.g., SEDD

if (!data_source %in% c("SASD", "SEDD", "SID")) {
  stop("Invalid data source. Choose from 'SASD', 'SEDD', or 'SID'.")
}

#--------------------------
# CONFIGURATION
#--------------------------
data_type <- "CORE"
missing_values <- as.character(quote(c(-99, -88, -66, -99.9999999, -88.8888888,
                    -66.6666666, -9, -8, -6, -5, -9999,
                    -8888, -6666, -99999999, -999999999,
                    -888888888, -666666666, -999, -888,
                    -666)))

#--------------------------
# FUNCTIONS
#--------------------------

get_column_positions <- function(year) {
  if (year == 2021) {
    readr::fwf_positions(
      start = c(1, 5, 10, 28, 32, 64, 69, 73, 75, 80),
      end = c(3, 8, 26, 30, 62, 67, 72, 73, 78, NA)
    )
  } else {
    readr::fwf_positions(
      start = c(1, 5, 10, 27, 31, 63, 68, 73, 75, 80),
      end = c(3, 8, 25, 29, 61, 66, 71, 73, 78, NA)
    )
  }
}

fetch_metadata <- function(data_source, year) {
  lower_source <- paste0(tolower(data_source), "c")
  meta_url <- paste0("https://hcup-us.ahrq.gov/db/state/",
                     lower_source, "/tools/filespecs/", state, "_",
                     data_source, "_", year, "_", data_type, ".loc")
  positions <- get_column_positions(year)
  readr::read_fwf(meta_url, positions, skip = 20)
}

read_hcup_data <- function(meta_df, year) {
  data_file <- paste0("../", state, "/", data_source, "/", state, "_", data_source, "_",
                      year, "_", data_type, ".asc")
  readr::read_fwf(
    file = data_file,
    col_positions = readr::fwf_positions(start = meta_df$X6, end = meta_df$X7, col_names = meta_df$X5),
    skip = 2,
    na = missing_values
  )
}

write_outputs <- function(df, year) {
  parquet_path <- here::here("storage", paste0(state, "_", data_source, "_", year, "_", data_type, ".parquet"))
  duckdb_path <- here::here("storage", paste0(state, "_", data_source, "_", year, "_", data_type, ".duckdb"))
  
  # Write Parquet
  df_arrow <- arrow_table(df)
  write_parquet(df_arrow, parquet_path)
  
  # Write DuckDB
  con <- dbConnect(duckdb::duckdb(), dbdir = duckdb_path, read_only = FALSE)
  arrow::to_duckdb(df, table_name = "temp_arrow_view", con = con)
  DBI::dbExecute(con, "CREATE TABLE hcup_data AS SELECT * FROM temp_arrow_view")
  dbDisconnect(con, shutdown = TRUE)
}

process_hcup <- function(year) {
  message("Processing: ", state, " ", data_source, " ", year)
  meta_df <- fetch_metadata(data_source, year)
  df <- read_hcup_data(meta_df, year)
  write_outputs(df, year)
  message("✅ Done: ", state, " ", data_source, " ", year)
}

#--------------------------
# EXECUTION
#--------------------------
for (year in years) {
  process_hcup(year)
}
