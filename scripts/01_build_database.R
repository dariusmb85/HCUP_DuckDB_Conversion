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
year <- args[2] # e.g., 2016 or 2015q1q3
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


get_column_positions <- function(loc_file) {
  lines <- readLines(loc_file)
  start_idx <- grep("^\\s*\\d+-\\s*\\d+\\s+", lines)
  spec_lines <- lines[start_idx]
  
  parsed <- lapply(spec_lines, function(line) {
    matches <- regmatches(line, regexec("^\\s*(\\d+)-\\s*(\\d+)\\s+(.*?)\\s*$", line))[[1]]
    if (length(matches) >= 4) {
      list(
        start = as.integer(matches[2]),
        end = as.integer(matches[3]),
        label = trimws(matches[4])
      )
    } else {
      NULL
    }
  })
  
  parsed <- Filter(Negate(is.null), parsed)
  colspec <- do.call(rbind, lapply(parsed, as.data.frame))
  colspec$label <- make.unique(colspec$label)  # Avoid duplicate col names
  colspec
}


fetch_metadata <- function(data_source, year) {
  lower_source <- paste0(tolower(data_source), "c")
  meta_url <- paste0("https://hcup-us.ahrq.gov/db/state/",
                     lower_source, "/tools/filespecs/", state, "_",
                     data_source, "_", year, "_", data_type, ".loc")
  positions <- get_column_positions(meta_url)
  # readr::read_fwf(meta_url, positions, skip = 20)
  readr::read_fwf(meta_url,
                  col_positions = fwf_positions(
                    start = positions$start,
                    end = positions$end,
                    col_names = positions$label
                  ),
                  skip = 20)
}

read_hcup_data <- function(meta_df, year) {
  data_file <- paste0(state, "/", data_source, "/", state, "_", data_source,
                      "_", year, "_", data_type, ".asc")
  readr::read_fwf(
    file = data_file,
    col_positions = readr::fwf_positions(start = meta_df$`Starting column of variable in ASCII file`,
                                         end = meta_df$`Ending column of variable in ASCII file`,
                                         col_names = meta_df$`Variable name`),
    skip = 2,
    na = missing_values
  )
}

write_outputs <- function(df, year) {
  parquet_path <- here::here(state, data_source, paste0(state, "_", data_source,
                                                        "_", year, "_", 
                                                        data_type, ".parquet"))
  duckdb_path <- here::here(state, data_source, paste0(state, "_", data_source,
                                                       "_", year, "_",
                                                       data_type, ".duckdb"))
  
  # Write Parquet
  df_arrow <- arrow_table(df)
  write_parquet(df_arrow, parquet_path)
  
  # Write DuckDB
  con <- dbConnect(duckdb::duckdb(), dbdir = duckdb_path, read_only = FALSE)
  DBI::dbWriteTable(con, "hcup_data", df, overwrite = TRUE)
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
