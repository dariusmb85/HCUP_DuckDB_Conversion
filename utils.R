# ------------------------------------------------------------
# File: R/utils.R
# ------------------------------------------------------------


#' Safely coerce to numeric (suppress warnings)
safe_as_numeric <- function(x) suppressWarnings(as.numeric(x))


#' Ensure required columns exist; otherwise stop with message
require_cols <- function(df, cols) {
missing <- setdiff(cols, names(df))
if (length(missing)) stop("Missing required columns: ", paste(missing, collapse = ", "))
df
}


#' Uppercase column names for HCUP consistency
normalize_colnames <- function(df) {
names(df) <- toupper(names(df))
df
}


#' Add YEAR from file name if not already present
add_year_from_file <- function(df) {
if (!"YEAR" %in% names(df)) {
if (!"FILE" %in% names(df)) stop("FILE column required to extract YEAR")
df$YEAR <- stringr::str_extract(df$FILE, "\\\d{4}") |> as.integer()
}
df
}


#' Convenient logical helper
nz <- function(x) !is.na(x)
