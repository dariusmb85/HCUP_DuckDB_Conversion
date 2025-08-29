# ------------------------------------------------------------
# File: R/io.R
# ------------------------------------------------------------


#' Read a vector of Parquet files, adding SOURCE and FILE
#' @param files character vector of parquet file paths
#' @param source_label "SID" or "SEDD"
#' @param cols optional character vector of columns to select for memory efficiency
read_hcup_files <- function(files, source_label, cols = NULL) {
purrr::map_dfr(files, function(file) {
message("Reading: ", basename(file))
df <- arrow::read_parquet(file)
df <- normalize_colnames(df)
if (!is.null(cols)) {
keep <- intersect(cols, names(df))
df <- dplyr::select(df, dplyr::all_of(keep))
}
df <- dplyr::mutate(df,
SOURCE = source_label,
FILE = basename(file)
)
df <- add_year_from_file(df)
df <- standardize_core_types(df)
df
})
}


#' List parquet files for a state+source, optionally filtered by years
#' @param root like "UT"; layout assumed: file.path(root, source)
#' @param source_label one of c("SID","SEDD")
#' @param years integer vector (e.g., 2017:2020) or NULL for all
list_source_files <- function(root, source_label, years = NULL) {
dir <- file.path(root, source_label)
files <- list.files(dir, pattern = "\\.parquet$", full.names = TRUE)
if (length(files) == 0) stop("No parquet files found in ", dir)
if (!is.null(years)) {
yr <- stringr::str_extract(basename(files), "\\\d{4}") |> as.integer()
files <- files[yr %in% years]
}
files[order(basename(files))]
}
