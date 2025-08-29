# ------------------------------------------------------------
# File: R/summaries.R
# ------------------------------------------------------------


#' Overall revisit summary
summary_overall <- function(df) {
  df |>
  dplyr::summarise(
    total_visits = dplyr::n(),
    total_revisits_30d = sum(.data$REVISIT_30DAY %||% 0L, na.rm = TRUE),
    percent_revisit = round(100 * .data$total_revisits_30d / .data$total_visits, 2)
  )
}


#' Cross-year transitions among 30-day revisits
cross_year_flows <- function(df) {
  req <- c("VISITLINK", "YEAR", "REVISIT_30DAY")
  df <- require_cols(df, req)


  df |>
  dplyr::arrange(.data$VISITLINK, .data$DAYSTOEVENT) |>
  dplyr::group_by(.data$VISITLINK) |>
  dplyr::mutate(
    PREV_YEAR = dplyr::lag(.data$YEAR)
  ) |>
  dplyr::ungroup() |>
  dplyr::filter(.data$REVISIT_30DAY == 1L, nz(.data$PREV_YEAR), .data$YEAR != .data$PREV_YEAR) |>
  dplyr::count(PREV_YEAR, YEAR, name = "n") |>
  dplyr::rename(from_year = .data$PREV_YEAR, to_year = .data$YEAR)
}


`%||%` <- function(a, b) if (!is.null(a)) a else b
