# ------------------------------------------------------------
# File: R/flags.R
# ------------------------------------------------------------


#' Flag a clean period and index event for a diagnosis code (regex)
flag_clean_period <- function(df, dx_code = "^250", clean_days = 180) {
req <- c("VISITLINK", "DAYSTOEVENT", "DX1")
df <- require_cols(df, req)


df |>
  dplyr::mutate(CONDITION_FLAG = grepl(dx_code, .data$DX1)) |>
  dplyr::arrange(.data$VISITLINK, .data$DAYSTOEVENT) |>
  dplyr::group_by(.data$VISITLINK) |>
  dplyr::mutate(
    PREV_TIME = dplyr::lag(.data$DAYSTOEVENT),
    CLEAN_PERIOD= dplyr::if_else(!nz(.data$PREV_TIME) | (.data$DAYSTOEVENT - .data$PREV_TIME >= clean_days), TRUE, FALSE),
    IS_INDEX = dplyr::if_else(dplyr::row_number() == 1 & .data$DAYSTOEVENT >= clean_days, TRUE, .data$CLEAN_PERIOD)
) |>
dplyr::ungroup()
}


#' Get prior events for patients with a target procedure (regex matched in PR1)
get_prior_events <- function(df, proc_code = "^36.1") {
  req <- c("VISITLINK", "DAYSTOEVENT", "PR1")
  df <- require_cols(df, req)


  df <- dplyr::mutate(df, PROC_FLAG = grepl(proc_code, .data$PR1))
  cabg_ids <- df |>
    dplyr::filter(.data$PROC_FLAG) |>
    dplyr::pull(.data$VISITLINK) |>
    unique()


df |>
  dplyr::filter(.data$VISITLINK %in% cabg_ids) |>
  dplyr::arrange(.data$VISITLINK, .data$DAYSTOEVENT) |>
  dplyr::group_by(.data$VISITLINK) |>
  dplyr::mutate(
    CABG_TIME = .data$DAYSTOEVENT[which(.data$PROC_FLAG)[1]],
    BEFORE_CABG = .data$DAYSTOEVENT < .data$CABG_TIME
  ) |>
  dplyr::filter(.data$BEFORE_CABG) |>
  dplyr::ungroup()
}
