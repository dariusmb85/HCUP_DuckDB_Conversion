# ------------------------------------------------------------
# File: R/revisit.R
# ------------------------------------------------------------


#' Compute inter-visit gaps and 30-day revisit (discharge-to-admit based)
#' Expects: sorted by VISITLINK, DAYSTOEVENT; has LOS numeric
compute_revisits <- function(df) {
req <- c("VISITLINK", "DAYSTOEVENT", "LOS")
df <- require_cols(df, req)


df |>
dplyr::arrange(.data$VISITLINK, .data$DAYSTOEVENT) |>
dplyr::group_by(.data$VISITLINK) |>
dplyr::mutate(
PREV_DAYSTOEVENT = dplyr::lag(.data$DAYSTOEVENT),
PREV_LOS = dplyr::lag(.data$LOS),
DAYS_BETWEEN = .data$DAYSTOEVENT - .data$PREV_DAYSTOEVENT,
GAP_DISCH_TO_NEXT= dplyr::if_else(nz(.data$DAYS_BETWEEN) & nz(.data$PREV_LOS),
.data$DAYS_BETWEEN - .data$PREV_LOS, NA_real_),
REVISIT_30DAY = dplyr::if_else(nz(.data$GAP_DISCH_TO_NEXT) &
.data$GAP_DISCH_TO_NEXT >= 0 & .data$GAP_DISCH_TO_NEXT <= 30,
1L, 0L)
) |>
dplyr::ungroup()
}


#' Add stable attributes (first non-missing)
add_stable_attributes <- function(df) {
df |>
dplyr::arrange(.data$VISITLINK, .data$DAYSTOEVENT) |>
dplyr::group_by(.data$VISITLINK) |>
dplyr::mutate(
AGE_FIRST = dplyr::first(stats::na.omit(.data$AGE)),
SEX_FIRST = dplyr::first(stats::na.omit(.data$FEMALE)),
PAYER_FIRST = dplyr::first(stats::na.omit(.data$PAY1))
) |>
dplyr::ungroup()
}

