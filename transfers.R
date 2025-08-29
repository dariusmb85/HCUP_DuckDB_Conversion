# ------------------------------------------------------------
# File: R/transfers.R
# ------------------------------------------------------------


#' Detect potential inter-facility transfers within a visit sequence
#' Requires columns: DISPUNIFORM, ASOURCE, DSHOSPID (case-insensitive accepted)
#' Code assumes HCUP coding where 2 indicates transfer
detect_transfers <- function(df) {
# Tolerate DISPuniform vs DISPUNIFORM casing
nm <- names(df)
disp_col <- nm[grepl("^DISP", nm)][1]
asrc_col <- nm[grepl("^ASOURCE$", nm)][1]
hosp_col <- nm[grepl("^DSHOSPID$", nm)][1]


needed <- c(disp_col, asrc_col, hosp_col)
if (any(is.na(needed))) return(df) # quietly skip if not available


df |>
dplyr::group_by(.data$VISITLINK) |>
dplyr::mutate(
DISP_X = as.character(.data[[disp_col]]),
ASRC_X = as.character(.data[[asrc_col]]),
HOSP_X = as.character(.data[[hosp_col]]),
IS_TRANSFER_OUT = .data$DISP_X == "2",
IS_TRANSFER_IN = .data$ASRC_X == "2",
POTENTIAL_TRANSFER = dplyr::if_else(
nz(dplyr::lag(.data$HOSP_X)) & .data$HOSP_X != dplyr::lag(.data$HOSP_X) &
nz(.data$DAYSTOEVENT) & nz(dplyr::lag(.data$DAYSTOEVENT)) & nz(dplyr::lag(.data$LOS)) &
.data$DAYSTOEVENT == dplyr::lag(.data$DAYSTOEVENT) + dplyr::lag(.data$LOS) &
.data$IS_TRANSFER_IN & dplyr::lag(.data$IS_TRANSFER_OUT), TRUE, FALSE)
) |>
dplyr::ungroup() |>
dplyr::select(-dplyr::any_of(c("DISP_X","ASRC_X","HOSP_X")))
}
