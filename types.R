# ------------------------------------------------------------
# File: R/types.R
# ------------------------------------------------------------


#' Standardize core types used downstream
#' - VisitLink: character
#' - DaysToEvent, LOS, YEAR: numeric/integer
#' - FEMALE, RACE, PAY1: integer (labels optional)
standardize_core_types <- function(df) {
df <- df |>
dplyr::mutate(
VISITLINK = as.character(.data$VISITLINK),
DAYSTOEVENT = safe_as_numeric(.data$DAYSTOEVENT),
LOS = safe_as_numeric(.data$LOS),
YEAR = safe_as_numeric(.data$YEAR),
FEMALE = safe_as_numeric(.data$FEMALE),
RACE = safe_as_numeric(.data$RACE),
PAY1 = safe_as_numeric(.data$PAY1)
)
df
}


#' Optional: apply label mappings for FEMALE/RACE
label_demographics <- function(df) {
female_labs <- c(`0` = "Male", `1` = "Female")
race_labs <- c(
`1` = "White", `2` = "Black", `3` = "Hispanic",
`4` = "Asian or Pacific Islander", `5` = "Native American",
`6` = "Other"
)
df |>
dplyr::mutate(
FEMALE_LAB = dplyr::recode(as.character(.data$FEMALE), !!!female_labs, .default = NA_character_),
RACE_LAB = dplyr::recode(as.character(.data$RACE), !!!race_labs, .default = NA_character_)
)
}
