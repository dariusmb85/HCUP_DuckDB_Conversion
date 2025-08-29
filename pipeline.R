# ------------------------------------------------------------
# File: R/pipeline.R
# ------------------------------------------------------------


#' Run the HCUP revisit pipeline for a single source (SID or SEDD)
#' @param state e.g., "UT" (used only for messaging/structure)
#' @param root directory containing subdirs SID/ or SEDD/
#' @param source one of c("SID","SEDD")
#' @param years integer vector, e.g., 2017:2020 (NULL = all)
#' @param dx_code optional regex for diagnosis clean-period flagger
#' @param proc_code optional regex for prior-events-of-procedure
#' @param cols optional set of columns to read (recommended)
run_revisit_pipeline <- function(state, root, source = c("SID","SEDD"), years = NULL,
dx_code = NULL, proc_code = NULL,
cols = c("VISITLINK","DAYSTOEVENT","LOS","DX1","PR1",
"FEMALE","RACE","PAY1","ASOURCE","DISPUNIFORM",
"DSHOSPID")) {
  source <- match.arg(source)
  message("== ", state, " :: SOURCE=", source, if (!is.null(years)) paste0(" :: YEARS=", paste(years, collapse=",")))


  # Source files and read
  files <- list_source_files(root, source, years)
  dat <- read_hcup_files(files, source_label = source, cols = cols)


  # Core revisit computation
  dat <- compute_revisits(dat)
  dat <- add_stable_attributes(dat)
  dat <- detect_transfers(dat)


  # Optional flaggers
  if (!is.null(dx_code)) dat <- flag_clean_period(dat, dx_code = dx_code)


  prior <- NULL
  if (!is.null(proc_code)) prior <- get_prior_events(dat, proc_code = proc_code)


  # Summaries
  overall <- summary_overall(dat)
  flows <- cross_year_flows(dat)


  list(
    revisit_df = dat,
    overall_summary = overall,
    cross_year_flows = flows,
    prior_events = prior
  )
}
