# ------------------------------------------------------------
# File: scripts/run_pipeline.R (optional command-line entrypoint)
# ------------------------------------------------------------


# Usage (interactive):
# source("R/utils.R"); source("R/types.R"); source("R/io.R");
# source("R/revisit.R"); source("R/transfers.R"); source("R/flags.R");
# source("R/summaries.R"); source("R/pipeline.R")
# res <- run_revisit_pipeline(state = "UT", root = "UT", source = "SID", years = 2017:2019,
# dx_code = "^250", proc_code = "^36.1")


# Usage (Rscript from terminal):
# Rscript scripts/run_pipeline.R UT ./UT SID 2017 2018 2019


if (identical(environment(), globalenv()) && !interactive()) {
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) < 3) {
    stop("Args: <STATE> <ROOT_DIR> <SOURCE: SID|SEDD> [YEARS ...]")
  }
  state <- args[[1]]
  root <- args[[2]]
  source <- args[[3]]
  years <- if (length(args) > 3) as.integer(args[4:length(args)]) else NULL


# Source modules
srcs <- c("R/utils.R","R/types.R","R/io.R","R/revisit.R","R/transfers.R","R/flags.R","R/summaries.R","R/pipeline.R")
sapply(srcs, function(f) source(f, local = TRUE))


res <- run_revisit_pipeline(state = state, root = root, source = source, years = years,
dx_code = "^250", proc_code = "^36.1")


print(res$overall_summary)
print(head(res$cross_year_flows))
}
