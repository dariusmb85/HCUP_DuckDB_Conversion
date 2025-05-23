library(DBI)
library(duckdb)
library(arrow)
library(tictoc)
library(dplyr)
library(ggplot2)
library(here)
library(stringr)
library(fs)

# Constants
TABLE_NAME <- "hcup_data"
QUERY <- sprintf("SELECT RACE, FEMALE, AVG(AGE) AS avg_age FROM %s GROUP BY RACE, FEMALE", TABLE_NAME)
RESULT_DIR <- here("results")
DATA_DIR <- here("storage/SID")

# Ensure output directory exists
dir_create(RESULT_DIR)

# Get matching .parquet and .duckdb files
parquet_files <- dir_ls(DATA_DIR, regexp = "\\.parquet$")
duckdb_files <- dir_ls(DATA_DIR, regexp = "\\.duckdb$")

# Get file tags (shared base names)
get_tag <- function(path, ext) str_remove(basename(path), paste0("\\.", ext, "$"))
tags <- intersect(get_tag(parquet_files, "parquet"), get_tag(duckdb_files, "duckdb"))

# Store results
results <- list()

for (tag in tags) {
  cat("\n==========================================\n")
  cat("Running query benchmarks for:", tag, "\n")
  
  # File paths
  parquet_path <- path(DATA_DIR, paste0(tag, ".parquet"))
  duckdb_path  <- path(DATA_DIR, paste0(tag, ".duckdb"))
  
  ## DuckDB query
  cat("Running DuckDB query...\n")
  con <- dbConnect(duckdb(), dbdir = duckdb_path, read_only = TRUE)
  tic()
  duckdb_result <- dbGetQuery(con, QUERY)
  dt <- toc(quiet = TRUE)
  duckdb_time <- dt$toc - dt$tic
  dbDisconnect(con, shutdown = TRUE)
  
  ## Arrow query
  cat("Running Arrow query...\n")
  ds <- open_dataset(parquet_path)
  tic()
  arrow_result <- ds %>%
    group_by(RACE, FEMALE) %>%
    summarise(avg_age = mean(AGE, na.rm = TRUE), .groups = "drop") %>%
    collect()
  t <- toc(quiet = TRUE)
  arrow_time <- t$toc - t$tic
  
  ## Write query results
  write.csv(duckdb_result, path(RESULT_DIR, paste0(tag, "_duckdb_query.csv")), row.names = FALSE)
  write.csv(arrow_result,  path(RESULT_DIR, paste0(tag, "_arrow_query.csv")), row.names = FALSE)
  
  ## Store timing info
  results[[tag]] <- tibble(
    tag = tag,
    engine = c("duckdb", "arrow"),
    time_sec = c(duckdb_time, arrow_time)
  )
}

# Combine and save timing results
timing_df <- bind_rows(results)
write.csv(timing_df, path(RESULT_DIR, "query_timing_summary.csv"), row.names = FALSE)

# Plot timing comparison
plot <- ggplot(timing_df, aes(x = tag, y = time_sec, fill = engine)) +
  geom_col(position = "dodge") +
  labs(title = "Query Execution Time by Engine", x = "Dataset", y = "Time (seconds)") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
ggsave(path(RESULT_DIR, "query_timing_plot.png"), plot, width = 8, height = 5)

cat("\n✅ Benchmarking complete. Results saved to:", RESULT_DIR, "\n")
