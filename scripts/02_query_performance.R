library(DBI)
library(duckdb)
library(arrow)
library(tictoc)
library(dplyr)
library(ggplot2)
library(here)
library(stringr)
library(fs)

# Ensure output directory exists
dir.create(here("results"), showWarnings = FALSE)

# Get all relevant files
parquet_files <- dir_ls(here("storage"), glob = "*.parquet")
duckdb_files <- dir_ls(here("storage"), glob = "*.duckdb")

# Extract common tags from file names
get_tag <- function(path, ext) {
  basename(path) |>
    str_remove(paste0("\\.", ext, "$"))
}

tags <- intersect(get_tag(parquet_files, "parquet"), get_tag(duckdb_files, "duckdb"))

# Establish connection just to get table name
con_tmp <- dbConnect(duckdb(), dbdir = duckdb_files, read_only = TRUE)
table_list <- dbListTables(con_tmp)
dbDisconnect(con_tmp, shutdown = TRUE)

# Pick first table (assuming there's only one main table per DB)
target_table <- table_list[1]

# Use in query
sql_query <- sprintf("SELECT RACE, FEMALE, AVG(AGE) AS avg_age FROM %s GROUP BY RACE, FEMALE", target_table)

# Store timing results
results <- list()

for (tag in tags) {
  cat("\n==========================================\n")
  cat("Running query benchmarks for:", tag, "\n")
  
  # Define file paths
  parquet_path <- here("storage", paste0(tag, ".parquet"))
  duckdb_path <- here("storage", paste0(tag, ".duckdb"))
  
  ## DuckDB
  con <- dbConnect(duckdb(), dbdir = duckdb_path, read_only = TRUE)
  
  cat("Running DuckDB query...\n")
  tic("DuckDB query")
  duckdb_result <- dbGetQuery(con, sql_query)
  duckdb_time <- toc(log = FALSE, quiet = TRUE)$toc - toc(log = FALSE, quiet = TRUE)$tic
  dbDisconnect(con, shutdown = TRUE)
  
  ## Arrow
  cat("Running Arrow query...\n")
  parquet_dataset <- open_dataset(parquet_path)
  tic("Arrow query")
  arrow_result <- parquet_dataset %>%
    group_by(RACE, FEMALE) %>%
    summarise(avg_age = mean(AGE, na.rm = TRUE)) %>%
    collect()
  arrow_time <- toc(log = FALSE, quiet = TRUE)$toc - toc(log = FALSE, quiet = TRUE)$tic
  
  # Save query results
  write.csv(duckdb_result, here("results", paste0(tag, "_duckdb_query.csv")), row.names = FALSE)
  write.csv(arrow_result, here("results", paste0(tag, "_arrow_query.csv")), row.names = FALSE)
  
  # Save timing info
  results[[tag]] <- data.frame(
    tag = tag,
    engine = c("duckdb", "arrow"),
    time_sec = c(duckdb_time, arrow_time)
  )
}

# Combine timing results
timing_df <- bind_rows(results)

# Save and plot timing summary
write.csv(timing_df, here("results", "query_timing_summary.csv"), row.names = FALSE)

ggplot(timing_df, aes(x = tag, y = time_sec, fill = engine)) +
  geom_col(position = "dodge") +
  labs(title = "Query Execution Time by Engine", y = "Time (seconds)", x = "Dataset") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

ggsave(here("results", "query_timing_plot.png"), width = 8, height = 5)
