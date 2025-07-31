library(arrow)
library(dplyr)
library(purrr)
library(stringr)
library(ggalluvial)
library(ggplot2)
#----------------------------
# 1. harmonized parquet reader
#----------------------------
read_hcup_files <- function(files, source_label) {
  map_dfr(files, function(file) {
    message("Reading: ", basename(file))
    df <- read_parquet(file)
    df <- df %>%
      mutate(across(everything(), as.character)) %>%
      mutate(SOURCE = source_label, FILE = basename(file))
    df
  })
}

#----------------------------
# 2. combine sid + sedd
#----------------------------
combine_sid_sedd <- function(sid_df, sedd_df) {
  shared_cols <- intersect(names(sid_df), names(sedd_df))
  bind_rows(
    sid_df %>% select(all_of(shared_cols)),
    sedd_df %>% select(all_of(shared_cols))
  )
}

#----------------------------
# 3. revisit analysis
#----------------------------
generate_revisit_df <- function(combined_df) {
  combined_df %>%
    filter(!is.na(VisitLink), !is.na(DaysToEvent)) %>%
    mutate(
      DaysToEvent = as.numeric(DaysToEvent)
    ) %>%
    arrange(VisitLink, DaysToEvent) %>%
    group_by(VisitLink) %>%
    mutate(
      prev_DaysToEvent = lag(DaysToEvent),
      prev_SOURCE = lag(SOURCE),
      days_between = DaysToEvent - prev_DaysToEvent,
      revisit_30day = ifelse(!is.na(days_between) & days_between <= 30, 1, 0)
    ) %>%
    ungroup()
}

#----------------------------
# 4. stable patient attributes
#----------------------------
add_stable_attributes <- function(df) {
  df %>%
    arrange(VisitLink, DaysToEvent) %>%
    group_by(VisitLink) %>%
    mutate(
      first_age = first(na.omit(AGE)),
      first_sex = first(na.omit(FEMALE)),
      first_payer = first(na.omit(PAY1))
    ) %>%
    ungroup()
}

#----------------------------
# 5. LOS-adjusted gap
#----------------------------
calculate_gap <- function(df) {
  df %>%
    mutate(LOS = as.numeric(LOS)) %>%
    group_by(VisitLink) %>%
    mutate(
      gap_discharge_to_next_admit = ifelse(
        !is.na(days_between) & !is.na(lag(LOS)),
        days_between - lag(LOS),
        NA
      )
    ) %>%
    ungroup()
}

#----------------------------
# 6. transfer detection
#----------------------------
detect_transfers <- function(df) {
  if (all(c("DISPuniform", "ASOURCE", "DSHOSPID") %in% names(df))) {
    df %>%
      group_by(VisitLink) %>%
      mutate(
        is_transfer_out = DISPuniform == "2",
        is_transfer_in = ASOURCE == "2",
        is_potential_transfer = ifelse(
          !is.na(lag(DSHOSPID)) &
            DSHOSPID != lag(DSHOSPID) &
            DaysToEvent == lag(DaysToEvent) + lag(LOS) &
            is_transfer_in & lag(is_transfer_out),
          TRUE, FALSE
        )
      ) %>%
      ungroup()
  } else {
    df
  }
}

#----------------------------
# 7. clean period flagger (example for diabetes)
#----------------------------
flag_clean_period <- function(df, dx_code = "^250", clean_days = 180) {
  df %>%
    mutate(condition_flag = grepl(dx_code, DX1)) %>%
    arrange(VisitLink, DaysToEvent) %>%
    group_by(VisitLink) %>%
    mutate(
      prev_event_time = lag(DaysToEvent),
      clean_period = ifelse(
        is.na(prev_event_time) | (DaysToEvent - prev_event_time >= clean_days),
        TRUE, FALSE
      ),
      is_index_event = ifelse(row_number() == 1 & DaysToEvent >= clean_days, TRUE, clean_period)
    ) %>%
    ungroup()
}

#----------------------------
# 8. prior events for target procedure (example for CABG)
#----------------------------
get_prior_events <- function(df, proc_code = "^36.1") {
  df <- df %>%
    mutate(proc_flag = grepl(proc_code, PR1))
  
  cabg_patients <- df %>%
    filter(proc_flag == TRUE) %>%
    pull(VisitLink) %>%
    unique()
  
  df %>%
    filter(VisitLink %in% cabg_patients) %>%
    arrange(VisitLink, DaysToEvent) %>%
    group_by(VisitLink) %>%
    mutate(
      cabg_event_time = DaysToEvent[which(proc_flag == TRUE)[1]],
      before_cabg = DaysToEvent < cabg_event_time
    ) %>%
    filter(before_cabg == TRUE) %>%
    ungroup()
}

#----------------------------
# 9. master workflow
#----------------------------
run_revisit_pipeline <- function(state, sid_dir, sedd_dir) {
  sid_files <- list.files(sid_dir, pattern = "\\.parquet$", full.names = TRUE)
  sedd_files <- list.files(sedd_dir, pattern = "\\.parquet$", full.names = TRUE)
  
  sid_files <- sid_files[order(str_extract(sid_files, "\\d{4}") %>% as.integer())]
  sedd_files <- sedd_files[order(str_extract(sedd_files, "\\d{4}") %>% as.integer())]
  
  sid_data <- read_hcup_files(sid_files[11:15], "SID")
  sedd_data <- read_hcup_files(sedd_files[11:15], "SEDD")
  
  combined <- combine_sid_sedd(sid_data, sedd_data)
  
  revisit <- generate_revisit_df(combined)
  revisit <- add_stable_attributes(revisit)
  revisit <- calculate_gap(revisit)
  revisit <- detect_transfers(revisit)
  
  # optionally run condition-based flaggers
  revisit <- flag_clean_period(revisit, dx_code = "^250", clean_days = 180)
  
  cabg_prior <- get_prior_events(revisit, proc_code = "^36.1")
  
  list(
    revisit_df = revisit,
    cabg_prior_events = cabg_prior
  )
}

#----------------------------
# Example run
#----------------------------

results <- run_revisit_pipeline(
  state = "UT",
  sid_dir = file.path("UT", "SID"),
  sedd_dir = file.path("UT", "SEDD")
)

# the processed revisit dataframe
revisit_df <- results$revisit_df

# events before CABG
cabg_events <- results$cabg_prior_events

# quick summary
revisit_df %>%
  summarise(
    total_visits = n(),
    total_revisits_30d = sum(revisit_30day, na.rm = TRUE),
    percent_revisit = round(100 * total_revisits_30d / total_visits, 2)
  )


# Randomly sample 1 VisitLink from the data
sample_patient <- revisit_df %>%
  filter(!is.na(VisitLink)) %>%
  distinct(VisitLink) %>%
  slice_sample(n = 1) %>%
  pull(VisitLink)

# Extract that patient's multi-year history
patient_history <- revisit_df %>%
  filter(VisitLink == sample_patient) %>%
  arrange(DaysToEvent) %>%
  select(VisitLink, YEAR, FILE, DaysToEvent, days_between, revisit_30day)

print(patient_history)

# Count Cross-Year Revisits
cross_year_summary <- revisit_df %>%
  group_by(VisitLink) %>%
  summarise(
    years_seen = n_distinct(YEAR),
    total_visits = n(),
    total_revisits_30d = sum(revisit_30day, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(desc(years_seen))

head(cross_year_summary)


# Add YEAR column
revisit_df <- revisit_df %>%
  mutate(YEAR = as.numeric(str_extract(FILE, "\\d{4}")))

# Identify cross-year revisits
cross_year_revisits <- revisit_df %>%
  filter(revisit_30day == 1) %>%
  mutate(prev_YEAR = lag(YEAR),
         prev_VisitLink = lag(VisitLink)) %>%
  filter(!is.na(prev_YEAR),
         VisitLink == prev_VisitLink,     # Same patient
         YEAR != prev_YEAR)               # Cross-year only

# Aggregate flows
cross_year_flows <- cross_year_revisits %>%
  count(prev_YEAR, YEAR, name="n") %>%
  rename(from_year = prev_YEAR, to_year = YEAR)

print(cross_year_flows)
