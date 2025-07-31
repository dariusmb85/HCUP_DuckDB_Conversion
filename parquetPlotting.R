# Load packages
library(dplyr)
library(ggplot2)
library(survival)
library(survminer)
library(ggalluvial)
library(sf)       # for geospatial if you want maps
library(tidyr)

dir.create("plots", showWarnings = FALSE)

# survival object
km <- survfit(Surv(days_between, revisit_30day) ~ 1, data = revisit_df)

# plot and save
km_plot <- ggsurvplot(
  km,
  conf.int = TRUE,
  title = "Kaplan-Meier Survival: Days to 30-Day Revisit",
  xlab = "Days Since Prior Visit",
  ylab = "Probability of No Revisit"
)
ggsave("plots/km_survival.png", plot = km_plot$plot, width=8, height=6, dpi=300)

p2 <- revisit_df %>%
  filter(revisit_30day == 1) %>%
  ggplot(aes(x = days_between)) +
  geom_histogram(binwidth = 2, color="black", fill="lightblue") +
  labs(
    title = "Histogram of Days Between Revisits (Within 30 Days)",
    x = "Days between visits",
    y = "Count"
  )

ggsave("plots/hist_days_between.png", plot=p2, width=8, height=6, dpi=300)

p3 <- revisit_df %>%
  filter(revisit_30day == 1) %>%
  ggplot(aes(axis1 = prev_SOURCE, axis2 = SOURCE)) +
  geom_alluvium(aes(fill = prev_SOURCE), width = 1/12) +
  geom_stratum() +
  geom_text(stat = "stratum", aes(label = after_stat(stratum))) +
  scale_x_discrete(limits = c("Previous", "Current"), expand = c(.05, .05)) +
  labs(
    title = "Patient Care Pathways: Transitions between Visit Sources",
    y = "Number of revisits"
  )
ggsave("plots/sankey_transitions.png", plot=p3, width=8, height=6, dpi=300)

if ("HOSP_NIS" %in% names(revisit_df)) {
  p4 <- revisit_df %>%
    group_by(HOSP_NIS) %>%
    summarise(revisit_rate = mean(revisit_30day, na.rm = TRUE)) %>%
    ggplot(aes(x=HOSP_NIS, y="30d", fill=revisit_rate)) +
    geom_tile() +
    labs(title="Heatmap of 30-Day Revisit Rates by Hospital", x="Hospital ID", y="Interval") +
    scale_fill_viridis_c()
  
  ggsave("plots/heatmap_hospitals.png", plot=p4, width=10, height=4, dpi=300)
}

ggsave("plots/zip_map.png", plot=p_map, width=8, height=6, dpi=300)

if ("DX1" %in% names(revisit_df)) {
  p6 <- revisit_df %>%
    group_by(DX1) %>%
    summarise(rate = mean(revisit_30day, na.rm = TRUE), count = n()) %>%
    filter(count > 100) %>%
    ggplot(aes(x=reorder(DX1, -rate), y=rate)) +
    geom_col(fill="steelblue") +
    labs(
      title = "30-Day Revisit Rate by Primary Diagnosis",
      x="Diagnosis Code",
      y="Revisit Rate"
    ) +
    theme(axis.text.x = element_text(angle=90, vjust=0.5))
  
  ggsave("plots/revisit_by_diagnosis.png", plot=p6, width=12, height=6, dpi=300)
}

if ("YEAR" %in% names(revisit_df)) {
  p7 <- revisit_df %>%
    group_by(YEAR) %>%
    summarise(revisits = sum(revisit_30day, na.rm = TRUE)) %>%
    ggplot(aes(x=YEAR, y=revisits)) +
    geom_line() +
    geom_point() +
    labs(title="Total 30-Day Revisits Over Time", x="Year", y="Count of revisits")
  
  ggsave("plots/cumulative_revisits.png", plot=p7, width=8, height=6, dpi=300)
}
