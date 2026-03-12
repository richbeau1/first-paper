# Data set formation scripts 
library(tidyverse)
library(zoo)
library(openair)
library(future)
library(data.table)
rm(list = ls())  #unload all variables

path <- getwd()
# Call midsection_core.R, which has functions to pull from Influx midsection database
source(paste0(path,"/SAMHE_relational_tables.R"))
source(paste0(path,"/load_external_tables.R"))

source(paste0(path,"/midsection_core.R"))
# Call "data_set_formation_scripts.R" which stores functions to clean raw data and form important datasets
source(paste0(path,"/data_set_formation_scripts.R"))

target_year <- "2025"

plan(multisession, workers = 11)
## ------------------------------------------------------------
## Month sequence + output dir
## ------------------------------------------------------------
months_seq <- seq.Date(
  as.Date(paste0(target_year, "-01-01")),
  as.Date(paste0(target_year, "-12-01")),
  by = "month"
)

out_dir <- file.path("IAQ_monthly_outputs", target_year)
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

## ------------------------------------------------------------
## Load AURN data for whole year (once)
## ------------------------------------------------------------
cat("\nLoading AURN outdoor data for", target_year, "...\n")
start_year <- target_year
current_year <- format(Sys.Date(), "%Y")
end_year <- ifelse(target_year == current_year,
                   as.character(as.numeric(target_year)), as.character(as.numeric(target_year) + 1))

aurn_meta <- importMeta(year = start_year:end_year) %>% 
  filter(!site_type %in% c("Urban Traffic", "Urban Industrial", "Suburban Industrial"))

df_Outdoor_AQ <- importAURN(year = start_year:end_year, data_type = "hourly", 
                            pollutant = "all", site = aurn_meta$code) %>% 
  rename(pm10_out = pm10)

df_Outdoor_AURN <- df_Outdoor_AQ %>% 
  mutate(DateTime = as.POSIXct(date), Date = as.Date(date)) %>% 
  # Convert outdoor data to local UK time (BST/GMT aware)
  mutate(DateTime = with_tz(DateTime, tzone = "Europe/London")) %>%
  rename(device_id = code) %>%
  filter_75PC_24hr_day_hourly() %>%
  rename(code = device_id) %>%
  mutate(DateCode = paste0(Date, code)) %>%
  group_by(DateCode) %>%
  filter(mean(is.na(pm2.5)) <= 0.25) %>%
  ungroup() %>%
  left_join(aurn_meta)

cat("AURN data loaded successfully\n")

## ------------------------------------------------------------
## Per-month processing function
## ------------------------------------------------------------
process_one_month <- function(month_start, df_outdoor_aurn_full) {
  
  month_start <- as.Date(month_start)
  
  # Base R approach (more robust):
  month_end <- seq(month_start, length = 2, by = "month")[2] - 1
  
  message("Processing ", month_start, " to ", month_end)
  
  cat("\n Pulling data")
  ## ---------------- Minutely (never saved) ----------------
  Indoor_min <- large_IAQ_timeseries(
    start_date = as.character(month_start),
    end_date   = as.character(month_end),
    interval   = "1m",
    ""
  ) %>%
    mutate(
      DateTime = as.POSIXct(DateTime),
      Date     = as.Date(DateTime)
    )
  
  cat("\nSuccessfully pulled data")
  Indoor_min_24 <- Indoor_min %>%
    filter(device_id %in% df_school_monitors$MonitorReferenceId) %>%
    filter_75PC_24hr_day_minutely() %>%
    clean_minutely_data() %>%
    clean_minutely_CO2()
  
  Indoor_min_SchHrs <- Indoor_min_24 %>%
    filter_75PC_school_day_minutely() %>%
    filter_school_days(term_dates, national_holidays_dates)
  
  ## ---------------- Hourly ----------------
  Indoor_hourly_24 <- Indoor_min_24 %>%
    group_by(DateTime = floor_date(DateTime, "hour"), device_id) %>%
    summarise(
      across(c(pm01, pm02, rco2, pm10, rhum, atmp, tvoc),
             ~mean(.x, na.rm = TRUE)),
      DateDevice = first(DateDevice),
      Date       = first(Date),
      .groups = "drop"
    )
  
  Indoor_hourly_SchHrs <- Indoor_min_SchHrs %>%
    group_by(DateTime = floor_date(DateTime, "hour"), device_id) %>%
    summarise(
      across(c(pm01, pm02, rco2, pm10, rhum, atmp, tvoc),
             ~mean(.x, na.rm = TRUE)),
      DateDevice = first(DateDevice),
      Date       = first(Date),
      .groups = "drop"
    )
  
  ## ---------------- Daily ----------------
  Indoor_daily_24 <- Indoor_min_24 %>%
    group_by(Date, device_id) %>%
    summarise(across(
      c(pm01, pm02, rco2, pm10, rhum, atmp, tvoc),
      list(
        mean   = ~mean(., na.rm = TRUE),
        median = ~median(., na.rm = TRUE),
        q25    = ~quantile(., 0.25, na.rm = TRUE),
        q75    = ~quantile(., 0.75, na.rm = TRUE)
      )
    ), .groups = "drop")
  
  cat("\nProcessing ventilation")
  ## ---------------- Ventilation (daily) ----------------
  T_occ <- 5 * 1.048
  
  df_pp_ventilation <- Indoor_hourly_24 %>%
    filter(hour(DateTime) >= 5 & hour(DateTime) <= 22) %>%
    left_join(df_school_monitors,
              by = c("device_id" = "MonitorReferenceId")) %>%
    mutate(
      C_excess = (rco2 - 412) / 1e6,
      primary_secondary = case_when(
        SchoolType %in% c(1,2,3) ~ "Primary",
        SchoolType %in% c(4,8,12,16,20,24,28) ~ "11-plus",
        TRUE ~ "Mixed"
      ),
      G_pp = if_else(primary_secondary == "Primary", 3.1, 4.32)
    ) %>%
    ungroup() %>%
    arrange(device_id, DateTime) %>%
    group_by(Date, device_id) %>%
    mutate(
      t_diff = replace_na(as.numeric(DateTime) - as.numeric(lag(DateTime, default = NA)), 0),
      C_trap = (t_diff / 3600) * (C_excess + lag(C_excess, default = 0)) / 2
    ) %>%
    ungroup() %>%
    group_by(Date, device_id) %>%
    summarise(
      pp_ventilation = 0.001 * T_occ * mean(G_pp) / sum(C_trap),
      CO2_mean = mean(rco2),
      DateDevice = first(DateDevice),
      .groups = "drop"
    )
  
  Indoor_daily_SchHrs <- Indoor_hourly_24 %>%
    filter_school_days(term_dates, national_holidays_dates) %>%
    group_by(Date, device_id) %>%
    summarise(across(
      c(pm01, pm02, rco2, pm10, rhum, atmp, tvoc),
      list(
        mean   = ~mean(., na.rm = TRUE),
        median = ~median(., na.rm = TRUE),
        q25    = ~quantile(., 0.25, na.rm = TRUE),
        q75    = ~quantile(., 0.75, na.rm = TRUE)
      )
    ),
    DateDevice = first(DateDevice),
    .groups = "drop") %>%
    left_join(df_pp_ventilation,
              by = c("Date", "device_id", "DateDevice"))
  
  cat("\nMatching indoor-outdoor data")
  ## ---------------- Indoor-Outdoor matching ----------------
  # Filter AURN data for this month
  df_outdoor_month <- df_outdoor_aurn_full %>%
    filter(Date >= month_start & Date <= month_end)
  
  In_Out_24 <- closest_site_each_day(
    Indoor_hourly_24 %>%
      left_join(df_school_chars %>% 
                  select("MonitorReferenceId", SchoolId_short, SchoolLongitude, SchoolLatitude) %>% 
                  rename("device_id" = "MonitorReferenceId")), 
    df_outdoor_month
  )
  
  In_Out_SchHrs <- closest_site_each_day(
    Indoor_hourly_SchHrs %>%
      left_join(df_school_chars %>% 
                  select("MonitorReferenceId", SchoolId_short, SchoolLongitude, SchoolLatitude) %>% 
                  rename("device_id" = "MonitorReferenceId")), 
    df_outdoor_month
  )
  
  ## ---------------- Ensemble summaries ----------------
  list(
    Indoor_hourly_24      = Indoor_hourly_24,
    Indoor_hourly_SchHrs  = Indoor_hourly_SchHrs,
    Indoor_daily_24       = Indoor_daily_24,
    Indoor_daily_SchHrs   = Indoor_daily_SchHrs,
    df_pp_ventilation     = df_pp_ventilation,
    In_Out_24             = In_Out_24,
    In_Out_SchHrs         = In_Out_SchHrs,
    SAMHE_Sch_hourly_mean = Indoor_hourly_SchHrs %>%
      group_by(DateTime) %>% summarise(across(everything(), ~mean(.x, na.rm = TRUE))),
    SAMHE_24_hourly_mean  = Indoor_hourly_24 %>%
      group_by(DateTime) %>% summarise(across(everything(), ~mean(.x, na.rm = TRUE))),
    SAMHE_Sch_daily_mean  = Indoor_daily_SchHrs %>%
      rename_with(~sub("_mean$", "", .x)) %>%
      group_by(Date) %>% summarise(across(everything(), ~mean(.x, na.rm = TRUE))),
    SAMHE_24_daily_mean   = Indoor_daily_24 %>%
      rename_with(~sub("_mean$", "", .x)) %>%
      group_by(Date) %>% summarise(across(everything(), ~mean(.x, na.rm = TRUE)))
  )
}

## ------------------------------------------------------------
## Run month-by-month and save
## ------------------------------------------------------------
for (m in months_seq) {
  
  m <- as.Date(m)   # ensure it's a Date
  
  outfile <- file.path(
    out_dir,
    paste0("IAQ_", format(m, "%Y_%m"), ".rds")
  )
  
  if (file.exists(outfile)) {
    message("Skipping ", basename(outfile))
    next
  }
  
  monthly_results <- process_one_month(m, df_Outdoor_AURN)
  saveRDS(monthly_results, outfile)
  #rm(monthly_results)
  gc()
}
