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

# Define your start and end dates for raw data
start_date <- paste0(target_year, "-08-01")
end_date <- paste0(target_year , "-10-31")

query_cores <- 11
plan(multisession, workers = query_cores)  # 

Indoor_min <- large_IAQ_timeseries(start_date = start_date , end_date = end_date, interval = "1m", "") %>%
    mutate(DateTime = as.POSIXct(DateTime)) %>%
    mutate(Date = as.Date(DateTime))

Indoor_min_24 <- Indoor_min %>%
  # ENSURE THAT ONLY SCHOOL MONITORS ARE INCLUDED, 3 options: 
  # 1. Keep only single monitor schools 
  #filter_SMM_schools(.,df_school_monitors, research_schools)  %>%
  # 2. OR keep only multiple monitor schools 
  # filter_MSM_schools(.,df_school_monitors, research_schools)  %>%
  # 3. OR keep all school monitors (remove any monitors not placed in schools)
   filter(device_id %in% df_school_monitors$MonitorReferenceId)%>%
  filter_75PC_24hr_day_minutely() %>%
  clean_minutely_data()%>%
  clean_minutely_CO2() 

##SLOW
Indoor_min_SchHrs <- Indoor_min_24 %>%
  filter_75PC_school_day_minutely() %>%
  filter_school_days(., term_dates, national_holidays_dates)

Indoor_hourly_SchHrs <- Indoor_min_SchHrs %>%
  group_by(DateTime = floor_date(DateTime, unit = "hour"), device_id) %>% 
  summarise(
    across(c(pm01, pm02, rco2, pm10, rhum, atmp, tvoc),
           ~ mean(.x, na.rm = TRUE),
           .names = "{.col}"),
    DateDevice = first(DateDevice),
    Date       = first(Date),
    .groups    = "drop"
  )

Indoor_hourly_24 <- Indoor_min_24 %>%
  group_by(DateTime = floor_date(DateTime, unit = "hour"), device_id) %>% 
  summarise(
    across(c(pm01, pm02, rco2, pm10, rhum, atmp, tvoc),
           ~ mean(.x, na.rm = TRUE),
           .names = "{.col}"),
    DateDevice = first(DateDevice),
    Date       = first(Date),
    .groups    = "drop"
  )

Indoor_daily_24 <- Indoor_min_24 %>% 
  group_by(Date,device_id) %>%
  summarise(across(c(pm01, pm02, rco2, pm10, rhum, atmp, tvoc), list( 
    mean = ~mean(., na.rm = TRUE),
    median = ~median(., na.rm = TRUE),
    q25 = ~quantile(., 0.25, na.rm = TRUE),
    q75 = ~quantile(., 0.75, na.rm = TRUE)
  )))


# Caclulate per person ventilation rate daily 
# Calculate ventilation rates based upon 24 hour data 
T_occ <- 5*1.048 # Occupancy factor (5) multiplied by metabolic rate (1.048) to give litres per second per person (L/s/p) for light activity (https://www.cibse.org/knowledge/cibse-handbook/3-people-and-activity/31-occupant-activity-levels)

df_pp_ventilation_ <- Indoor_hourly_24 %>%
  filter_SMM_schools(.,df_school_monitors, research_schools)  %>%
  filter(hour(DateTime) >= 5 & hour(DateTime) <= 22) %>%  
  left_join(df_school_monitors, by = c("device_id" = "MonitorReferenceId"))%>%
  mutate(schoolday_Id = paste0(device_id, "-", as.Date(DateTime) )) %>%
  mutate(C_excess = (rco2 -412)/1000000)%>%
  mutate(primary_secondary = case_when(
    (SchoolType %in% c(1,2,3)) ~ "Primary",
    (SchoolType %in% c(4,8,12,16,20,24,28)) ~ "11-plus",
    .default = "Mixed"))%>% 
  mutate(G_pp = case_when(primary_secondary == "Primary" ~ 3.1,  #2.12 calc
                          primary_secondary == "11-plus" ~ 4.32,
                          primary_secondary == "Mixed" ~ 4.32)) %>% 
  ungroup()%>%
  arrange(device_id, DateTime) %>%
  group_by(Date, device_id) %>%
  mutate(t_diff = replace_na(  as.numeric(DateTime) - as.numeric(lag(DateTime, default = NA)) , 0)   )%>%
  mutate(C_trap = (t_diff /3600) * (C_excess + lag(C_excess, default = 0)) / 2) %>%
  mutate(C_integral = cumsum(C_trap))  %>%
  ungroup() %>%
  group_by(SchoolId_long, device_id, Date) %>% 
  summarise(pp_ventilation = 0.001*T_occ * mean(G_pp) / sum(C_trap) , CO2_mean = mean(rco2)) %>% 
  arrange(desc(pp_ventilation)) %>%
  mutate(DateDevice = paste(Date, device_id))


# Calculate school day daily data
Indoor_daily_SchHrs <- Indoor_hourly_24 %>%
  filter_school_days(., term_dates = term_dates,
                     national_holidays_dates = national_holidays_dates) %>% 
  group_by(Date, device_id) %>%
  summarise(across(c(pm01, pm02, rco2, pm10, rhum, atmp, tvoc), list( 
    mean = ~mean(., na.rm = TRUE),
    median = ~median(., na.rm = TRUE),
    q25 = ~quantile(., 0.25, na.rm = TRUE),
    q75 = ~quantile(., 0.75, na.rm = TRUE)
  )),
  DateDevice=first(DateDevice),
  Date=first(Date))  %>%
  # 
  left_join(df_pp_ventilation_, by =c("Date", "device_id", "DateDevice"))

### Ensemble data sets - taking averages across all schools 
# The mean of the minutely mean concentrations of all schools reporting each hour
SAMHE_min_SchHrs <- Indoor_min_SchHrs %>%  
  group_by(DateTime) %>%
  summarise(across(c(pm01, pm02, rco2, pm10, rhum, atmp, tvoc), list(
    mean = ~mean(., na.rm = TRUE),
    median = ~median(., na.rm = TRUE),
    q25 = ~quantile(., 0.25, na.rm = TRUE),
    q75 = ~quantile(., 0.75, na.rm = TRUE)
  )))

# The mean of the hourly mean concentrations of all schools reporting each hour
SAMHE_Sch_hourly_mean <- Indoor_hourly_SchHrs %>%  
  group_by(DateTime) %>%
  summarise(across(c(pm01, pm02, rco2, pm10, rhum, atmp, tvoc), list(
    mean = ~mean(., na.rm = TRUE),
    median = ~median(., na.rm = TRUE),
    q25 = ~quantile(., 0.25, na.rm = TRUE),
    q75 = ~quantile(., 0.75, na.rm = TRUE)
  )))

# The mean of the school day mean concentrations of all schools reporting each school day 
SAMHE_Sch_daily_mean <- Indoor_daily_SchHrs %>%  
  rename(pm01 = pm01_mean, pm02 = pm02_mean,  rco2= rco2_mean, pm10 = pm10_mean, rhum = rhum_mean, atmp = atmp_mean, tvoc = tvoc_mean)  %>%
  group_by(Date) %>%
  summarise(across(c(pm01, pm02, rco2, pm10, rhum, atmp, tvoc), list(
    mean = ~mean(., na.rm = TRUE),
    median = ~median(., na.rm = TRUE),
    q25 = ~quantile(., 0.25, na.rm = TRUE),
    q75 = ~quantile(., 0.75, na.rm = TRUE)
  )))

# The mean of the minutely mean concentrations of all schools reporting each hour
SAMHE_min_24 <- Indoor_min_24 %>%  
  group_by(DateTime) %>%
  summarise(across(c(pm01, pm02, rco2, pm10, rhum, atmp, tvoc), list(
    mean = ~mean(., na.rm = TRUE),
    median = ~median(., na.rm = TRUE),
    q25 = ~quantile(., 0.25, na.rm = TRUE),
    q75 = ~quantile(., 0.75, na.rm = TRUE)
  )))

# The mean of the hourly mean concentrations of all schools reporting each hour
SAMHE_24_hourly_mean <- Indoor_hourly_24 %>%  
  group_by(DateTime) %>%
  summarise(across(c(pm01, pm02, rco2, pm10, rhum, atmp, tvoc), list(
    mean = ~mean(., na.rm = TRUE),
    median = ~median(., na.rm = TRUE),
    q25 = ~quantile(., 0.25, na.rm = TRUE),
    q75 = ~quantile(., 0.75, na.rm = TRUE)
  )))

# The mean of the school day mean concentrations of all schools reporting each school day 
SAMHE_24_daily_mean <- Indoor_daily_24 %>%  
  rename(pm01 = pm01_mean, pm02 = pm02_mean,  rco2= rco2_mean, pm10 = pm10_mean, rhum = rhum_mean, atmp = atmp_mean, tvoc = tvoc_mean)  %>%
  group_by(Date) %>%
  summarise(across(c(pm01, pm02, rco2, pm10, rhum, atmp, tvoc), list(
    mean = ~mean(., na.rm = TRUE),
    median = ~median(., na.rm = TRUE),
    q25 = ~quantile(., 0.25, na.rm = TRUE),
    q75 = ~quantile(., 0.75, na.rm = TRUE)
  )))


### Outdoor data ##############################################################
start_year <- target_year
end_year <- as.character(as.numeric(target_year) + 1)

aurn_meta <- importMeta(year = start_year:end_year) %>% 
  filter(!site_type %in% c("Urban Traffic", "Urban Industrial", "Suburban Industrial"))

df_Outdoor_AQ <- importAURN(year = start_year:end_year, data_type = "hourly", pollutant = "all", site = aurn_meta$code) %>% 
  rename(pm10_out = pm10)

df_Outdoor_AURN <- df_Outdoor_AQ %>% 
  mutate(DateTime = as.POSIXct(date), Date = as.Date(date)) %>% 
  # Convert outdoor data to local UK time (BST/GMT aware)
  mutate(DateTime = with_tz(DateTime, tzone = "Europe/London")) %>%
  rename(device_id = code) %>%
  filter_75PC_24hr_day_hourly %>%
  rename(code = device_id)%>%
  mutate(DateCode = paste0(Date,code)) %>%
  group_by(DateCode) %>%  # Replace group_column with the actual grouping column(s) you have
  filter(mean(is.na(pm2.5)) <= 0.25) %>%
  left_join(aurn_meta )



In_Out_24 <- closest_site_each_day(Indoor_hourly_24 %>%
                                     left_join(df_school_chars %>% select("MonitorReferenceId", SchoolId_short, SchoolLongitude, SchoolLatitude) %>% rename("device_id" ="MonitorReferenceId")), 
                                   df_Outdoor_AURN)


In_Out_SchHrs <- closest_site_each_day(Indoor_hourly_SchHrs %>%
                                     left_join(df_school_chars %>% select("MonitorReferenceId",  SchoolId_short, SchoolLongitude, SchoolLatitude)%>% rename("device_id" ="MonitorReferenceId")), 
                                     df_Outdoor_AURN )


save(
  Indoor_hourly_SchHrs,
  Indoor_hourly_24,
  Indoor_daily_24,
  df_pp_ventilation_,
  Indoor_daily_SchHrs,
  SAMHE_Sch_hourly_mean,
  SAMHE_Sch_daily_mean,
  SAMHE_24_hourly_mean,
  SAMHE_24_daily_mean,
  aurn_meta,
  df_Outdoor_AQ,
  df_Outdoor_AURN,
  In_Out_24,
  In_Out_SchHrs,
  file = paste0("SAMHE_", target_year, ".RData")
)

