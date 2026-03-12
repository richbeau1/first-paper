# Dataset formation functions 
# Formats POSIXct to RFC3339 Zulu for Flux time literals
library(readr )
to_flux_time <- function(x) {
  x <- lubridate::with_tz(as.POSIXct(x, tz = "UTC"), tzone = "UTC")
  format(x, "%Y-%m-%dT%H:%M:%SZ")
}

### parallel large IAQ w/ diagnostics
large_IAQ_timeseries <- function(start_date, end_date, interval, device_ids, tz = "UTC") {
  
  cat("\n=== large_IAQ_timeseries DIAGNOSTICS ===\n")
  cat("Start date:", start_date, "\n")
  cat("End date:", end_date, "\n")
  cat("Interval:", interval, "\n")
  cat("Device IDs:", if(is.character(device_ids) && nchar(device_ids) == 0) "EMPTY STRING" else device_ids, "\n")
  cat("Timezone:", tz, "\n")
  flush.console()
  
  # Normalize inputs to POSIXct
  start_ts <- as.POSIXct(start_date, tz = tz)
  end_ts   <- as.POSIXct(end_date,   tz = tz)
  
  cat("Normalized start_ts:", as.character(start_ts), "\n")
  cat("Normalized end_ts:", as.character(end_ts), "\n")
  flush.console()
  
  # Decide chunk size: 10 minutes if 1m, else daily
  by_arg <- if (identical(interval, "1m")) "10 min" else "1 day"
  cat("Chunk size (by_arg):", by_arg, "\n")
  flush.console()
  
  # Build chunk boundaries
  cat("Building chunk boundaries...\n")
  flush.console()
  cuts <- seq(from = start_ts, to = end_ts, by = by_arg)
  if (tail(cuts, 1) < end_ts) cuts <- c(cuts, end_ts)
  
  n_chunks <- length(cuts) - 1L
  cat("Number of chunks to process:", n_chunks, "\n")
  flush.console()
  
  if (n_chunks <= 0L) {
    cat("No chunks to process - returning empty data.frame\n")
    return(data.frame())
  }
  
  cat("Starting parallel processing with future.apply::future_lapply...\n")
  flush.console()
  
  # Use future.apply for true parallel execution
  df_list <- future.apply::future_lapply(seq_len(n_chunks), function(i) {
    s <- cuts[i]
    e <- cuts[i + 1]
    
    # Print progress (may not show in parallel mode)
    cat(sprintf("Processing chunk %d/%d: %s to %s\n", 
                i, n_chunks, 
                as.character(s), 
                as.character(e)))
    flush.console()
    
    tryCatch({
      result <- IAQ_timeseries(
        start_date = to_flux_time(s),
        end_date   = to_flux_time(e),
        interval   = interval,
        device_ids = device_ids
      )
      
      cat(sprintf("Chunk %d complete: %d rows\n", i, nrow(result)))
      flush.console()
      
      return(result)
    }, error = function(err) {
      warning(sprintf("Chunk %d failed: %s", i, err$message))
      cat(sprintf("ERROR in chunk %d: %s\n", i, err$message))
      flush.console()
      return(NULL)
    })
  }, future.seed = TRUE)
  
  cat("\nParallel processing complete. Processing results...\n")
  flush.console()
  
  # Filter out NULL and empty results
  cat("Filtering NULL and empty results...\n")
  flush.console()
  df_list <- Filter(function(x) !is.null(x) && is.data.frame(x) && nrow(x) > 0, df_list)
  
  cat("Valid chunks remaining:", length(df_list), "\n")
  flush.console()
  
  if (length(df_list) == 0) {
    warning("No data returned from any chunks")
    cat("WARNING: No data returned from any chunks\n")
    return(data.frame())
  }
  
  # Ensure all data.frames have consistent structure before binding
  cat("Standardizing column structure...\n")
  flush.console()
  all_cols <- unique(unlist(lapply(df_list, names)))
  cat("Total unique columns:", length(all_cols), "\n")
  flush.console()
  
  # Standardize each chunk to have all columns in same order
  df_list <- lapply(df_list, function(df) {
    missing_cols <- setdiff(all_cols, names(df))
    if (length(missing_cols) > 0) {
      df[missing_cols] <- NA
    }
    df[, all_cols]
  })
  
  # Bind rows
  cat("Binding rows...\n")
  flush.console()
  final_df <- dplyr::bind_rows(df_list)
  
  cat("Final data.frame dimensions:", nrow(final_df), "rows x", ncol(final_df), "cols\n")
  test_df <<- final_df
  cat("=== large_IAQ_timeseries COMPLETE ===\n\n")
  flush.console()
  
  return(final_df)
}
########

# function to filter a data set for only Single Mobile Monitor schools 
# Create a list of Single moving monitor schools (MonitoringType = 1)

filter_SMM_schools <- function(Indoor_minute, df_school_monitors, research_schools){
  SMM_schools <- research_schools %>% filter(MonitoringType == 1) %>% 
    left_join(df_school_monitors, by ="SchoolId_long") %>%
    rename("device_id" = "MonitorReferenceId") %>% 
    filter(!is.na(device_id))
  
  Indoor_minute <- Indoor_minute  %>%
    filter(device_id %in% SMM_schools$device_id)
  return(Indoor_minute)
}

# function to filter a data set for only Multiple Static Monitor schools 
filter_MSM_schools <- function(Indoor_minute, df_school_monitors, research_schools){
  MSM_schools <- research_schools %>% filter(MonitoringType == 2) %>% 
    left_join(df_school_monitors, by ="SchoolId_long") %>%
    rename("device_id" = "MonitorReferenceId") %>% 
    filter(!is.na(device_id))
  
  Indoor_minute <- Indoor_minute  %>%
    filter(device_id %in% MSM_schools$device_id)
  return(Indoor_minute)
}

# function to remove any days of data with very high values 
clean_minutely_data <- function(df_IAQ_minutely){
  
  df_IAQ_minutely <- df_IAQ_minutely %>% 
    mutate(Date = as.Date(DateTime, format = "%Y-%m-%d")) %>%
    mutate(DateDevice = paste(Date, device_id))
  
  df_daily_summary <- df_IAQ_minutely %>% 
    group_by(device_id, Date) %>%
    summarise(co2_max = max(rco2), pm02_max = max(pm02), temp_max = max(atmp) )
  
  bad_days <- df_daily_summary %>% 
    filter((co2_max > 5000)|(pm02_max >1000) |(temp_max >50)) %>%
    mutate(DateDevice = paste(Date, device_id))
  
  df_IAQ_minutely <- df_IAQ_minutely %>% 
    filter(!DateDevice %in% bad_days$DateDevice)
  return(df_IAQ_minutely)
}

# function that filters data to only included days for which the mean CO2 over 
#   the school day is greater than 480 ppm AND the rate of change of CO2 
#   concentration is greater than 100ppm per 15mins at some point in the school day. 
clean_minutely_CO2 <- function(df_IAQ_minutely) {
  ## STAGE 0: make sure complete school days of data 
  
  # Format a Date and DateDevice column
  df_IAQ_minutely <- df_IAQ_minutely %>% 
    mutate(Date = as.Date(DateTime, format = "%Y-%m-%d")) 
  
  #COMBINE STEP 0 and STEP 1
  good_days_1 <- df_IAQ_minutely %>% 
    filter(hour(DateTime) >= 9 & hour(DateTime) <= 15) %>%
    group_by(Date,device_id) %>%
    summarise(co2_mean = mean(rco2), 
              N = sum(!is.na(DateTime)), 
              Date = first(Date), 
              device_id = first(device_id))%>% 
    # Take only sch days more than 75% data
    filter(N > 315) %>% # sch day = 7 hours, 420 minutes, 75% = 315 minutes 
    # Take only daily means greater than 480
    filter((co2_mean > 480))    # collect days when daily mean > 480  
  
  # filter for good days only 
  df_IAQ_minutely <- df_IAQ_minutely %>% 
    semi_join(good_days_1, by = c("Date", "device_id"))
  
  ## STAGE 2: FILTER BASED ON CO2 GRADIENT
  good_days_2 <- df_IAQ_minutely %>% 
    # Create 5 min rolling average (in case of noise)
    mutate(co2_rol_av = rollmean(rco2, k=5, fill=NA, align='right')) %>%
    # Create column for rolling diff btwn 7 mins before and after
    mutate(rolling_co2_diff =  lead(co2_rol_av, 7) - lag(co2_rol_av, 7)) %>%
    filter(hour(DateTime) >= 9 & hour(DateTime) <= 15) %>%
    group_by(Date,device_id) %>%
    # Collect days for which rolling diff exceed 100 btwn 9 and 4
    filter(any(rolling_co2_diff > 100))%>% 
    ungroup()
  # Filter for DateDevice in good_days 
  df_IAQ_minutely <- df_IAQ_minutely %>% 
    semi_join(good_days_2, by = c("Date", "device_id"))
  
  return(df_IAQ_minutely)
}

# function that filters data to only included days for which the mean CO2 over 
#   the school day is greater than 480 ppm AND the rate of change of CO2 
#   concentration is greater than 100ppm per 15mins at some point in the school day. 
clean_CO2_15m <- function(df_IAQ_minutely) {
  setDT(df_IAQ_minutely)
  
  df_IAQ_minutely[, DateTime := as.POSIXct(DateTime)]
  df_IAQ_minutely[, Date := as.Date(DateTime)]
  df_IAQ_minutely[, hour := hour(DateTime)]
  
  # Stage 1
  good_days_1 <- df_IAQ_minutely[hour >= 9 & hour <= 15, .(
    co2_mean = mean(rco2, na.rm = TRUE),
    N = .N
  ), by = .(Date, device_id)][
    N > 21 & co2_mean > 480
  ]
  
  df_IAQ_minutely <- df_IAQ_minutely[good_days_1, on = .(Date, device_id), nomatch = NULL]
  
  # Stage 2: CO2 gradient (200 over 30 mins = 2 intervals of 15m)
  df_IAQ_minutely[order(device_id, DateTime),
                  rolling_co2_diff := shift(rco2, n = -1, type = "lead") - rco2,
                  by = device_id
  ]
  
  good_days_2 <- df_IAQ_minutely[hour >= 9 & hour <= 15][,
                                                         .(has_gradient = any(rolling_co2_diff > 200, na.rm = TRUE)),
                                                         by = .(Date, device_id)
  ][has_gradient == TRUE]
  
  df_IAQ_minutely[, c("hour", "rolling_co2_diff") := NULL]
  df_IAQ_minutely[good_days_2, on = .(Date, device_id), nomatch = NULL]
}

term_dates_files <- list.files(paste0(path, "/term_nh_dates"))
term_dates <-  read_csv(paste0(path, "/term_nh_dates/", term_dates_files[[1]]), 
                        col_types = cols(
                          academic_year = col_character(),
                          term_name = col_character(),
                          start_date = col_date(),
                          end_date = col_date())) 
national_holidays_dates <- read_csv(paste0(path, "/term_nh_dates/", term_dates_files[[2]]), 
                                    col_types = cols(date = col_date()))


# Filter based on date

filter_school_days <- function(Indoor_minute, term_dates, national_holidays_dates) {
  setDT(Indoor_minute)
  setDT(term_dates)
  setDT(national_holidays_dates)
  
  Indoor_minute[, DateTime := as.POSIXct(DateTime)]
  Indoor_minute[, Date := as.Date(DateTime)]
  Indoor_minute[, hour := hour(DateTime)]
  Indoor_minute[, weekday := weekdays(DateTime)]
  
  # Filter conditions using data.table syntax
  result <- Indoor_minute[
    !Date %in% national_holidays_dates$date &
      !weekday %in% c("Saturday", "Sunday") &
      hour >= 9 & hour <= 15
  ]
  
  # Efficiently check term dates using binary search
  setkey(term_dates, start_date, end_date)
  result <- result[, in_term := {
    any(sapply(seq_len(.N), function(i) {
      any(Date[i] >= term_dates$start_date & 
            Date[i] <= term_dates$end_date + 1)
    }))
  }, by = Date][in_term == TRUE]
  
  result[, c("hour", "weekday", "in_term") := NULL]
  result[]
}


# Filter that takes school-day data (between 9 and 4) and filters for complete data
filter_75PC_school_day_minutely <- function(Minutely_data) {
  setDT(Minutely_data)
  Minutely_data[, Date := as.Date(DateTime)]
  
  good_days <- Minutely_data[, .N, by = .(Date, device_id)][N > 315]
  Minutely_data[good_days, on = .(Date, device_id), nomatch = NULL]
}

filter_75PC_school_day_15m <- function(IAQ_data) {
  setDT(IAQ_data)
  IAQ_data[, Date := as.Date(DateTime)]
  
  good_days <- IAQ_data[, .N, by = .(Date, device_id)][N >= 21]
  IAQ_data[good_days, on = .(Date, device_id), nomatch = NULL]
}

filter_75PC_school_day_hourly <- function(IAQ_data) {
  setDT(IAQ_data)
  IAQ_data[, Date := as.Date(DateTime)]
  
  good_days <- IAQ_data[, .N, by = .(Date, device_id)][N > 5]
  IAQ_data[good_days, on = .(Date, device_id), nomatch = NULL]
}

filter_75PC_24hr_day_minutely <- function(Minutely_data) {
  setDT(Minutely_data)
  Minutely_data[, Date := as.Date(DateTime)]
  
  good_days <- Minutely_data[, .N, by = .(Date, device_id)][N > 1080]
  Minutely_data[good_days, on = .(Date, device_id), nomatch = NULL]
}

filter_75PC_24hr_day_hourly <- function(Hourly_data) {
  setDT(Hourly_data)
  Hourly_data[, Date := as.Date(DateTime)]
  
  good_days <- Hourly_data[, .N, by = .(Date, device_id)][N >= 18]
  Hourly_data[good_days, on = .(Date, device_id), nomatch = NULL]
}

## Finds closest AURN sites every day
closest_site_each_day <- function(Indoor_hourly, Outdoor_hourly) {
  # Safety checks (optional but recommended)
  required_indoor <- c("Date", "DateTime", "DateDevice", "SchoolId_short", "SchoolLatitude", "SchoolLongitude")
  required_outdoor <- c("Date", "DateTime", "code", "latitude", "longitude")
  stopifnot(all(required_indoor %in% names(Indoor_hourly)))
  stopifnot(all(required_outdoor %in% names(Outdoor_hourly)))
  
  # Process each date independently to ensure nearest station is chosen from same-day stations
  unique_dates <- unique(Indoor_hourly$Date)
  
  results <- map_dfr(unique_dates, function(DateI) {
    # Indoor data for this date
    filtered_indoor <- Indoor_hourly %>%
      ungroup() %>%
      filter(Date == DateI, !is.na(SchoolLatitude), !is.na(SchoolLongitude))
    
    if (nrow(filtered_indoor) == 0) return(NULL)
    
    # Unique indoor devices (one row per device, per date)
    unique_date_device <- filtered_indoor %>%
      distinct(DateDevice, SchoolId_short, SchoolLatitude, SchoolLongitude,.keep_all = TRUE)
    
    # Outdoor stations for this date (one row per code, per date)
    filtered_outdoor <- Outdoor_hourly %>%
      ungroup() %>%
      filter(Date == DateI)
    
    unique_date_code <- filtered_outdoor %>%
      distinct(code, latitude, longitude, Date,.keep_all = TRUE)
    
    if (nrow(unique_date_code) == 0) return(NULL)
    
    # Compute geodesic distance matrix (meters) between indoor devices and outdoor stations
    indoor_coords  <- unique_date_device %>% select(SchoolLatitude, SchoolLongitude) %>% rename(latitude = SchoolLatitude, longitude= SchoolLongitude)
    outdoor_coords <- unique_date_code %>% select(latitude, longitude)
    dm <- geodist::geodist(indoor_coords, outdoor_coords, measure = "geodesic")  # meters
    
    # For each indoor device (row), find index of nearest station (min distance across columns)
    nearest_j <- max.col(-dm, ties.method = "first")
    nearest_dist_m <- dm[cbind(seq_len(nrow(dm)), nearest_j)]
    nearest_codes <- unique_date_code$code[nearest_j]
    
    # Build nearest-station summary per device
    closest_tbl <- unique_date_device %>%
      transmute(
        SchoolId_short,
        closest_AURN = nearest_codes,
        km_distance_to_AURN = as.numeric(nearest_dist_m) / 1000
      )
    
    # Join back to all indoor rows for the date, then link with outdoor readings at the chosen station
    filtered_indoor %>%
      left_join(closest_tbl, by = "SchoolId_short") %>%
      left_join(
        filtered_outdoor,
        by = c("closest_AURN" = "code", "DateTime", "Date")
      )
  })
  
  return(results)
}

