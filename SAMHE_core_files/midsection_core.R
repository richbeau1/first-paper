library(influxdbclient)
## Load username/pwd
path <- getwd()
cred_file <- paste0(path,"/db_credentials.txt")
creds <- readLines(cred_file)
creds <- trimws(creds)
creds <- setNames(
  sub("^[^=]+=", "", creds),
  sub("=.*$", "", creds)
)

midsection_token <- creds[["token"]]

#Connect to the main InfluxDB database
client <- InfluxDBClient$new(url = "https://westeurope-1.azure.cloud2.influxdata.com",
                             token = midsection_token,
                             org = "samhe")
buckets <- client$query('buckets()', POSIXctCol = NULL) #List all buckets in client


query_string <- function(flux_query){ #Take a line by line string and remove spaces so flux can handle it
  query_string<- gsub("[\r\n]", "", flux_query)  #makes query parameters into something fluxdb can handle 
  return(query_string)
}

# Take list of dataframes returned from flux and create a single df

unlist_df <- function(flux_list){
  df<- data.frame()
  for (x in 1:length(flux_list)) {
    #df<- rbind(df, flux_list[[x]])
    if( length(flux_list[[x]]) ==length(flux_list[[1]]) ){ #Assumes 1 is the right length
      #df<- bind_rows(df, flux_list[[x]])
      df<- rbind(df, flux_list[[x]])
    }
  }
  return(df)
}

#FUNCTIONS -----------
IAQ_timeseries <- function(start_date, end_date, interval, device_ids){#Return midsection timeseries for list of monitor IDs
  #print("Querying all device ids")
  monitor_filter_string <- "" 
  
  #put together query out of arguments
  bucket_string <- 'from(bucket: "prod") '
  range_string <- paste0('|> range(start: ', start_date,', stop: ', end_date, ')')
  interval_string <- paste0('|> window(every: ', interval, ')')
  averaging_string <- paste0('|>mean()')
  finish_string <- '|> group(columns: [ "device_id"]) |> duplicate(column: "_start", as: "_time")|> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value") '
  #print(paste0("Monitors filter string =  ",  monitor_filter_string))
  
  #Assemble query from components and run it
  assembled_query <- paste0(bucket_string, range_string, monitor_filter_string, interval_string,  averaging_string, finish_string)
  query_noline <- query_string(assembled_query)
  
  #print(query_noline)
  
  df <- client$query(query_noline) %>%  #Query FLUX and organise output
    unlist_df() %>%
    #filter(!(device_id %in% !!excluded_sensors)) %>% #Uncomment this
    select(- "time") %>%
    rename(DateTime= "_time") %>%
    relocate(DateTime,  device_id, pm01, pm02, pm10, rhum, atmp, rco2, tvoc)
  
}

