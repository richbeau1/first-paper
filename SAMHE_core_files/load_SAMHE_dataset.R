## Load official SAMHE dataset - minutely data not yet included
library(dplyr)
#rm(list = ls())  #unload all variables

#In Rstudio this sets the source file directory to file dir
source_directory <- setwd(dirname(rstudioapi::getActiveDocumentContext()$path))


root_dir <- paste0(source_directory, "/IAQ_monthly_outputs")
files <- list.files(
  root_dir,
  pattern = "\\.rds$",
  full.names = TRUE,
  recursive = TRUE
)

## Read all monthly lists
monthly_lists <- lapply(files, readRDS)
# collect all table names
all_names <- unique(unlist(lapply(monthly_lists, names)))

# recombine by table name
IAQ_all <- setNames(
  lapply(all_names, function(nm) {
    bind_rows(lapply(monthly_lists, `[[`, nm))
  }),
  all_names
)

## Main SAMHE tables

Indoor_hourly_24      <- IAQ_all$Indoor_hourly_24 #Hourly means across all schools
Indoor_hourly_SchHrs  <- IAQ_all$Indoor_hourly_SchHrs #Hourly means across only 0900-1600

Indoor_daily_24      <- IAQ_all$Indoor_daily_24 #24 -hr daily means across all schools
Indoor_daily_SchHrs   <- IAQ_all$Indoor_daily_SchHrs # Daily means across only 0900-1600, filtered for occupancy

df_pp_ventilation     <- IAQ_all$df_pp_ventilation # per person ventilation rate calulated by method used by Finneran and Burridge

In_Out_24             <- IAQ_all$In_Out_24 # 24 hour daily means periods including outdoor data from closest AURN station	
In_Out_SchHrs         <- IAQ_all$In_Out_SchHrs # 0900-1600 daily means including ourdoor data from closest AURN station

