## Set working directory to this script's folder (RStudio only)
rm(list = ls())  #unload all variables

get_current_script_path <- function() {
  # 1. Called via Rscript or commandArgs
  args <- commandArgs(trailingOnly = FALSE)
  file_arg <- "--file="
  path_from_args <- sub(file_arg, "", args[grep(file_arg, args)])
  if (length(path_from_args) == 1 && nzchar(path_from_args)) {
    return(normalizePath(path_from_args))
  }
  
  # 2. Called via source() [works in most cases]
  if (!is.null(sys.frames()[[1]]$ofile)) {
    return(normalizePath(sys.frames()[[1]]$ofile))
  }
  
  # 3. RStudio interactive
  if (requireNamespace("rstudioapi", quietly = TRUE) && rstudioapi::isAvailable()) {
    ctx_path <- rstudioapi::getActiveDocumentContext()$path
    if (nzchar(ctx_path)) {
      return(normalizePath(ctx_path))
    }
  }
  
  return(NA_character_)
}

script_path <- get_current_script_path()
if (!is.na(script_path)) {
  setwd(dirname(script_path))
}

path <- dirname(script_path)

library("pacman")
pacman::p_load(dplyr, DBI, odbc, tidyr, stringr, PostcodesioR)

## Load username/pwd
cred_file <- "db_credentials.txt"
creds <- readLines(cred_file)
creds <- trimws(creds)
creds <- setNames(
  sub("^[^=]+=", "", creds),
  sub("=.*$", "", creds)
)

uid <- creds[["UID"]]
pwd <- creds[["PWD"]]


## Make connection to relational db - odbc driver 18 must be installed
#--- Connect to relational database ---
con_relational <- dbConnect(
  odbc(),
  Driver                  = "ODBC Driver 18 for SQL Server",
  Server                  = "sql-imp-samhe-prod.database.windows.net",
  Database                = "sqldb-samhe-prod",
  UID                     = uid,
  PWD                     = pwd,
  Port                    = 1433,
  Encrypt                 = "yes",
  TrustServerCertificate  = "no",
  Authentication          = "SqlPassword"
)

#--- Safe read helper ---
safe_read <- function(con, table) {
  tryCatch(
    dbReadTable(con, table),
    error = function(e) {
      message(paste("Falling back to dbGetQuery for", table))
      dbGetQuery(con, paste0("SELECT * FROM ", table))
    }
  )
}

postcode_format <- function(x) {
  if (is.na(x) || x == "") return(NA_character_)
  x <- toupper(trimws(x))                     # remove spaces and capitalise
  x <- gsub("[^A-Z0-9]", "", x)               # remove any non-alphanumeric
  # insert a space before the last 3 characters (if not already there)
  formatted <- sub("([A-Z0-9]+)([0-9][A-Z]{2})$", "\\1 \\2", x)
  # ensure valid length (5–8 chars inc. space)
  if (nchar(formatted) < 5 || nchar(formatted) > 8) return(NA_character_)
  return(formatted)
}

#--- Load views ---
research_schools                <- safe_read(con_relational, "vw_Research_Schools")
research_monitors               <- safe_read(con_relational, "vw_Research_Monitors")
research_activities             <- safe_read(con_relational, "vw_Research_Activities")
research_activity_steps         <- safe_read(con_relational, "vw_Research_ActivitySteps")
research_user_scores            <- safe_read(con_relational, "vw_Research_UserScores")
research_school_establishments  <- safe_read(con_relational, "vw_Research_SchoolEstablishments")
research_SchoolUsers            <- safe_read(con_relational, "vw_Research_SchoolUsers")
research_UserAchievementProgresses <- safe_read(con_relational, "vw_Research_UserAchievementProgresses")
research_Rooms                  <- safe_read(con_relational, "vw_Research_Rooms")
research_monitor_locs           <- safe_read(con_relational, "vw_Research_MonitorLocationHistories")
monitor_requests <- safe_read(con_relational,"vw_Research_RequestMonitorSubmissions")


research_activity_steps_progress <- dbGetQuery(
  con_relational,
  "
  SELECT 
    Id, 
    ActivityId, 
    SchoolUserId, 
    ActivityStatus, 
    ActiveStepId, 
    DateCreated, 
    DateModified, 
    CAST(StepsState AS nvarchar(max))   AS StepsState,
    CAST(PreviousSteps AS nvarchar(max)) AS PreviousSteps,
    AttemptNo
  FROM vw_Research_ActivityUserProgresses
  "
)

#--- Define ID consistency ---
cat("Defining SchoolId_long and SchoolId_short\n")

names(monitor_requests)[names(monitor_requests) == "SchoolId"] <- "SchoolId_long"
names(research_schools)[names(research_schools) == "SchoolId"] <- "SchoolId_long"
names(research_SchoolUsers)[names(research_SchoolUsers) == "SchoolId"] <- "SchoolId_short"
names(research_monitors)[names(research_monitors) == "SchoolId"] <- "SchoolId_short"
names(research_school_establishments)[names(research_school_establishments) == "Name"] <- "SchoolName"
names(research_school_establishments)[names(research_school_establishments) == "Postcode"] <- "SchoolAddressPostCode"

#--- Create monitor request dataframe ---
df_monitor_requests <- monitor_requests %>%
  select(
    SchoolId_long,
    SchoolName,
    SchoolFundingType,
    SchoolType,
    SchoolSize,
    SchoolAddressPostCode,
    DateCreated_MonitorRequest = DateCreated,
    DateValidated_MonitorRequest = DateValidated,
    SchoolBuildingAge,
    EstablishmentId,
    ConsentToContact
  ) %>%
  mutate(EstablishmentId = strtoi(EstablishmentId))

df_research_monitors <- research_monitors%>%select(SchoolId_short, MonitorReferenceId, DateCreated_Monitor = DateCreated, DateActivated_Monitor = DateActivated, Location_Monitor = Location, RoomId )

df_research_schools <- research_schools%>%select(SchoolId_short = Id, SchoolId_long,  DateCreated_Schools = DateCreated, SchoolStatus, HasAccessToWebApp, MonitoringType)

df_school_monitors<-  left_join(left_join(df_monitor_requests, df_research_schools, by = "SchoolId_long") , df_research_monitors, by = "SchoolId_short") %>%filter(!(is.na(MonitorReferenceId)))#Only those schools which have monitors


df_school_monitors$SchoolAddressPostCode <- vapply(
  df_school_monitors$SchoolAddressPostCode,
  postcode_format,
  character(1)
)

rm(df_research_schools, df_research_monitors, monitor_requests)