##Load external tables for local authority, IMD, urban and Rural, GIAS
#to be run after SAMHE essentials
pacman::p_load(Microsoft365R)

df_school_chars <- df_school_monitors

# path = "C:/SAMHE"
path_external <- paste0(path,"/external_tables")

postcode_nations <- read.csv(paste0(path_external, '/postcode_nations.csv')) #Load postcode nation list
## Connect to shared onedrive:


df_school_chars <- df_school_monitors%>% mutate(pcode_region = if_else (
  grepl("^[0-9]{1,}$", (substr(SchoolAddressPostCode, 2, 2))),  #check if 2nd pcode char is a number
  (substr(SchoolAddressPostCode, 1, 1)),
  (substr(SchoolAddressPostCode, 1, 2)))
)%>% left_join(postcode_nations, by = c("pcode_region" = "Postcode.area"))%>%select(-Postcode.area.name, pcode_region)

##===========================================================================================================================================================
cat("\nLoading IMD and Local authority files")
###IMD from: https://data.cdrc.ac.uk/dataset/index-multiple-deprivation-imd/resource/cdrc-harmonised-imd-2019
#Connect postcode to LSOA (lower Super Output area) from: https://geoportal.statistics.gov.uk/datasets/e7824b1475604212a2325cd373946235/about

LSOA_file <- list.files(paste0(path_external, "/LSOA"))
IMD_file <- list.files(paste0(path_external, "/uk_imd"))

#RUC are for england and wales only
RUC_file <-list.files(paste0(path_external, "/rural_urban_classification"))

df_LSOA <- read.csv(paste0(path_external, "/LSOA/", LSOA_file)) %>%select(Postcode = pcds, LSOA = lsoa11cd, OutputArea = oa11cd)
df_school_chars <- df_school_chars %>% left_join(df_LSOA, by = c("SchoolAddressPostCode" = "Postcode" ))

df_IMD <- read.csv(paste0(path_external, "/uk_imd/", IMD_file)) %>%select(LSOA, LocalAuthority = LANAME, IMD_decile = SOA_decile)
df_school_chars <- df_school_chars %>% left_join(df_IMD, by = "LSOA")


#Merge RUC data by country
df_RUC <-  read.csv(paste0(path_external, "/rural_urban_classification/", RUC_file[[1]])) %>%select(OutputArea = OA11CD, RUC_code = RUC11CD, RUC = RUC11)


df_RUC_scot <- read.csv(paste0(path_external, "/rural_urban_classification/", RUC_file[[2]])) %>%select(OutputArea = OUTPUTAREA, RUC_code = UR2FOLD)%>%
  mutate(RUC = case_when(RUC_code == 1 ~ "Urban Scotland", 
                         RUC_code ==2 ~ "Rural Scotland"))

df_RUC_NI <- read.csv(paste0(path_external, "/rural_urban_classification/", RUC_file[[3]])) %>%
  select(OutputArea = SA2011_Code, RUC_code = "Rural_Urban")%>%
  mutate(RUC = case_when(RUC_code== "Rural" ~ "Rural NI", 
                         ((RUC_code =="Urban")|(RUC_code =="Mixed urban/rural")) ~ "Urban NI"))


df_school_chars_NI <- df_school_chars %>%filter(Country == "Northern Ireland")%>%
  left_join(df_RUC_NI, by = "OutputArea") 

df_school_chars_CI <- df_school_chars %>%filter(Country == "Channel Islands")

df_school_chars_CI$RUC_code <- "channel island"
df_school_chars_CI$RUC <- "Rural"


df_school_chars_eng_wal <- df_school_chars %>%filter((Country == "England") | (Country == "Wales"))%>%
  left_join(df_RUC, by = "OutputArea") 

df_school_chars_scot <- df_school_chars %>%filter(Country == "Scotland")%>%
  left_join(df_RUC_scot, by = "OutputArea")


#Do this last when I've got a clearer idea of main monitor stats

#rm(df_school_chars_eng_wal, df_school_chars_scot, df_school_chars_NI, df_school_monitors_CI)
##BELOW HERE IS FOR ENGLAND AND WALES ONLY

##=========================================== School Education level + FSM   ================================================================
#Pull data from local GIAS download for England and Wales Schools
cat("\nLoading GIAS data for England and Wales Schools")
cat("\nScottish NI equivalent not yet incorporated") #https://www.gov.scot/publications/school-level-summary-statistics/
GIAS_file <- list.files(paste0(path_external, "/GIAS"))
#filter what I think are useful GIAS things but can add later
df_GIAS <- read.csv(paste0(path_external, "/GIAS/", GIAS_file)) %>%
  select(EstablishmentId = URN, SchoolName = EstablishmentName, EstablishmentType = TypeOfEstablishment..name., EstablishmentTypeGroup = EstablishmentTypeGroup..name. , EducationPhase = PhaseOfEducation..name., Gender = Gender..name.,  PercentageFSM, Boarders = Boarders..name., NurseryProvision = NurseryProvision..name., SixthForm = OfficialSixthForm..name., ReligiousCharacter = ReligiousCharacter..name. , SchoolCapacity, NumberOfPupils, Town, SchoolAddressPostCode = Postcode, OfstedRating = OfstedRating..name., OpenOrClosed = EstablishmentStatus..name.)
#Incorporate data to df_school_chars and df_users

## Scotland school stats
Scotland_file <- list.files(paste0(path_external, "/scottish_school_stats"))
df_scot_schools <- read.csv(paste0(path_external, "/scottish_school_stats/", Scotland_file))%>%
  select(EstablishmentId = SeedCode,  EducationPhase = SchoolType, PercentageFSM)

#NI_file
NI_file <- list.files(paste0(path_external, "/NI_school_stats"))
df_NI_schools <- read.csv(paste0(path_external, "/NI_school_stats/", NI_file))%>%
  select(EstablishmentId = DE.ref,  EducationPhase = school.type, PercentageFSM)

#df_school_chars_eng_wal <- df_school_chars_eng_wal %>%left_join(df_GIAS%>%select(EstablishmentId, EducationPhase, PercentageFSM), by = "EstablishmentId")
df_school_chars_scot <- df_school_chars_scot %>%left_join(df_scot_schools%>%select(EstablishmentId, EducationPhase, PercentageFSM), by = "EstablishmentId")
df_school_chars_NI <- df_school_chars_NI %>%left_join(df_NI_schools%>%select(EstablishmentId, EducationPhase, PercentageFSM), by = "EstablishmentId")

df_school_chars_CI$EducationPhase <- NA
df_school_chars_CI$PercentageFSM <- NA

#df_school_chars <- rbind(df_school_chars_eng_wal, df_school_chars_scot, df_school_chars_NI,df_school_chars_CI)

df_school_chars_eng_wal_2 <- df_school_chars_eng_wal %>%left_join(df_GIAS%>%select(EstablishmentId, EducationPhase, PercentageFSM, SchoolCapacity, NumberOfPupils), by = "EstablishmentId")

df_school_chars <- merge(df_school_chars_eng_wal_2, df_school_chars_scot, all = TRUE)%>%merge(df_school_chars_NI, all = TRUE)%>%
  merge(df_school_chars_CI, all = TRUE)

##Load EPC certificates

EPC_file <- list.files(paste0(path_external, "/EPC_certs"))

df_EW_EPC_schools <- read.csv(paste0(path_external, "/EPC_certs/", EPC_file[[1]]))%>% 
  select(POSTCODE,EPCBand = ASSET_RATING_BAND, EPCAssetRating = ASSET_RATING, MainHeatingFuel = MAIN_HEATING_FUEL, FloorArea = FLOOR_AREA, 
       BuildingEnvironment = BUILDING_ENVIRONMENT)

df_scot_EPCs <- read.csv(paste0(path_external, "/EPC_certs/", EPC_file[[2]]))%>% 
  select(POSTCODE,EPCBand = ASSET_RATING_BAND, EPCAssetRating = ASSET_RATING, MainHeatingFuel = MAIN_HEATING_FUEL, FloorArea = FLOOR_AREA,
         BuildingEnvironment = BUILDING_ENVIRONMENT)

combined_EPC <- rbind(df_EW_EPC_schools, df_scot_EPCs)

postcode_heating_fuel <- combined_EPC %>% group_by(POSTCODE) %>%
  mutate(unique_fuels = n_distinct(MainHeatingFuel))%>% filter(unique_fuels <2)%>%
  select(POSTCODE, MainHeatingFuel)%>%distinct()

postcode_EPC_band <- combined_EPC %>% group_by(POSTCODE) %>%
  mutate(unique_bands = n_distinct(EPCBand))%>% filter(unique_bands <2)%>%
  select(POSTCODE, EPCBand)%>%distinct()

postcode_Floor_Area <- combined_EPC %>% group_by(POSTCODE) %>%
  mutate(unique_area = n_distinct(FloorArea))%>% filter(unique_area <2)%>%
  select(POSTCODE, FloorArea)%>%distinct()

postcode_building_environment <- combined_EPC %>% group_by(POSTCODE) %>%
  mutate(unique_be = n_distinct(BuildingEnvironment))%>% filter(unique_be <2)%>%
  select(POSTCODE, BuildingEnvironment)%>%distinct()

df_school_chars <- df_school_chars %>% left_join(postcode_EPC_band, 
                                                           by = c("SchoolAddressPostCode" = "POSTCODE")) %>% 
  left_join(postcode_heating_fuel, by = c("SchoolAddressPostCode" = "POSTCODE"))%>%
  left_join(postcode_Floor_Area, by = c("SchoolAddressPostCode" = "POSTCODE"))%>%
  left_join(postcode_building_environment, by = c("SchoolAddressPostCode" = "POSTCODE"))

df_school_chars <- df_school_chars %>% mutate(FractionalOccupancy = NumberOfPupils/SchoolCapacity)


rm(df_school_chars_eng_wal, df_school_chars_scot, df_school_chars_NI, df_school_chars_CI, df_school_chars_eng_wal_2)

#%>%
 # select(EstablishmentId = SeedCode,  EducationPhase = SchoolType, PercentageFSM)

#### England school funding data


cat("loading funding data for english schools")

########################
# Add long lats to df_school_chars
library(dplyr)
library(purrr)
library(tibble)
library(PostcodesioR)

# Clean function: uppercase, trim, keep only alphanumerics and single spaces
clean_postcode <- function(x) {
  x <- toupper(trimws(x))
  x <- gsub("[^A-Z0-9 ]", "", x)      # remove non-alphanumerics
  x <- gsub("\\s+", " ", x)           # collapse multiple spaces to one
  x
}

df_school_chars <- df_school_chars %>%
  mutate(pc_clean = clean_postcode(SchoolAddressPostCode),
         valid    = map_lgl(pc_clean, postcode_validation))  # TRUE/FALSE validity
# postcode_validation checks validity  ^1^  ^2^

library(purrr)
library(tibble)
library(PostcodesioR)

coords <- map_df(df_school_chars$SchoolAddressPostCode, function(pc_raw) {
  pc <- clean_postcode(pc_raw)
  res <- suppressWarnings(tryCatch(postcode_lookup(pc), error = function(e) NULL))  #  ^1^  ^2^ 
  if (is.null(res) || all(is.na(res$latitude))) {
    term <- suppressWarnings(tryCatch(terminated_postcode(pc), error = function(e) NULL))  #  ^1^  ^2^ 
    if (!is.null(term) && nrow(term) > 0 && !all(is.na(term$latitude))) {
      tibble(SchoolLatitude = term$latitude, SchoolLongitude = term$longitude)
    } else {
      tibble(SchoolLatitude = NA_real_, SchoolLongitude = NA_real_)
    }
  } else {
    tibble(SchoolLatitude = res$latitude, SchoolLongitude = res$longitude)
  }
})

# Then bind to df_school_chars
df_school_chars <- dplyr::bind_cols(df_school_chars, coords)
