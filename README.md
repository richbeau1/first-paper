# 1st_Paper: SAMHE/AURN Indoor–Outdoor PM2.5 Analysis

## Project description

This folder contains the working code, processed analysis objects, figures, and contextual mapping outputs
for the first PhD paper on indoor and outdoor PM2.5 in UK schools.

The analysis uses SAMHE indoor air-quality monitor data paired with outdoor DEFRA AURN PM2.5 data. 
The main research focus is the relationship between indoor and outdoor PM2.5, school-hours exposure, 
indoor/outdoor ratios, ventilation and CO2 context, school-level characteristics, 
and spatial/contextual factors including wood-burning and ammonia mapping.

## Project owner

Richard Beaumont  
PhD researcher, University of Sheffield  
Project: Indoor air quality, PM2.5 infiltration, and UK schools

## Folder structure

```text
1st_Paper/
│
├── analysis_core_objects.RData
├── streamlined_1st_paper_code_self_....Rmd
├── 1st_Paper.Rproj
├── README.md
├── figures/
│
├── SAMHE_core_files/
│   ├── IAQ_monthly_outputs/
│   ├── external_tables/
│   ├── term_nh_dates/
│   ├── load_SAMHE_dataset.R
│   ├── load_external_tables.R
│   ├── SAMHE_relational_tables.R
│   ├── data_set_formation.R
│   ├── data_set_formation_scripts.R
│   ├── midsection_core.R
│   └── db_credentials.txt
│
├── contextual_mapping/
│   ├── woodburning_mapping/
│   └── ammonia_mapping/
│
├── archive_old_rmd/
├── archive_old_data/
└── archive_other_files/
```

## Main files

### `analysis_core_objects.RData`

Main saved analysis cache for the paper. This file should be loaded at the start of the streamlined analysis R Markdown.

It contains:

```r
In_Out_24
In_Out_SchHrs
In_Out_OutSchHrs
df_school_chars
```

Current checked dimensions:

```text
In_Out_24             24,993,165 rows   35 columns
In_Out_SchHrs          4,431,854 rows   35 columns
In_Out_OutSchHrs      10,858,257 rows   35 columns
df_school_chars            1,673 rows   42 columns
```

### `streamlined_1st_paper_code_self_....Rmd`

Main working R Markdown file for the paper figures and analysis. This should be the primary file used for ongoing analysis.

### `figures/`

Stores exported figures from the analysis, mainly PNG files for use in Google Slides, posters, and paper drafts.

### `SAMHE_core_files/`

Source/rebuild folder for SAMHE data processing. This folder contains the monthly `.rds` outputs and scripts used to load or recreate the main SAMHE/AURN analysis objects.

Use this folder only when rebuilding the core datasets. Routine paper analysis should use `analysis_core_objects.RData`.

### `contextual_mapping/woodburning_mapping/`

Wood-burning contextual mapping work for the first paper.

### `contextual_mapping/ammonia_mapping/`

Ammonia and secondary PM2.5 contextual mapping work for the first paper.

### `archive_old_rmd/`

Older or duplicate R Markdown files retained for provenance. These should not be used as the main analysis files unless recovering previous code.

### `archive_old_data/`

Older or duplicate data backups retained for safety. These should not normally be loaded for current analysis.

## Data provenance

The main SAMHE/AURN datasets were created from monthly pre-processed `.rds` files stored in:

```text
SAMHE_core_files/IAQ_monthly_outputs/
```

The monthly files were loaded using:

```r
source("load_SAMHE_dataset.R")
```

This script combines the same-named tables across monthly `.rds` files into larger objects, including:

```r
In_Out_24
In_Out_SchHrs
Indoor_hourly_24
Indoor_hourly_SchHrs
Indoor_daily_24
Indoor_daily_SchHrs
df_pp_ventilation
```

The main objects used for the paper were then saved into:

```text
analysis_core_objects.RData
```

## Main datasets

### `In_Out_24`

Full paired indoor/outdoor SAMHE–AURN dataset. Includes all valid 24-hour/all-time observations.

Expected key variables include:

```text
DateTime
SchoolId_short
device_id
pm02
pm2.5
rco2
site
longitude
latitude
SchoolLongitude
SchoolLatitude
km_distance_to_AURN
```

### `In_Out_SchHrs`

School-hours paired indoor/outdoor dataset. This is a subset of the full paired dataset restricted to school-hours, term-time, and occupied conditions.

This is the main dataset for most paper analyses.

### `In_Out_OutSchHrs`

Term-time outside-school-hours dataset. Created from `In_Out_24` using dates present in `In_Out_SchHrs`, but keeping hours outside the school-hours window.

Used for comparison with occupied school-hours conditions.

### `df_school_chars`

School and monitor metadata table. Includes school identifiers, monitoring type, postcode-derived coordinates, IMD decile, rural/urban classification, local authority, school phase, EPC/building information, heating fuel, floor area, and related contextual variables.

Key variables include:

```text
SchoolId_short
MonitorReferenceId
MonitoringType
IMD_decile
RUC
LocalAuthority
EducationPhase
BuildingEnvironment
MainHeatingFuel
FloorArea
SchoolLatitude
SchoolLongitude
```

## Standard analysis start-up

Use this at the start of the main R Markdown analysis:

```r
library(dplyr)
library(tidyr)
library(ggplot2)
library(openair)
library(sf)
library(lubridate)
library(rnaturalearth)
library(ggpubr)
library(viridis)

setwd("/Users/richardbeaumont/R_projects/1st_Paper")

fig_dir <- "figures"
dir.create(fig_dir, showWarnings = FALSE)

who_pm25 <- 5 # WHO 2021 annual PM2.5 guideline concentration (µg m-3)

sheffield_purple <- "#7A1FA2"
defra_orange <- "#E57200"

load("analysis_core_objects.RData")
```

## Active dataset convention

The main analysis uses an active dataset selector:

```r
active_dataset_name <- "school_hours"
```

Options:

```text
all_time              = In_Out_24
school_hours          = In_Out_SchHrs
outside_school_hours  = In_Out_OutSchHrs
```

The main paper analysis usually uses:

```text
school_hours
```

HEPA-monitoring schools are excluded from the main analysis dataset using `MonitoringType == 2`, except for the specific HEPA comparison figure.

## Main analysis outputs

Planned or current figures include:

```text
Figure 1: SAMHE schools and nearest AURN site pairing map
Figure 2: Daily mean indoor/outdoor PM2.5 time series
Figure 2: Annual mean indoor vs outdoor PM2.5 scatter
Figure 3: Annual and seasonal I/O ratio analysis
Figure 4: Indoor PM2.5 by ventilation/building environment
Figure 5: Indoor PM2.5 by IMD decile
Figure 8: CO2 and PM2.5 I/O ratio
Figure 9: HEPA vs non-HEPA I/O ratio comparison
```

Contextual outputs will include wood-burning and ammonia mapping where relevant.

## File naming conventions

Use clear names beginning with figure number or analysis type.

Examples:

```text
Figure_1_SAMHE_AURN_pairing_map.png
Figure_2_daily_indoor_outdoor_PM25_timeseries.png
Figure_4_indoor_PM25_by_ventilation_type_school_hours.png
Figure_5_indoor_PM25_by_IMD_decile_school_hours.png
```

Avoid spaces in new file names. Use underscores.

## Data availability and access

The processed data files are not stored on GitHub because they are too large and may include restricted or controlled-access research data.

The GitHub repository is intended to store:

```text
R scripts
R Markdown files
README.md
.gitignore
figure-generation code
documentation
```

The following files and folders are excluded from GitHub and stored separately:

```text
analysis_core_objects.RData
SAMHE_core_files/IAQ_monthly_outputs/
large .RData, .rds, .csv, and .parquet files
db_credentials.txt
```

Researchers who require access to the processed analysis data should contact the project owner. 
Access will depend on SAMHE data governance, permissions, and any applicable data-sharing agreements.

The main analysis dataset used for this paper is:

```text
analysis_core_objects.RData
```

This file is backed up separately to Google Drive and the Sheffield X Drive.

## Sensitive and restricted files

The file:

```text
SAMHE_core_files/db_credentials.txt
```

contains database credentials or access information and must not be committed to GitHub, 
shared publicly, or deposited in an open repository.

Large processed data files should also not be committed to GitHub. 
They should be stored in secure institutional or approved cloud storage and shared only through controlled access.

## Backup and preservation

The following files and folders should be backed up to Google Drive and the Sheffield X Drive:

```text
analysis_core_objects.RData
streamlined_1st_paper_code_self_....Rmd
README.md
figures/
SAMHE_core_files/
contextual_mapping/
```

Do not rely on RStudio’s automatic `.RData` workspace image as the project backup.

When closing RStudio, choose:

```text
Don't Save
```

if prompted to save the workspace image. The important saved analysis file is:

```text
analysis_core_objects.RData
```

## Suggested `.gitignore`

The repository should exclude large data files, credentials, and local workspace files.

```gitignore
# R workspace and history
.RData
.Rhistory
.Rproj.user/

# Large analysis data
*.RData
*.Rdata
*.rda
*.rds
*.csv
*.parquet

# SAMHE monthly outputs and credentials
SAMHE_core_files/IAQ_monthly_outputs/
SAMHE_core_files/db_credentials.txt
db_credentials.txt

# Generated outputs that can be recreated
figures/*.png
figures/*.pdf
figures/*.svg
```

Figures can be included or excluded depending on the repository purpose. 
If the GitHub repository is used to document final outputs, selected final figures may be committed manually.

## Reproducibility notes

The streamlined analysis should be reproducible from:

```text
analysis_core_objects.RData
streamlined_1st_paper_code_self_....Rmd
```

The full dataset rebuild requires:

```text
SAMHE_core_files/
SAMHE_core_files/IAQ_monthly_outputs/
df_school_chars or equivalent metadata
```

The monthly `.rds` loading route can be memory intensive because the original loader creates `IAQ_all` by loading and binding multiple large monthly tables. For routine analysis, use `analysis_core_objects.RData` rather than rebuilding from monthly files.

## Last checked

2026-08-07

Checked that:

```text
analysis_core_objects.RData exists
In_Out_24 exists
In_Out_SchHrs exists
In_Out_OutSchHrs exists
df_school_chars exists
```