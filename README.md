# 1st_Paper: SAMHE/AURN Indoor–Outdoor PM2.5 Analysis

## Project description

This repository contains the code and documentation for the first PhD paper on indoor and outdoor PM2.5 in UK schools.

The analysis uses SAMHE indoor air-quality monitor data paired with outdoor DEFRA AURN PM2.5 data,
along with contextual data and potential pollution sources.
The main focus is the relationship between indoor and outdoor PM2.5, school-hours exposure, 
indoor/outdoor ratios, ventilation and CO2 context, school-level characteristics, 
and contextual spatial factors including wood-burning and ammonia emissions mapping.

## Project owner

Richard Beaumont  
PhD researcher, University of Sheffield  
Project: Indoor air quality, PM2.5 infiltration in UK schools

## Required data file

This repository does not contain the main processed analysis dataset.

To reproduce the main analysis, the following file is required:

```text
analysis_core_objects.RData
```

This file must be placed in the project root folder before running the main R Markdown file:

```text
1st_Paper/analysis_core_objects.RData
```

The file is not stored on GitHub because it is too large and may contain restricted or controlled-access research data.
It is stored separately in secure project backup locations.

Researchers who require access to `analysis_core_objects.RData` should contact the project owner. 
Access will depend on SAMHE data governance, permissions, and any applicable data-sharing agreements.

The main analysis loads this file using:

```r
load("analysis_core_objects.RData")
```

If this file is not present in the project root folder, the main analysis will not run.

## Repository contents

The GitHub repository contains code and documentation only.

It should include:

```text
README.md
.gitignore
1st_Paper.Rproj
streamlined_1st_paper_code_self_contained_chunks.Rmd
SAMHE_core_files/ scripts
archive_old_rmd/
small documentation files
```

It does not include:

```text
analysis_core_objects.RData
large .RData, .rds, .csv, or .parquet files
raw SAMHE output data
database credentials
raw NAEI ammonia data
raw or processed wood-burning mapping data
large spatial files
```

## Local project structure

The intended local project structure is:

```text
1st_Paper/
│
├── analysis_core_objects.RData
├── streamlined_1st_paper_code_self_contained_chunks.Rmd
├── 1st_Paper.Rproj
├── README.md
├── .gitignore
├── figures/
│
├── SAMHE_core_files/
│   ├── IAQ_monthly_outputs/              # local data only; not on GitHub
│   ├── external_tables/                  # local data only; not on GitHub
│   ├── term_nh_dates/                    # local data only; not on GitHub
│   ├── load_SAMHE_dataset.R
│   ├── load_external_tables.R
│   ├── SAMHE_relational_tables.R
│   ├── data_set_formation.R
│   ├── data_set_formation_scripts.R
│   ├── midsection_core.R
│   ├── year_data_formation.R
│   └── db_credentials.txt                # local only; never commit
│
├── contextual_mapping/
│   ├── woodburning_mapping/              # local contextual mapping work
│   └── ammonia_mapping/
│       ├── raw_data/                     # local NAEI data only
│       ├── processed_data/               # local derived data only
│       └── figures/
│
├── archive_old_rmd/
├── archive_old_data/
└── archive_other_files/
```

## Main analysis file

The main working analysis file is:

```text
streamlined_1st_paper_code_self_contained_chunks.Rmd
```

This is the primary file for generating the paper figures and analyses.

Older R Markdown files are stored in:

```text
archive_old_rmd/
```

These are retained for backup only and should not normally be used for the current analysis.

## Main processed data object

The required file:

```text
analysis_core_objects.RData
```

contains the core processed objects used by the analysis:

```r
In_Out_24
In_Out_SchHrs
In_Out_OutSchHrs
df_school_chars
```

### `In_Out_24`

Full paired indoor/outdoor SAMHE–AURN dataset. Includes all valid 24-hour/all-time paired observations.

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

School-hours paired indoor/outdoor dataset. This is the main dataset for most paper analyses.

It is restricted to school-hours, term-time, and occupied conditions.

### `In_Out_OutSchHrs`

Term-time outside-school-hours dataset. 
This was created from `In_Out_24` using dates present in `In_Out_SchHrs`, 
but keeping hours outside the school-hours window.

It is used for comparison with occupied school-hours conditions.

### `df_school_chars`

School and monitor metadata table.

Expected key variables include:

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

## Data provenance

All files originated from the SAMHE project, which is a UKRI-funded research project on indoor air quality in schools,
along with defra AURN outdoor monitoring data.

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

Routine analysis should use `analysis_core_objects.RData` rather than rebuilding the full dataset from monthly files.

The full dataset rebuild requires the local monthly data folders and appropriate permissions. 
It may also require database credentials, external metadata files, and access to the original SAMHE data sources.

## How to run the main analysis

1. Open the RStudio project:

```text
1st_Paper.Rproj
```

2. Ensure this file is present in the project root folder:

```text
analysis_core_objects.RData
```

3. Open:

```text
streamlined_1st_paper_code_self_contained_chunks.Rmd
```

4. Run the setup chunk.

5. Run the `load_saved_core_objects` chunk.

6. Run the figure and analysis chunks as required.

The standard setup is:

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
```

The main data-loading command is:

```r
load("analysis_core_objects.RData")
```

## Active dataset convention

The main analysis uses an active dataset selector:

```r
active_dataset_name <- "school_hours"
```

Available options are:

```text
all_time              = In_Out_24
school_hours          = In_Out_SchHrs
outside_school_hours  = In_Out_OutSchHrs
```

The main paper analysis usually uses:

```text
school_hours
```

HEPA-monitoring schools are excluded from the main analysis dataset using:

```r
MonitoringType == 2
```

The HEPA comparison figure should use the original HEPA-inclusive data.

## Main analysis outputs

Planned or current outputs include:

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

Figures are exported to:

```text
figures/
```

For Google Slides and poster workflows, PNG outputs are preferred.

## Contextual mapping

Contextual mapping outputs are kept separate from the main SAMHE/AURN analysis cache.

### Wood-burning mapping

Wood-burning mapping is stored locally in:

```text
contextual_mapping/woodburning_mapping/
```

This folder may contain raw data, derived data, spatial files, and project-specific code. It is excluded from GitHub.

### Ammonia mapping

Ammonia mapping is stored locally in:

```text
contextual_mapping/ammonia_mapping/
```

The intended local structure is:

```text
contextual_mapping/ammonia_mapping/
├── raw_data/
├── processed_data/
└── figures/
```

The ammonia analysis uses NAEI gridded NH3 emissions data. 
These data should be described as emissions context, not ambient ammonia concentration.

The intended method is:

```text
school coordinates
→ NAEI NH3 emissions raster
→ extract emissions value at school location
→ extract mean emissions within 1 km, 2 km, 5 km, and 10 km buffers
→ save school-level ammonia context table
→ join to PM2.5 summaries when needed
```

Suggested outputs include:

```text
school_ammonia_context_long.rds
school_ammonia_context_wide.rds
school_pm25_ammonia_summary.rds
ammonia_pm25_spearman_correlations.rds
```

These outputs are local processed data files and should not be committed to GitHub.

Suggested methods wording:

```text
NAEI NH3 emissions were used as a gridded source-pressure variable. 
School coordinates were intersected with sector-specific and total NH3 emissions rasters. 
Mean emissions were extracted within circular buffers of 1, 2, 5, and 10 km around each school. 
These variables were analysed as contextual indicators of local ammonia emissions, not as estimates 
of ambient NH3 concentration.
```

## File naming conventions

Use clear file names beginning with the figure number or analysis type.

Examples:

```text
Figure_1_SAMHE_AURN_pairing_map.png
Figure_2_daily_indoor_outdoor_PM25_timeseries.png
Figure_4_indoor_PM25_by_ventilation_type_school_hours.png
Figure_5_indoor_PM25_by_IMD_decile_school_hours.png
NAEI_NH3_sector_boxplot_1km_buffer.png
```

Avoid spaces in new file names. Use underscores.

## Backup and preservation

This project has two backup types.

### Code and documentation backup

Code and documentation are backed up using GitHub.

This includes:

```text
README.md
1st_Paper.Rproj
R scripts
R Markdown files
archive_old_rmd/
small documentation files
```

### Secure data backup

Large data files, processed analysis objects, credentials, 
and contextual mapping data are not stored on GitHub.

These are backed up separately.

The secure backup should include:

```text
analysis_core_objects.RData
streamlined_1st_paper_code_self_contained_chunks.Rmd
README.md
figures/
SAMHE_core_files/
contextual_mapping/
```


