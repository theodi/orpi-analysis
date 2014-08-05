#------------------------------------------------
# Adapt for your purpose
setwd('~/git/orpi-analysis')

# List of packages for session
packages <- c('dplyr', 'ggplot2', 'Hmisc', 'data.table')

# Install CRAN packages (if not already installed)
.inst <- packages %in% installed.packages()
if(length(packages[!.inst]) > 0) install.packages(packages[!.inst])

# Load packages into session 
sapply(packages, require, character.only = TRUE)

# Options
theme_set(theme_minimal(base_family = "Helvetica Neue"))
options(stringsAsFactors = FALSE)
# options("scipen"=100, "digits"=4) # no scientific notation

# Functions
source('functions.r')

#------------------------------------------------
# Stored data locally
# I'm using DPLYR and DATATABLE
all_arrivals_yesterday <- tbl_dt(readRDS('data/data-2014-08-04.RData'))
length(unique(all_arrivals_yesterday$body.train_id))

# what % were delayed at the final destination? note that there may be more
# than one final destination arrival records for the same train id!
delayed_at_final_destination <- all_arrivals_yesterday[(all_arrivals_yesterday$body.planned_event_type == "DESTINATION") & (all_arrivals_yesterday$body.variation_status == "LATE"), ]
length(unique(delayed_at_final_destination$body.train_id)) / length(unique(all_arrivals_yesterday$body.train_id))

# what was the average delay in minutes of trains that were delayed at their 
# final destination? (will ignore that there are duplicates)
mean(delayed_at_final_destination$body.timetable_variation)


