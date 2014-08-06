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
# options(digits.secs = 0)

# Functions
source('functions.r')

#------------------------------------------------
# Stored data locally
# I'm using DPLYR and DATATABLE
all_arrivals_yesterday <- tbl_dt(readRDS('data/data-2014-08-04.RData'))

dim(all_arrivals_yesterday)
str(all_arrivals_yesterday)

# TODO: drop some (almost) empty columns
sapply(all_arrivals_yesterday, count.missing)
sapply(all_arrivals_yesterday, pct.missing)
sapply(all_arrivals_yesterday, count.empty)
sapply(all_arrivals_yesterday, pct.empty)
sapply(all_arrivals_yesterday, count.unique)
sapply(all_arrivals_yesterday, pct.unique)
