# Adapt for your purpose
setwd('~/git/orpi-analysis')

# List of packages for session
packages <- c('dplyr', 'ggplot2', 'ggthemes')

# Install CRAN packages (if not already installed)
.inst <- packages %in% installed.packages()
if(length(packages[!.inst]) > 0) install.packages(packages[!.inst])

# Load packages into session 
sapply(packages, require, character.only = TRUE)

# Options
theme_set(theme_minimal(base_family = "Helvetica Neue"))
options(stringsAsFactors = FALSE)
options("scipen" = 100, "digits" = 4) # less scientific notation
# options(digits.secs = 0)
source('/Users/Ulrich/git/R-projects/ODI-colours.R')

# Functions
source('functions.r')
source('clean-rail-data.R')
#------------------------------------------------

hist_pass <- read.csv("passenger-data/historic-passenger-journeys-DfT.csv")
names(hist_pass) <- c("year", "year-ch", "journeys")

ggplot(passengers, aes(year, journeys)) + geom_point(color = 'purple', size = 2) + geom_rangeframe() + ylim(0,2000) + ylab("passenger journeys in million")
