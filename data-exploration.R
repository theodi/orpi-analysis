# Adapt for your purpose
setwd('~/git/orpi-analysis')

# List of packages for session
packages <- c('dplyr')

# Install CRAN packages (if not already installed)
.inst <- packages %in% installed.packages()
if(length(packages[!.inst]) > 0) install.packages(packages[!.inst])

# Load packages into session 
sapply(packages, require, character.only=TRUE)