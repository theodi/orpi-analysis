# Adapt for your purpose
setwd('~/git/orpi-analysis')

# List of packages for session
packages <- c('dplyr', 'ggplot2', 'ggthemes', 'scales')

# Install CRAN packages (if not already installed)
.inst <- packages %in% installed.packages()
if(length(packages[!.inst]) > 0) install.packages(packages[!.inst])

# Load packages into session 
sapply(packages, require, character.only = TRUE)

# Options
theme_set(theme_minimal(base_family = "Helvetica Neue", base_size = 16))
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
# hist_pass$year <- as.Date(strptime(hist_pass$year, format = "%Y"))

ggplot(hist_pass, aes(year, journeys)) + geom_point(shape = 15, color = odi_dBlue, size = 3) + geom_rangeframe(sides = "b") + 
  ylim(0,2100) + ylab("Passenger journeys in million") + xlab("Year") + 
  scale_x_continuous(breaks = c(1900, 1919, 1938, 1946, 1950, 1960, 1970, 1980, 1990, 2000, 2008, 2013)) +
  theme(panel.grid.minor = element_blank(), panel.grid.major.x = element_blank(), panel.grid.major.y = element_line(colour = odi_dBlue, size = 0.5, linetype = 'dotted'), axis.ticks.y = element_blank())
ggsave("graphics/historic-passenger-numbers.png", width = 16, height = 4)
ggsave("graphics/historic-passenger-numbers.pdf", width = 16, height = 4)

