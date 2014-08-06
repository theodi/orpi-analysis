#------------------------------------------------
# Adapt for your purpose
setwd('~/git/orpi-analysis')

# List of packages for session
packages <- c('dplyr', 'ggplot2', 'Hmisc')

# Install CRAN packages (if not already installed)
.inst <- packages %in% installed.packages()
if(length(packages[!.inst]) > 0) install.packages(packages[!.inst])

# Load packages into session 
sapply(packages, require, character.only = TRUE)

# Options
theme_set(theme_minimal(base_family = "Helvetica Neue"))
options(stringsAsFactors = FALSE)
options("scipen" = 100, "digits" = 4) # no scientific notation
# options(digits.secs = 0)

# Functions
source('functions.r')
source('clean-rail-data.R')
#------------------------------------------------
# Stored data locally - ALL ARRIVALS 2014-08-04
all <- readRDS('data/data-2014-08-04.RData')

# Clean with script
all <- raildata.clean(all)

# I'm using DPLYR
all <- tbl_df(all)

#------------------------------------------------
dim(all)
str(all)

# TODO: drop some (almost) empty columns
as.matrix(sapply(all, count.missing))
as.matrix(sapply(all, pct.missing))
#sapply(all, count.empty)
#sapply(all, pct.empty)
as.matrix(sapply(all, count.unique))
as.matrix(sapply(all, pct.unique)

# Some descriptive stats for delays
describe(all$body.timetable_variation)
describe(all$body.variation_status)

describe(all[all$body.variation_status %in% 'EARLY', 'body.timetable_variation'])
describe(all[all$body.variation_status %in% 'LATE', 'body.timetable_variation'])

# This looks reasonable: two categories disappear; table perhaps more useful
ggplot(data = all[all$body.timetable_variation > 0, ]) + geom_boxplot(aes(x = body.variation_status, y = body.timetable_variation)) + coord_flip()

#------------------------------------------------
delayed_final_dest <- test[(test$body.planned_event_type == "DESTINATION") & (test$body.variation_status == "LATE"), ]
delayed <- test[test$body.variation_status %in% "LATE", ]

#------------------------------------------------
test <- row.sample(all, 10000)

# Problem is body.gbtt_timestamp with 30-40% missing
test[, 'time_diff'] <- as.numeric(test[, 'body.actual_timestamp'] - test[, 'body.gbtt_timestamp'])/60

ggplot(data = test) + geom_point(aes(x = time_diff, y = body.timetable_variation), shape = 1) + 
  facet_wrap(~ body.variation_status) + ylim(0, 50)

# Correlations
cor(test[test$body.variation_status %in% 'LATE', c('time_diff', 'body.timetable_variation')], use = "complete.obs")
cor(test[test$body.variation_status %in% 'EARLY', c('time_diff', 'body.timetable_variation')], use = "complete.obs")


