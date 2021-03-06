#------------------------------------------------
# Adapt for your purpose
setwd('~/git/orpi-analysis')

# List of packages for session
packages <- c('dplyr', 'ggplot2', 'Hmisc', 'lubridate')

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
# Stored data locally - GF data prep from 19aug
readRDS('data/16aug-18aug-test.RData')
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

# This looks reasonable: two categories have only zeros; table perhaps more useful
ggplot(data = all[all$body.timetable_variation > 0, ]) + geom_boxplot(aes(x = body.variation_status, y = body.timetable_variation)) + coord_flip()
table(all[all$body.timetable_variation <= 10, 'body.timetable_variation'], all[all$body.timetable_variation <= 10, 'body.variation_status'])

ggplot(data = all, aes(x = hour(body.actual_timestamp))) + geom_bar(binwidth = 1, color = 'white', fill = odi_dBlue, origin = -0.5) +
  scale_x_continuous(breaks = seq(0, 24, 2))

#------------------------------------------------
# Remember to filter on:
# - Arrivals
# - Passenger trains
# - LATE status
# - and potentially delays > 1 or >= 3 minutes
#------------------------------------------------
# Indicator for third character being 1, 2 or 9.
all$class1_2_9 <- ifelse(grepl("^..[129]", all[, 'body.train_id']) , TRUE, FALSE)

# Here the additional condition excludes around 0.1%
all$passenger  <- ifelse(all[, 'class1_2_9'] == TRUE & all[, 'body.toc_id'] != 0, TRUE, FALSE)

# Using DPLYR 
delayed <- all %>%
  filter(passenger == TRUE) %>%
  filter(body.variation_status == "LATE") %>%
  filter(body.planned_event_type == 'ARRIVAL')


#------------------------------------------------
test <- row.sample(all, 10000)

# Problem is body.gbtt_timestamp with 30-40% missing
test[, 'time_diff'] <- as.numeric(test[, 'body.actual_timestamp'] - test[, 'body.gbtt_timestamp'])/60

ggplot(data = test) + geom_point(aes(x = time_diff, y = body.timetable_variation), shape = 1) + 
  facet_wrap(~ body.variation_status, nrow = 1) + ylim(0, 50) + xlim(-50, 50)

# Correlations
cor(test[test$body.variation_status %in% 'LATE', c('time_diff', 'body.timetable_variation')], use = "complete.obs")
cor(test[test$body.variation_status %in% 'EARLY', c('time_diff', 'body.timetable_variation')], use = "complete.obs")

table(test[, 'time_diff'] - test[, 'body.timetable_variation'])
dotplot(table(test[, 'time_diff'] - test[, 'body.timetable_variation']), horizontal = F)

#------------------------------------------------
#--------------- Passenger trains ---------------
# https://groups.google.com/forum/#!topic/openraildata-talk/A-3pV_5ZfNc
# Passenger operators operate both trains that are in service and those 
# that are empty. On a very simplified level, if you just want a best effort 
# using just the realtime feed you can look at class 1, 2 and 9 services. This 
# will be the third character in the train_id field.
# 
# Together with the toc_id field not being 00, this should give you all the 
# passenger trains. It will also include a couple of other services such as 
# staff only services. To discount these, you would probably need to look at 
# the timetable feed and the "category" field on each service when the train 
# activation (0001 message type) comes through and map via the schedule UID.

# Is toc_id enough? Don't think so:
table(all$body.toc_id, is.na(all$body.gbtt_timestamp))

# Indicator for third character being 1, 2 or 9.
test$class1_2_9 <- ifelse(grepl("^..[129]", test[, 'body.train_id']) , TRUE, FALSE)
# This is somehow tricky with NAs, but none here.
sum(test[, 'class1_2_9'], na.rm = T)

# Here the additional condition excludes around 0.1%
test$passenger  <- ifelse(test[, 'class1_2_9'] == TRUE & test[, 'body.toc_id'] != 0, TRUE, FALSE)

# DPLYR test
test_vis <- test %>%
  filter(passenger == TRUE) %>%
  filter(body.timetable_variation >= 3) %>% 
  filter(body.variation_status == "LATE") %>%
  filter(body.planned_event_type == 'ARRIVAL') %>%
  mutate(hour_timetable = hour(body.actual_timestamp)) %>%
  group_by(hour_timetable) %>%
  summarise(
    mean_delayed_pass = mean(body.timetable_variation, na.rm = TRUE),
    median_delayed_pass = median(body.timetable_variation, na.rm = TRUE),
    no_trains = n() # equiv to length(var)
    )

ggplot(data = test_vis, aes(x = hour_timetable, y = mean_delayed_pass, group = 1)) + ylim(0, NA) + scale_x_continuous(breaks=0:23) +
  geom_point(color = odi_purple, stat = 'identity') + geom_line(color = odi_purple) + coord_polar() # why offset?

#------------------------------------------------
#--------------- Potential metrics --------------
## Percent delayed
pct_delayed <- nrow(delayed) / nrow(all[all$passenger == TRUE & all$body.planned_event_type == 'ARRIVAL', ]) # Make sure there are no empty rows etc.
format.pct(pct_delayed)

## Average minutes
mean_delayed <- mean(delayed[, 'body.timetable_variation'])
median_delayed <- median(delayed[, 'body.timetable_variation'])
format.min(mean_delayed)
format.min(median_delayed)

## Average minutes for > 1 min delay
mean_delayed_more1 <- mean(delayed[delayed[, 'body.timetable_variation'] > 1, 'body.timetable_variation'])
# mean(filter(delayed, body.timetable_variation > 1)$body.timetable_variation)
median_delayed_more1 <- median(delayed[delayed[, 'body.timetable_variation'] > 1, 'body.timetable_variation'])
format.min(mean_delayed_more1)
format.min(median_delayed_more1)

## Average minutes for >= 3 min delay
mean_delayed_more3 <- mean(delayed[delayed[, 'body.timetable_variation'] >= 3, 'body.timetable_variation'])
# mean(filter(delayed, body.timetable_variation > 1)$body.timetable_variation)
median_delayed_more3 <- median(delayed[delayed[, 'body.timetable_variation'] >= 3, 'body.timetable_variation'])
format.min(mean_delayed_more3)
format.min(median_delayed_more3)

## Percent of trains delayed for more than 10 min
pct_delayed_more10 <- nrow(delayed[delayed[, 'body.timetable_variation'] > 10, ]) / nrow(all[all$passenger == TRUE & all$body.planned_event_type == 'ARRIVAL', ]) # Make sure there are no empty rows etc.
format.pct(pct_delayed_more10)

## Percent of trains delayed for more than 30 min
pct_delayed_more30 <- nrow(delayed[delayed[, 'body.timetable_variation'] > 30, ]) / nrow(all[all$passenger == TRUE & all$body.planned_event_type == 'ARRIVAL', ]) # Make sure there are no empty rows etc.
format.pct(pct_delayed_more30)





