#------------------------------------------------
# Working directory
setwd('~/git/orpi-analysis')
load("all_days_ranking_13_aug_30_sep.RData")

# List of packages for session
packages <- c('ggplot2', 'ggthemes', 'scales')
sapply(packages, require, character.only = TRUE)
source("multiplot-function.R")

#------------------------------------------------

# EXCLUDE FAILED days
rail  <- daily_stats_20_oct[!(daily_stats_20_oct$date %in% c(as.Date("2014-08-15"), 
                                                             as.Date("2014-08-21"), 
                                                             as.Date("2014-09-16"), 
                                                             as.Date("2014-10-12"))), ]
# Rename row numbers
row.names(rail) <- NULL 

sapply(rail, mean)

first_date <- min(rail$date)
last_date <- max(rail$date)
max_delay <- max(rail$average_delay)
# Mark weekends
rail$weekends <- ifelse(weekdays(rail$date) %in% c('Saturday','Sunday'), 1, 0)

ggplot(rail, aes(x = date, y = no_of_trains, fill = factor(weekends))) + 
  geom_bar(stat = "identity") + geom_hline(yintercept = seq(5000, 20000, 5000), col = "white") +
  xlab("") + ylab("Count of trains") + scale_y_continuous(labels = comma) +  
  scale_fill_manual(values = c("1" = odi_dBlue, "0" = odi_mBlue), guide = 'none') +
  scale_x_date(limits = c(first_date + 1, last_date - 1), breaks = "5 day",  labels = date_format("%d %b")) +
  theme(panel.grid.minor = element_blank(), panel.grid.major = element_blank())
ggsave("graphics/trains-per-day.png", width = 16, height = 4)


ggplot(rail, aes(x = date, y = total_lost_minutes / 1000000, fill = factor(weekends))) + 
  geom_bar(stat = "identity") + geom_hline(yintercept = seq(2.5, 15, 2.5), col = "white") +
  xlab("") + ylab("Gross lost minutes in million") + scale_y_continuous(labels = comma) +  
  scale_fill_manual(values = c("1" = odi_red, "0" = odi_orange), guide = 'none') +
  scale_x_date(limits = c(first_date + 1, last_date - 1), breaks = "5 day",  labels = date_format("%d %b")) +
  theme(panel.grid.minor = element_blank(), panel.grid.major = element_blank())
ggsave("graphics/lost-minutes-per-day.png", width = 16, height = 4)

ggplot(rail, aes(x = date, y = average_delay)) + 
  geom_point(colour = odi_dPink, size = 3) + geom_ribbon(ymin = 5, ymax = max_delay + 1, fill = odi_red, alpha = 0.15) +
  xlab("") + ylab("Average delay in minutes") + ylim(3.5, 7) +
  scale_x_date(limits = c(first_date + 2, last_date - 2), breaks = "1 day", labels = date_format("%d %b")) +
  theme(panel.grid.minor = element_blank(), axis.ticks.y = element_blank(), axis.text.x = element_text(angle = 90, vjust = 0.5))
ggsave("graphics/average-delay-per-day.png", width = 16, height = 4)


# Short graphs for comparison


p1 <- ggplot(rail, aes(x = date, y = total_lost_minutes / 1000000, fill = factor(weekends))) + 
  geom_bar(stat = "identity") + geom_hline(yintercept = seq(2.5, 10, 2.5), col = "white") +
  xlab("") + ylab("Gross lost minutes in million") + scale_y_continuous(labels = comma, limits = c(0, 10)) +  
  coord_cartesian(xlim = c(as.Date("2014-09-18") - 0.5, as.Date("2014-09-22") + 0.5)) + 
  scale_fill_manual(values = c("1" = odi_red, "0" = odi_orange), guide = 'none') +
  scale_x_date(breaks = "1 day", labels = date_format("%d %b")) +
  theme(panel.grid.minor = element_blank(), axis.ticks.y = element_blank(), axis.text.x = element_text(angle = 90, vjust = 0.5))

p2 <- ggplot(rail, aes(x = date, y = average_delay)) + 
  geom_point(colour = odi_dPink, size = 3) + geom_ribbon(ymin = 5, ymax = max_delay + 1, fill = odi_red, alpha = 0.15) +
  xlab("") + ylab("Average delay in minutes") + ylim(3.5, 7) +
  coord_cartesian(xlim = c(as.Date("2014-09-18") - 0.5, as.Date("2014-09-22") + 0.5)) + 
  scale_x_date(breaks = "1 day", labels = date_format("%d %b")) +
  theme(panel.grid.minor = element_blank(), axis.ticks.y = element_blank(), axis.text.x = element_text(angle = 90, vjust = 0.5))

# ggsave doesn't work here
png("graphics/compare-selected-days-delays.png", width = 6, height = 4, units = "in", res = 300)
multiplot(p1, p2, cols = 2)
dev.off()

# CORRELATION

cor(rail$average_delay, rail$total_lost_minutes)

ggplot(rail, aes(average_delay, total_lost_minutes / 1000000, shape = factor(weekends))) + 
  geom_point(colour = odi_turquoise, size = 5) + 
  xlab("Average delay in minutes") +
  ylab("Gross lost minutes in million") + scale_y_continuous(labels = comma, limits = c(0, NA))  +
  scale_shape_manual(values = c("1" = 15, "0" = 19), guide = 'none') +
  theme(axis.text = element_text(size = 20), axis.title = element_text(size = 20))
ggsave("graphics/correlation-delay-lost-min.png", width = 12, height = 8)


  


