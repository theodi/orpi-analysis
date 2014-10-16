#------------------------------------------------
# Working directory
setwd('~/git/orpi-analysis')
load("all_days_ranking_13_aug_30_sep.RData")

# List of packages for session
packages <- c('ggplot2', 'ggthemes', 'scales')
sapply(packages, require, character.only = TRUE)

#------------------------------------------------

# EXCLUDE FAILED days
rail  <- all_days_ranking_13_aug_30_sep[!(all_days_ranking_13_aug_30_sep$date %in% c(as.Date("2014-08-15"), as.Date("2014-08-21"), as.Date("2014-09-16"))), ]

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


ggplot(rail, aes(x = date, y = total_lost_minutes / 1000, fill = factor(weekends))) + 
  geom_bar(stat = "identity") + geom_hline(yintercept = seq(3000, 9000, 3000), col = "white") +
  xlab("") + ylab("Gross lost minutes in thousands") + scale_y_continuous(labels = comma) +  
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






