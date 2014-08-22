# Pre-requisites:
# - the 's3cmd' open source tool, available at http://s3tools.org/s3cmd ; s3cmd 
#   must be configured with your AWS credentials before being called from this
#   script

library(dplyr)
library(memoise)
library(RCurl)
library(rjson)

AWS_BUCKET_NAME <- "orpi-nrod-store"
CORPUS_DOWNLOAD_URL <- "https://raw.githubusercontent.com/theodi/orpi-corpus/master/corpus.csv"
RIGHT_TIME <- 1
MINIMUM_DELAY <- 5
HEAVY_DELAY <- 30

# let's see integer numerics as such!
options(digits=12)

# download_data creates a data.frame of all events for train journeys that 
# run on the specified date, including the events for those same trains up to
# ~EXTRA_HOURS hours in the previous day and ~EXTRA_HOURS hours in the 
# following day.
# Note that a series of "MD5 signatures do not match" warnings will be 
# generated to stderr: this is caused by s3cmd not managing correctly
# the MD5 of multipart uploads
download_data_not_memoised <- function (target_date = (Sys.Date() - 1), EXTRA_HOURS = 3) {

    # Returns the list of all available files that could include events
    # that took place in the specified target date and within the specified
    # hours; hourEnd is *included* in the results
    get_files_list <- function (target_date, hourStart = 0, hourEnd = 23) {
        # gets the files list from S3
        path <- paste0("s3://", AWS_BUCKET_NAME, "/", formatC(format(target_date, "%Y"), width=4, flag="0"), "/", formatC(format(target_date, "%m"), width=2, flag="0"), "/", formatC(format(target_date, "%d"), width=2, flag="0"), "/")
        s3cmd_command <- paste0("/usr/local/bin/s3cmd ls ", path)
        available_files <- read.table(pipe(s3cmd_command), header = F, sep="", colClasses = "character")
        available_files <- available_files[, ncol(available_files)]  
        # if specified, filters out the hours outside of the specified interval
        grep_regexpr <- paste0("^", path, "arrivals_", formatC(format(target_date, "%Y"), width=4, flag="0"), formatC(format(target_date, "%m"), width=2, flag="0"), formatC(format(target_date, "%d"), width=2, flag="0"), "(", paste(formatC(seq(hourStart, hourEnd), width=2, flag="0"), collapse="|"), ")")
        available_files <- grep(grep_regexpr, available_files, value = TRUE)        
        # returns the list in chronological order        
        sort(available_files)
    }
    
    # create the list of files that I need to read
    target_date <- as.Date(target_date)
    yesterday <- as.Date(target_date - 1)
    tomorrow <- as.Date(target_date + 1) 
    files_list <- c(
        get_files_list(yesterday, 23 - (EXTRA_HOURS - 1), 23), 
        get_files_list(target_date), 
        get_files_list(tomorrow, 0, EXTRA_HOURS - 1)
    )
    
    # read them
    results <- data.frame();
    sapply(files_list, function (filename) {
        print(paste0("Reading ", filename, "..."));
        results <<- rbind(results, read.csv(pipe(paste0("/usr/local/bin/s3cmd get ", filename, " -")), header = TRUE, stringsAsFactors = FALSE))
    });

    # convert timestamps to POSIXct
    timestamp_column_names <- grep("_timestamp$", names(results), value = TRUE)    
    sapply(timestamp_column_names, function (timestamp_column_name) {
        # make empty values into NAs
        results[, timestamp_column_name] <<- ifelse(results[, timestamp_column_name] == "", NA, results[, timestamp_column_name])  
        # makes non-NA values to POSIXct
        results[, timestamp_column_name] <<- as.POSIXct(results[, timestamp_column_name], origin = '1970-01-01')
    })
    
    # drop rows that have NA for body.planned_timestamp
    results <- results[!is.na(results$body.planned_timestamp), ]
    
    # copy body.planned_timestamp to body.gbtt_timestamp where the latter is 
    # undefined; note that if I don't specify as.POSIXct the date is converted
    # back to an epoch-style timestamp
    results$body.gbtt_timestamp <- as.POSIXct(ifelse(is.na(results$body.gbtt_timestamp), results$body.planned_timestamp, results$body.gbtt_timestamp), origin = '1970-01-01')

    # the value of body.current_train_id can be either of "", NA or "null" to
    # represent that the train has not changed id
    results$body.current_train_id <- ifelse(results$body.current_train_id %in% c("", "null"), NA, results$body.current_train_id)
    
    # identify all train ids for events that happened in the target day
    min_possible_date <- as.POSIXct(paste0(formatC(format(target_date, "%Y"), width=4, flag="0"), "/", formatC(format(target_date, "%m"), width=2, flag="0"), "/", formatC(format(target_date, "%d"), width=2, flag="0"), " 00:00"))
    max_possible_date_not_included <- as.POSIXct(paste0(formatC(format(tomorrow, "%Y"), width=4, flag="0"), "/", formatC(format(tomorrow, "%m"), width=2, flag="0"), "/", formatC(format(tomorrow, "%d"), width=2, flag="0"), " 00:00"))
    train_ids_in_scope <- results[(results$body.actual_timestamp >= rep(min_possible_date, nrow(results))) & (results$body.actual_timestamp < rep(max_possible_date_not_included, nrow(results))), ]$body.train_id
    # filter out the trains that don't belong to the list above
    results <- results[results$body.train_id %in% train_ids_in_scope, ]
    
    # make $body.timetable_variation sign-aware
    results$body.timetable_variation <- ifelse(results$body.variation_status == "EARLY", -1 * results$body.timetable_variation, results$body.timetable_variation)

    # drop the trains that changed id (e.g. there were none on 13/8/2014)
    changed_id_trains <- unique(results[!is.na(results$body.current_train_id),]$body.train_id)
    results <- results[!(results$body.train_id %in% changed_id_trains), ]

    # identify trains that changed *any* of their planned locations (e.g. 
    # stations they stop at) and drop their entire journeys (e.g. there were 7 
    # out of 473162 on 13/8/2014)
    changed_location_trains <- unique(results[!is.na(results$body.original_loc_stanox), ]$body.train_id)
    results <- results[!(results$body.train_id %in% changed_location_trains), ]

    # drop the columns I do not need
    results <- results[, names(results) %in% c("body.train_id", 
      "body.actual_timestamp", "body.event_type", "body.loc_stanox", 
      "body.gbtt_timestamp", "body.timetable_variation")]
    
    # sort by train and expected timestamp for the events
    results <- results[with(results, order(body.train_id, body.gbtt_timestamp)), ]    
    
    return(results)
}

download_data <- memoise(download_data_not_memoised)

# Not all location data shows the arrival of trains at intermediate stations
# in a journey, typically when the timetable sets identical arrival and 
# re-departure times. 
# This function integrates the data for the specified stanox or list of stanox
# with all the missing arrivals, inferred from the existence of previous
# events in the life of the train, and returns that stanox data only.
integrate_with_missing_arrivals_not_memoised <- function (day_data, stanox) {
    if (is.vector(stanox)) {
        return(unique(do.call(rbind, lapply(stanox, function (stanox) integrate_with_missing_arrivals(day_data, stanox)))))
    } else {
        # extracts the data that exists already about this location
        location_data <- day_data[day_data$body.loc_stanox == stanox, ]
        # find the list of trains that I can only see departing
        trains_that_depart_only <- unique(location_data$body.train_id[!(location_data$body.train_id %in% unique(location_data[location_data$body.event_type == 'ARRIVAL', ]$body.train_id))])
        trains_that_depart_only <- location_data[location_data$body.train_id %in% trains_that_depart_only, c("body.train_id", "body.gbtt_timestamp")]
        if (nrow(trains_that_depart_only) > 0) {
            # find the earliest recorded event in the train life
            earliest_events <- day_data %.% 
                filter(body.train_id %in% trains_that_depart_only$body.train_id) %.% 
                group_by(body.train_id) %.% 
                summarise(earliest_event = min(body.gbtt_timestamp))
            # if the earliest event is earlier than the departure at this station, 
            # the train must have arrived at this station, too!
            trains_that_must_have_arrived <- inner_join(trains_that_depart_only, earliest_events, by = "body.train_id")
            trains_that_must_have_arrived <- trains_that_must_have_arrived[trains_that_must_have_arrived$body.gbtt_timestamp > trains_that_must_have_arrived$earliest_event, ]$body.train_id
            if (length(trains_that_must_have_arrived) > 0) {
                # add dummy arrival records
                dummy_arrivals <- location_data[(location_data$body.train_id %in% trains_that_must_have_arrived) & (location_data$body.event_type == 'DEPARTURE'), ]
                dummy_arrivals$body.event_type <- 'ARRIVAL'
                location_data <<- rbind(location_data, dummy_arrivals)                    
            }
        }
        return(location_data)
    }
}

integrate_with_missing_arrivals <- memoise(integrate_with_missing_arrivals_not_memoised)

# If the 'stanox' parameter is specified (single stanox or vector of stanox codes), 
# this function calculates the average delay for all trains arriving to or 
# departing from that station as recorded in 'day_data'. Otherwise, it returns a 
# data.frame with all average delays for each stanox listed in 'clean_day_data'.
calculate_station_rank_not_memoised <- function (day_data, stanox = NULL) {
    if (is.null(stanox)) {
        # if stanox is not specified, do the job for all stations
        return(calculate_station_rank(day_data, sort(unique(day_data$body.loc_stanox))))
    } else if (is.vector(stanox)) {
        # if stanox is a vector, do the job for the listed stations only
        return(do.call(rbind, lapply(stanox, function (stanox) calculate_station_rank(day_data, stanox))))
    } else {
        # if stanox is not a vector, do the job for that station only
        station_data_only <- integrate_with_missing_arrivals(day_data, stanox)
        # starts calculating the stats for the location
        no_of_trains <- length(unique(station_data_only$body.train_id))
        no_of_right_time_trains <- length(unique(station_data_only[station_data_only$body.timetable_variation <= RIGHT_TIME, ]$body.train_id))
        delayed_station_data_only <- station_data_only[station_data_only$body.timetable_variation >= MINIMUM_DELAY, ]
        no_of_delayed_trains <- length(unique(delayed_station_data_only$body.train_id))
        no_of_heavily_delayed_trains <- length(unique(delayed_station_data_only[delayed_station_data_only$body.timetable_variation >= HEAVY_DELAY, ]$body.train_id))
        return(data.frame(
            stanox = c(stanox),
            no_of_trains = c(no_of_trains),
            no_of_right_time_trains = c(no_of_right_time_trains),
            no_of_delayed_trains = c(no_of_delayed_trains),
            no_of_heavily_delayed_trains = c(no_of_heavily_delayed_trains),
            average_delay = c(ifelse(nrow(delayed_station_data_only) > 0, mean(delayed_station_data_only$body.timetable_variation), 0)),
            perc_of_right_time_trains = c(no_of_right_time_trains / no_of_trains),
            perc_of_delayed_trains = c(no_of_delayed_trains / no_of_trains),
            perc_of_heavily_delayed_trains = c(no_of_heavily_delayed_trains / no_of_trains)
        ))
    }
}

calculate_station_rank <- memoise(calculate_station_rank_not_memoised)

calculate_segment_rank_not_memoised <- function (clean_day_data, from = NULL, to = NULL) {
    if (is.null(from) | is.null(to)) {

        # This functions generates a list of c(from = [stanox1], to = [stanox2]) 
        # representing all segments connecting two stations by at least one train that 
        # does not stop at any intermediate station. The direction of the train is not
        # relevant and the segment is represented by the two stanox codes in 
        # alphabetical order.
        generate_all_segments <- function (clean_day_data) {
            clean_day_data <- clean_day_data[with(clean_day_data, order(body.train_id, body.gbtt_timestamp)), c("body.train_id", "body.loc_stanox")]
            segments <- do.call(rbind, lapply(unique(clean_day_data$body.train_id), function (train_id) {
                stations <- unique(clean_day_data[clean_day_data$body.train_id == train_id, ]$body.loc_stanox)
                return(do.call(rbind, lapply(1:(length(stations) - 1), function (i) {
                    segment <- sort(c(stations[i], stations[i+1]))
                    return(data.frame(from = c(segment[1]), to = c(segment[2])))
                })))    
            }))        
            return(unique(segments))
        }
    
        # LOTS OF CODE GOES HERE
        
        return(generate_all_segments(clean_day_data))
    
    } else {
        if (from > to) { temp <- from; from <- to; to <- temp }
        
        
        return(data.frame(
            from = c(from),
            to = c(to),
            no_of_trains = # number of trains that actually transited through the segment, either direction
            no_of_delayed_trains = # number of trains delayed at either station
            no_of_heavily_delayed_trains = # same, worst case at either station
            average_delay = # average of the averages between departure and arrival at the extremes
            perc_of_delayed_trains = # whatever
            perc_of_heavily_delayed_trains = # whatever
        ))
    }
} 

#### UBER ORPI
# a) weighted mean of the average delay at all stations vs the number of trains stopping at that station
# b) define the mean delay for each train, and then calculate the mean of that vs all trains
# c) mean of everything

overall_average_delay  <- mean(clean_day_data[clean_day_data$body.timetable_variation >= MINIMUM_DELAY, ]$body.timetable_variation)

# early mapping

make_geojson <- function (reporting_points_ranking, filename) {
    # load the latest version of the corpus and drop the nodes that have no geographic coordinates
    corpus <- read.csv(text = getURL(CORPUS_DOWNLOAD_URL))
    corpus <- corpus[!is.na(corpus$LAT) & !is.na(corpus$LON), c("X3ALPHA", "STANOX", "LAT", "LON", "NLCDESC")]
    names(corpus) <- c("crs", "stanox", "lat", "lon", "description")
    # join with the reporting points ranking data
    reporting_points_ranking <- left_join(reporting_points_ranking, corpus, by = "stanox")
    # GIANFRANCO: let's include matching diagnostics if it makes sense here.
    # drop the reporting points that don't have latlong
    reporting_points_ranking <- reporting_points_ranking[!(is.na(reporting_points_ranking$lat) | is.na(reporting_points_ranking$lon)), ]
    # create the JSON
    json_structure <- list(
        type = "FeatureCollection",
        features = sapply(lapply(split(reporting_points_ranking, seq_along(reporting_points_ranking[, 1])), as.list), function (rp) {
            return(list(
                type = "Feature",
                geometry = list(type = "Point", coordinates = c(rp$lon, rp$lat)),
                properties = list(
                    "title" = rp$description,
                    "description" = paste0("This is the description for ", rp$description),
                    "marker-size" = "large",
                    "marker-symbol" = "rail"
                )
            ))              
        })
    )
    fileConn <- file("foo.geojson")
    writeLines(toJSON(json_structure), fileConn)
    close(fileConn)
    return(json_structure)
}
