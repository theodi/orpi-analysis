library(dplyr)
library(memoise)
library(rjson)

RIGHT_TIME <- 1
MINIMUM_DELAY <- 5
HEAVY_DELAY <- 30

source('./download-from-S3.R')
source('./download-corpus.R')

# let's see integer numerics as such!
options(digits=12)

# Not all location data shows the arrival of trains at intermediate stations
# in a journey, typically when the timetable sets identical arrival and 
# re-departure times. 
# This function integrates the data for the specified stanox or list of stanox
# with all the missing arrivals, inferred from the existence of previous
# events in the life of the train, and returns that stanox data only.
integrate_with_missing_arrivals <- memoise(function (day_data, stanox) {
    if (is.vector(stanox) && (length(stanox) > 1)) {
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
})

# If the 'stanox' parameter is specified (single stanox or vector of stanox codes), 
# this function calculates the average delay for all trains arriving to or 
# departing from that station as recorded in 'day_data'. Otherwise, it returns a 
# data.frame with all average delays for each stanox listed in 'clean_day_data'.
calculate_station_rank <- memoise(function (day_data, stanox = NULL) {
    if (is.null(stanox)) {
        # if stanox is not specified, do the job for all stations
        return(calculate_station_rank(day_data, sort(unique(day_data$body.loc_stanox))))
    } else if (is.vector(stanox) && (length(stanox) > 1)) {
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
            perc_of_right_time_trains = c(no_of_right_time_trains / no_of_trains),
            no_of_delayed_trains = c(no_of_delayed_trains),
            perc_of_delayed_trains = c(no_of_delayed_trains / no_of_trains),
            no_of_heavily_delayed_trains = c(no_of_heavily_delayed_trains),
            perc_of_heavily_delayed_trains = c(no_of_heavily_delayed_trains / no_of_trains),
            average_delay = c(ifelse(nrow(delayed_station_data_only) > 0, mean(delayed_station_data_only$body.timetable_variation), 0))
        ))
    }
})

# This functions generates a list of c(from = [stanox1], to = [stanox2]) 
# representing all segments connecting two stations by at least one train that 
# does not stop at any intermediate station. The direction of the train is not
# relevant and the segment is represented by the two stanox codes in 
# alphabetical order.
generate_all_segments <- memoise(function (day_data) {
    # drop the trains that stop at one station only
    trains_with_one_station_only <- unique(day_data %.%
        group_by(body.train_id) %.%
        summarise(no_of_stations = length(unique(body.loc_stanox))) %.%
        filter(no_of_stations < 2))
    day_data <- day_data[!(day_data$body.train_id %in% trains_with_one_station_only$body.train_id), ]
    # the sorting below is instrumental
    day_data <- day_data[with(day_data, order(body.train_id, body.gbtt_timestamp)), c("body.train_id", "body.loc_stanox")]
    segments <- do.call(rbind, lapply(unique(day_data$body.train_id), function (train_id) {
        # for each train, identify all stations it goes through
        stations <- unique(day_data[day_data$body.train_id == train_id, ]$body.loc_stanox)
        return(do.call(rbind, lapply(1:(length(stations) - 1), function (i) {
            # for each station, create one segment between each consecutive
            # station
            segment <- sort(c(stations[i], stations[i+1]))
            return(data.frame(from_stanox = c(segment[1]), to_stanox = c(segment[2])))
        })))    
    }))        
    return(unique(segments))
})

calculate_segment_rank <- memoise(function (day_data, from_stanox = NULL, to_stanox = NULL) {
    if (is.null(from_stanox) || is.null(to_stanox)) {
        segments <- generate_all_segments(day_data)
        return(do.call(rbind, lapply(lapply(split(segments, seq_along(segments[, 1])), as.list), function (segment) calculate_segment_rank(day_data, segment$from_stanox, segment$to_stanox))))
    } else {
        if (from_stanox > to_stanox) { temp <- from_stanox; from_stanox <- to_stanox; to_stanox <- temp }
        segment_trains <- intersect(
            unique(day_data[day_data$body.loc_stanox == from_stanox, ]$body.train_id),
            unique(day_data[day_data$body.loc_stanox == to_stanox, ]$body.train_id)
        )
        segment_data <- integrate_with_missing_arrivals(day_data, c(from_stanox, to_stanox))
        segment_data <- segment_data[segment_data$body.train_id %in% segment_trains, ]
        no_of_trains <- length(unique(segment_data$body.train_id))
        right_time_trains <- segment_data[segment_data$body.timetable_variation <= RIGHT_TIME, ]
        no_of_right_time_trains <- length(unique(right_time_trains$body.train_id))
        delayed_trains <- segment_data[segment_data$body.timetable_variation >= MINIMUM_DELAY, ]
        no_of_delayed_trains <- length(unique(delayed_trains$body.train_id))
        heavily_delayed_trains <- delayed_trains[delayed_trains$body.timetable_variation >= HEAVY_DELAY, ]
        no_of_heavily_delayed_trains <- length(unique(heavily_delayed_trains$body.train_id))
        return(data.frame(
            from_stanox = c(from_stanox),
            to_stanox = c(to_stanox),
            no_of_trains = c(no_of_trains),
            no_of_right_time_trains = c(no_of_right_time_trains),
            perc_of_right_time_trains = c(no_of_right_time_trains / no_of_trains),
            no_of_delayed_trains = c(no_of_delayed_trains),
            perc_of_delayed_trains = c(no_of_delayed_trains / no_of_trains),
            no_of_heavily_delayed_trains = c(no_of_heavily_delayed_trains),
            perc_of_heavily_delayed_trains = c(no_of_heavily_delayed_trains / no_of_trains),
            average_delay = c(ifelse(nrow(delayed_trains) > 0, mean(delayed_trains$body.timetable_variation), 0))
        ))
    }
}) 

#### UBER ORPI
# a) weighted mean of the average delay at all stations vs the number of trains stopping at that station
# b) define the mean delay for each train, and then calculate the mean of that vs all trains
# c) mean of everything

overall_average_delay  <- mean(clean_day_data[clean_day_data$body.timetable_variation >= MINIMUM_DELAY, ]$body.timetable_variation)

# early mapping

make_geojson <- function (stations_ranking, segments_ranking, filename = NULL) {
    # load the latest version of the corpus
    corpus <- download_corpus()[, c('STANOX', 'LAT', 'LON', 'Station.Name')]
    # drop the stations that have no coordinates
    corpus <- corpus[!is.na(corpus$LAT) & !is.na(corpus$LON), ]
    stations_ranking <- stations_ranking[stations_ranking$stanox %in% corpus$STANOX, ]
    segments_ranking <- segments_ranking[(segments_ranking$from_stanox %in% corpus$STANOX) & (segments_ranking$to_stanox %in% corpus$STANOX), ]
    # enhancing the station ranking data with the lat lon
    # oddly, dplyr does not support different left and right names for joins
    names(corpus)[names(corpus) == 'STANOX'] <- 'stanox'
    stations_ranking <- left_join(stations_ranking, corpus, by = "stanox")
    # enhancing the segment ranking data with the lat lon
    names(corpus)[names(corpus) == 'stanox'] <- 'from_stanox'
    segments_ranking <- left_join(segments_ranking, corpus, by = "from_stanox")
    names(segments_ranking)[names(segments_ranking) == 'LAT'] <- 'from_lat'
    names(segments_ranking)[names(segments_ranking) == 'LON'] <- 'from_lon'
    names(corpus)[names(corpus) == 'from_stanox'] <- 'to_stanox'
    segments_ranking <- left_join(segments_ranking, corpus, by = "to_stanox")
    names(segments_ranking)[names(segments_ranking) == 'LAT'] <- 'to_lat'
    names(segments_ranking)[names(segments_ranking) == 'LON'] <- 'to_lon'
    max_segment_delay <- max(segments_ranking$average_delay)
    # create the JSON
    json_structure <- c(
        "type" = "FeatureCollection",
        "features" = list(
            # the stations
            unname(lapply(lapply(split(stations_ranking, seq_along(stations_ranking[, 1])), as.list), function (rp) {
                return(c(
                    'type' = "Feature",
                    'geometry' = list(type = "Point", coordinates = c(rp$LON, rp$LAT)),
                    'properties' = list(
                        "title" = rp$Station.Name,
                        "description" = paste0("This is the description for ", rp$Station.Name),
                        "marker-size" = "large",
                        "marker-symbol" = "rail"
                    )
                ))              
            })),
            # the segments
            unname(lapply(lapply(split(segments_ranking, seq_along(segments_ranking[, 1])), as.list), function (segment) {
                return(c(
                    'type' = "Feature",
                    'geometry' = list(type = "LineString", coordinates = list(c(segment$from_lon, segment$from_lat), c(segment$to_lon, segment$to_lat))),
                    properties = list(
                        "title" = paste0(segment$from_stanox, '_', segment$to_stanox),
                        "stroke" = substring(col2hcl("red", l = segment$average_delay / max_segment_delay), 1, 7),
                        "stroke-opacity" = 1.0,
                        "stroke-width" = 5.0
                    )
                ))              
            }))
        )
    )
    if(!is.null(filename)) {
        fileConn <- file(filename)
        writeLines(toJSON(json_structure), fileConn)
        close(fileConn)
    }
    return(json_structure)
}
