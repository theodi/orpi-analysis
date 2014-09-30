library(dplyr)
library(memoise)
library(rjson)

RIGHT_TIME <- 1
MINIMUM_DELAY <- 5
HEAVY_DELAY <- 30
PASSENGERS_JOURNEYS_PER_DAY_UK_WIDE <- 4360000
AVG_DELAYED_TRAINS_PER_DAY <- 2219

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
            trains_that_must_have_arrived <- unique(trains_that_must_have_arrived[trains_that_must_have_arrived$body.gbtt_timestamp > trains_that_must_have_arrived$earliest_event, ]$body.train_id)
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
        # station_data_only <- integrate_with_missing_arrivals(day_data, stanox)
        # if you need turning off integration, replace the line above with this below:
        station_data_only <- day_data[day_data$body.loc_stanox == stanox, ]
        # calculate the stats for the location
        no_of_trains <- length(unique(station_data_only$body.train_id))
        no_of_right_time_trains <- length(unique(station_data_only[station_data_only$body.timetable_variation <= RIGHT_TIME, ]$body.train_id))
        perc_of_right_time_trains <- no_of_right_time_trains / no_of_trains
        delayed_station_data_only <- station_data_only[station_data_only$body.timetable_variation >= MINIMUM_DELAY, ]
        no_of_delayed_trains <- length(unique(delayed_station_data_only$body.train_id))
        perc_of_delayed_trains <- no_of_delayed_trains / no_of_trains
        no_of_heavily_delayed_trains <- length(unique(delayed_station_data_only[delayed_station_data_only$body.timetable_variation >= HEAVY_DELAY, ]$body.train_id))
        perc_of_heavily_delayed_trains <- no_of_heavily_delayed_trains / no_of_trains
        not_right_time_delays <- station_data_only[station_data_only$body.timetable_variation > RIGHT_TIME, "body.timetable_variation"]
        average_delay <- ifelse(length(not_right_time_delays) > 0, mean(not_right_time_delays), 0)        
        corpus <- download_corpus()
        corpus$Entries.Total <- as.numeric(corpus$Entries.Total)
        # Doesn't take into account fluctuations in no. of trains - adjusted globally
        station_people_weight <- corpus[corpus$STANOX == stanox, "Entries.Total"] / sum(corpus[, "Entries.Total"]) * PASSENGERS_JOURNEYS_PER_DAY_UK_WIDE
        total_lost_minutes <- average_delay * station_people_weight * (1 - perc_of_right_time_trains)
        return(data.frame(
            stanox = c(stanox),
            no_of_trains = c(no_of_trains),
            no_of_right_time_trains = c(no_of_right_time_trains),
            perc_of_right_time_trains = c(perc_of_right_time_trains),
            no_of_delayed_trains = c(no_of_delayed_trains),
            perc_of_delayed_trains = c(perc_of_delayed_trains),
            no_of_heavily_delayed_trains = c(no_of_heavily_delayed_trains),
            perc_of_heavily_delayed_trains = c(perc_of_heavily_delayed_trains),
            average_delay = c(average_delay),
            station_people_weight = c(station_people_weight),
            total_lost_minutes = c(total_lost_minutes)
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
        perc_of_right_time_trains <- no_of_right_time_trains / no_of_trains
        delayed_trains <- segment_data[segment_data$body.timetable_variation >= MINIMUM_DELAY, ]
        no_of_delayed_trains <- length(unique(delayed_trains$body.train_id))
        perc_of_delayed_trains <- no_of_delayed_trains / no_of_trains
        heavily_delayed_trains <- delayed_trains[delayed_trains$body.timetable_variation >= HEAVY_DELAY, ]
        no_of_heavily_delayed_trains <- length(unique(heavily_delayed_trains$body.train_id))
        perc_of_heavily_delayed_trains <- no_of_heavily_delayed_trains / no_of_trains
        not_right_time_delays <- segment_data[segment_data$body.timetable_variation > RIGHT_TIME, "body.timetable_variation"]
        average_delay <- ifelse(length(not_right_time_delays) > 0, mean(not_right_time_delays), 0)
        return(data.frame(
            from_stanox = c(from_stanox),
            to_stanox = c(to_stanox),
            no_of_trains = c(no_of_trains),
            no_of_right_time_trains = c(no_of_right_time_trains),
            perc_of_right_time_trains = c(perc_of_right_time_trains),
            no_of_delayed_trains = c(no_of_delayed_trains),
            perc_of_delayed_trains = c(perc_of_delayed_trains),
            no_of_heavily_delayed_trains = c(no_of_heavily_delayed_trains),
            perc_of_heavily_delayed_trains = c(perc_of_heavily_delayed_trains),
            average_delay = c(average_delay)
        ))
    }
}) 

# when looking at the data in its entirety, a "right time" train is a train
# that was "right time" at all its stops, while a delayed train is a train 
# that was delayed at any of its stops.
calculate_day_rank <- function (date_from, date_to = NULL) {
    if (class(date_from) != "Date") date_from <- as.Date(date_from, origin = '1970-01-01')
    if (!is.null(date_to) && (class(date_to) != "Date")) date_to <- as.Date(date_to, origin = '1970-01-01')
    return(calculate_day_rank_memoised(date_from, date_to))
}

calculate_day_rank_memoised <- memoise(function (date_from, date_to) {
    if (!is.null(date_to)) {
        date_range <- sapply(seq(0, date_to - date_from), function (x) { as.Date(date_from + x) });
        return(do.call(rbind, lapply(date_range, function (d) calculate_day_rank(d))))
    } else {
        cat(paste0("Downloading data for ", date_from, "...\n"))
        day_data <- download_data(paste0(formatC(format(date_from, "%Y"), width=4, flag="0"), "-", formatC(format(date_from, "%m"), width=2, flag="0"), "-", formatC(format(date_from, "%d"), width=2, flag="0")))
        cat(paste0("Calculating rankings for ", date_from, "...\n"))
        stations_ranking <- calculate_station_rank(day_data)
        no_of_trains <- length(unique(day_data$body.train_id))
        temp <- day_data %.%
            group_by(body.train_id) %.%
            summarise(
                # what about the missing arrivals integration???
                no_of_events = length(body.train_id),
                no_of_right_time_events = sum(body.timetable_variation <= RIGHT_TIME),
                no_of_delayed_events = sum(body.timetable_variation >= MINIMUM_DELAY),
                no_of_heavily_delayed_events = sum(body.timetable_variation >= HEAVY_DELAY)
            )
        no_of_right_time_trains <- nrow(temp[temp$no_of_events == temp$no_of_right_time_events, ])
        perc_of_right_time_trains <- no_of_right_time_trains / no_of_trains
        no_of_delayed_trains <- nrow(temp[temp$no_of_delayed_events > 1, ])
        perc_of_delayed_trains <- no_of_delayed_trains / no_of_trains
        no_of_heavily_delayed_trains <- nrow(temp[temp$no_of_heavily_delayed_events > 1, ])
        perc_of_heavily_delayed_trains <- no_of_heavily_delayed_trains / no_of_trains
        # Hack for number of trains NOT right time
        weights <- (stations_ranking$no_of_trains - stations_ranking$no_of_right_time_trains) / (no_of_trains - no_of_right_time_trains)
        average_delay  <- weighted.mean(stations_ranking$average_delay, weights)
        # Lost minutes adjusted by no. of trains
        total_lost_minutes <- sum(stations_ranking$total_lost_minutes) * (no_of_delayed_trains / AVG_DELAYED_TRAINS_PER_DAY)
        return(data.frame(
            date = c(date_from),
            no_of_trains = c(no_of_trains),
            no_of_right_time_trains = c(no_of_right_time_trains),
            perc_of_right_time_trains = c(perc_of_right_time_trains),
            no_of_delayed_trains = c(no_of_delayed_trains),
            perc_of_delayed_trains = c(perc_of_delayed_trains),
            no_of_heavily_delayed_trains = c(no_of_heavily_delayed_trains),
            perc_of_heavily_delayed_trains = c(perc_of_heavily_delayed_trains),
            average_delay = c(average_delay),
            total_lost_minutes = c(total_lost_minutes)
        ))
    }
})

make_geojson <- function (stations_ranking, segments_ranking, filename = NULL) {

    filename <- 'foo2.geojson'
    
    fix_columns_format_for_display <- function (df) {
        # convert all columns start by 'perc_' in a more readable format
        perc_column_names <- grep("^perc_", names(df), value = TRUE)
        for (perc_column_name in perc_column_names) {
            df[, perc_column_name] <- ifelse(df[, perc_column_name] > 0, percent(df[, perc_column_name]), "0%")
        }
        # truncate the decimals for the average delay
        df$average_delay <- round(df$average_delay, 1)
        return(df)        
    }
    
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
    stations_ranking <- fix_columns_format_for_display(stations_ranking)
    
    # enhancing the segment ranking data with the lat lon
    names(corpus)[names(corpus) == 'stanox'] <- 'from_stanox'
    segments_ranking <- left_join(segments_ranking, corpus, by = "from_stanox")
    names(segments_ranking)[names(segments_ranking) == 'LAT'] <- 'from_lat'
    names(segments_ranking)[names(segments_ranking) == 'LON'] <- 'from_lon'
    names(corpus)[names(corpus) == 'from_stanox'] <- 'to_stanox'
    segments_ranking <- left_join(segments_ranking, corpus, by = "to_stanox")
    names(segments_ranking)[names(segments_ranking) == 'LAT'] <- 'to_lat'
    names(segments_ranking)[names(segments_ranking) == 'LON'] <- 'to_lon'
    # segments_ranking <- fix_columns_format_for_display(segments_ranking)
    
    # to support the segments colouring
    min_segment_delay <- min(segments_ranking$average_delay)
    max_segment_delay <- max(segments_ranking$average_delay)
    min_alpha <- 10
    min_opacity <- 30
    exp_base <- (100 - min_opacity) ^ (1 / (max_segment_delay - min_segment_delay))
    
    # drops and renames the columns to something more human
    stations_ranking <- stations_ranking[, names(stations_ranking) %in% c('Station.Name', 'no_of_trains', 'perc_of_delayed_trains', 'LAT', 'LON')]
    # TODO: renaming columns by assuming their position is bad!!!
    names(stations_ranking) <- c('No. of trains', '% of delayed trains', 'LAT', 'LON', 'Station name')
    
    # create the JSON
    json_structure <- list(
        "type" = "FeatureCollection",
        "features" = c(
            # the stations
            unname(lapply(lapply(split(stations_ranking, seq_along(stations_ranking[, 1])), as.list), function (rp) {
                return(list(
                    'type' = "Feature",
                    'geometry' = list(type = "Point", coordinates = c(rp$LON, rp$LAT)),
                    'properties' = do.call(c, list(
                        rp[names(rp) %in% c('Station name', 'No. of trains', '% of delayed trains', 'LAT', 'LON')],
                        "marker-size" = "large",
                        "marker-symbol" = "rail"
                    ))
                ))              
            })),
            # the segments
            unname(lapply(lapply(split(segments_ranking, seq_along(segments_ranking[, 1])), as.list), function (segment) {
                return(list(
                    'type' = "Feature",
                    'geometry' = list(type = "LineString", coordinates = list(c(segment$from_lon, segment$from_lat), c(segment$to_lon, segment$to_lat))),
                    properties = do.call(c, list(
                        # uncomment below if you want all stats calculated for
                        # segments to be part of the GeoJSON
                        # segment[!(names(segment) %in% c('from_stanox', 'to_stanox', 'from_lat', 'from_lon', 'to_lat', 'to_lon'))],
                        "average_delay" = segment$average_delay, 
                        "stroke" = ifelse(segment$average_delay == 0, "#BEBEBE", "#FF0000"),
                        "stroke-opacity" = ifelse(segment$average_delay == 0, min_opacity, round((min_alpha + exp_base ^ (segment$average_delay - min_segment_delay)) / 100, 2)),
                        "stroke-width" = ifelse(segment$average_delay == 0, 2, 3)
                    ))
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
