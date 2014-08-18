# Pre-requisites:
# - the 's3cmd' open source tool, available at http://s3tools.org/s3cmd ; s3cmd 
#   must be configured with your AWS credentials before being called from this
#   script

AWS_BUCKET_NAME <- "orpi-nrod-store"

# let's see integer numerics as such!
options(digits=12)

# download_data creates a data.frame of all events for train journeys that 
# run on the specified date, including the events for those same trains up to
# ~EXTRA_HOURS hours in the previous day and ~EXTRA_HOURS hours in the 
# following day.
# Note that a series of "MD5 signatures do not match" warnings will be 
# generated to stderr: this is caused by s3cmd not managing correctly
# the MD5 of multipart uploads 
download_data <- function (target_date = (Sys.Date() - 1), EXTRA_HOURS = 3) {

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
    
    return(results)
}

drop_dirty_trains <- function (day_data) {
    # drop the trains that changed id (e.g. there were none on 13/8/2014)
    changed_id_trains <- unique(day_data[!is.na(day_data$body.current_train_id),]$body.train_id)
    day_data <- day_data[!(day_data$body.train_id %in% changed_id_trains), ]
    # identify trains that changed *any* of their planned locations (e.g. 
    # stations they stop at) and drop their entire journeys (e.g. there were 7 
    # out of 473162 on 13/8/2014)
    changed_location_trains <- unique(day_data[!is.na(day_data$body.original_loc_stanox), ]$body.train_id)
    day_data <- day_data[!(day_data$body.train_id %in% changed_location_trains), ]
    # drop the columns I do not need
    day_data <- day_data[, names(day_data) %in% c("body.train_id", 
       "body.actual_timestamp", "body.event_type", "body.loc_stanox", 
       "body.gbtt_timestamp", "body.timetable_variation")]
    # sort by train and expected timestamp for the events
    day_data <- day_data[with(day_data, order(body.train_id, body.gbtt_timestamp)), ]    
    return(day_data)
}

# this function should be applied only to data that was pre-processed using
# drop_dirty_trains above
fill_in_missing_arrivals <- function (clean_day_data) {
    train_ids <- unique(clean_day_data$body.train_id)
    sapply(train_ids, function (train_id) {
        # for the train being examined...
        intermediate_stations_data <- clean_day_data[clean_day_data$body.train_id == train_id, ]
        # drop the departure at origin
        intermediate_stations_data <- tail(intermediate_stations_data, nrow(intermediate_stations_data) - 1)         
        # drop the arrival at destination
        intermediate_stations_data <- head(intermediate_stations_data, nrow(intermediate_stations_data) - 1)         
        # identify the intermediate stations that have no arrival data
        stations_without_arrival <- intermediate_stations_data[intermediate_stations_data$body.event_type == 'DEPARTURE', ]$body.loc_stanox
        stations_without_arrival <- stations_without_arrival[!(stations_without_arrival %in% intermediate_stations_data[intermediate_stations_data$body.event_type == 'ARRIVAL', ]$body.loc_stanox)]
        # create dummy arrival data by duplicating the departure data
        dummy_arrival_data <- intermediate_stations_data[(intermediate_stations_data$body.loc_stanox %in% stations_without_arrival) & (intermediate_stations_data$body.event_type == 'DEPARTURE'), ]
        dummy_arrival_data$body.event_type <- 'ARRIVAL'
        # add the dummy data to the original dataset 
        clean_day_data <<- rbind(clean_day_data, dummy_arrival_data)        
    })    
    clean_day_data <- clean_day_data[with(clean_day_data, order(body.train_id, body.gbtt_timestamp, body.event_type)), ]    
    return(clean_day_data)
}

# examples

# how many train services we recorded yesterday?
all_arrivals_yesterday <- download_data()
length(unique(all_arrivals_yesterday$body.train_id))

# how many train services we recorded on 4 August 2014?
all_arrivals_4_august_2014 <- download_data(as.Date("2014/08/04"))
length(unique(all_arrivals_yesterday$body.train_id))

# what % were delayed at the final destination? note that there may be more
# than one final destination arrival records for the same train id!
delayed_at_final_destination <- all_arrivals_yesterday[(all_arrivals_yesterday$body.planned_event_type == "DESTINATION") & (all_arrivals_yesterday$body.variation_status == "LATE"), ]
length(unique(delayed_at_final_destination$body.train_id)) / length(unique(all_arrivals_yesterday$body.train_id))

# what was the average delay in minutes of trains that were delayed at their 
# final destination? (will ignore that there are duplicates)
mean(delayed_at_final_destination$body.timetable_variation)

