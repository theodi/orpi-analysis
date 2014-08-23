# Pre-requisites:
# - the 's3cmd' open source tool, available at http://s3tools.org/s3cmd ; s3cmd 
#   must be configured with your AWS credentials before being called from this
#   script

library(scales)
library(memoise)

# download_data creates a data.frame of all events for train journeys that 
# run on the specified date, including the events for those same trains up to
# ~EXTRA_HOURS hours in the previous day and ~EXTRA_HOURS hours in the 
# following day.
# Note that a series of "MD5 signatures do not match" warnings will be 
# generated to stderr: this is caused by s3cmd not managing correctly
# the MD5 of multipart uploads
download_data_not_memoised <- function (target_date = (Sys.Date() - 1), EXTRA_HOURS = 3, AWS_BUCKET_NAME = "orpi-nrod-store") {
    
    # downloads the reference corpus to get the list of stations that is relevant
    corpus <- download_corpus() 
    
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
        cat(paste0("Reading ", filename, "...\n"));
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
    
    # drop rows that do not belong to relevant stations
    results <- results[results$body.loc_stanox %in% corpus$stanox, ]
    
    # drop rows that have NA for body.planned_timestamp
    latest_row_count <- nrows(results)
    results <- results[!is.na(results$body.planned_timestamp), ]
    change_row_count <- latest_row_count - nrows(results)
    cat(paste0("Dropping rows that have NA for body.planned_timestamp: ", change_row_count, ", ", percent(change_row_count / latest_row_count), " of total.\n"))
    
    # copy body.planned_timestamp to body.gbtt_timestamp where the latter is 
    # undefined; note that if I don't specify as.POSIXct the date is converted
    # back to an epoch-style timestamp
    latest_row_count <- nrows(results)
    change_row_count <- sum(is.na(results$body.gbtt_timestamp))
    cat(paste0("Copying body.planned_timestamp to body.gbtt_timestamp where the latter is undefined: ", change_row_count, ", ", percent(change_row_count / latest_row_count), " of total.\n"))
    results$body.gbtt_timestamp <- as.POSIXct(ifelse(is.na(results$body.gbtt_timestamp), results$body.planned_timestamp, results$body.gbtt_timestamp), origin = '1970-01-01')
    
    # the value of body.current_train_id can be either of "", NA or "null" to
    # represent that the train has not changed id
    latest_row_count <- nrows(results)
    results$body.current_train_id <- ifelse(results$body.current_train_id %in% c("", "null"), NA, results$body.current_train_id)
    change_row_count <- latest_row_count - nrows(results)
    cat(paste0("Dropping trains that have changed id: ", change_row_count, ", ", percent(change_row_count / latest_row_count), " of total.\n"))
    
    # identify trains that changed *any* of their planned locations (e.g. 
    # stations they stop at) and drop their entire journeys (e.g. there were 7 
    # out of 473162 on 13/8/2014)
    latest_row_count <- nrows(results)
    changed_location_trains <- unique(results[!is.na(results$body.original_loc_stanox), ]$body.train_id)
    results <- results[!(results$body.train_id %in% changed_location_trains), ]
    change_row_count <- latest_row_count - nrows(results)
    cat(paste0("Dropping trains that have changed any of their planned stops: ", change_row_count, ", ", percent(change_row_count / latest_row_count), " of total.\n"))
    
    # identify all train ids for events that happened in the target day
    min_possible_date <- as.POSIXct(paste0(formatC(format(target_date, "%Y"), width=4, flag="0"), "/", formatC(format(target_date, "%m"), width=2, flag="0"), "/", formatC(format(target_date, "%d"), width=2, flag="0"), " 00:00"))
    max_possible_date_not_included <- as.POSIXct(paste0(formatC(format(tomorrow, "%Y"), width=4, flag="0"), "/", formatC(format(tomorrow, "%m"), width=2, flag="0"), "/", formatC(format(tomorrow, "%d"), width=2, flag="0"), " 00:00"))
    train_ids_in_scope <- results[(results$body.actual_timestamp >= rep(min_possible_date, nrow(results))) & (results$body.actual_timestamp < rep(max_possible_date_not_included, nrow(results))), ]$body.train_id
    # filter out the trains that don't belong to the list above
    results <- results[results$body.train_id %in% train_ids_in_scope, ]
    
    # make $body.timetable_variation sign-aware
    results$body.timetable_variation <- ifelse(results$body.variation_status == "EARLY", -1 * results$body.timetable_variation, results$body.timetable_variation)
    
    # TODO is this a duplicate?
    # drop the trains that changed id (e.g. there were none on 13/8/2014)
    # changed_id_trains <- unique(results[!is.na(results$body.current_train_id),]$body.train_id)
    # results <- results[!(results$body.train_id %in% changed_id_trains), ]
    
    # drop the columns I do not need
    results <- results[, names(results) %in% c("body.train_id", 
                                               "body.actual_timestamp", "body.event_type", "body.loc_stanox", 
                                               "body.gbtt_timestamp", "body.timetable_variation")]
    
    # sort by train and expected timestamp for the events
    results <- results[with(results, order(body.train_id, body.gbtt_timestamp)), ]    
    
    return(results)
}

download_data <- memoise(download_data_not_memoised)
