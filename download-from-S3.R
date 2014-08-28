# Pre-requisites:
# - the 's3cmd' open source tool, available at http://s3tools.org/s3cmd ; s3cmd 
#   must be configured with your AWS credentials before being called from this
#   script

library(scales)
library(memoise)

source('./download-corpus.R')

# download_data creates a data.frame of all events for train journeys that 
# run on the specified date, including the events for those same trains up to
# ~EXTRA_HOURS hours in the previous day and ~EXTRA_HOURS hours in the 
# following day.
# Note that a series of "MD5 signatures do not match" warnings will be 
# generated to stderr: this is caused by s3cmd not managing correctly
# the MD5 of multipart uploads
download_data <- function (target_date = (Sys.Date() - 1), EXTRA_HOURS = 3, AWS_BUCKET_NAME = "orpi-nrod-store", S3CMD_PATH = "/usr/local/bin/s3cmd") {
    target_date <- as.Date(target_date)
    return(download_data_memoised(target_date, EXTRA_HOURS, AWS_BUCKET_NAME, S3CMD_PATH))
}

download_data_memoised <- memoise(function (target_date, EXTRA_HOURS, AWS_BUCKET_NAME, S3CMD_PATH) {
    
    # downloads the reference corpus to get the list of stations that is relevant
    corpus <- download_corpus() 
    
    # Returns the list of all available files that could include events
    # that took place in the specified target date and within the specified
    # hours; hourEnd is *included* in the results
    get_files_list <- function (target_date, hourStart = 0, hourEnd = 23) {
        # gets the files list from S3
        path <- paste0("s3://", AWS_BUCKET_NAME, "/", formatC(format(target_date, "%Y"), width=4, flag="0"), "/", formatC(format(target_date, "%m"), width=2, flag="0"), "/", formatC(format(target_date, "%d"), width=2, flag="0"), "/")
        s3cmd_command <- paste0(S3CMD_PATH, " ls ", path)
        output_of_s3cmd_command <- pipe(s3cmd_command)
        # the one below is a dirty trick to see if there are results or not,
        # should be improved
        available_files <- read.table(output_of_s3cmd_command, header = F, sep="", colClasses = "character")
        available_files <- available_files[, ncol(available_files)]  
        # if specified, filters out the hours outside of the specified interval
        grep_regexpr <- paste0("^", path, "arrivals_", formatC(format(target_date, "%Y"), width=4, flag="0"), formatC(format(target_date, "%m"), width=2, flag="0"), formatC(format(target_date, "%d"), width=2, flag="0"), "(", paste(formatC(seq(hourStart, hourEnd), width=2, flag="0"), collapse="|"), ")")
        available_files <- grep(grep_regexpr, available_files, value = TRUE)        
        # returns the list in chronological order        
        return(sort(available_files))
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
        results <<- rbind(results, read.csv(pipe(paste0(S3CMD_PATH, " get ", filename, " -")), header = TRUE, stringsAsFactors = FALSE))
    });

    # drop rows that do not belong to relevant stations
    latest_stations_count <- length(unique(results$body.loc_stanox))
    results <- results[results$body.loc_stanox %in% corpus$STANOX, ]
    change_stations_count <- latest_stations_count - length(unique(results$body.loc_stanox))
    cat(paste0("Dropping locations not referenced in the corpus: ", ifelse(change_stations_count > 0, paste0(change_stations_count, ", ", percent(change_stations_count / latest_stations_count), " of total."), "none."), "\n"))
    
    # convert timestamps to POSIXct
    timestamp_column_names <- grep("_timestamp$", names(results), value = TRUE)    
    for (timestamp_column_name in timestamp_column_names) {
        # make empty values into NAs
        results[, timestamp_column_name] <- ifelse(results[, timestamp_column_name] == "", NA, results[, timestamp_column_name])  
        # makes non-NA values to POSIXct
        results[, timestamp_column_name] <- as.POSIXct(results[, timestamp_column_name], origin = '1970-01-01')
    }
    
    # drop rows that have NA for body.planned_timestamp
    latest_row_count <- nrow(results)
    results <- results[!is.na(results$body.planned_timestamp), ]
    change_row_count <- latest_row_count - nrow(results)
    cat(paste0("Dropping rows that have NA for body.planned_timestamp: ", ifelse(change_row_count > 0, paste0(change_row_count, ", ", percent(change_row_count / latest_row_count), " of total."), "none."), "\n"))
    
    # copy body.planned_timestamp to body.gbtt_timestamp where the latter is 
    # undefined; note that if I don't specify as.POSIXct the date is converted
    # back to an epoch-style timestamp
    latest_row_count <- nrow(results)
    change_row_count <- sum(is.na(results$body.gbtt_timestamp))
    cat(paste0("Copying body.planned_timestamp to body.gbtt_timestamp where the latter is undefined: ", ifelse(change_row_count > 0, paste0(change_row_count, ", ", percent(change_row_count / latest_row_count), " of total."), "none."), "\n"))
    results$body.gbtt_timestamp <- as.POSIXct(ifelse(is.na(results$body.gbtt_timestamp), results$body.planned_timestamp, results$body.gbtt_timestamp), origin = '1970-01-01')
    
    # the value of body.current_train_id can be either of "", NA or "null" to
    # represent that the train has not changed id; I change them all to NA
    results$body.current_train_id <- ifelse(results$body.current_train_id %in% c("", "null"), NA, results$body.current_train_id)

    # drop the trains that changed id (e.g. there were none on 13/8/2014)
    latest_train_count <- length(unique(results$body.train_id))
    changed_id_trains <- unique(results[!is.na(results$body.current_train_id),]$body.train_id)
    results <- results[!(results$body.train_id %in% changed_id_trains), ]
    changed_train_count <- latest_train_count - length(unique(results$body.train_id))
    cat(paste0("Dropping trains that changed id: ", ifelse(changed_train_count > 0, paste0(changed_train_count, ", ", percent(changed_train_count / latest_train_count), " of total."), "none."), "\n"))
    
    # identify trains that changed *any* of their planned locations (e.g. 
    # stations they stop at) and drop their entire journeys (e.g. there were 7 
    # out of 473162 on 13/8/2014)
    latest_train_count <- length(unique(results$body.train_id))
    changed_location_trains <- unique(results[results$body.original_loc_stanox %in% corpus$STANOX, ]$body.train_id)
    results <- results[!(results$body.train_id %in% changed_location_trains), ]
    changed_train_count <- latest_train_count - length(unique(results$body.train_id))
    cat(paste0("Dropping trains that have changed any of their planned stops: ", ifelse(changed_train_count > 0, paste0(changed_train_count, ", ", percent(changed_train_count / latest_train_count), " of total."), "none."), "\n"))
    
    # identify all train ids for events that happened in the target day
    min_possible_date <- as.POSIXct(paste0(formatC(format(target_date, "%Y"), width=4, flag="0"), "/", formatC(format(target_date, "%m"), width=2, flag="0"), "/", formatC(format(target_date, "%d"), width=2, flag="0"), " 00:00"))
    max_possible_date_not_included <- as.POSIXct(paste0(formatC(format(tomorrow, "%Y"), width=4, flag="0"), "/", formatC(format(tomorrow, "%m"), width=2, flag="0"), "/", formatC(format(tomorrow, "%d"), width=2, flag="0"), " 00:00"))
    train_ids_in_scope <- results[(results$body.actual_timestamp >= rep(min_possible_date, nrow(results))) & (results$body.actual_timestamp < rep(max_possible_date_not_included, nrow(results))), ]$body.train_id
    # filter out the trains that don't belong to the list above
    results <- results[results$body.train_id %in% train_ids_in_scope, ]
    
    # make $body.timetable_variation sign-aware
    results$body.timetable_variation <- ifelse(results$body.variation_status == "EARLY", -1 * results$body.timetable_variation, results$body.timetable_variation)
    
    # drop the columns I do not need
    results <- results[, names(results) %in% c("body.train_id", 
        "body.actual_timestamp", "body.event_type", "body.loc_stanox",
        "body.gbtt_timestamp", "body.timetable_variation")]
    
    # sort by train and expected timestamp for the events
    results <- results[with(results, order(body.train_id, body.gbtt_timestamp)), ]    
    
    return(results)
})
