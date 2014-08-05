# Pre-requisites:
# - the 's3cmd' open source tool, available at http://s3tools.org/s3cmd ; s3cmd 
#   must be configured with your AWS credentials before being called from this
#   script

AWS_BUCKET_NAME <- "orpi-nrod-store"

# download_data creates a data.frame from all arrival log files from the 
# specified date; if no date is specified, the date of yesterday is used.
# Note that a series of "MD5 signatures do not match" warnings will be 
# generated to stderr: this is caused by s3cmd not managing correctly
# the MD5 of multipart uploads 
download_data <- function (year = format((Sys.Date() - 1), "%Y"), month = format((Sys.Date() - 1), "%m"), day = format(Sys.Date() - 1, "%d")) {
    path <- paste0("s3://", AWS_BUCKET_NAME, "/", year, "/", month, "/", day, "/")
    grep_string <- paste0("^", path , "arrivals_", year, formatC(month, width=2, flag="0"), formatC(day, width=2, flag="0"))
    s3cmd_command <- paste0("/usr/local/bin/s3cmd ls ", path)
    available_files <- read.table(pipe(s3cmd_command), header = F, sep="", colClasses = "character")  
    available_files <- grep(grep_string, available_files[, ncol(available_files)], value = TRUE) 
    results <- data.frame();
    sapply(available_files, function (filename) {
        print(paste0("Reading ", filename, "..."));
        results <<- rbind(results, read.csv(pipe(paste0("/usr/local/bin/s3cmd get ", filename, " -")), header = TRUE))
    });
    return(results)
}

# examples

# how many train services we recorded yesterday?
all_arrivals_yesterday <- download_data()
length(unique(all_arrivals_yesterday$body.train_id))

# what % were delayed at the final destination? note that there may be more
# than one final destination arrival records for the same train id!
delayed_at_final_destination <- all_arrivals_yesterday[(all_arrivals_yesterday$body.planned_event_type == "DESTINATION") & (all_arrivals_yesterday$body.variation_status == "LATE"), ]
length(unique(delayed_at_final_destination$body.train_id)) / length(unique(all_arrivals_yesterday$body.train_id))

# what was the average delay in minutes of trains that were delayed at their 
# final destination? (will ignore that there are duplicates)
mean(delayed_at_final_destination$body.timetable_variation)

#------------------------------------------------
test <- row.sample(all_arrivals_yesterday, 1000)

# Convery UNIX times into R formats
# Milliseconds too precise for conversion
convert.unix <- function(x) as.POSIXct((x + 0.1)/1000, origin = '1970-01-01')

test$body.actual_timestamp <- convert.unix(test$body.actual_timestamp)

test$body.auto_expected <- as.logical(test$body.auto_expected)

test$body.correction_ind <- as.logical(test$body.correction_ind)

# TODO: train ID is not useful
table(all_arrivals_yesterday$body.current_train_id)
test$body.current_train_id <- as.character(test$body.current_train_id)
test$body.current_train_id[test$body.current_train_id == ""] <- NA

test$body.delay_monitoring_point <- as.logical(test$body.delay_monitoring_point)

# TODO - this should happen for all variable before factor conversion?
test$body.direction_ind[test$body.direction_ind == ""] <- NA
test$body.direction_ind <- droplevels(test$body.direction_ind)

test$body.gbtt_timestamp <- convert.unix(test$body.gbtt_timestamp)

# TODO - this should happen for all variable before factor conversion?
test$body.line_ind[test$body.line_ind == ""] <- NA
test$body.line_ind <- droplevels(test$body.line_ind)

test$body.offroute_ind <- as.logical(test$body.offroute_ind)

# TODO
# body.original_loc_stanox
# body.original_loc_timestamp

test$body.planned_timestamp <- convert.unix(test$body.planned_timestamp)

# TODO - this should happen for all variable before factor conversion?
test$body.platform[test$body.platform == ""] <- NA
test$body.platform <- droplevels(test$body.platform)

# TODO - this should happen for all variable before factor conversion?
test$body.route[test$body.route == ""] <- NA
test$body.route <- droplevels(test$body.route)

test$body.train_file_address <- as.character(test$body.train_file_address)
# TODO more missings as 'null'?
test$body.train_file_address[test$body.train_file_address == "null"] <- NA

test$body.train_id <- as.character(test$body.train_id)

test$body.train_terminated <- as.logical(test$body.train_terminated)

test$header.msg_queue_timestamp <- convert.unix(test$header.msg_queue_timestamp)

test$header.source_dev_id <- as.character(test$header.source_dev_id)
test$header.source_dev_id[test$header.source_dev_id == ""] <- NA

test$header.user_id <- as.character(test$header.user_id)
test$header.user_id[test$header.user_id == ""] <- NA


str(test)

