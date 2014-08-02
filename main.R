# Pre-requisites:
# - the 's3cmd' open source tool, available at http://s3tools.org/s3cmd ; s3cmd 
#   must be configured with your AWS credentials before being called from this
#   script

AWS_BUCKET_NAME <- "open_rail_performance_index"

# download_data creates a data.frame from all arrival log files from the 
# specified date; if no date is specified, the date of yesterday is used.
# Note that a series of "MD5 signatures do not match" warnings will be 
# generated to stderr: this is caused by s3cmd not managing correctly
# the MD5 of multipart uploads 
download_data <- function (year = format((Sys.Date() - 1), "%Y"), month = format((Sys.Date() - 1), "%m"), day = format(Sys.Date(), "%d")) {
    available_files <- grep(paste0("^s3://", AWS_BUCKET_NAME, "/arrivals_", year, formatC(month, width=2, flag="0"), formatC(day, width=2, flag="0")), read.table(pipe(paste0("/usr/local/bin/s3cmd ls s3://", AWS_BUCKET_NAME, "/")), header = F, sep=" ", colClasses = "character")[, c(8)], value = TRUE)  
    results <- data.frame();
    sapply(available_files, function (filename) {
        print(paste0("Reading ", filename, "..."));
        results <<- rbind(results, read.csv(pipe(paste0("/usr/local/bin/s3cmd get ", filename, " -")), header = TRUE))
    });
    return(results)
}

# examples

# how many train services we recorded yesterday?
all_arrivals <- download_data()
length(unique(all_arrivals$body.train_id))

# what % were delayed at the final destination? note that there may be more
# than one final destination arrival records for the same train id!
delayed_at_final_destination <- all_arrivals[(all_arrivals$body.planned_event_type == "DESTINATION") & (all_arrivals$body.variation_status == "LATE"), ]
length(unique(delayed_at_final_destination$body.train_id)) / length(unique(all_arrivals$body.train_id))

# what was the average delay in minutes of trains that were delayed at their 
# final destination? (will ignore that there are duplicates)
mean(delayed_at_final_destination$body.timetable_variation)




