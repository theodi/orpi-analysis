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
    path <- paste0("s3://", AWS_BUCKET_NAME, "/", formatC(year, width=4, flag="0"), "/", formatC(month, width=2, flag="0"), "/", formatC(day, width=2, flag="0"), "/")
    grep_string <- paste0("^", path , "arrivals_", year, formatC(month, width=2, flag="0"), formatC(day, width=2, flag="0"))
    s3cmd_command <- paste0("/usr/local/bin/s3cmd ls ", path)
    print(s3cmd_command)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
# all_arrivals_yesterday <- download_data()
=======
>>>>>>> FETCH_HEAD
=======
>>>>>>> FETCH_HEAD
=======
>>>>>>> FETCH_HEAD
all_arrivals_yesterday <- download_data(2014, 8, 4)
length(unique(all_arrivals_yesterday$body.train_id))

# what % were delayed at the final destination? note that there may be more
# than one final destination arrival records for the same train id!
delayed_at_final_destination <- all_arrivals_yesterday[(all_arrivals_yesterday$body.planned_event_type == "DESTINATION") & (all_arrivals_yesterday$body.variation_status == "LATE"), ]
length(unique(delayed_at_final_destination$body.train_id)) / length(unique(all_arrivals_yesterday$body.train_id))

# what was the average delay in minutes of trains that were delayed at their 
# final destination? (will ignore that there are duplicates)
mean(delayed_at_final_destination$body.timetable_variation)

