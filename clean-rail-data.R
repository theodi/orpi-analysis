# This script cleans the data
# It converts UNIX times to POSIx
# Changes empty strings to NAs
# Changes variable classes eg to logical

raildata.clean <- function(x){
  x <- row.sample(all_arrivals_yesterday, 1000)
  
  # Convery UNIX times into R formats
  # Milliseconds too precise for conversion
  convert.unix <- function(x) as.POSIXct((x + 0.1)/1000, origin = '1970-01-01')
  
  x$body.actual_timestamp <- convert.unix(x$body.actual_timestamp)
  
  x$body.auto_expected <- as.logical(x$body.auto_expected)
  
  x$body.correction_ind <- as.logical(x$body.correction_ind)
  
  # TODO: train ID is not useful
  table(all_arrivals_yesterday$body.current_train_id)
  x$body.current_train_id <- as.character(x$body.current_train_id)
  x$body.current_train_id[x$body.current_train_id == ""] <- NA
  
  x$body.delay_monitoring_point <- as.logical(x$body.delay_monitoring_point)
  
  # TODO - this should happen for all variable before factor conversion?
  x$body.direction_ind[x$body.direction_ind == ""] <- NA
  x$body.direction_ind <- droplevels(x$body.direction_ind)
  
  x$body.gbtt_timestamp <- convert.unix(x$body.gbtt_timestamp)
  
  # TODO - this should happen for all variable before factor conversion?
  x$body.line_ind[x$body.line_ind == ""] <- NA
  x$body.line_ind <- droplevels(x$body.line_ind)
  
  x$body.offroute_ind <- as.logical(x$body.offroute_ind)
  
  # TODO
  # body.original_loc_stanox
  # body.original_loc_timestamp
  
  x$body.planned_timestamp <- convert.unix(x$body.planned_timestamp)
  
  # TODO - this should happen for all variable before factor conversion?
  x$body.platform[x$body.platform == ""] <- NA
  x$body.platform <- droplevels(x$body.platform)
  
  # TODO - this should happen for all variable before factor conversion?
  x$body.route[x$body.route == ""] <- NA
  x$body.route <- droplevels(x$body.route)
  
  x$body.train_file_address <- as.character(x$body.train_file_address)
  # TODO more missings as 'null'?
  x$body.train_file_address[x$body.train_file_address == "null"] <- NA
  
  x$body.train_id <- as.character(x$body.train_id)
  
  x$body.train_terminated <- as.logical(x$body.train_terminated)
  
  x$header.msg_queue_timestamp <- convert.unix(x$header.msg_queue_timestamp)
  
  x$header.source_dev_id <- as.character(x$header.source_dev_id)
  x$header.source_dev_id[x$header.source_dev_id == ""] <- NA
  
  x$header.user_id <- as.character(x$header.user_id)
  x$header.user_id[x$header.user_id == ""] <- NA
  result(x)
}

