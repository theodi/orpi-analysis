setwd("/Users/Ulrich/git/orpi-analysis")

api_logs <- list.files(pattern = "stations-train-count-2014-10*")

for (file in api_logs) {
  print(file)
  load(file)
  assign(paste0("id_", make.names(file)),  train_uid_list) # paste alone doesn't work
  rm(train_uid_list)
}


masterlist <- mget(ls(pattern = "^id_"))
rm(list = ls(pattern = "^id_"))

# This combines the lists into one by adding them to the correct entry
keys <- unique(unlist(lapply(masterlist, names)))
all_trains <- setNames(do.call(mapply, c(FUN = c, lapply(masterlist, `[`, keys))), keys)

# This gives the number of trains in GB for one day (if complete)
length(unique(unlist(all_trains)))

# This per station
trains_per_station <- lapply(all_trains, function(x) length(unique(x)))
