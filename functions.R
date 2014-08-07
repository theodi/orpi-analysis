row.sample <- function(dta, rep = 20) {
  dta <- as.data.frame(dta, stringsAsFactors=FALSE) # for single variables
  dta[sample(1:nrow(dta), rep, replace=FALSE), ] 
} 

mode.stat <- function(x) {
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}

capwords <- function(s, strict = FALSE) {
  cap <- function(s) paste(toupper(substring(s, 1, 1)),
{s <- substring(s, 2); if(strict) tolower(s) else s},
                           sep = "", collapse = " " )
  sapply(strsplit(s, split = " "), cap, USE.NAMES = !is.null(names(s)))
}

'%ni%' <- Negate('%in%')

count.missing <- function(x) sum(is.na(x))
pct.missing <- function(x) as.numeric(formatC(sum(is.na(x)) * 100 / length(x), format = "fg", digits = 3))

count.empty  <- function(x){sum(as.character(x) == "", na.rm = TRUE)}
pct.empty <- function(x) as.numeric(formatC(sum(as.character(x) == "", na.rm = TRUE) * 100 / length(x), format = "fg", digits = 3))

count.unique <- function(x) length(unique(x))
pct.unique <- function(x) as.numeric(formatC(length(unique(x)) * 100 / length(x), format = "fg", digits = 3))

# Format percentage
format.pct <- function(x, digits = 1, format = "f", ...) {
  paste0(formatC(100 * x, format = format, digits = digits, ...), "%")
}

# Format minutes
format.min <- function(x, digits = 1, format = "f", ...) {
  paste0(formatC(x, format = format, digits = digits, ...), " min")
}

# Show missing data in table as default
table = function (..., useNA = 'ifany') base::table(..., useNA = useNA)

read.ssl <- function(url, ...){
  tmpFile <- tempfile()
  download.file(url, destfile = tmpFile, method = "curl")
  url.data <- read.csv(tmpFile, ...)
  return(url.data)
}
