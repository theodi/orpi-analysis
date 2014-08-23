library(memoise)
library(RCurl)

download_corpus <- memoise(function (CORPUS_DOWNLOAD_URL = "https://raw.githubusercontent.com/theodi/orpi-corpus/master/data/corpus.csv") {
    corpus <- read.csv(text = getURL(CORPUS_DOWNLOAD_URL))
    # drop the stations that have no coordinates: note that because the output
    # of this function is used as reference for all existing stations, the data
    # regarding those stations will be remove from all analysis calculations
    corpus <- corpus[!is.na(corpus$LAT) & !is.na(corpus$LON), c("X3ALPHA", "STANOX", "LAT", "LON", "NLCDESC")]
    return(corpus)
})