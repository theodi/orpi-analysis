library(memoise)
library(RCurl)

download_corpus <- memoise(function (CORPUS_DOWNLOAD_URL = "https://raw.githubusercontent.com/theodi/orpi-corpus/master/data/corpus.csv") {
    return(read.csv(text = getURL(CORPUS_DOWNLOAD_URL)))
})