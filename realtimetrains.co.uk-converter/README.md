*realtimetrains-converter.js* is a NodeJS script aimed at converting the log files kindly offered by Realtimetrains.co.uk to a format that is compatible with the ORPI analysis scripts. 

Invokation is simple: just specify the location of the ORPI corpus, the output folder and the files that need conversion:

    $ node realtimetrains-converter.js --corpus corpus.csv --out output_folder/ ../archive-2013-03-*  

The command line above will convert all March 2013 archive files and save them to *output_folder*.

The output files will have the same name as the input files plus ".csv". 

The output file format mimics the format used by ORPI to store Network Rail's "TRAIN_MVT_ALL_TOC" feed, e.g. there is one CSV line per event (arrival / departure) and not one JSON per train service as in the Realtimetrains.co.uk log files. Note that Realtimetrains.co.uk's *serviceUid* is used as the train services' unique identifier instead than Network Rail feed's *train_id*. All content that is not used by the analysis scripts is dropped in the conversion.