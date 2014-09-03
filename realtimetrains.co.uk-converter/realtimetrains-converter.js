var // https://github.com/caolan/async
	async = require('async'),
	// http://www.adaltas.com/projects/node-csv/
	// Note we're still using version 0.3.x as 0.4.x is unstable
	csv = require('csv'),
	fs = require('fs'),
	path = require('path'),
	readline = require('readline'), 
	// https://github.com/chevex/yargs
	argv = require('yargs')
		.usage('Usage: --corpus <filename> --out <output folder> <input filename 1> <input filename 2...>')
		.demand([ 'corpus', 'out' ])
		.default('corpus', '../../orpi-corpus/data/corpus.csv')
		.argv,
	_ = require('underscore');

var OUTPUT_COLUMNS = [ "body.train_id", "body.toc_id", "body.loc_stanox",
	"body.event_type", "body.gbtt_timestamp", "body.actual_timestamp", 
	"body.timetable_variation" ];

var stanoxBy3alpha = { },
	stanoxByTiploc = { };

var fetchCorpus = function (corpusFilename, callback) {
	if (!fs.existsSync(corpusFilename)) {
		callback(new Error("The specified filename for the corpus does not exist."));
	} else {
		csv()
			.from.path(argv.corpus, { 'columns': true })
			.to.array(function (data) { 
				data.forEach(function (station) {
					stanoxBy3alpha[station['3ALPHA']] = station.STANOX;
					stanoxByTiploc[station.TIPLOC] = station.STANOX;
				});
				callback(null); 
			});			
	}
}

var convert = function (inFile, outFile, callback) {

	var inStream = fs.createReadStream(inFile),
		outStream = fs.createWriteStream(outFile),
		noOfRecords = 0,
		noOfIrregularJsonRecords = 0,
		noOfNonPassengerTrains = 0,
		noOfTrainsLackingSomeRealtimeInformation = 0,
		noOfTrainsWithAnyLocationCancelled = 0, 
		noOfTrainsReferencingUnknownStations = 0,
		unknownStations = [ ];

	var dateToCSVDate = function (d) {
		return d.getFullYear() + "/" + (d.getMonth() < 9 ? '0' : '') + (d.getMonth() + 1) + "/" + (d.getDate() < 10 ? '0' : '') + d.getDate() + " " + (d.getHours() < 10 ? '0' : '') + d.getHours() + ":" + (d.getMinutes() < 10 ? '0' : '') + d.getMinutes();
	}

	// write the output CSV header
	outStream.write(OUTPUT_COLUMNS.map(function (columnName) { return JSON.stringify(columnName); }).join(","));

	// use of readline inspired by http://stackoverflow.com/a/16013228
	// but BROKEN!!!
	var	rl = readline.createInterface({
		    	input: inStream,
		    	// output: outStream,
		    	terminal: false
			});

	outStream.on('end', function () {
		callback(null, {
			'noOfRecords': noOfRecords,
			'noOfIrregularJsonRecords': noOfIrregularJsonRecords,
			'noOfNonPassengerTrains': noOfNonPassengerTrains,
			'noOfTrainsWithAnyLocationCancelled': noOfTrainsWithAnyLocationCancelled,
			'noOfTrainsLackingSomeRealtimeInformation': noOfTrainsLackingSomeRealtimeInformation,
			'noOfTrainsReferencingUnknownStations': noOfTrainsReferencingUnknownStations,
			'unknownStations': unknownStations,
		});
	})

	rl.on('line', function (line) {

		var	recordAsJson = null;

		var writeTrain = function (train) {

			var createOrpiRecord = function (ar, location) {
				var event_type,
					stanox = stanoxBy3alpha[location.crs] || stanoxByTiploc[location.tiploc],
					timetableVariation,
					gbttTimestamp,
					actualTimestamp;
				if (ar === 'a') {
					eventType = 'ARRIVAL';
					gbttTimestamp = new Date(recordAsJson.runDate + " " + location.gbttBookedArrival.substring(0, 2) + ":" + location.gbttBookedArrival.substring(2, 4));
					if (location.gbttBookedArrivalNextDay) gbttTimestamp = new Date(gbttTimestamp.valueOf() + 86400000);
					actualTimestamp = new Date(recordAsJson.runDate + " " + location.realtimeArrival.substring(0, 2) + ":" + location.realtimeArrival.substring(2, 4));
					if (location.realtimeArrivalNextDay) actualTimestamp = new Date(actualTimestamp.valueOf() + 86400000);
				} else {
					eventType = 'DEPARTURE';
					gbttTimestamp = new Date(recordAsJson.runDate + " " + location.gbttBookedDeparture.substring(0, 2) + ":" + location.gbttBookedDeparture.substring(2, 4));
					if (location.gbttBookedDepartureNextDay) gbttTimestamp = new Date(gbttTimestamp.valueOf() + 86400000);
					actualTimestamp = new Date(recordAsJson.runDate + " " + location.realtimeDeparture.substring(0, 2) + ":" + location.realtimeDeparture.substring(2, 4));
					if (location.realtimeDepartureNextDay) actualTimestamp = new Date(actualTimestamp.valueOf() + 86400000);
				}
				timetableVariation = (actualTimestamp - gbttTimestamp) / 60000;
				if (gbttTimestamp < originTimestamp) gbttTimestamp = new Date(gbttTimestamp.valueOf() + 86400000);
				if (actualTimestamp < originTimestamp) actualTimestamp = new Date(actualTimestamp.valueOf() + 86400000);
				return { 
					// note that body.serviceUid does not have the same 
					// meaning as in the log ORPI stores, but using 
					// realtimetrains.co.uk' serviceUid is fit to the 
					// same purpose
					"body.train_id": train.serviceUid,
					// TODO: toc_id should be a number for consistency with 
					// ORPI's logs
					"body.toc_id": train.atocCode,
					"body.loc_stanox": stanox,
					"body.event_type": eventType,
					"body.gbtt_timestamp": dateToCSVDate(gbttTimestamp),
					"body.actual_timestamp": dateToCSVDate(actualTimestamp),
					"body.timetable_variation": timetableVariation,
				};
			};

			// some origin records do not have a publicTime but have a 
			// workingTime, that includes seconds
			var outRecord,
				originTimestamp = recordAsJson.origin[0].publicTime || recordAsJson.origin[0].workingTime.substring(0, 4);
			originTimestamp = new Date(train.runDate + " " + originTimestamp.substring(0, 2) + ":" + originTimestamp.substring(2, 4));
			train.locations.forEach(function (location) {
				if (location.gbttBookedArrival) {
					outRecord = createOrpiRecord('a', location);
					outStream.write('\n' + OUTPUT_COLUMNS.map(function (columnName) { return JSON.stringify(outRecord[columnName]); }).join(","));	
				}
				if (location.gbttBookedDeparture) {
					outRecord = createOrpiRecord('d', location);
					outStream.write('\n' + OUTPUT_COLUMNS.map(function (columnName) { return JSON.stringify(outRecord[columnName]); }).join(","));					
				}
			});

		}

		// for each line...
		noOfRecords++;
		// try parsing as JSON...
		try { recordAsJson = JSON.parse(line); } 
		catch (err) { noOfIrregularJsonRecords++; }
		if (recordAsJson) {
			// discard if not a valid JSON
			if (!recordAsJson.isPassenger) {
				// discard if not a passenger train
				noOfNonPassengerTrains++;
			} else {
				// drop information about stations that are not referenced
				// in the ORR reports
				var anyUnknownStations = false;
				recordAsJson.locations = recordAsJson.locations.filter(function (location) {
					var stationId = stanoxBy3alpha[location.crs] || stanoxByTiploc[location.tiploc];
					if (!stationId) {
						unknownStations = _.uniq(unknownStations.concat(location.crs || location.tiploc)).sort();
						anyUnknownStations = true;
						return false;
					} else {
						return true;
					}
				});
				if (anyUnknownStations) noOfTrainsReferencingUnknownStations++;
				// drop information about cancelled stops; this is not ideal and 
				// is done just for consistency with the ORPI's own logs, where
				// cancellations are not visible 
				var anyCancellations = false;
				recordAsJson.locations = recordAsJson.locations.filter(function (location) {
					if (location.displayAs === "CANCELLED_CALL") {
						anyCancellations = true;
						return false;
					} else {
						return true;
					}
				});
				if (anyCancellations) noOfTrainsWithAnyLocationCancelled++;
				if (!_.every(recordAsJson.locations, function hasAllRealtimeInformation (location) {
				 	return((location.gbttBookedArrival ? location.realtimeArrival : true) &&
	   				       (location.gbttBookedDeparture ? location.realtimeDeparture : true));
				})) {
					// discard if any location lacks realtime information
					noOfTrainsLackingSomeRealtimeInformation++;
				} else {
					// finally, convert!
					writeTrain(recordAsJson);
				}
			}
		}

	});

}

var main = function () {

	fetchCorpus(argv.corpus, function (err) {
		var consolidatedStats = { };
		async.eachSeries(argv._, function (inputFilename, callback) {
			console.log("Converting " + inputFilename + "...");
			convert(inputFilename, path.join(argv.out, path.basename(inputFilename)) + ".csv", function (err, conversionStats) {
				_.keys(conversionStats).forEach(function (key) {
					consolidatedStats[key] = _.isArray(conversionStats[key]) ? 
						_.uniq((!consolidatedStats[key] ? [ ] : consolidatedStats[key]).concat(conversionStats[key])).sort() :
						(!consolidatedStats[key] ? 0 : consolidatedStats[key]) + conversionStats[key]; 
				});
				callback(null);
			});	
		}, function (err) {
			console.log("Conversion of " + consolidatedStats.noOfRecords + " trains completed:");
			console.log("- " + consolidatedStats.noOfIrregularJsonRecords + " (" + (consolidatedStats.noOfIrregularJsonRecords / consolidatedStats.noOfRecords * 100).toFixed(1) + "%) trains dropped as described by invalid JSON.");
			console.log("- " + consolidatedStats.noOfNonPassengerTrains + " (" + (consolidatedStats.noOfNonPassengerTrains / consolidatedStats.noOfRecords * 100).toFixed(1) + "%) trains dropped as not passenger ones.");
			console.log("- " + consolidatedStats.noOfTrainsWithAnyLocationCancelled + " (" + (consolidatedStats.noOfTrainsWithAnyLocationCancelled / consolidatedStats.noOfRecords * 100).toFixed(1) + "%) trains had one or more cancelled stops information dropped.");
			console.log("- " + consolidatedStats.noOfTrainsLackingSomeRealtimeInformation + " (" + (consolidatedStats.noOfTrainsLackingSomeRealtimeInformation / consolidatedStats.noOfRecords * 100).toFixed(1) + "%) trains dropped as lacking some realtime information.");
			console.log("- " + consolidatedStats.noOfTrainsReferencingUnknownStations + " (" + (consolidatedStats.noOfTrainsReferencingUnknownStations / consolidatedStats.noOfRecords * 100).toFixed(1) + "%) trains stopped at one or more stations that are not in the corpus, those calls have been dropped.");
			console.log("- The referenced stations that are not in the corpus are (3ALPHA or TIPLOC): " + consolidatedStats.unknownStations.join(", ") + ".");
		});
	});
}

main();
