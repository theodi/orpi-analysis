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

var fetchCorpus = function (corpusFilename, callback) {
	if (!fs.existsSync(corpusFilename)) {
		callback(new Error("The specified filename for the corpus does not exist."));
	} else {
		csv()
			.from.path(argv.corpus, { 'columns': true })
			.to.array(function (data) {
				var corpus = { };
				data.forEach(function (station) {
					corpus[station["3ALPHA"]] = station.STANOX;
				});
				callback(null, corpus); 
			});			
	}
}

var convert = function (corpus, inFile, outFile, callback) {

	var noOfRecords = 0,
		noOfIrregularJsonRecords = 0,
		noOfNonPassengerTrains = 0,
		noOfTrainsLackingSomeRealtimeInformation = 0,
		noOfTrainsWithAnyLocationCancelled = 0;

	var dateToCSVDate = function (d) {
		return d.getFullYear() + "/" + (d.getMonth() < 9 ? '0' : '') + (d.getMonth() + 1) + "/" + (d.getDate() < 10 ? '0' : '') + d.getDate() + " " + (d.getHours() < 10 ? '0' : '') + d.getHours() + ":" + (d.getMinutes() < 10 ? '0' : '') + d.getMinutes();
	}

	var inStream = fs.createReadStream(inFile),
		outStream = fs.createWriteStream(outFile),
		// use of readline inspired by http://stackoverflow.com/a/16013228
		// but BROKEN!!!
		rl = readline.createInterface({
		    	input: inStream,
		    	// output: outStream,
		    	terminal: false
			});

	rl.on('close', function () {
		outStream.close();
		callback(null, {
			'noOfRecords': noOfRecords,
			'noOfIrregularJsonRecords': noOfIrregularJsonRecords,
			'noOfNonPassengerTrains': noOfNonPassengerTrains,
			'noOfTrainsWithAnyLocationCancelled': noOfTrainsWithAnyLocationCancelled,
			'noOfTrainsLackingSomeRealtimeInformation': noOfTrainsLackingSomeRealtimeInformation,
		});
	})

	rl.on('line', function (line) {

		var	recordAsJson = null;

		var writeTrain = function (train) {

			var createOrpiRecord = function (ar, location) {
				var event_type,
					timetableVariation,
					gbttTimestamp,
					actualTimestamp;
				if (ar === 'a') {
					eventType = 'ARRIVAL';
					gbttTimestamp = new Date(recordAsJson.runDate + " " + location.gbttBookedArrival.substring(0, 2) + ":" + location.gbttBookedArrival.substring(2, 4));
					actualTimestamp = new Date(recordAsJson.runDate + " " + location.realtimeArrival.substring(0, 2) + ":" + location.realtimeArrival.substring(2, 4));
				} else {
					eventType = 'DEPARTURE';
					gbttTimestamp = new Date(recordAsJson.runDate + " " + location.gbttBookedDeparture.substring(0, 2) + ":" + location.gbttBookedDeparture.substring(2, 4));
					actualTimestamp = new Date(recordAsJson.runDate + " " + location.realtimeDeparture.substring(0, 2) + ":" + location.realtimeDeparture.substring(2, 4));
				}
				timetableVariation = (actualTimestamp - gbttTimestamp) / 60000;
				// fix for trains that originate in one day and travel
				// through the next 
				if (gbttTimestamp < originTimestamp) gbttTimestamp = new Date(gbttTimestamp.valueOf() + 86400000);
				if (actualTimestamp < originTimestamp) actualTimestamp = new Date(actualTimestamp.valueOf() + 86400000);
				return { 
					// note that body.train_id does not have the same 
					// meaning as in the log ORPI stores, but using 
					// realtimetrains.co.uk' trainIdentity is fit to the 
					// same purpose
					"body.train_id": train.trainIdentity,
					"body.stanox": corpus[location.crs],
					"body.event_type": eventType,
					"body.gbtt_timestamp": dateToCSVDate(gbttTimestamp),
					"body.actual_timestamp": dateToCSVDate(actualTimestamp),
					"body.timetable_variation": timetableVariation,
				};
			}

			var originTimestamp = new Date(train.runDate + " " + recordAsJson.origin[0].publicTime.substring(0, 2) + ":" + recordAsJson.origin[0].publicTime.substring(2, 4));
			train.locations.forEach(function (location) {
				if (location.gbttBookedArrival) {
					// console.log(JSON.stringify(createOrpiRecord('a', location)));
					outStream.write(JSON.stringify(createOrpiRecord('a', location)) + '\n');					
				}
				if (location.gbttBookedDeparture) {
					// console.log(JSON.stringify(createOrpiRecord('d', location)));
					outStream.write(JSON.stringify(createOrpiRecord('d', location)) + '\n');					
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

	fetchCorpus(argv.corpus, function (err, corpus) {
		var consolidatedStats = { };
		async.eachSeries(argv._, function (inputFilename, callback) {
			console.log("Converting " + inputFilename + "...");
			convert(corpus, inputFilename, path.join(argv.out, path.basename(inputFilename)), function (err, conversionStats) {
				_.keys(conversionStats).forEach(function (key) {
					consolidatedStats[key] = (!consolidatedStats[key] ? 0 : consolidatedStats[key]) + conversionStats[key]; 
				});
				callback(null);
			});	
		}, function (err) {
			console.log("Conversion of " + consolidatedStats.noOfRecords + " trains completed:");
			console.log("- " + consolidatedStats.noOfIrregularJsonRecords + " (" + (consolidatedStats.noOfIrregularJsonRecords / consolidatedStats.noOfRecords * 100).toFixed(1) + "%) trains dropped as described by invalid JSON.");
			console.log("- " + consolidatedStats.noOfNonPassengerTrains + " (" + (consolidatedStats.noOfNonPassengerTrains / consolidatedStats.noOfRecords * 100).toFixed(1) + "%) trains dropped as not passenger ones.");
			console.log("- " + consolidatedStats.noOfTrainsWithAnyLocationCancelled + " (" + (consolidatedStats.noOfTrainsWithAnyLocationCancelled / consolidatedStats.noOfRecords * 100).toFixed(1) + "%) trains had one or more cancelled stops information dropped.");
			console.log("- " + consolidatedStats.noOfTrainsLackingSomeRealtimeInformation + " (" + (consolidatedStats.noOfTrainsLackingSomeRealtimeInformation / consolidatedStats.noOfRecords * 100).toFixed(1) + "%) trains dropped as lacking some realtime information.");
		});
	});
}

main();
