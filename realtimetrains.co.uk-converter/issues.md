# For Tom
- the *lateness* fields sometimes do not correspond to the difference between the gbtt and the realtime timestamps! I've decided to ignore them and re-calculate them vs the timestamps.
- See example2.json: all 'location' records miss the *realtimeDeparture* and/or *realtimeArrival* fields. Does it mean that they were on time or that we just don't know?

# For Yargs
- when using wildcards in filenames, the value of parameters is different depending 

console.log(require('yargs').argv.foo);

In the first case, it returns just the first file in the folder, in the second case, it returns "*"

$ node temp.js --foo *
$ node temp.js --foo "*"