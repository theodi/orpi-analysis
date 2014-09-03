# For Tom

# For myself
- too few records in the output, what's happening?!?!?
- orpi-corpus should drop carriage returns from the column names

# For Yargs
- when using wildcards in filenames, the value of parameters is different depending 

console.log(require('yargs').argv.foo);

In the first case, it returns just the first file in the folder, in the second case, it returns "*"

$ node temp.js --foo *
$ node temp.js --foo "*"