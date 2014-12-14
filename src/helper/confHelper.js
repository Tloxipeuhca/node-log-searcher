var  path = require('path'),
  winston = require('winston');


var conf = require(path.resolve('conf.json'));
// Init winston for log
if (conf.winston) {
  winston.remove(winston.transports.Console);
  if (conf.winston.console) {
    winston.add(winston.transports.Console, conf.winston.console);
  }
  if (conf.winston.file) {
    winston.add(winston.transports.File, conf.winston.file);
  }
} 


module.exports = require(path.resolve(process.argv[2]));