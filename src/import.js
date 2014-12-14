var      _ = require('lodash'),
     async = require('async'),
      conf = require("./helper/confHelper.js"),
        fs = require('fs'),
lineReader = require('line-reader');
    moment = require('moment'),
   sqlite3 = require('sqlite3').verbose(),
      uuid = require('node-uuid/uuid'),
   winston = require('winston');

// How to execute
// arguments
//   argv2 path to conf file
//   argv3 path to log file
// scripts
//   node src/import.js conf/default.json log/default.txt


var filePath = process.argv[3];
var db = new sqlite3.Database(conf.schema);
db.serialize(function() {
  var createScript = getCreateTableScipt(conf);
  db.run(createScript);

  var lineNumber = 0;
  var sql = db.prepare(getInsertScript(conf));
  lineReader.eachLine(filePath, function(line) {
    var params = getParams(line, conf);
    winston.debug(lineNumber, JSON.stringify(params));
    sql.run(getParams(line, conf));
    lineNumber++;
  }).then(function () {
    console.log("I'm done!!");

    sql.finalize();
    db.close();
  });

/*    db.each("SELECT rowid AS id, * FROM "+conf.tableName, function(err, row) {
      console.log(JSON.stringify(row));
    });*/


  // });
});




function getCreateTableScipt(conf) {
  return "CREATE TABLE IF NOT EXISTS "+conf.tableName+" ("+getCreateFiledsScript(conf)+")";
}

function getCreateFiledsScript(conf) {
  var script = [];
  _.each(conf.fields, function(filed) {
    script.push(filed.name+' '+filed.type);
  })
  return script.join(', ');
}

function getInsertScript(conf) {
  var params = [];
  for (var i=0; i<conf.fields.length; i++) {
    params.push('?');
  }
  return "INSERT INTO "+conf.tableName+"("+getInsertFileds(conf)+") VALUES("+params.join(', ')+")";
}

function getInsertFileds(conf) {
  return _.pluck(conf.fields, "name").join(', ');
}

function getParams(text, conf) {
  var result = executeRegex(text, conf.regex);
  var convertedParams = [];
  var offset = 1;
  _.each(conf.fields, function(field, index) {
    if (field.generated) {
      offset--;
      if (field.generated === "uuid") {
        convertedParams.push(uuid.v4())
      }
      else {
        convertedParams.push(null);
      }
    }
    else if (result.length < index+offset) {
      convertedParams.push(null);
    }
    else if (field.type === 'DATETIME') {
      var date = moment(result[index+offset], field.format);
      convertedParams.push(moment(date).format('YYYY-MM-DD HH:mm:ss.000000'));
    }
    else if (field.type === 'INT') {
      convertedParams.push(parseInt(result[index+offset]));
    }
    else {
      convertedParams.push(result[index+offset]);
    }
  })
  return convertedParams;
}

function executeRegex(text, regexCollection) {
  for (var i=0; i<regexCollection.length; i++) {
    var result = text.trim().match(new RegExp(regexCollection[i]));
    if (result) {
      return result;
    }
  }
  return null;
}