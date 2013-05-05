var randy = require('randy');
var format = require('util').format;
var socketClient = require('socket.io-client');

function getLatency(callback) {
    var start = new Date().getTime();
    var socket = socketClient.connect('http://localhost:3000', {'force new connection': true});
    var err = null;
    var msg = randy.randInt().toString();
    socket.on('connect', function() {
        socket.emit('kafkaRequest', { paramValue: msg });
        socket.on('kafkaResponse', function(data) {
            var expected = (msg + msg + msg + msg);
            if (data.paramValue !== expected) {
                err = format("Wrong response, expected %s; got %s", expected, data.paramValue);
            }
        });
        socket.on('disconnect', function() {
            callback(err, new Date().getTime() - start);
        });
    });
}

var logLatency = function(finished) {
    getLatency(function(err, lat) {
        if (err) {
            console.log(format("An error was encountered:\n%s", err));
        } else {
            console.log(lat);
            finished();
        }
    });
}

var activeConns = 0;
var connLimit = 10;
var numTestCases = 1000;

var handler = function() {
    numTestCases--;
    activeConns--;
    var lackingConns = connLimit - activeConns;
    for (var i = 0; i < lackingConns && numTestCases > 0; i++) {
        logLatency(handler);
        activeConns++;
    }
}

logLatency(handler);
