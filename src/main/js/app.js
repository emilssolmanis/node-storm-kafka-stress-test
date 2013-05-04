/**
 * Module dependencies.
 */

var express = require('express')
  , routes = require('./routes')
  , user = require('./routes/user')
  , http = require('http')
  , path = require('path');

var app = express();

var io = require('socket.io');

var Producer = require('prozess').Producer;
var Consumer = require('prozess').Consumer;

var kafkaCallbacks = {};

var options = {host : 'localhost', topic : 'node-kafka-storm-output', partition : 0, offset : 0};
var consumer = new Consumer(options);
consumer.connect(function(err){
    if (err) {  throw err; }
    console.log("consumer connected!!");
    setInterval(function() {
        // console.log("===================================================================");
        // console.log(new Date());
        // console.log("consuming: " + consumer.topic);
        consumer.consume(function(err, messages) {
            if (!err && messages.length > 0) {
                messages.forEach(function (msg) {
                    var result = JSON.parse(msg.payload.toString('utf-8'));
                    if (result.user in kafkaCallbacks) {
                        kafkaCallbacks[result.user](result);
                        delete kafkaCallbacks[result.user];
                    }
                });
                // console.log(messages.map(function(elem) { return elem.payload.toString('utf-8'); }));
            }
        });
    }, 100);
});

var producer = new Producer('node-kafka-storm', {host : 'localhost'});
producer.connect();
console.log("producing for ", producer.topic);
producer.on('error', function(err){
    console.log("some general error occurred: ", err);  
});
producer.on('brokerReconnectError', function(err){
    console.log("could not reconnect: ", err);  
    console.log("will retry on next send()");  
});

// all environments
app.set('port', process.env.PORT || 3000);
app.set('views', __dirname + '/views');
app.set('view engine', 'jade');
app.use(express.favicon());
// app.use(express.logger('dev'));
app.use(express.bodyParser());
app.use(express.methodOverride());
app.use(app.router);
app.use(express.static(path.join(__dirname, 'public')));

// development only
if ('development' == app.get('env')) {
  app.use(express.errorHandler());
}

app.get('/', routes.index);
app.get('/users', user.list);
app.get('/kafka/:kafkaParam', function(req, res) {
    res.render('kafka', {});
});

var server = http.createServer(app);
var io_serv = io.listen(server);
io_serv.set('log level', 1);

io_serv.sockets.on('connection', function(socket) {
    socket.on('kafkaRequest', function(data) {
        data.user = socket.id;
        kafkaCallbacks[data.user] = function(result) {
                delete result.user;
                socket.emit('kafkaResponse', result);
        }
        producer.send(JSON.stringify(data), function(err) {
            if (err) {
                console.log("send error: ", err);
            }
        });
    });
    
    socket.on('disconnect', function() {
        delete kafkaCallbacks[socket.id];
    });
});

server.listen(app.get('port'), function() {
  console.log('Express server listening on port ' + app.get('port'));
});
