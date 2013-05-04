var socketClient = require('socket.io-client');

var socket = socketClient.connect('http://localhost:3000');
socket.on('connect', function() {
    socket.emit('kafkaRequest', { paramValue: "1" });
    socket.on('kafkaResponse', function(data) {
        console.log(data);
    });
    socket.on('disconnect', function() {
    });
});
