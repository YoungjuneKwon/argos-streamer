# Overview
Server application for streaming MQTT packets. Take specific topics and keep them in files for a certain period of time under a specific folder.

- Can add or delete topics that are in service
- Can control how topics are stored in time, number and capacity
- Added / deleted details of the previous operation are managed when the service is restarted.
- Can check the amount of storage per topic in the managed folder
- Each topic can view the last time the message was received
- Can print a list of topics currently working on

# Installation
```bash
$ npm install argos-streamer
```
# Usage
## Standalone
```bash
$ npm install argos-streamer
$ cd node_modules/argos-streamer
$ npm run server
```
## Quick Start
```javascript
var config = {
    "argos-home":"./var",
    "mqtt":{"host":"argos.winm2m.com","port":1883}
};

var streamer = new (require("./streamer"))(config); 
streamer.start();
```

## HTTP Service
```javascript
var config = {
    "argos-home":"./var",
    "http-port":5080,
    "websocket-port":5090,
    "mqtt":{"host":"argos.winm2m.com","port":1883}
};

var streamer = new (require("./streamer"))(config); 

var express = require('express'); 
var bodyParser = require('body-parser'); 

var app = express();
app.use(bodyParser.json(), bodyParser.urlencoded({extended: true}));

streamer.setupHTTP("/argos/streamer", app, {sendResult:function(req, res, r) { res.send(r); }});
streamer.start();

app.listen(config["http-port"]);
```
