const _=require('underscore');
const fs = require('fs');
const express = require('express');
const bodyParser = require('body-parser');

var CONFIG_DEFAULT = {
    "argos-home":"./var",
    "http-port":5080,
    "websocket-port":5090,
    "mqtt":{"host":"argos.winm2m.com","port":1883}    
};

var config = _.clone(CONFIG_DEFAULT);
if(process.argv.length > 2 && fs.existsSync(process.argv[2])) {
    config = _.extend(config, JSON.parse(fs.readFileSync(process.argv[2], 'utf8')));
}

var app = express();
app.use(bodyParser.json(), bodyParser.urlencoded({extended: true}));
var streamer = new (require("./streamer"))(config);

streamer.install("/argos/streamer", app, {sendResult:function(req, res, r) { res.send(r); }});
streamer.start();

app.listen(config["http-port"]);
