const fs = require('fs');
const mkdirp = require('mkdirp');
const path = require('path');
const mqtt = require('mqtt');
const _ = require('underscore');

function Streamer(config) {
    this.config = config;
    this.context = {};
    this.prepare();
}

Streamer.prototype.prepare = function() {
    this.context.path = {};
    this.context.path.root = this.config["argos-home"]+path.sep+"streamer",
    this.context.path.files = this.context.path.root + path.sep + "files";
    this.context.path.model = this.context.path.root + path.sep + "model.json";

    this.validatePath(this.context.path.root);
    this.validatePath(this.context.path.files);
    this.validateJSONFile(this.context.path.model);

    this.context.subscriptions = [];
    this.reloadModel();
}

Streamer.prototype.validatePath = function(path) {
    if(!fs.existsSync(path)) mkdirp.sync(path);
}

Streamer.prototype.validateJSONFile = function(path) {
    if(!fs.existsSync(path)) fs.writeFileSync(path,"{}");
}

Streamer.prototype.reloadModel = function() {
    this.context.model = JSON.parse(fs.readFileSync(this.context.path.model));
    
}

Streamer.prototype.refreshSubscriptions = function() {
    console.log("refreshSubscriptions");
    var that = this;
    _.each(_.filter(this.context.model.sources, function(i) { return i.type=="mqtt"}), function(m) {
        if(!_.isUndefined(_.findWhere(that.context.subscriptions, {"topic":m.params.topic}))) return;
        console.log("subscribe "+m.params.topic);
        that.mqttClient.subscribe(m.params.topic);
        that.context.subscriptions.push({topic:m.params.topic, source:m});
    });
}

Streamer.prototype.onMQTTMessage = function(t, m) {
    var s = _.findWhere(this.context.subscriptions, {"topic":t});
    this.saveMessage(s, m);
}

Streamer.prototype.saveMessage = function(s, m) {
    var targetPath = this.context.path.files + path.sep + s.source.params.topic.substring(1);
    this.validatePath(targetPath);
    var now = new Date().getTime();
    var serial = 0;
    while(fs.existsSync(targetPath + path.sep + now + "-" + serial)) serial++;
    var file = targetPath + path.sep + now + "-" + serial;
    fs.writeFileSync(file, m);

    this.applyLimit(s);
}

Streamer.prototype.applyLimit = function(s) {
    var targetPath = this.context.path.files + path.sep + s.source.params.topic.substring(1);
    var limit = s.source.limit;
    var files = fs.readdirSync(targetPath);
    var removed = [];
    var now = new Date().getTime();
    
    if(!_.isUndefined(limit.time)) {
        _.each(files, function(f) {
            if(removed.indexOf(f) >= 0) return;
            var fullPath = targetPath+path.sep+f;
            var stat = fs.statSync(fullPath);
            if(stat.birthtimeMs + limit.time >= now) return;
            removed.push(f);
            fs.unlinkSync(fullPath);
        });
    }
    
}

Streamer.prototype.connectMQTTServer = function(cb) {
    var that = this;
    this.mqttClient = mqtt.connect(this.config.mqtt);
    this.mqttClient.on('connect', function() {
        if(_.isUndefined(that.mqttConnected)) {
            that.mqttConnected = true;
            that.mqttClient.on("message", function(t, m) {
                that.onMQTTMessage(t, m);
            });
            cb();
        }
    });
}

Streamer.prototype.start = function() {
    console.log("streamer starting with config, "+JSON.stringify(this.config));
    console.log("model, "+JSON.stringify(this.context.model));

    var that = this;
    this.connectMQTTServer(function() {
        that.refreshSubscriptions();
    });
}

module.exports = Streamer;
