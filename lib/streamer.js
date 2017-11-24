const fs = require('fs');
const mkdirp = require('mkdirp');
const path = require('path');
const mqtt = require('mqtt');
const _ = require('underscore');
const io = require('socket.io');
const async = require('async');
const getSize = require('get-folder-size');

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

Streamer.prototype.sourceForTopic = function(topic) {
    var context = {};
    _.each(_.filter(this.context.model.sources, function(i) { return i.type=="mqtt"}), function(source) {
        if(source.params.topic != topic) return;
        context.result = source;
    });
    return context.result;
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
}

Streamer.prototype.applyLimit = function(s, cb) {
    var targetPath = this.fullpathForSource(s);
    var limit = s.limit;
    var now = new Date().getTime();
    this.filteredFileList(
        targetPath,
        function(stat) { return stat.birthtimeMs + limit.time < now;},
        function(filtered) {
            async.eachSeries(filtered, function(fullPath, nxt) {
                fs.unlink(fullPath, function() { nxt(); });
            }, function() {
                return cb(filtered.length);
            });
        }
    );
}

Streamer.prototype.startLimitManager = function() {
    var term = 1000;
    var that = this;
    (function loop() {
        var sources = that.context.model.sources;
        var totalRemoved = 0;
        async.eachSeries(sources, function(s, nxt) {
            that.applyLimit(s, function(removed) {
                totalRemoved += removed;
                nxt();
            });
        }, function() {
            setTimeout(function() { loop(); }, term);
        });
    })();
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

Streamer.prototype.install = function(prefix, app, handler) {
    var that = this;
    this.initWS();

    app.get(prefix+"/series-after", function(req, res) {
        var topic=req.query.topic;
        var timestamp=req.query.timestamp;
        that.seriesAfter(topic, timestamp, function(err, r) {
            handler.sendResult(req, res, r);
        });
    });

    app.get(prefix+"/size-for-topic", function(req, res) {
        var topic = req.query.topic;
        that.sizeForTopic(topic, function(err, size) {
            handler.sendResult(req, res, {size:size});
        });
    });
}

Streamer.prototype.initWS = function() {
    var that = this;
    this.io = new io();
    this.io.on('connection', function(client){
        console.log("connection established, "+client);
        client.on('series-after', function(msg) {
            var req = JSON.parse(msg);
            var topic=req.topic;
            var timestamp=req.timestamp;
            that.seriesAfter(topic, timestamp, function(err, r) {
                client.emit('series-after', r);
            });
        });
    });
    this.io.listen(this.config["websocket-port"]);
}

Streamer.prototype.start = function() {
    console.log("streamer starting with config, "+JSON.stringify(this.config));
    console.log("model, "+JSON.stringify(this.context.model));

    var that = this;
    this.connectMQTTServer(function() {
        that.refreshSubscriptions();
    });

    this.startLimitManager();
}

Streamer.prototype.sizeForAllSources = function(cb) {
    var result = [];
    var that = this;
    async.eachSeries(this.context.model.sources, function(source, nxt) {
        that.sizeForSource(source, function(size) {
            result.push({source:source, size:size});
            nxt();
        });
    }, function() {
        cb(null, result);
    });
}

Streamer.prototype.sizeForSource = function(source, cb) {
    var targetPath = "";
    if(source.type == "mqtt") {
        targetPath = this.fullpathForTopic(source.params.topic);
    }
    if(targetPath != "") getSize(targetPath, function(err, size) {cb(null, size); });
    else cb("invalid source type", -1);
}

Streamer.prototype.sizeForTopic = function(topic, cb) {
    var source = this.sourceForTopic(topic);
    if(_.isUndefined(source)) return cb("invalid topic", -1);
    this.sizeForSource(source, cb);
}

Streamer.prototype.fullpathForSource = function(source) {
    if(source.type == "mqtt") return this.fullpathForTopic(source.params.topic);
    return "";
}

Streamer.prototype.fullpathForTopic = function(topic) {
    return this.context.path.files + path.sep + topic.substring(1);
}

Streamer.prototype.filteredFileList = function(targetPath, filter, cb) {
    var filtered = [];
    fs.readdir(targetPath, function(err, files) {
        async.eachSeries(files, function(f, nxt) {
            var fullPath = targetPath+path.sep+f;
            fs.stat(fullPath, function(err, stat) {
                if(!filter(stat)) return nxt();
                filtered.push(fullPath);
                nxt();
            });
        }, function() { cb(filtered); });
    });
}

Streamer.prototype.seriesAfter = function(topic, timestamp, cb)  {
    var targetPath = this.fullpathForTopic(topic);
    var result = [];
    this.filteredFileList(
        targetPath,
        function(stat) { return stat.birthtimeMs > timestamp; },
        function(filtered) {
            filtered.sort();
            async.eachSeries(filtered, function(fullPath, nxt) {
                fs.readFile(fullPath, function(err, data) {
                    result.push(data.toString());
                    nxt();
                });
            }, function() {
                cb(null, result);
            });
        }
    );
};

module.exports = Streamer;
