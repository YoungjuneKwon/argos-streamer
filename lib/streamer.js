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

    this.reloadModel();
    this.startWatchModel();
}

Streamer.prototype.validatePath = function(path) {
    if(!fs.existsSync(path)) mkdirp.sync(path);
}

Streamer.prototype.validateJSONFile = function(path) {
    if(!fs.existsSync(path)) fs.writeFileSync(path,"{}");
}

Streamer.prototype.reloadModel = function() {
    this.context.model = JSON.parse(fs.readFileSync(this.context.path.model));
    if(_.isUndefined(this.context.model.sources)) {
        this.context.model.sources = [];
        this.saveModel();
    }
}

Streamer.prototype.saveModel = function() {
    fs.writeFileSync(this.context.path.model, JSON.stringify(this.context.model));
}

Streamer.prototype.startWatchModel = function() {
    var that = this;
    fs.watchFile(this.context.path.model, function() {
        that.reloadModel();
        if(that.mqttConnected) that.refreshSubscriptions();
    });
}

Streamer.prototype.refreshSubscriptions = function() {
    console.log("refreshSubscriptions");
    var that = this;
    async.series([
        function(nxt) {
            if(_.isUndefined(that.context.subscriptions)) {
                that.context.subscriptions = [];
                return nxt();
            }
            async.eachSeries(that.context.subscriptions, function(s, n2) {
                that.mqttClient.unsubscribe(s.topic, function() { n2(); });
            }, function() { that.context.subscriptions = []; nxt(); });
        },
        function(nxt) {
            _.each(_.filter(that.context.model.sources, function(i) { return i.type=="mqtt"}), function(m) {
                if(!_.isUndefined(_.findWhere(that.context.subscriptions, {"topic":m.params.topic}))) return;
                console.log("subscribe "+m.params.topic);
                that.mqttClient.subscribe(m.params.topic);
                that.context.subscriptions.push({topic:m.params.topic, source:m});
            });
        }
    ]);
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
    var that = this;
    var context = {removed:0};
    async.series([
        function(nxt) {
            if(_.isUndefined(limit.time)) return nxt();
            var now = new Date().getTime();
            that.filteredFileList(
                targetPath,
                function(stat) { return stat.birthtimeMs + limit.time < now;},
                function(filtered) {
                    async.eachSeries(filtered, function(fullPath, nxt) {
                        fs.unlink(fullPath, function() { nxt(); });
                    }, function() {
                        context.removed += filtered.length;
                        nxt();
                    });
                }
            );
        },
        function(nxt) {
            if(_.isUndefined(limit.size)) return nxt();
            var size = 0;
            var accumulated = 0;
            if(limit.size.indexOf("mb") > 0)
                size = parseInt(limit.size.substring(0,limit.size.length-2)) * 1024 * 1024;
            else if(limit.size.indexOf("kb") > 0)
                size = parseInt(limit.size.substring(0,limit.size.length-2)) * 1024;
            else size = parseInt(limit.size);
            that.filteredFileList(
                targetPath,
                function(stat) {
                    accumulated += stat.size;
                    return size < accumulated;
                },
                function(filtered) {
                    async.eachSeries(filtered, function(fullPath, nxt) {
                        fs.unlink(fullPath, function() { nxt(); });
                    }, function() {
                        context.removed += filtered.length;
                        nxt();
                    });
                }
            );
        },
        function(nxt) {
            if(_.isUndefined(limit.count)) return nxt();
            var count = 0;
            that.filteredFileList(
                targetPath,
                function(stat) {
                    count++;
                    return limit.count < count;
                },
                function(filtered) {
                    async.eachSeries(filtered, function(fullPath, nxt) {
                        fs.unlink(fullPath, function() { nxt(); });
                    }, function() {
                        context.removed += filtered.length;
                        nxt();
                    });
                }
            );
        }
    ], function() {
        cb(context.removed);
    });
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

Streamer.prototype.setupHTTP = function(prefix, app, handler) {
    var that = this;
    this.initWS();

    app.get(prefix+"/series-after", function(req, res) {
        var topic=req.query.topic;
        var timestamp=req.query.timestamp;
        that.seriesAfter(topic, timestamp, function(err, r) {
            handler.sendResult(req, res, r);
        });
    });

    app.get(prefix+"/info-for-topic", function(req, res) {
        var topic = req.query.topic;
        that.infoForTopic(topic, function(err, result) {
            handler.sendResult(req, res, result);
        });
    });

    app.get(prefix+"/info-for-all-sources/:size/:page", function(req, res) {
        var size = parseInt(req.params.size);
        var page = parseInt(req.params.page);
        that.infoForAllSources(size, page, function(err, result) {
            handler.sendResult(req, res, result);
        });
    });

    app.get(prefix+"/info-for-all-sources", function(req, res) {
        that.infoForAllSources(0, 0, function(err, result) {
            handler.sendResult(req, res, result);
        });
    });

    app.post(prefix+"/upsert-by-topic", function(req, res) {
        var topic = req.body.topic;
        var limit = req.body.limit;
        that.upsertMQTTTopic(topic, limit, function(err, r) {
            handler.sendResult(req, res, {result:"0000", source:r});
        });
    });

    app.delete(prefix+"/delete-by-topic", function(req, res) {
        var topic = req.body.topic;
        that.deleteMQTTTopic(topic, function(err, r) {
            handler.sendResult(req, res, {result:"0000", source:r});
        });
    });
}

Streamer.prototype.initWS = function() {
    if(_.isUndefined(this.config["websocket-port"])) return;
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

Streamer.prototype.infoForAllSources = function(size, page, cb) {
    var sources = _.clone(this.context.model.sources);
    
    if(size > 0) sources = sources.splice(size * page, size);
    var result = [];
    var that = this;
    async.eachSeries(sources, function(source, nxt) {
        that.infoForSource(source, function(err, info) {
            result.push(info);
            nxt();
        });
    }, function() {
        cb(null, result);
    });
    
}

Streamer.prototype.infoForSource = function(source, cb) {
    var result = {source:source};
    var that = this;
    async.series([
        function(nxt) {
            that.sizeForSource(source, function(err, size) {
                result.size = size;
                nxt();
            });
        },
        function(nxt) {
            fs.readdir(that.fullpathForSource(source), function(err, files) {
                if(_.isUndefined(files)) {
                    result.last = -1;
                    return nxt();
                }
                files.sort();
                result.last = parseFloat(files.pop().split("-")[0]);
                nxt();
            });
        }
    ], function() {
        cb(null, result);
    });
}

Streamer.prototype.infoForTopic = function(topic, cb) {
    var source = this.sourceForTopic(topic);
    if(_.isUndefined(source)) return cb("invalid topic");
    this.infoForSource(source, cb);
}

Streamer.prototype.fullpathForSource = function(source) {
    if(source.type == "mqtt") return this.fullpathForTopic(source.params.topic);
    return "";
}

Streamer.prototype.fullpathForTopic = function(topic) {
    return this.context.path.files + path.sep + topic.substring(1);
}

Streamer.prototype.upsertMQTTTopic = function(topic, limit, cb) {
    var source = this.sourceForTopic(topic);
    if(_.isUndefined(source)) this.insertMQTTSource(topic, limit, cb);
    else this.updateSource(source, limit, cb);
}

Streamer.prototype.insertMQTTSource = function(topic, limit, cb) {
    var source = {type:"mqtt", limit:limit, params:{topic:topic}};
    this.context.model.sources.push(source);
    this.saveModel();
    cb(null, source);
}

Streamer.prototype.deleteMQTTTopic = function(topic, cb) {
    var source = this.sourceForTopic(topic);
    this.context.model.sources.splice(this.context.model.sources.indexOf(source), 1);
    this.saveModel();
    cb(null, source);
}

Streamer.prototype.updateSource = function(source, limit, cb) {
    source.limit = limit;
    this.saveModel();
    cb(null, source);
}

Streamer.prototype.filteredFileList = function(targetPath, filter, cb) {
    var filtered = [];
    fs.readdir(targetPath, function(err, files) {
        if(!_.isUndefined(files)) files.sort();
        async.eachSeries(files, function(f, nxt) {
            var fullPath = targetPath+path.sep+f;
            fs.stat(fullPath, function(err, stat) {
                if(err != null || _.isUndefined(stat)) return nxt();
                if(!filter(stat)) return nxt();
                filtered.push(fullPath);
                nxt();
            });
        }, function() { cb(filtered); });
    });
}

Streamer.prototype.seriesAfter = function(topic, timestamp, cb)  {
    var targetPath = this.fullpathForTopic(topic);
    timestamp = parseFloat(timestamp);
    var result = [];
    this.filteredFileList(
        targetPath,
        function(stat) { return stat.birthtimeMs > timestamp; },
        function(filtered) {
            async.eachSeries(filtered, function(fullPath, nxt) {
                fs.readFile(fullPath, function(err, data) {
                    if(err != null || _.isUndefined(data)) return nxt();
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
