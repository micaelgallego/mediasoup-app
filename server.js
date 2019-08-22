System.register(["./config", "express", "fs", "https", "kurento-client", "mediasoup", "socket.io", "child_process"], function (exports_1, context_1) {
    "use strict";
    var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
        return new (P || (P = Promise))(function (resolve, reject) {
            function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
            function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
            function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
            step((generator = generator.apply(thisArg, _arguments || [])).next());
        });
    };
    var __generator = (this && this.__generator) || function (thisArg, body) {
        var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
        return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
        function verb(n) { return function (v) { return step([n, v]); }; }
        function step(op) {
            if (f) throw new TypeError("Generator is already executing.");
            while (_) try {
                if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
                if (y = 0, t) op = [op[0] & 2, t.value];
                switch (op[0]) {
                    case 0: case 1: t = op; break;
                    case 4: _.label++; return { value: op[1], done: false };
                    case 5: _.label++; y = op[1]; op = [0]; continue;
                    case 7: op = _.ops.pop(); _.trys.pop(); continue;
                    default:
                        if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                        if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                        if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                        if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                        if (t[2]) _.ops.pop();
                        _.trys.pop(); continue;
                }
                op = body.call(thisArg, _);
            } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
            if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
        }
    };
    var _this, config_1, colors, express_1, fs_1, https_1, kurento_client_1, mediasoup_1, socket_io_1, child_process_1, expressApp, httpsServer, msWorker, _msRouter, _msTransport, _msProducer, _msConsumer, sessions, finalUsers, recordings, server;
    _this = this;
    var __moduleName = context_1 && context_1.id;
    function createWorker() {
        return new Promise(function (resolve, reject) {
            mediasoup_1["default"].createWorker({
                logLevel: config_1["default"].mediasoup.worker.logLevel,
                logTags: config_1["default"].mediasoup.worker.logTags,
                rtcMinPort: config_1["default"].mediasoup.worker.rtcMinPort,
                rtcMaxPort: config_1["default"].mediasoup.worker.rtcMaxPort
            })
                .then(function (worker) {
                resolve(worker);
            })["catch"](function (error) {
                reject(error);
            });
        });
    }
    function createRouter(worker) {
        return new Promise(function (resolve, reject) {
            if (!worker) {
                reject(new Error("mediasoup Worker is not initialized"));
            }
            var mediaCodecs = config_1["default"].mediasoup.router.mediaCodecs;
            worker
                .createRouter({
                mediaCodecs: mediaCodecs
            })
                .then(function (router) {
                resolve(router);
            })["catch"](function (error) {
                reject(error);
            });
        });
    }
    function createWebRtcTransport(router) {
        return new Promise(function (resolve, reject) {
            var webrtcConfig = config_1["default"].mediasoup.webRtcTransport;
            webrtcConfig.listenIps = [
                {
                    ip: config_1["default"].internalIp,
                    announcedIp: null
                }
            ];
            router
                .createWebRtcTransport(webrtcConfig)
                .then(function (response) {
                resolve(response);
            })["catch"](function (error) {
                reject(error);
            });
        });
    }
    function removeFinalUser(userId) {
        finalUsers["delete"](userId);
        sessions.forEach(function (session) {
            var index = session.users.indexOf(userId);
            if (index !== -1) {
                session.users.splice(index, 1);
            }
        });
    }
    return {
        setters: [
            function (config_1_1) {
                config_1 = config_1_1;
            },
            function (express_1_1) {
                express_1 = express_1_1;
            },
            function (fs_1_1) {
                fs_1 = fs_1_1;
            },
            function (https_1_1) {
                https_1 = https_1_1;
            },
            function (kurento_client_1_1) {
                kurento_client_1 = kurento_client_1_1;
            },
            function (mediasoup_1_1) {
                mediasoup_1 = mediasoup_1_1;
            },
            function (socket_io_1_1) {
                socket_io_1 = socket_io_1_1;
            },
            function (child_process_1_1) {
                child_process_1 = child_process_1_1;
            }
        ],
        execute: function () {
            colors = require('colors/safe');
            expressApp = express_1["default"]();
            expressApp.use(express_1["default"].json());
            expressApp.use(express_1["default"].static(__dirname));
            expressApp.use(function (error, req, res, next) {
                if (error) {
                    console.warn("Express app error,", error.message);
                    error.status = error.status || (error.name === "TypeError" ? 400 : 500);
                    res.statusMessage = error.message;
                    res.status(error.status).send(String(error));
                }
                else {
                    next();
                }
            });
            httpsServer = https_1["default"].createServer({
                cert: fs_1["default"].readFileSync("./cert/cert.pem"),
                key: fs_1["default"].readFileSync("./cert/key.pem")
            }, expressApp);
            httpsServer.on("error", function (err) {
                console.error("HTTPS error:", err.message);
            });
            httpsServer.on("tlsClientError", function (err) {
                console.error("TLS error:", err.message);
            });
            httpsServer.listen(config_1["default"].port, config_1["default"].ip, function () {
                console.log("Server is running and listening on https://%s:%s", config_1["default"].ip, config_1["default"].port);
            });
            sessions = new Map();
            finalUsers = new Map();
            recordings = new Map();
            server = socket_io_1["default"](httpsServer, {
                path: config_1["default"].path,
                serveClient: false,
                pingTimeout: config_1["default"].ws.pingTimeout,
                pingInterval: config_1["default"].ws.pingInterval,
                transports: ["websocket"]
            });
            server.on("connect", function (socket) {
                console.log("Client connected from %s:%s", socket.request.connection.remoteAddress, socket.request.connection.remotePort);
                finalUsers.set(socket.id, {
                    ws: socket,
                    producerTransports: new Map(),
                    consumerTransports: new Map(),
                    producers: new Map(),
                    consumers: new Map()
                });
                console.log(finalUsers.keys());
                socket.on("disconnect", function (reason) {
                    console.log("WebSocket %s is closed. Reason: %s", socket.id, reason);
                    removeFinalUser(socket.id);
                });
                socket.on("error", function (error) {
                    console.error("Error on websocket " + socket.id, error);
                });
                socket.on("joinRoom", function (data, callback) { return __awaiter(_this, void 0, void 0, function () {
                    var sessionId, session, session_1;
                    return __generator(this, function (_a) {
                        sessionId = data.sessionId;
                        session = sessions.get(sessionId);
                        if (!session) {
                            createRouter(msWorker)
                                .then(function (router) {
                                console.log("mediasoup Router initialized for session %s", sessionId);
                                sessions.set(sessionId, {
                                    router: router,
                                    users: [socket.id]
                                });
                                console.log(router.rtpCapabilities);
                                callback({
                                    rtpCapabilities: router.rtpCapabilities
                                });
                            })["catch"](function (error) {
                                console.error("Error initializing mediasoup router for session " + sessionId, error);
                            });
                        }
                        else {
                            session_1 = sessions.get(sessionId);
                            session_1.users.push(socket.id);
                            callback({
                                rtpCapabilities: session_1.router.rtpCapabilities
                            });
                        }
                        return [2];
                    });
                }); });
                socket.on("createProducerTransport", function (data, callback) { return __awaiter(_this, void 0, void 0, function () {
                    var sessionId, sessionRouter;
                    return __generator(this, function (_a) {
                        sessionId = data.sessionId;
                        sessionRouter = sessions.get(sessionId).router;
                        createWebRtcTransport(sessionRouter)
                            .then(function (transport) {
                            console.log("mediasoup WebRtcTransport initialized for user %s in session %s (producer)", socket.id, sessionId);
                            finalUsers
                                .get(socket.id)
                                .producerTransports.set(transport.id, transport);
                            callback({
                                transportOptions: {
                                    id: transport.id,
                                    iceParameters: transport.iceParameters,
                                    iceCandidates: transport.iceCandidates,
                                    dtlsParameters: transport.dtlsParameters,
                                    sctpParameters: transport.sctpParameters
                                },
                                rtpCapabilities: sessionRouter.rtpCapabilities
                            });
                        })["catch"](function (error) {
                            console.error("Error initializing mediasoup WebRtcTransport for user " +
                                socket.id +
                                " in session " +
                                sessionId +
                                "(producer)", error);
                        });
                        return [2];
                    });
                }); });
                socket.on("connectProducerTransport", function (data, callback) { return __awaiter(_this, void 0, void 0, function () {
                    var producerTransport;
                    return __generator(this, function (_a) {
                        producerTransport = finalUsers
                            .get(socket.id)
                            .producerTransports.get(data.transportId);
                        producerTransport.connect({
                            dtlsParameters: data.dtlsParameters
                        });
                        callback();
                        return [2];
                    });
                }); });
                socket.on("produce", function (data, callback) { return __awaiter(_this, void 0, void 0, function () {
                    var producerTransport;
                    return __generator(this, function (_a) {
                        producerTransport = finalUsers
                            .get(socket.id)
                            .producerTransports.get(data.transportId);
                        producerTransport
                            .produce({
                            kind: data.kind,
                            rtpParameters: data.rtpParameters
                        })
                            .then(function (producer) {
                            console.log("mediasoup Producer initialized for user %s. {kind: %s, type: %s}", socket.id, producer.kind, producer.type);
                            finalUsers.get(socket.id).producers.set(producer.id, producer);
                            producer.on("transportclose", function () { });
                            producer.on("score", function (_score) { });
                            producer.on("videoorientationchange", function (_videoOrientation) { });
                            callback({
                                id: producer.id
                            });
                            socket.broadcast.emit("newProducer");
                        })["catch"](function (error) {
                            console.error("Error producing", error);
                        });
                        return [2];
                    });
                }); });
                socket.on("pauseProducer", function (data, callback) { return __awaiter(_this, void 0, void 0, function () {
                    var producer;
                    return __generator(this, function (_a) {
                        producer = finalUsers.get(socket.id).producers.get(data.producerId);
                        if (!producer.paused) {
                            producer.pause();
                        }
                        callback();
                        return [2];
                    });
                }); });
                socket.on("resumeProducer", function (data, callback) { return __awaiter(_this, void 0, void 0, function () {
                    var producer;
                    return __generator(this, function (_a) {
                        producer = finalUsers.get(socket.id).producers.get(data.producerId);
                        if (!producer.paused) {
                            return [2];
                        }
                        producer.resume();
                        callback();
                        return [2];
                    });
                }); });
                socket.on("closeProducer", function (data, callback) { return __awaiter(_this, void 0, void 0, function () {
                    var producer;
                    return __generator(this, function (_a) {
                        producer = finalUsers.get(socket.id).producers.get(data.producerId);
                        if (producer.closed) {
                            return [2];
                        }
                        producer.close();
                        callback();
                        return [2];
                    });
                }); });
                socket.on("createConsumerTransport", function (data, callback) { return __awaiter(_this, void 0, void 0, function () {
                    var sessionId, sessionRouter;
                    return __generator(this, function (_a) {
                        sessionId = data.sessionId;
                        sessionRouter = sessions.get(sessionId).router;
                        createWebRtcTransport(sessionRouter)
                            .then(function (transport) {
                            console.log("mediasoup WebRtcTransport initialized for user %s in session %s (consumer)", socket.id, sessionId);
                            finalUsers
                                .get(socket.id)
                                .consumerTransports.set(transport.id, transport);
                            callback({
                                transportOptions: {
                                    id: transport.id,
                                    iceParameters: transport.iceParameters,
                                    iceCandidates: transport.iceCandidates,
                                    dtlsParameters: transport.dtlsParameters,
                                    sctpParameters: transport.sctpParameters
                                },
                                rtpCapabilities: sessionRouter.rtpCapabilities
                            });
                        })["catch"](function (error) {
                            console.error("Error initializing mediasoup WebRtcTransport for user " +
                                socket.id +
                                " in session " +
                                sessionId +
                                "(consumer)", error);
                        });
                        return [2];
                    });
                }); });
                socket.on("connectConsumerTransport", function (data, callback) { return __awaiter(_this, void 0, void 0, function () {
                    var consumerTransport;
                    return __generator(this, function (_a) {
                        consumerTransport = finalUsers
                            .get(socket.id)
                            .consumerTransports.get(data.transportId);
                        consumerTransport.connect({
                            dtlsParameters: data.dtlsParameters
                        });
                        callback();
                        return [2];
                    });
                }); });
                socket.on("consume", function (data, callback) { return __awaiter(_this, void 0, void 0, function () {
                    var sessionRouter, videoProducerId, audioProducerId, consumerTransport;
                    return __generator(this, function (_a) {
                        sessionRouter = sessions.get(data.sessionId).router;
                        videoProducerId = data.videoProducerId;
                        audioProducerId = data.audioProducerId;
                        if (!sessionRouter.canConsume({
                            producerId: data.videoProducerId,
                            rtpCapabilities: data.rtpCapabilities
                        })) {
                            console.error("Can NOT consume video");
                            return [2];
                        }
                        if (!sessionRouter.canConsume({
                            producerId: data.audioProducerId,
                            rtpCapabilities: data.rtpCapabilities
                        })) {
                            console.error("Can NOT consume audio");
                            return [2];
                        }
                        consumerTransport = finalUsers
                            .get(socket.id)
                            .consumerTransports.get(data.transportId);
                        consumerTransport
                            .consume({
                            producerId: videoProducerId,
                            rtpCapabilities: data.rtpCapabilities,
                            paused: false
                        })
                            .then(function (videoConsumer) {
                            console.log("mediasoup VIDEO Consumer initialized for user %s for Producer %s. {kind: %s, type: %s}", socket.id, videoConsumer.producerId, videoConsumer.kind, videoConsumer.type);
                            finalUsers
                                .get(socket.id)
                                .consumers.set(videoConsumer.id, videoConsumer);
                            consumerTransport
                                .consume({
                                producerId: audioProducerId,
                                rtpCapabilities: data.rtpCapabilities,
                                paused: false
                            })
                                .then(function (audioConsumer) {
                                console.log("mediasoup AUDIO Consumer initialized for user %s for Producer %s. {kind: %s, type: %s}", socket.id, audioConsumer.producerId, audioConsumer.kind, audioConsumer.type);
                                finalUsers
                                    .get(socket.id)
                                    .consumers.set(audioConsumer.id, audioConsumer);
                                var response = {
                                    video: {
                                        id: videoConsumer.id,
                                        producerId: videoConsumer.producerId,
                                        kind: videoConsumer.kind,
                                        rtpParameters: videoConsumer.rtpParameters,
                                        type: videoConsumer.type,
                                        paused: videoConsumer.producerPaused
                                    },
                                    audio: {
                                        id: audioConsumer.id,
                                        producerId: audioConsumer.producerId,
                                        kind: audioConsumer.kind,
                                        rtpParameters: audioConsumer.rtpParameters,
                                        type: audioConsumer.type,
                                        paused: audioConsumer.producerPaused
                                    }
                                };
                                if (videoConsumer.type === "simulcast" ||
                                    videoConsumer.type === "svc") {
                                    videoConsumer
                                        .setPreferredLayers({
                                        spatialLayer: 2,
                                        temporalLayer: 2
                                    })
                                        .then(function () {
                                        callback(response);
                                    });
                                }
                                else {
                                    callback(response);
                                }
                            })["catch"](function (error) {
                                console.error("Error consuming audio", error);
                            });
                        })["catch"](function (error) {
                            console.error("Error consuming video", error);
                        });
                        return [2];
                    });
                }); });
                socket.on("pauseConsumer", function (data, callback, _err) { return __awaiter(_this, void 0, void 0, function () {
                    var consumer;
                    return __generator(this, function (_a) {
                        consumer = finalUsers.get(socket.id).consumers.get(data.consumerId);
                        if (!consumer.paused) {
                            consumer.pause();
                        }
                        callback();
                        return [2];
                    });
                }); });
                socket.on("resumeConsumer", function (data, callback) { return __awaiter(_this, void 0, void 0, function () {
                    var consumer;
                    return __generator(this, function (_a) {
                        consumer = finalUsers.get(socket.id).consumers.get(data.consumerId);
                        if (consumer.paused) {
                            consumer.resume();
                        }
                        callback();
                        return [2];
                    });
                }); });
                socket.on("closeConsumer", function (data, callback) { return __awaiter(_this, void 0, void 0, function () {
                    var consumer;
                    return __generator(this, function (_a) {
                        consumer = finalUsers.get(socket.id).consumers.get(data.consumerId);
                        if (!consumer.closed) {
                            consumer.close();
                        }
                        callback();
                        return [2];
                    });
                }); });
                socket.on("publisherStats", function (data, callback) { return __awaiter(_this, void 0, void 0, function () {
                    var producer;
                    return __generator(this, function (_a) {
                        producer = finalUsers.get(socket.id).producers.get(data.producerId);
                        producer.getStats().then(function (stats) {
                            callback(stats);
                        });
                        return [2];
                    });
                }); });
                socket.on("subscriberStats", function (data, callback) { return __awaiter(_this, void 0, void 0, function () {
                    var consumer;
                    return __generator(this, function (_a) {
                        consumer = finalUsers.get(socket.id).consumers.get(data.consumerId);
                        consumer.getStats().then(function (stats) {
                            callback(stats);
                        });
                        return [2];
                    });
                }); });
                var stopRecordingCallbackFunction;
                var stopRecordingCallback = function () {
                    if (stopRecordingCallbackFunction) {
                        stopRecordingCallbackFunction();
                    }
                };
                socket.on("record", function (data, callback) { return __awaiter(_this, void 0, void 0, function () {
                    var sessionId, hasAudio, hasVideo, audioProducerId, videoProducerId, recording, sessionRouter, cmdFileIn, cmdFileOut, cmdFormat, cmdCodec, ffmpegStarted, recordingStarted, cmdProgram, cmdArgStr, ffmpegProcess;
                    return __generator(this, function (_a) {
                        sessionId = data.sessionId;
                        if (!recordings.get(sessionId)) {
                            recordings.set(sessionId, {
                                ffmpegProcess: undefined,
                                transports: [],
                                consumers: []
                            });
                        }
                        hasAudio = data.hasAudio;
                        hasVideo = data.hasVideo;
                        audioProducerId = data.audioProducerId;
                        videoProducerId = data.videoProducerId;
                        recording = recordings.get(sessionId);
                        sessionRouter = sessions.get(sessionId).router;
                        if (hasAudio) {
                            sessionRouter
                                .createPlainRtpTransport(config_1["default"].mediasoup.plainRtpTransport)
                                .then(function (plainRtpTransport) {
                                recording.transports.push(plainRtpTransport);
                                plainRtpTransport
                                    .connect({
                                    ip: config_1["default"].mediasoup.plainRtpTransport.listenIp.ip,
                                    port: config_1["default"].mediasoup.plainRtpTransport.appData.recvPort.audioPort
                                })
                                    .then(function () {
                                    console.log("AUDIO PlainRtpTransport connected");
                                    plainRtpTransport
                                        .consume({
                                        producerId: audioProducerId,
                                        rtpCapabilities: sessionRouter.rtpCapabilities,
                                        paused: true
                                    })
                                        .then(function (consumer) {
                                        recording.consumers.push(consumer);
                                        console.log("PlainRtpTransport consuming AUDIO");
                                    })["catch"](function (error) {
                                        console.error(error);
                                    });
                                })["catch"](function (error) {
                                    console.error(error);
                                });
                            })["catch"](function (error) {
                                console.error(error);
                            });
                        }
                        if (hasVideo) {
                            sessionRouter
                                .createPlainRtpTransport(config_1["default"].mediasoup.plainRtpTransport)
                                .then(function (plainRtpTransport) {
                                recording.transports.push(plainRtpTransport);
                                plainRtpTransport
                                    .connect({
                                    ip: config_1["default"].mediasoup.plainRtpTransport.listenIp.ip,
                                    port: config_1["default"].mediasoup.plainRtpTransport.appData.recvPort.videoPort
                                })
                                    .then(function () {
                                    console.log("VIDEO PlainRtpTransport connected");
                                    plainRtpTransport
                                        .consume({
                                        producerId: videoProducerId,
                                        rtpCapabilities: sessionRouter.rtpCapabilities,
                                        paused: true
                                    })
                                        .then(function (consumer) {
                                        recording.consumers.push(consumer);
                                        console.log("PlainRtpTransport consuming VIDEO");
                                    })["catch"](function (error) {
                                        console.error(error);
                                    });
                                })["catch"](function (error) {
                                    console.error(error);
                                });
                            })["catch"](function (error) {
                                console.error(error);
                            });
                        }
                        cmdFileIn = __dirname + "/recording/input.sdp";
                        cmdFileOut = "";
                        cmdFormat = "";
                        cmdCodec = "-an -vn";
                        if (hasAudio && hasVideo) {
                            cmdFileOut = __dirname + "/recording/recording.webm";
                            cmdFormat = "-f webm -flags +global_header";
                            cmdCodec = "-map 0:a:0 -map 0:v:0 -acodec copy -vcodec copy";
                        }
                        else if (hasAudio) {
                            cmdFileOut = __dirname + "/recording/recording.webm";
                            cmdFormat = "-f webm -flags +global_header";
                            cmdCodec = "-acodec copy -vn";
                        }
                        else if (hasVideo) {
                            cmdFileOut = __dirname + "/recording/recording.webm";
                            cmdFormat = "-f webm -flags +global_header";
                            cmdCodec = "-vcodec copy -an";
                        }
                        ffmpegStarted = false;
                        recordingStarted = false;
                        cmdProgram = "ffmpeg";
                        cmdArgStr = [
                            "-protocol_whitelist file,rtp,udp",
                            "-nostdin",
                            "-fflags +genpts",
                            "-i",
                            cmdFileIn,
                            cmdCodec,
                            cmdFormat,
                            "-y",
                            cmdFileOut
                        ].join(" ");
                        console.log("Run command: " + cmdProgram + " " + cmdArgStr);
                        ffmpegProcess = child_process_1["default"].spawn(cmdProgram, cmdArgStr.split(" "), {
                            detached: true
                        });
                        recording.ffmpegProcess = ffmpegProcess;
                        ffmpegProcess.on("exit", function (code, signal) {
                            console.log("Recording process exited with " + ("code " + code + " and signal " + signal));
                            if (!signal || signal === "SIGINT") {
                                console.log("Recording successfully stopped");
                            }
                            else {
                                console.error("Error stopping recording");
                            }
                            if (stopRecordingCallback) {
                                stopRecordingCallback();
                            }
                        });
                        ffmpegProcess.on("disconnect", function () {
                            console.log("Recording process disconnect");
                        });
                        ffmpegProcess.on("error", function (error) {
                            console.log("Recording process error", error);
                        });
                        ffmpegProcess.on("close", function () {
                            console.log("Recording process closed");
                        });
                        ffmpegProcess.on("message", function () {
                            console.log("Recording process message");
                        });
                        ffmpegProcess.stderr.on("data", function (data) {
                            console.log(data.toString());
                            if (data.toString().startsWith("ffmpeg version") && !ffmpegStarted) {
                                ffmpegStarted = true;
                                setTimeout(function () {
                                    recording.consumers.forEach(function (consumer) {
                                        consumer.resume();
                                    });
                                }, 1000);
                            }
                            else if (data.toString().startsWith("frame=") && !recordingStarted) {
                                recordingStarted = true;
                                callback();
                            }
                        });
                        return [2];
                    });
                }); });
                socket.on("stopRecord", function (data, callback) { return __awaiter(_this, void 0, void 0, function () {
                    var recording;
                    return __generator(this, function (_a) {
                        recording = recordings.get(data.sessionId);
                        stopRecordingCallbackFunction = callback;
                        recording.ffmpegProcess.kill("SIGINT");
                        setTimeout(function () {
                            recording.consumers.forEach(function (consumer) {
                                consumer.close();
                            });
                            recording.transports.forEach(function (transport) {
                                transport.close();
                            });
                        }, 3000);
                        return [2];
                    });
                }); });
                var webRtcEp = null;
                var webRtcEpCandidates = [];
                socket.on("connectKurento", function (data, callback) { return __awaiter(_this, void 0, void 0, function () {
                    var sdpOffer, kurentoUri, kurentoVideoPort;
                    var _this = this;
                    return __generator(this, function (_a) {
                        sdpOffer = data.sdpOffer;
                        kurentoUri = "ws://localhost:8888/kurento";
                        kurentoVideoPort = 0;
                        kurento_client_1["default"](kurentoUri, function (err, kurentoClient) { return __awaiter(_this, void 0, void 0, function () {
                            var _this = this;
                            return __generator(this, function (_a) {
                                if (err) {
                                    console.log("Cannot connect to Kurento Media Server at " + kurentoUri);
                                    return [2, callback("Cannot connect to Kurento Media Server at " +
                                            kurentoUri +
                                            ", error: " +
                                            err)];
                                }
                                kurentoClient.create("MediaPipeline", function (err, pipeline) { return __awaiter(_this, void 0, void 0, function () {
                                    var _this = this;
                                    return __generator(this, function (_a) {
                                        pipeline.create("WebRtcEndpoint", function (err, _webRtcEp) { return __awaiter(_this, void 0, void 0, function () {
                                            var candidate, sessionId, sessionRouter, videoProducerId, rtpTransport, videoPort, videoConsumer, videoSsrc, videoCname;
                                            var _this = this;
                                            return __generator(this, function (_a) {
                                                switch (_a.label) {
                                                    case 0:
                                                        webRtcEp = _webRtcEp;
                                                        webRtcEp.on("OnIceCandidate", function (event) {
                                                            var candidate = kurento_client_1["default"].getComplexType("IceCandidate")(event.candidate);
                                                            socket.emit("kurentoIceCandidate", candidate);
                                                        });
                                                        while (webRtcEpCandidates.length) {
                                                            candidate = webRtcEpCandidates.shift();
                                                            webRtcEp.addIceCandidate(candidate);
                                                        }
                                                        webRtcEp.processOffer(sdpOffer, function (err, sdpAnswer) {
                                                            socket.emit("kurentoAnswer", sdpAnswer);
                                                        });
                                                        webRtcEp.gatherCandidates(function (_err) { });
                                                        sessionId = data.sessionId;
                                                        sessionRouter = sessions.get(sessionId).router;
                                                        videoProducerId = data.videoProducerId;
                                                        return [4, sessionRouter.createPlainRtpTransport(config_1["default"].mediasoup.plainRtpTransport)];
                                                    case 1:
                                                        rtpTransport = _a.sent();
                                                        videoPort = rtpTransport.tuple.localPort;
                                                        return [4, rtpTransport.consume({
                                                                producerId: videoProducerId,
                                                                rtpCapabilities: sessionRouter.rtpCapabilities,
                                                                paused: true
                                                            })];
                                                    case 2:
                                                        videoConsumer = _a.sent();
                                                        videoSsrc = videoConsumer.rtpParameters.encodings[0].ssrc;
                                                        videoCname = videoConsumer.rtpParameters.rtcp.cname;
                                                        pipeline.create("RtpEndpoint", function (err, rtpEp) { return __awaiter(_this, void 0, void 0, function () {
                                                            var rtpSdpOffer;
                                                            var _this = this;
                                                            return __generator(this, function (_a) {
                                                                rtpEp.connect(webRtcEp, "VIDEO", function (err) {
                                                                    if (err) {
                                                                        console.error("Kurento ERROR:", err);
                                                                    }
                                                                });
                                                                rtpSdpOffer = "v=0\r\n" +
                                                                    "o=- 0 0 IN IP4 127.0.0.1\r\n" +
                                                                    "s=-\r\n" +
                                                                    "c=IN IP4 127.0.0.1\r\n" +
                                                                    "t=0 0\r\n" +
                                                                    "m=video " + videoPort + " RTP/AVP 120\r\n" +
                                                                    "a=rtcp-mux\r\n" +
                                                                    "a=rtpmap:120 VP8/90000\r\n" +
                                                                    "a=sendonly\r\n" +
                                                                    "a=ssrc:" + videoSsrc + " cname:" + videoCname + "\r\n" +
                                                                    "";
                                                                console.log("RTP SDP Offer from app:\n" + rtpSdpOffer);
                                                                rtpEp.processOffer(rtpSdpOffer, function (err, rtpSdpAnswer) { return __awaiter(_this, void 0, void 0, function () {
                                                                    var vPortRegex, vPortMatch;
                                                                    var _this = this;
                                                                    return __generator(this, function (_a) {
                                                                        switch (_a.label) {
                                                                            case 0:
                                                                                console.log("RTP SDP Answer from Kurento:\n" + rtpSdpAnswer);
                                                                                vPortRegex = /m=video (\d+) RTP\/AVP 120/;
                                                                                vPortMatch = vPortRegex.exec(rtpSdpAnswer);
                                                                                if (vPortMatch) {
                                                                                    kurentoVideoPort = parseInt(vPortMatch[1], 10);
                                                                                    console.log("Kurento RTP video port: " + kurentoVideoPort);
                                                                                }
                                                                                else {
                                                                                    console.warn("SDP regex doesn't match");
                                                                                }
                                                                                return [4, rtpTransport.connect({
                                                                                        ip: config_1["default"].mediasoup.plainRtpTransport.listenIp.ip,
                                                                                        port: kurentoVideoPort
                                                                                    })];
                                                                            case 1:
                                                                                _a.sent();
                                                                                console.log("VIDEO PlainRtpTransport RTP connected to " +
                                                                                    config_1["default"].mediasoup.plainRtpTransport.listenIp.ip +
                                                                                    ":" +
                                                                                    kurentoVideoPort);
                                                                                console.log("VIDEO PlainRtpTransport RTCP connected to " +
                                                                                    config_1["default"].mediasoup.plainRtpTransport.listenIp.ip +
                                                                                    ":" +
                                                                                    (kurentoVideoPort + 1));
                                                                                setTimeout(function () { return __awaiter(_this, void 0, void 0, function () {
                                                                                    return __generator(this, function (_a) {
                                                                                        switch (_a.label) {
                                                                                            case 0: return [4, videoConsumer.resume()];
                                                                                            case 1:
                                                                                                _a.sent();
                                                                                                return [2];
                                                                                        }
                                                                                    });
                                                                                }); }, 1000);
                                                                                return [2];
                                                                        }
                                                                    });
                                                                }); });
                                                                return [2];
                                                            });
                                                        }); });
                                                        return [2];
                                                }
                                            });
                                        }); });
                                        return [2];
                                    });
                                }); });
                                return [2];
                            });
                        }); });
                        socket.on("appIceCandidate", function (data, _callback) { return __awaiter(_this, void 0, void 0, function () {
                            var candidate;
                            return __generator(this, function (_a) {
                                candidate = kurento_client_1["default"].getComplexType("IceCandidate")(data.candidate);
                                if (webRtcEp) {
                                    webRtcEp.addIceCandidate(candidate);
                                }
                                else {
                                    webRtcEpCandidates.push(candidate);
                                }
                                return [2];
                            });
                        }); });
                        return [2];
                    });
                }); });
            });
            createWorker()
                .then(function (worker) {
                msWorker = worker;
                msWorker.on("died", function () {
                    console.error("mediasoup Worker died, exiting in 2 seconds... [pid:%d]", worker.pid);
                    setTimeout(function () { return process.exit(1); }, 2000);
                });
                console.log("mediasoup worker initialized\n");
            })["catch"](function (_err) {
                console.error("Error initializing mediasoup worker");
            });
            mediasoup_1["default"].observer.on("newworker", function (worker) {
                console.log(colors.yellow("OBSERVER API: new worker created [worke.pid:%d]"), worker.pid);
                worker.observer.on("close", function () {
                    console.log(colors.yellow("OBSERVER API: worker closed [worker.pid:%d]"), worker.pid);
                });
                worker.observer.on("newrouter", function (router) {
                    console.log(colors.yellow("OBSERVER API: new router created [worker.pid:%d, router.id:%s]"), worker.pid, router.id);
                    router.observer.on("close", function () {
                        console.log(colors.yellow("OBSERVER API: router closed [router.id:%s]"), router.id);
                    });
                    router.observer.on("newtransport", function (transport) {
                        console.log(colors.yellow("OBSERVER API: new transport created [worker.pid:%d, router.id:%s, transport.id:%s]"), worker.pid, router.id, transport.id);
                        transport.observer.on("close", function () {
                            console.log(colors.yellow("OBSERVER API: transport closed [transport.id:%s]"), transport.id);
                        });
                        transport.observer.on("newproducer", function (producer) {
                            console.log(colors.yellow("OBSERVER API: new producer created [worker.pid:%d, router.id:%s, transport.id:%s, producer.id:%s]"), worker.pid, router.id, transport.id, producer.id);
                            producer.observer.on("close", function () {
                                console.log(colors.yellow("OBSERVER API: producer closed [producer.id:%s]"), producer.id);
                            });
                        });
                        transport.observer.on("newconsumer", function (consumer) {
                            console.log(colors.yellow("OBSERVER API: new consumer created [worker.pid:%d, router.id:%s, transport.id:%s, consumer.id:%s]"), worker.pid, router.id, transport.id, consumer.id);
                            consumer.observer.on("close", function () {
                                console.log(colors.yellow("OBSERVER API: consumer closed [consumer.id:%s]"), consumer.id);
                            });
                        });
                        transport.observer.on("newdataproducer", function (dataProducer) {
                            console.log(colors.yellow("OBSERVER API: new data producer created [worker.pid:%d, router.id:%s, transport.id:%s, dataProducer.id:%s]"), worker.pid, router.id, transport.id, dataProducer.id);
                            dataProducer.observer.on("close", function () {
                                console.log(colors.yellow("OBSERVER API: data producer closed [dataProducer.id:%s]"), dataProducer.id);
                            });
                        });
                        transport.observer.on("newdataconsumer", function (dataConsumer) {
                            console.log(colors.yellow("OBSERVER API: new data consumer created [worker.pid:%d, router.id:%s, transport.id:%s, dataConsumer.id:%s]"), worker.pid, router.id, transport.id, dataConsumer.id);
                            dataConsumer.observer.on("close", function () {
                                console.log(colors.yellow("OBSERVER API: data consumer closed [dataConsumer.id:%s]"), dataConsumer.id);
                            });
                        });
                    });
                });
            });
        }
    };
});
