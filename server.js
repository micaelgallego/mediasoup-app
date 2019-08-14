const SERVER_CONFIG = require("./config");
const Colors = require("colors/safe");
const Express = require("express");
const Fs = require("fs");
const Https = require("https");
const Kurento = require("kurento-client");
const MediaSoup = require("mediasoup");
const SocketServer = require("socket.io");
const Spawn = require("child_process").spawn;

expressApp = Express();
expressApp.use(Express.json());
expressApp.use(Express.static(__dirname));

expressApp.use((error, req, res, next) => {
  if (error) {
    console.warn("Express app error,", error.message);
    error.status = error.status || (error.name === "TypeError" ? 400 : 500);
    res.statusMessage = error.message;
    res.status(error.status).send(String(error));
  } else {
    next();
  }
});

httpsServer = Https.createServer(
  {
    cert: Fs.readFileSync("./cert/cert.pem"),
    key: Fs.readFileSync("./cert/key.pem")
  },
  expressApp
);

httpsServer.on("error", err => {
  console.error("HTTPS error:", err.message);
});

httpsServer.on("tlsClientError", err => {
  console.error("TLS error:", err.message);
});

httpsServer.listen(SERVER_CONFIG.port, SERVER_CONFIG.ip, () => {
  console.log(
    "Server is running and listening on https://%s:%s",
    SERVER_CONFIG.ip,
    SERVER_CONFIG.port
  );

  // Set internal ip in SDP file
  /*
    var fs = require('fs');
    const sdpPaths = ['./recording/input.sdp'];
    const REGEX = /c=IN IP4 \d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}/g;
    const REPLACEMENT = 'c=IN IP4 ' + SERVER_CONFIG.internalIp;

    sdpPaths.forEach(sdpPath => {
        fs.readFile(sdpPath, 'utf8', (err, data) => {
            if (err) {
                return console.log(err);
            }
            const result = data.replace(REGEX, REPLACEMENT);
            fs.writeFile(sdpPath, result, 'utf8', (err) => {
                if (err) {
                    console.error('Error overwriting recording SP file', err);
                    process.exit(1);
                }
            });
        });
    });
    */
});

// A worker represents a mediasoup C++ subprocess that runs in a single CPU core and handles Router instances
let msWorker;

// A router enables injection, selection and forwarding of media streams through Transport instances created on it.
// Developers may think of a mediasoup router as if it were a “multi-party conference room”, although mediasoup is
// much more low level than that and doesn't constrain itself to specific high level use cases (for instance, a
// “multi-party conference room” could involve various mediasoup routers, even in different physicals hosts)
let msRouter;

// A transport connects an endpoint with a mediasoup router and enables transmission of media in both directions by
// means of Producer and Consumer instances created on it. mediasoup implements the following transport classes:
// [WebRtcTransport, PlainRtpTransport, PipeTransport]
let msTransport;

// A producer represents an audio or video source being injected into a mediasoup router. It's created on top of
// a transport that defines how the media packets are carried
let msProducer;

// A consumer represents an audio or video source being forwarded from a mediasoup router to an endpoint. It's
// created on top of a transport that defines how the media packets are carried
let msConsumer;

// Collection of sessions
let sessions = new Map();
// Collection of users (websocket connections)
let finalUsers = new Map();
// Collection of recordings
let recordings = new Map();

const socketServer = SocketServer(httpsServer, {
  path: SERVER_CONFIG.path,
  serveClient: false,
  pingTimeout: SERVER_CONFIG.ws.pingTimeout,
  pingInterval: SERVER_CONFIG.ws.pingInterval,
  transports: ["websocket"]
  // log: false,
});

socketServer.on("connect", socket => {
  console.log(
    "Client connected from %s:%s",
    socket.request.connection.remoteAddress,
    socket.request.connection.remotePort
  );

  finalUsers.set(socket.id, {
    ws: socket,
    producerTransports: new Map(),
    consumerTransports: new Map(),
    producers: new Map(),
    consumers: new Map()
  });
  console.log(finalUsers.keys());

  socket.on("disconnect", reason => {
    console.log("WebSocket %s is closed. Reason: %s", socket.id, reason);
    removeFinalUser(socket.id);
  });

  socket.on("error", error => {
    console.error("Error on websocket " + socket.id, error);
  });

  socket.on("joinRoom", async (data, callback) => {
    const sessionId = data.sessionId;
    let session = sessions.get(sessionId);
    if (!session) {
      // New created session
      createRouter(msWorker)
        .then(router => {
          console.log("mediasoup Router initialized for session %s", sessionId);
          sessions.set(sessionId, {
            router: router,
            users: [socket.id]
          });

          console.log(router.rtpCapabilities);

          callback({
            rtpCapabilities: router.rtpCapabilities
          });
        })
        .catch(error => {
          console.error(
            "Error initializing mediasoup router for session " + sessionId,
            error
          );
        });
    } else {
      // Add user to existing session
      const session = sessions.get(sessionId);
      session.users.push(socket.id);
      callback({
        rtpCapabilities: session.router.rtpCapabilities
      });
    }
  });

  /**
   * Publish stuff
   */

  socket.on("createProducerTransport", async (data, callback) => {
    const sessionId = data.sessionId;
    const sessionRouter = sessions.get(sessionId).router;
    createWebRtcTransport(sessionRouter)
      .then(transport => {
        console.log(
          "mediasoup WebRtcTransport initialized for user %s in session %s (producer)",
          socket.id,
          sessionId
        );
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
      })
      .catch(error => {
        console.error(
          "Error initializing mediasoup WebRtcTransport for user " +
            socket.id +
            " in session " +
            sessionId +
            "(producer)",
          error
        );
      });
  });

  socket.on("connectProducerTransport", async (data, callback) => {
    let producerTransport = finalUsers
      .get(socket.id)
      .producerTransports.get(data.transportId);
    producerTransport.connect({
      dtlsParameters: data.dtlsParameters
    });
    callback();
  });

  socket.on("produce", async (data, callback) => {
    let producerTransport = finalUsers
      .get(socket.id)
      .producerTransports.get(data.transportId);
    producerTransport
      .produce({
        kind: data.kind,
        rtpParameters: data.rtpParameters
      })
      .then(producer => {
        console.log(
          "mediasoup Producer initialized for user %s. {kind: %s, type: %s}",
          socket.id,
          producer.kind,
          producer.type
        );
        finalUsers.get(socket.id).producers.set(producer.id, producer);

        producer.on("transportclose", () => {});
        producer.on("score", score => {});
        producer.on("videoorientationchange", videoOrientation => {});

        callback({
          id: producer.id
        });
        // Broadcast new producer
        socket.broadcast.emit("newProducer");
      })
      .catch(error => {
        console.error("Error producing", error);
      });
  });

  socket.on("pauseProducer", async (data, callback) => {
    const producer = finalUsers.get(socket.id).producers.get(data.producerId);
    if (!producer.paused) {
      producer.pause();
    }
    callback();
  });

  socket.on("resumeProducer", async (data, callback) => {
    const producer = finalUsers.get(socket.id).producers.get(data.producerId);
    if (!producer.paused) {
      return;
    }
    producer.resume();
    callback();
  });

  socket.on("closeProducer", async (data, callback) => {
    const producer = finalUsers.get(socket.id).producers.get(data.producerId);
    if (producer.closed) {
      return;
    }
    producer.close();
    callback();
  });

  /**
   * Subscribe stuff
   */

  socket.on("createConsumerTransport", async (data, callback) => {
    const sessionId = data.sessionId;
    let sessionRouter = sessions.get(sessionId).router;
    createWebRtcTransport(sessionRouter)
      .then(transport => {
        console.log(
          "mediasoup WebRtcTransport initialized for user %s in session %s (consumer)",
          socket.id,
          sessionId
        );
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
      })
      .catch(error => {
        console.error(
          "Error initializing mediasoup WebRtcTransport for user " +
            socket.id +
            " in session " +
            sessionId +
            "(consumer)",
          error
        );
      });
  });

  socket.on("connectConsumerTransport", async (data, callback) => {
    let consumerTransport = finalUsers
      .get(socket.id)
      .consumerTransports.get(data.transportId);
    consumerTransport.connect({
      dtlsParameters: data.dtlsParameters
    });
    callback();
  });

  socket.on("consume", async (data, callback) => {
    const sessionRouter = sessions.get(data.sessionId).router;
    const videoProducerId = data.videoProducerId;
    const audioProducerId = data.audioProducerId;
    if (
      !sessionRouter.canConsume({
        producerId: data.videoProducerId,
        rtpCapabilities: data.rtpCapabilities
      })
    ) {
      console.error("Can NOT consume video");
      return;
    }
    if (
      !sessionRouter.canConsume({
        producerId: data.audioProducerId,
        rtpCapabilities: data.rtpCapabilities
      })
    ) {
      console.error("Can NOT consume audio");
      return;
    }
    let consumerTransport = finalUsers
      .get(socket.id)
      .consumerTransports.get(data.transportId);
    consumerTransport
      .consume({
        producerId: videoProducerId,
        rtpCapabilities: data.rtpCapabilities,
        paused: false
      })
      .then(videoConsumer => {
        console.log(
          "mediasoup VIDEO Consumer initialized for user %s for Producer %s. {kind: %s, type: %s}",
          socket.id,
          videoConsumer.producerId,
          videoConsumer.kind,
          videoConsumer.type
        );
        finalUsers
          .get(socket.id)
          .consumers.set(videoConsumer.id, videoConsumer);
        consumerTransport
          .consume({
            producerId: audioProducerId,
            rtpCapabilities: data.rtpCapabilities,
            paused: false
          })
          .then(audioConsumer => {
            console.log(
              "mediasoup AUDIO Consumer initialized for user %s for Producer %s. {kind: %s, type: %s}",
              socket.id,
              audioConsumer.producerId,
              audioConsumer.kind,
              audioConsumer.type
            );
            finalUsers
              .get(socket.id)
              .consumers.set(audioConsumer.id, audioConsumer);

            const response = {
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

            if (
              videoConsumer.type === "simulcast" ||
              videoConsumer.type === "svc"
            ) {
              videoConsumer
                .setPreferredLayers({
                  spatialLayer: 2,
                  temporalLayer: 2
                })
                .then(() => {
                  callback(response);
                });
            } else {
              callback(response);
            }
          })
          .catch(error => {
            console.error("Error consuming audio", error);
          });
      })
      .catch(error => {
        console.error("Error consuming video", error);
      });
  });

  socket.on("pauseConsumer", async (data, callback, errback) => {
    const consumer = finalUsers.get(socket.id).consumers.get(data.consumerId);
    if (!consumer.paused) {
      consumer.pause();
    }
    callback();
  });

  socket.on("resumeConsumer", async (data, callback) => {
    const consumer = finalUsers.get(socket.id).consumers.get(data.consumerId);
    if (consumer.paused) {
      consumer.resume();
    }
    callback();
  });

  socket.on("closeConsumer", async (data, callback) => {
    const consumer = finalUsers.get(socket.id).consumers.get(data.consumerId);
    if (!consumer.closed) {
      consumer.close();
    }
    callback();
  });

  socket.on("publisherStats", async (data, callback) => {
    const producer = finalUsers.get(socket.id).producers.get(data.producerId);
    producer.getStats().then(stats => {
      callback(stats);
    });
  });

  socket.on("subscriberStats", async (data, callback) => {
    const consumer = finalUsers.get(socket.id).consumers.get(data.consumerId);
    consumer.getStats().then(stats => {
      callback(stats);
    });
  });

  /**
   * Recording stuff
   */

  let stopRecordingCallbackFunction;
  const stopRecordingCallback = () => {
    if (!!stopRecordingCallbackFunction) {
      stopRecordingCallbackFunction();
    }
  };

  socket.on("record", async (data, callback) => {
    // Session handling
    // ================

    const sessionId = data.sessionId;
    if (!recordings.get(sessionId)) {
      recordings.set(sessionId, {
        ffmpegProcess: undefined,
        transports: [],
        consumers: []
      });
    }

    const hasAudio = data.hasAudio;
    const hasVideo = data.hasVideo;
    const audioProducerId = data.audioProducerId;
    const videoProducerId = data.videoProducerId;

    const recording = recordings.get(sessionId);

    // Transport and consumer
    // ======================

    const sessionRouter = sessions.get(sessionId).router;

    if (hasAudio) {
      sessionRouter
        .createPlainRtpTransport(SERVER_CONFIG.mediasoup.plainRtpTransport)
        .then(plainRtpTransport => {
          recording.transports.push(plainRtpTransport);

          plainRtpTransport
            .connect({
              ip: SERVER_CONFIG.mediasoup.plainRtpTransport.listenIp.ip,
              port:
                SERVER_CONFIG.mediasoup.plainRtpTransport.listenPort.audioPort
            })
            .then(() => {
              console.log("AUDIO PlainRtpTransport connected");
              plainRtpTransport
                .consume({
                  producerId: audioProducerId,
                  rtpCapabilities: sessionRouter.rtpCapabilities,
                  paused: true
                })
                .then(consumer => {
                  recording.consumers.push(consumer);
                  console.log("PlainRtpTransport consuming AUDIO");
                })
                .catch(error => {
                  console.error(error);
                });
            })
            .catch(error => {
              console.error(error);
            });
        })
        .catch(error => {
          console.error(error);
        });
    }

    if (hasVideo) {
      sessionRouter
        .createPlainRtpTransport(SERVER_CONFIG.mediasoup.plainRtpTransport)
        .then(plainRtpTransport => {
          recording.transports.push(plainRtpTransport);

          plainRtpTransport
            .connect({
              ip: SERVER_CONFIG.mediasoup.plainRtpTransport.listenIp.ip,
              port:
                SERVER_CONFIG.mediasoup.plainRtpTransport.listenPort.videoPort
            })
            .then(() => {
              console.log("VIDEO PlainRtpTransport connected");
              plainRtpTransport
                .consume({
                  producerId: videoProducerId,
                  rtpCapabilities: sessionRouter.rtpCapabilities,
                  paused: true
                })
                .then(consumer => {
                  recording.consumers.push(consumer);
                  console.log("PlainRtpTransport consuming VIDEO");
                })
                .catch(error => {
                  console.error(error);
                });
            })
            .catch(error => {
              console.error(error);
            });
        })
        .catch(error => {
          console.error(error);
        });
    }

    // FFmpeg recording
    // ================

    /* NOTE
     * ----
     *
     * The objective here is to record the RTP stream as is received from
     * the media server, i.e. WITHOUT TRANSCODING. Hence the "codec copy"
     * commands in FFmpeg.
     *
     * '-map 0:x:0' ensures that one media of each type is used.
     */

    let cmdFileIn = __dirname + "/recording/input.sdp";
    let cmdFileOut = "";
    let cmdFormat = "";
    let cmdCodec = "-an -vn";

    if (hasAudio && hasVideo) {
      // Fails on FFmpeg 2.x and 4.x:
      // [sdp] Could not find codec parameters for stream 1 (Video: vp8, yuv420p): unspecified size
      // [webm] dimensions not set
      cmdFileOut = __dirname + "/recording/recording.webm";
      cmdFormat = "-f webm -flags +global_header";

      cmdCodec = "-map 0:a:0 -map 0:v:0 -acodec copy -vcodec copy";
    } else if (hasAudio) {
      // Fails on FFmpeg 2.x and 4.x:
      // [opus] No extradata present
      // Could not write header for output file #0 (incorrect codec parameters ?): Invalid data found when processing input
      // cmdFileOut = __dirname + '/recording/recording.opus';
      // cmdFormat = '-f opus';

      // cmdFileOut = __dirname + '/recording/recording.ogg';
      // cmdFormat = '-f opus -flags +global_header';

      // Works because WEBM accepts incorrect OPUS media
      // (plays with ffplay but not with VLC)
      cmdFileOut = __dirname + "/recording/recording.webm";
      cmdFormat = "-f webm -flags +global_header";

      cmdCodec = "-acodec copy -vn";
    } else if (hasVideo) {
      // Works because WEBM accepts VP8 media
      // (plays with ffplay and with VLC)
      cmdFileOut = __dirname + "/recording/recording.webm";
      cmdFormat = "-f webm -flags +global_header";

      cmdCodec = "-vcodec copy -an";
    }

    let ffmpegStarted = false;
    let recordingStarted = false;

    // const cmdProgram = '/usr/bin/ffmpeg';  // System installed
    const cmdProgram = "ffmpeg"; // Possibly user installed
    const cmdArgStr = [
      "-protocol_whitelist file,rtp,udp", // Only for FFmpeg 4.x
      "-nostdin",
      //'-loglevel debug',
      // '-analyzeduration 5M',
      // '-probesize 5M',
      // '-thread_queue_size 512',
      // '-s 640x480',
      // '-video_size 640x480',
      "-fflags +genpts",
      "-i",
      cmdFileIn,
      cmdCodec,
      cmdFormat,
      "-y",
      cmdFileOut
    ].join(" ");

    console.log("Run command: " + cmdProgram + " " + cmdArgStr);

    let ffmpegProcess = Spawn(cmdProgram, cmdArgStr.split(" "), {
      detached: true
    });
    recording.ffmpegProcess = ffmpegProcess;

    ffmpegProcess.on("exit", (code, signal) => {
      console.log(
        "Recording process exited with " + `code ${code} and signal ${signal}`
      );
      if (!signal || signal === "SIGINT") {
        console.log("Recording successfully stopped");
      } else {
        console.error("Error stopping recording");
      }
      if (!!stopRecordingCallback) {
        stopRecordingCallback();
      }
    });

    ffmpegProcess.on("disconnect", () => {
      console.log("Recording process disconnect");
    });

    ffmpegProcess.on("error", error => {
      console.log("Recording process error", error);
    });

    ffmpegProcess.on("close", () => {
      console.log("Recording process closed");
    });

    ffmpegProcess.on("message", () => {
      console.log("Recording process message");
    });

    ffmpegProcess.stderr.on("data", data => {
      console.log(data.toString());

      if (data.toString().startsWith("ffmpeg version") && !ffmpegStarted) {
        ffmpegStarted = true;

        setTimeout(function() {
          recording.consumers.forEach(consumer => {
            consumer.resume();
          });
        }, 1000);
      } else if (data.toString().startsWith("frame=") && !recordingStarted) {
        recordingStarted = true;
        callback();
      }
    });
  });

  socket.on("stopRecord", async (data, callback) => {
    const sessionId = data.sessionId;
    const recording = recordings.get(data.sessionId);

    stopRecordingCallbackFunction = callback;
    recording.ffmpegProcess.kill("SIGINT");

    setTimeout(function() {
      recording.consumers.forEach(consumer => {
        consumer.close();
      });
      recording.transports.forEach(transport => {
        transport.close();
      });
    }, 3000);
  });

  // ------------------------------------------------------------------------

  /* Kurento Media Server
   * ====================
   */

  let webRtcEp = null;
  let webRtcEpCandidates = [];

  socket.on("connectKurento", async (data, callback) => {
    // Kurento client
    // ==============

    const sdpOffer = data.sdpOffer;
    const kurentoUri = "ws://localhost:8888/kurento";
    let kurentoVideoPort = 0;
    let msVideoConsumer = null;

    Kurento(kurentoUri, (err, kurentoClient) => {
      if (err) {
        console.log("Cannot connect to Kurento Media Server at " + kurentoUri);
        return callback(
          "Cannot connect to Kurento Media Server at " +
            kurentoUri +
            ", error: " +
            err
        );
      }

      kurentoClient.create("MediaPipeline", (err, pipeline) => {
        pipeline.create("WebRtcEndpoint", (err, _webRtcEp) => {
          webRtcEp = _webRtcEp;

          webRtcEp.on("OnIceCandidate", event => {
            const candidate = Kurento.getComplexType("IceCandidate")(
              event.candidate
            );
            socket.emit("kurentoIceCandidate", candidate);
          });

          while (webRtcEpCandidates.length) {
            const candidate = webRtcEpCandidates.shift();
            webRtcEp.addIceCandidate(candidate);
          }

          webRtcEp.processOffer(sdpOffer, (err, sdpAnswer) => {
            socket.emit("kurentoAnswer", sdpAnswer);
          });

          webRtcEp.gatherCandidates(err => {});

          //J
          // webRtcEp.connect(webRtcEp, (err) => {
          // });
          pipeline.create("RtpEndpoint", (err, rtpEp) => {
            const rtpSdpOffer =
              "v=0\r\n" +
              "o=- 0 0 IN IP4 127.0.0.1\r\n" +
              "s=-\r\n" +
              "c=IN IP4 127.0.0.1\r\n" +
              "t=0 0\r\n" +
              "m=video 5004 RTP/AVP 120\r\n" +
              "a=rtpmap:120 VP8/90000\r\n" +
              "a=rtcp-fb:120 goog-remb\r\n" +
              "a=sendonly\r\n" +
              "";

            rtpEp.processOffer(rtpSdpOffer, (err, rtpSdpAnswer) => {
              console.log("Kurento RTP SDP Answer:\n" + rtpSdpAnswer);

              const vPortRegex = /m=video (\d+) RTP\/AVP 120/;
              const vPortMatch = vPortRegex.exec(rtpSdpAnswer);
              if (vPortMatch) {
                kurentoVideoPort = parseInt(vPortMatch[1], 10);
                console.log("Kurento RTP video port: " + kurentoVideoPort);
              } else {
                console.warn("SDP regex doesn't match");
              }
            });

            rtpEp.connect(webRtcEp, err => {
              /*
               * FIXME: Instead of a timer, the creation of Kurento endpoints
               * and MediaSoup transports/consumers should be properly
               * coordinated.
               */
              setTimeout(() => {
                if (msVideoConsumer) {
                  console.log("MediaSoup RTP VIDEO consumer START");
                  msVideoConsumer.resume();
                } else {
                  console.error(
                    "MediaSoup RTP VIDEO consumer not created; you made a mistake somewhere!"
                  );
                }
              }, 1000);
            });
          });
        });
      });
    });

    socket.on("appIceCandidate", async (data, callback) => {
      const candidate = Kurento.getComplexType("IceCandidate")(data.candidate);

      if (webRtcEp) {
        webRtcEp.addIceCandidate(candidate);
      } else {
        webRtcEpCandidates.push(candidate);
      }
    });

    // MediaSoup transport and consumer
    // ================================

    const sessionId = data.sessionId;
    const sessionRouter = sessions.get(sessionId).router;
    const videoProducerId = data.videoProducerId;
    const hasVideo = true;

    // Same code as in socket.on('record')
    if (hasVideo) {
      sessionRouter
        .createPlainRtpTransport(SERVER_CONFIG.mediasoup.plainRtpTransport)
        .then(plainRtpTransport => {
          plainRtpTransport
            .connect({
              ip: SERVER_CONFIG.mediasoup.plainRtpTransport.listenIp.ip,
              //port: SERVER_CONFIG.mediasoup.plainRtpTransport.listenPort.videoPort
              port: kurentoVideoPort
            })
            .then(() => {
              console.log("VIDEO PlainRtpTransport connected");
              plainRtpTransport
                .consume({
                  producerId: videoProducerId,
                  rtpCapabilities: sessionRouter.rtpCapabilities,
                  paused: true
                })
                .then(consumer => {
                  console.log("PlainRtpTransport consuming VIDEO");
                  msVideoConsumer = consumer;
                })
                .catch(error => {
                  console.error(error);
                });
            })
            .catch(error => {
              console.error(error);
            });
        })
        .catch(error => {
          console.error(error);
        });
    }
  });

  // ------------------------------------------------------------------------
});

function createWorker() {
  return new Promise((resolve, reject) => {
    MediaSoup.createWorker({
      logLevel: SERVER_CONFIG.mediasoup.worker.logLevel,
      logTags: SERVER_CONFIG.mediasoup.worker.logTags,
      rtcMinPort: SERVER_CONFIG.mediasoup.worker.rtcMinPort,
      rtcMaxPort: SERVER_CONFIG.mediasoup.worker.rtcMaxPort
    })
      .then(worker => {
        resolve(worker);
      })
      .catch(error => {
        reject(error);
      });
  });
}

function createRouter(worker) {
  return new Promise((resolve, reject) => {
    if (!worker) {
      reject(new Error("mediasoup Worker is not initialized"));
    }
    const mediaCodecs = SERVER_CONFIG.mediasoup.router.mediaCodecs;
    worker
      .createRouter({
        mediaCodecs
      })
      .then(router => {
        resolve(router);
      })
      .catch(error => {
        reject(error);
      });
  });
}

function createWebRtcTransport(router) {
  return new Promise((resolve, reject) => {
    let webrtcConfig = SERVER_CONFIG.mediasoup.webRtcTransport;
    webrtcConfig["listenIps"] = [
      {
        ip: SERVER_CONFIG.internalIp,
        announcedIp: null
      }
    ];
    router
      .createWebRtcTransport(webrtcConfig)
      .then(response => {
        resolve(response);
      })
      .catch(error => {
        reject(error);
      });
  });
}

function removeFinalUser(userId) {
  // Remove from collection of users
  finalUsers.delete(userId);
  // Remove from any session to which it was connected
  sessions.forEach(session => {
    var index = session.users.indexOf(userId);
    if (index !== -1) {
      session.users.splice(index, 1);
    }
  });
}

createWorker()
  .then(worker => {
    msWorker = worker;
    msWorker.on("died", () => {
      console.error(
        "mediasoup Worker died, exiting in 2 seconds... [pid:%d]",
        worker.pid
      );
      setTimeout(() => process.exit(1), 2000);
    });
    console.log("mediasoup worker initialized\n");
  })
  .catch(error => {
    console.error("Error initializing mediasoup worker");
  });

/**
 * mediasoup Observer API
 */
MediaSoup.observer.on("newworker", worker => {
  console.log(
    Colors.yellow("OBSERVER API: new worker created [worke.pid:%d]"),
    worker.pid
  );

  worker.observer.on("close", () => {
    console.log(
      Colors.yellow("OBSERVER API: worker closed [worker.pid:%d]"),
      worker.pid
    );
  });

  worker.observer.on("newrouter", router => {
    console.log(
      Colors.yellow(
        "OBSERVER API: new router created [worker.pid:%d, router.id:%s]"
      ),
      worker.pid,
      router.id
    );

    router.observer.on("close", () => {
      console.log(
        Colors.yellow("OBSERVER API: router closed [router.id:%s]"),
        router.id
      );
    });

    router.observer.on("newtransport", transport => {
      console.log(
        Colors.yellow(
          "OBSERVER API: new transport created [worker.pid:%d, router.id:%s, transport.id:%s]"
        ),
        worker.pid,
        router.id,
        transport.id
      );

      transport.observer.on("close", () => {
        console.log(
          Colors.yellow("OBSERVER API: transport closed [transport.id:%s]"),
          transport.id
        );
      });

      transport.observer.on("newproducer", producer => {
        console.log(
          Colors.yellow(
            "OBSERVER API: new producer created [worker.pid:%d, router.id:%s, transport.id:%s, producer.id:%s]"
          ),
          worker.pid,
          router.id,
          transport.id,
          producer.id
        );

        producer.observer.on("close", () => {
          console.log(
            Colors.yellow("OBSERVER API: producer closed [producer.id:%s]"),
            producer.id
          );
        });
      });

      transport.observer.on("newconsumer", consumer => {
        console.log(
          Colors.yellow(
            "OBSERVER API: new consumer created [worker.pid:%d, router.id:%s, transport.id:%s, consumer.id:%s]"
          ),
          worker.pid,
          router.id,
          transport.id,
          consumer.id
        );

        consumer.observer.on("close", () => {
          console.log(
            Colors.yellow("OBSERVER API: consumer closed [consumer.id:%s]"),
            consumer.id
          );
        });
      });

      transport.observer.on("newdataproducer", dataProducer => {
        console.log(
          Colors.yellow(
            "OBSERVER API: new data producer created [worker.pid:%d, router.id:%s, transport.id:%s, dataProducer.id:%s]"
          ),
          worker.pid,
          router.id,
          transport.id,
          dataProducer.id
        );

        dataProducer.observer.on("close", () => {
          console.log(
            Colors.yellow(
              "OBSERVER API: data producer closed [dataProducer.id:%s]"
            ),
            dataProducer.id
          );
        });
      });

      transport.observer.on("newdataconsumer", dataConsumer => {
        console.log(
          Colors.yellow(
            "OBSERVER API: new data consumer created [worker.pid:%d, router.id:%s, transport.id:%s, dataConsumer.id:%s]"
          ),
          worker.pid,
          router.id,
          transport.id,
          dataConsumer.id
        );

        dataConsumer.observer.on("close", () => {
          console.log(
            Colors.yellow(
              "OBSERVER API: data consumer closed [dataConsumer.id:%s]"
            ),
            dataConsumer.id
          );
        });
      });
    });
  });
});
