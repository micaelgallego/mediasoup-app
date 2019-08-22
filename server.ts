import SERVER_CONFIG from "./config";

var colors = require('colors/safe');

import express, { NextFunction, Request, Response } from "express";
import fs from "fs";
import https from "https";
import Kurento, { WebRtcEndpoint, IceCandidate } from "kurento-client";
import mediasoup, { Worker, Router, WebRtcTransport, Producer, Consumer, PlainRtpTransport, RtcpFeedback } from "mediasoup";
import socketServer from "socket.io";
import Spawn from "child_process";

const expressApp = express();
expressApp.use(express.json());
expressApp.use(express.static(__dirname));

expressApp.use((error: any, req: Request, res: Response, next: NextFunction) => {
  if (error) {
    console.warn("Express app error,", error.message);
    error.status = error.status || (error.name === "TypeError" ? 400 : 500);
    res.statusMessage = error.message;
    res.status(error.status).send(String(error));
  } else {
    next();
  }
});

const httpsServer = https.createServer(
  {
    cert: fs.readFileSync("./cert/cert.pem"),
    key: fs.readFileSync("./cert/key.pem")
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
let msWorker: Worker;

// A router enables injection, selection and forwarding of media streams through Transport instances created on it.
// Developers may think of a mediasoup router as if it were a “multi-party conference room”, although mediasoup is
// much more low level than that and doesn't constrain itself to specific high level use cases (for instance, a
// “multi-party conference room” could involve various mediasoup routers, even in different physicals hosts)
let _msRouter;

// A transport connects an endpoint with a mediasoup router and enables transmission of media in both directions by
// means of Producer and Consumer instances created on it. mediasoup implements the following transport classes:
// [WebRtcTransport, PlainRtpTransport, PipeTransport]
let _msTransport;

// A producer represents an audio or video source being injected into a mediasoup router. It's created on top of
// a transport that defines how the media packets are carried
let _msProducer;

// A consumer represents an audio or video source being forwarded from a mediasoup router to an endpoint. It's
// created on top of a transport that defines how the media packets are carried
let _msConsumer;

interface Session {
  router: Router;
  users: string[];
}

// Collection of sessions
let sessions = new Map<string, Session>();

interface User {
  ws: socketServer.Socket,
  producerTransports: Map<string, WebRtcTransport>,
  consumerTransports: Map<string, WebRtcTransport>,
  producers: Map<string, Producer>,
  consumers: Map<string, Consumer>
}

// Collection of users (websocket connections)
let finalUsers = new Map<string, User>();

interface Recording {
  ffmpegProcess: any;
  transports: PlainRtpTransport[],
  consumers: Consumer[]
}

// Collection of recordings
let recordings = new Map<string, Recording>();

const server = socketServer(httpsServer, {
  path: SERVER_CONFIG.path,
  serveClient: false,
  pingTimeout: SERVER_CONFIG.ws.pingTimeout,
  pingInterval: SERVER_CONFIG.ws.pingInterval,
  transports: ["websocket"]
  // log: false,
});

server.on("connect", socket => {
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

          // At this point, the computed router.rtpCapabilities includes the
          // router codecs enhanced with retransmission and RTCP capabilities,
          // and the list of RTP header extensions supported by mediasoup.

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
        producer.on("score", _score => {});
        producer.on("videoorientationchange", _videoOrientation => {});

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

  socket.on("pauseConsumer", async (data, callback, _err) => {
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

  let stopRecordingCallbackFunction: () => {};
  const stopRecordingCallback = () => {
    if (stopRecordingCallbackFunction) {
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
              ip: SERVER_CONFIG.rtpConnection.remoteIp,
              port: SERVER_CONFIG.rtpConnection.recvPort.audioPort
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
              ip: SERVER_CONFIG.rtpConnection.remoteIp,
              port: SERVER_CONFIG.rtpConnection.recvPort.videoPort
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

    let ffmpegProcess = Spawn.spawn(cmdProgram, cmdArgStr.split(" "), {
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
      if (stopRecordingCallback) {
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
   *
   * - To send RTP to mediasoup: https://mediasoup.org/documentation/v3/communication-between-client-and-server/#producing-media-from-an-external-endpoint
   * - To receive RTP from mediasoup: https://mediasoup.org/documentation/v3/communication-between-client-and-server/#consuming-media-in-an-external-endpoint
   */

  let webRtcEp: WebRtcEndpoint = null;
  let webRtcEpCandidates: IceCandidate[] = [];

  socket.on("connectKurento", async (data, callback) => {
    // Kurento client
    // ==============

    const sdpOffer = data.sdpOffer;
    const kurentoUri = "ws://localhost:8888/kurento";
    let kurentoVideoPort = 0;

    Kurento(kurentoUri, async (err, kurentoClient) => {
      if (err) {
        console.log("Cannot connect to Kurento Media Server at " + kurentoUri);
        return callback(
          "Cannot connect to Kurento Media Server at " +
            kurentoUri +
            ", error: " +
            err
        );
      }

      kurentoClient.create("MediaPipeline", async (err, pipeline) => {
        // Kurento WebRtcEndpoint
        // ----------------------

        pipeline.create("WebRtcEndpoint", async (err, _webRtcEp) => {
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

          webRtcEp.gatherCandidates(_err => {});

          //J
          // webRtcEp.connect(webRtcEp, err => {
          //   if (err) {
          //     console.error("Kurento ERROR:", err);
          //   }
          //   startMsConsumer();
          // });

          // Kurento RtpEndpoint
          // -------------------

          const sessionId = data.sessionId;
          const sessionRouter = sessions.get(sessionId).router;
          const videoProducerId = data.videoProducerId;

          const rtpTransport = await sessionRouter.createPlainRtpTransport(
            SERVER_CONFIG.mediasoup.plainRtpTransport
          );

          const videoPort = rtpTransport.tuple.localPort;

          const videoConsumer = await rtpTransport.consume({
            producerId: videoProducerId,
            rtpCapabilities: sessionRouter.rtpCapabilities,
            paused: true
          });

          const videoSsrc = videoConsumer.rtpParameters.encodings[0].ssrc;
          const videoCname = videoConsumer.rtpParameters.rtcp.cname;

          pipeline.create("RtpEndpoint", async (err, rtpEp) => {
            rtpEp.connect(webRtcEp, "VIDEO", err => {
              if (err) {
                console.error("Kurento ERROR:", err);
              }
            });

            // prettier-ignore
            const rtpSdpOffer =
              "v=0\r\n" +
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

            rtpEp.processOffer(rtpSdpOffer, async (err, rtpSdpAnswer) => {
              console.log("RTP SDP Answer from Kurento:\n" + rtpSdpAnswer);

              const vPortRegex = /m=video (\d+) RTP\/AVP 120/;
              const vPortMatch = vPortRegex.exec(rtpSdpAnswer);
              if (vPortMatch) {
                kurentoVideoPort = parseInt(vPortMatch[1], 10);
                console.log("Kurento RTP video port: " + kurentoVideoPort);
              } else {
                console.warn("SDP regex doesn't match");
              }

              await rtpTransport.connect({
                ip: SERVER_CONFIG.rtpConnection.remoteIp,
                port: kurentoVideoPort
                // rtcpPort: Same as RTP, due to rtcp-mux
              });
              console.log(
                "VIDEO PlainRtpTransport RTP connected to " +
                SERVER_CONFIG.rtpConnection.remoteIp +
                  ":" +
                  kurentoVideoPort
              );
              console.log(
                "VIDEO PlainRtpTransport RTCP connected to " +
                SERVER_CONFIG.rtpConnection.remoteIp +
                  ":" +
                  (kurentoVideoPort + 1)
              );

              // FIXME: Why is this artificial delay needed?
              // Without it, the media doesn't really arrive
              setTimeout(async () => {
                await videoConsumer.resume();
              }, 1000);
            });
          });
        });
      });
    });

    socket.on("appIceCandidate", async (data, _callback) => {
      const candidate = Kurento.getComplexType("IceCandidate")(data.candidate);

      if (webRtcEp) {
        webRtcEp.addIceCandidate(candidate);
      } else {
        webRtcEpCandidates.push(candidate);
      }
    });
  });

  // ------------------------------------------------------------------------
});

function createWorker() {
  return new Promise<Worker>((resolve, reject) => {
    mediasoup.createWorker({
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

function createRouter(worker: Worker) {
  return new Promise<Router>((resolve, reject) => {
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

function createWebRtcTransport(router: Router) {
  return new Promise<WebRtcTransport>((resolve, reject) => {
    let webrtcConfig = SERVER_CONFIG.mediasoup.webRtcTransport;
    webrtcConfig.listenIps = [
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

function removeFinalUser(userId: string) {
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
  .catch(_err => {
    console.error("Error initializing mediasoup worker");
  });

/**
 * mediasoup Observer API
 */
mediasoup.observer.on("newworker", worker => {
  console.log(
    colors.yellow("OBSERVER API: new worker created [worke.pid:%d]"),
    worker.pid
  );

  worker.observer.on("close", () => {
    console.log(
      colors.yellow("OBSERVER API: worker closed [worker.pid:%d]"),
      worker.pid
    );
  });

  worker.observer.on("newrouter", router => {
    console.log(
      colors.yellow(
        "OBSERVER API: new router created [worker.pid:%d, router.id:%s]"
      ),
      worker.pid,
      router.id
    );

    router.observer.on("close", () => {
      console.log(
        colors.yellow("OBSERVER API: router closed [router.id:%s]"),
        router.id
      );
    });

    router.observer.on("newtransport", transport => {
      console.log(
        colors.yellow(
          "OBSERVER API: new transport created [worker.pid:%d, router.id:%s, transport.id:%s]"
        ),
        worker.pid,
        router.id,
        transport.id
      );

      transport.observer.on("close", () => {
        console.log(
          colors.yellow("OBSERVER API: transport closed [transport.id:%s]"),
          transport.id
        );
      });

      transport.observer.on("newproducer", producer => {
        console.log(
          colors.yellow(
            "OBSERVER API: new producer created [worker.pid:%d, router.id:%s, transport.id:%s, producer.id:%s]"
          ),
          worker.pid,
          router.id,
          transport.id,
          producer.id
        );

        producer.observer.on("close", () => {
          console.log(
            colors.yellow("OBSERVER API: producer closed [producer.id:%s]"),
            producer.id
          );
        });
      });

      transport.observer.on("newconsumer", consumer => {
        console.log(
          colors.yellow(
            "OBSERVER API: new consumer created [worker.pid:%d, router.id:%s, transport.id:%s, consumer.id:%s]"
          ),
          worker.pid,
          router.id,
          transport.id,
          consumer.id
        );

        consumer.observer.on("close", () => {
          console.log(
            colors.yellow("OBSERVER API: consumer closed [consumer.id:%s]"),
            consumer.id
          );
        });
      });

      transport.observer.on("newdataproducer", dataProducer => {
        console.log(
          colors.yellow(
            "OBSERVER API: new data producer created [worker.pid:%d, router.id:%s, transport.id:%s, dataProducer.id:%s]"
          ),
          worker.pid,
          router.id,
          transport.id,
          dataProducer.id
        );

        dataProducer.observer.on("close", () => {
          console.log(
            colors.yellow(
              "OBSERVER API: data producer closed [dataProducer.id:%s]"
            ),
            dataProducer.id
          );
        });
      });

      transport.observer.on("newdataconsumer", dataConsumer => {
        console.log(
          colors.yellow(
            "OBSERVER API: new data consumer created [worker.pid:%d, router.id:%s, transport.id:%s, dataConsumer.id:%s]"
          ),
          worker.pid,
          router.id,
          transport.id,
          dataConsumer.id
        );

        dataConsumer.observer.on("close", () => {
          console.log(
            colors.yellow(
              "OBSERVER API: data consumer closed [dataConsumer.id:%s]"
            ),
            dataConsumer.id
          );
        });
      });
    });
  });
});
