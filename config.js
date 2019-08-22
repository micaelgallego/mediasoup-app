System.register([], function (exports_1, context_1) {
    "use strict";
    var Ip, config;
    var __moduleName = context_1 && context_1.id;
    return {
        setters: [],
        execute: function () {
            Ip = require("ip");
            console.log("Internal IP is %s", Ip.address());
            config = {
                ip: "127.0.0.1",
                internalIp: Ip.address(),
                port: 8080,
                path: "/server",
                ws: {
                    pingInterval: 25000,
                    pingTimeout: 5000
                },
                mediasoup: {
                    worker: {
                        logLevel: "warn",
                        logTags: [
                            "info",
                            "ice",
                            "dtls",
                            "rtp",
                            "srtp",
                            "rtcp"
                        ],
                        rtcMinPort: 32256,
                        rtcMaxPort: 65535
                    },
                    router: {
                        mediaCodecs: [
                            {
                                kind: "audio",
                                mimeType: "audio/opus",
                                preferredPayloadType: 109,
                                clockRate: 48000,
                                channels: 2
                            },
                            {
                                kind: "video",
                                mimeType: "video/VP8",
                                preferredPayloadType: 120,
                                clockRate: 90000,
                                parameters: {
                                    "x-google-start-bitrate": 1000
                                }
                            }
                        ]
                    },
                    webRtcTransport: {
                        listenIps: [],
                        enableUdp: true,
                        enableTcp: true,
                        preferUdp: true,
                        minimumAvailableOutgoingBitrate: 300000,
                        initialAvailableOutgoingBitrate: 600000
                    },
                    plainRtpTransport: {
                        listenIp: {
                            ip: "127.0.0.1"
                        },
                        rtcpMux: true,
                        comedia: false,
                        appData: {
                            recvPort: {
                                audioPort: 5006,
                                videoPort: 5004
                            }
                        }
                    }
                }
            };
            exports_1("default", config);
        }
    };
});
