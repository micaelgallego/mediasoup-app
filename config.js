const Ip = require("ip");
console.log("Internal IP is %s", Ip.address());

module.exports = {
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
        // 'rtx',
        // 'bwe',
        // 'score',
        // 'simulcast',
        // 'svc'
      ],
      rtcMinPort: 32256,
      rtcMaxPort: 65535
    },
    router: {
      // RtpCodecCapability
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

    // WebRtcTransportOptions
    webRtcTransport: {
      enableUdp: true,
      enableTcp: true,
      preferUdp: true,
      minimumAvailableOutgoingBitrate: 300000,
      initialAvailableOutgoingBitrate: 600000
    },

    // PlainRtpTransportOptions
    plainRtpTransport: {
      listenIp: {
        //ip: Ip.address(),
        ip: "127.0.0.1"
        //announcedIp: "192.168.1.19"
      },
      rtcpMux: true,
      comedia: false,
      listenPort: {
        audioPort: 5006,
        videoPort: 5004
      }
    }
  }
};
