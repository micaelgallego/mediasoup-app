import { WorkerSettings, RouterOptions, WebRtcTransportOptions, PlainRtpTransportOptions } from "mediasoup";

const Ip = require("ip");

console.log("Internal IP is %s", Ip.address());

const config = {
  ip: "127.0.0.1",
  internalIp: Ip.address(),
  port: 8080,
  path: "/server",
  ws: {
    pingInterval: 25000,
    pingTimeout: 5000
  },
  mediasoup: {

    worker: <WorkerSettings>{
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

    router: <RouterOptions>{
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

    webRtcTransport: <WebRtcTransportOptions>{
      listenIps: [] as { ip: string; announcedIp: string }[],
      enableUdp: true,
      enableTcp: true,
      preferUdp: true,
      minimumAvailableOutgoingBitrate: 300000,
      initialAvailableOutgoingBitrate: 600000
    },

    plainRtpTransport: <PlainRtpTransportOptions>{
      listenIp: {
        //ip: Ip.address(),
        ip: "127.0.0.1"
        //announcedIp: "192.168.1.19"
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

export default config;