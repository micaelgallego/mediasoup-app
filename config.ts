import { MediaSoupConfig } from "mediasoup";

const Ip = require("ip");

console.log("Internal IP is %s", Ip.address());

const mediasoup: MediaSoupConfig = {
  
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
    listenIps: [] as { ip: string; announcedIp: string }[],
    enableUdp: true,
    enableTcp: true,
    preferUdp: true,
    minimumAvailableOutgoingBitrate: 300000,
    initialAvailableOutgoingBitrate: 600000
  },

  plainRtpTransport: {
    listenIp: "127.0.0.1",
    rtcpMux: true,
    comedia: false,
  }
}

const config = {
  ip: "127.0.0.1",
  internalIp: Ip.address(),
  port: 8080,
  path: "/server",
  ws: {
    pingInterval: 25000,
    pingTimeout: 5000
  },
  rtpConnection: {
    remoteIp: "127.0.0.1",
    recvPort: {
      audioPort: 5006,
      videoPort: 5004
    }
  },
  mediasoup
};

export default config;