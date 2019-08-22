declare module 'kurento-client' {

    const Kurento: KurentoType;

    export default Kurento;

    export interface KurentoType {

        (kmsUri: string, callback: KurentoClientCallback): void;

        getComplexType(type: "IceCandidate"): ObjectCreator<IceCandidate>;

    }

    export type ObjectCreator<Type> = (value: any) => Type;
    
    export type KurentoClientCallback = (error: any, kurentoClient: KurentoClient) => void;
    
    export interface KurentoClient {

        create(className: "MediaPipeline", callback:(error:any, pipeline: MediaPipeline) => void): Promise<void>;

    }

    export interface MediaPipeline {

        create(className: "WebRtcEndpoint", callback:(error:any, webRtcEndpoint: WebRtcEndpoint) => void): Promise<void>;

        create(className: "RtpEndpoint", callback:(error:any, rtpEndpoint: RtpEndpoint) => void): Promise<void>;

    }

    export interface MediaElement {

    }

    export interface SdpEndpoint extends MediaElement {

        processOffer(sdpOffer: string, callback: (error: any, sdpAnser: string) => void): void;
    }

    export interface WebRtcEndpoint extends SdpEndpoint {

        on(eventType: "OnIceCandidate", handler: (event: IceCandidateEvent) => void): void;

        addIceCandidate(iceCandidate: IceCandidate): void;


        gatherCandidates(callback: (error: any) => void): void;
    }

    export interface RtpEndpoint extends SdpEndpoint {

        connect(element: MediaElement, track: string, handler: (error: any) => void): void;
        
    }

    export interface IceCandidateEvent {
        candidate: string;
    }

    export interface IceCandidate {
        //TODO
    }



}