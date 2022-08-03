const { webrtc } = just.library('webrtc')

/*
STUN Protocol
https://datatracker.ietf.org/doc/html/rfc8489

Client Initiated SIP
https://datatracker.ietf.org/doc/html/rfc5626

ICE
https://datatracker.ietf.org/doc/html/rfc8445

DTLS for STUN
https://datatracker.ietf.org/doc/html/rfc7350
*/

class PeerConnection {
  constructor (stun) {
    this.stun = stun
    this.candidates = []
    this.sdp = ''
    this.label = ''
    this.peer = webrtc.createConnection(stun, 
    () => {
      // onDescription
      this.sdp = webrtc.getDescription(this.peer)
      this.onDescription(this.sdp[0], this.sdp[1])
    },
    () => {
      // onCandidate
      const candidates = webrtc.getCandidates(this.peer)
      for (const candidate of candidates) {
        const [str, mid] = candidate
        if (!this.candidates.some(c => c === str)) {
          this.onCandidate(str)
          this.candidates.push(str)
        }
      }
    },
    () => {
      // onDataChannelOpen
      this.label = webrtc.getLabel(this.peer)
      this.onOpen(this.label)
    },
    () => {
      // onMessage
      const message = webrtc.getMessage(this.peer)
      if (message) this.onMessage(message)
    },
    () => {
      // onClose
      this.onClose()
    },
    )
    this.onDescription = () => {}
    this.onCandidate = () => {}
    this.onOpen = () => {}
    this.onMessage = () => {}
    this.onClose = () => {}
  }

  get state () {
    return webrtc.getState(this.peer)
  }

  get gatheringState () {
    return webrtc.getGatheringState(this.peer)
  }

  createOffer (mid) {
    return webrtc.createOffer(this.peer, mid)
  }

  setRemoteDescription (sdp, type) {
    return webrtc.setRemoteDescription(this.peer, sdp, type)
  }

  addCandidate (candidate, mid) {
    return webrtc.addCandidate(this.peer, candidate, mid)
  }

  send (message) {
    return webrtc.send(this.peer, message)
  }
}

function createConnection (stun) {
  return new PeerConnection(stun)
}

module.exports = { createConnection, webrtc }
