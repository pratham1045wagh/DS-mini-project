/**
 * Unit 2: Message-Oriented Communication Module
 * Handles asynchronous, event-driven inter-node communication for replication notices,
 * gossip state updates, and peer notifications.
 */

const WebSocket = require('ws');

class MessageQueue {
  constructor(nodeId, wsPort) {
    this.nodeId = nodeId;
    this.wsPort = wsPort;
    this.server = null;
    this.peerSockets = new Map(); // targetNodeId -> WebSocket connection
    this.listeners = new Map(); // eventType -> array of callback handlers
  }

  startServer() {
    return new Promise((resolve) => {
      this.server = new WebSocket.Server({ port: this.wsPort }, () => {
        console.log(`[MessageQueue] Inter-Node Message Broker running on WS port ${this.wsPort} (Node ${this.nodeId})`);
        resolve();
      });

      this.server.on('connection', (socket) => {
        socket.on('message', (rawMsg) => {
          try {
            const message = JSON.parse(rawMsg);
            this.handleIncomingMessage(message, socket);
          } catch (e) {
            console.error(`[MessageQueue] Received malformed message:`, e);
          }
        });
      });
    });
  }

  on(eventType, handler) {
    if (!this.listeners.has(eventType)) {
      this.listeners.set(eventType, []);
    }
    this.listeners.get(eventType).push(handler);
  }

  handleIncomingMessage(message, socket) {
    const { type, senderId, payload } = message;
    if (senderId && !this.peerSockets.has(senderId)) {
      this.peerSockets.set(senderId, socket);
    }

    const handlers = this.listeners.get(type) || [];
    handlers.forEach(fn => fn(payload, senderId));
  }

  async connectToPeer(peerId, peerWsUrl) {
    if (this.peerSockets.has(peerId)) {
      const existing = this.peerSockets.get(peerId);
      if (existing.readyState === WebSocket.OPEN) return existing;
    }

    return new Promise((resolve, reject) => {
      const ws = new WebSocket(peerWsUrl);

      ws.on('open', () => {
        this.peerSockets.set(peerId, ws);
        // Handshake
        ws.send(JSON.stringify({
          type: 'HANDSHAKE',
          senderId: this.nodeId,
          payload: { timestamp: Date.now() }
        }));
        resolve(ws);
      });

      ws.on('message', (rawMsg) => {
        try {
          const message = JSON.parse(rawMsg);
          this.handleIncomingMessage(message, ws);
        } catch (e) {}
      });

      ws.on('error', (err) => {
        this.peerSockets.delete(peerId);
        reject(err);
      });

      ws.on('close', () => {
        this.peerSockets.delete(peerId);
      });
    });
  }

  async sendToPeer(peerId, peerWsUrl, type, payload) {
    try {
      let socket = this.peerSockets.get(peerId);
      if (!socket || socket.readyState !== WebSocket.OPEN) {
        socket = await this.connectToPeer(peerId, peerWsUrl);
      }

      const packet = JSON.stringify({
        type,
        senderId: this.nodeId,
        payload,
        timestamp: Date.now()
      });

      socket.send(packet);
      return true;
    } catch (err) {
      console.error(`[MessageQueue] Failed to send message '${type}' to peer ${peerId}:`, err.message);
      return false;
    }
  }

  close() {
    if (this.server) this.server.close();
    this.peerSockets.forEach(ws => ws.close());
    this.peerSockets.clear();
  }
}

module.exports = MessageQueue;
