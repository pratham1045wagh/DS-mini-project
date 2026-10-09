/**
 * Storage Node Implementation
 * Runs an independent Express HTTP server & WebSocket MessageQueue on a dedicated port.
 * Manages local chunk persistence, heartbeat signaling, and P2P replication.
 *
 * FA-1 Improvements:
 *  - Clear structured P2P replication log sequence (started → transferred → verified → successful)
 *  - replicateToPeers returns success/failure result array
 *  - Improved kill/revive simulation
 */

const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const http = require('http');
const MessageQueue = require('../communication/messageQueue');
const StreamHandler = require('../communication/streamHandler');
const ReplicationEngine = require('./replicationEngine');
const { LamportClock } = require('../middleware/lamportClock');

class StorageNode {
  constructor(nodeConfig, coordinatorConfig, globalConfig) {
    this.id = nodeConfig.id;
    this.name = nodeConfig.name;
    this.host = nodeConfig.host;
    this.httpPort = nodeConfig.httpPort;
    this.wsPort = nodeConfig.wsPort;
    this.capacity = nodeConfig.capacity;

    this.coordinatorUrl = `http://${coordinatorConfig.HOST}:${coordinatorConfig.PORT}`;
    this.coordinatorRpcUrl = `http://${coordinatorConfig.HOST}:${coordinatorConfig.PORT}${coordinatorConfig.RPC_ENDPOINT}`;
    this.globalConfig = globalConfig;

    this.dataDir = path.join(globalConfig.SYSTEM.DATA_DIR, this.id);
    this.streamHandler = new StreamHandler(globalConfig.SYSTEM.CHUNK_SIZE);
    this.messageQueue = new MessageQueue(this.id, this.wsPort);
    this.replicationEngine = new ReplicationEngine(this);
    this.lamportClock = new LamportClock(this.id, 0);

    this.app = express();
    this.server = null;
    this.heartbeatTimer = null;
    this.isKilled = false; // Fault simulation flag

    // Ensure disk storage dir exists
    if (!fs.existsSync(this.dataDir)) {
      fs.mkdirSync(this.dataDir, { recursive: true });
    }
  }

  async start() {
    this.setupRoutes();
    await this.messageQueue.startServer();

    return new Promise((resolve) => {
      this.server = this.app.listen(this.httpPort, () => {
        console.log(`[StorageNode] ${this.name} (${this.id}) running on HTTP:${this.httpPort} | WS:${this.wsPort}`);
        this.registerWithCoordinator();
        this.startHeartbeat();
        resolve();
      });
    });
  }

  setupRoutes() {
    this.app.use(cors());
    this.app.use(express.raw({ type: 'application/octet-stream', limit: '100mb' }));
    this.app.use(express.json());

    // Middleware to check if node is simulated "DEAD" / "FAILED"
    this.app.use((req, res, next) => {
      if (this.isKilled) {
        return res.status(503).json({ error: `Node ${this.id} is FAILED (Simulated Fault)` });
      }
      next();
    });

    // ── Store Chunk API (Stream Upload & P2P Replication Receiver) ─────────
    this.app.post('/api/node/store-chunk', async (req, res) => {
      const { docId, chunkIndex, hash, isReplication, lamportClock } = req.query;
      const chunkBuffer = req.body;

      if (!docId || chunkIndex === undefined || !chunkBuffer || chunkBuffer.length === 0) {
        return res.status(400).json({ error: 'Missing chunk parameters or empty body' });
      }

      // Lamport logical clock update
      if (lamportClock) {
        this.lamportClock.update(parseInt(lamportClock), 'Receive Chunk');
      } else {
        this.lamportClock.tick('Store Chunk Local');
      }

      const source = isReplication === 'true' ? 'P2P replication' : 'primary upload';

      // ── FA-1 Log Sequence ──────────────────────────────────────────────
      console.log(`[StorageNode ${this.id}] P2P replication started: receiving chunk ${docId}#${chunkIndex} via ${source} (L=${this.lamportClock.getTime()})`);

      // Verify Hash Integrity (SHA-256)
      const isValid = this.streamHandler.verifyChunkIntegrity(chunkBuffer, hash);
      if (!isValid) {
        console.error(`[StorageNode ${this.id}] CHECKSUM MISMATCH: chunk ${docId}#${chunkIndex} SHA-256 failed!`);
        return res.status(400).json({ error: 'Chunk integrity verification failed (SHA-256 mismatch)' });
      }

      const filePath = path.join(this.dataDir, `${docId}_chunk_${chunkIndex}.bin`);
      fs.writeFileSync(filePath, chunkBuffer);

      console.log(`[StorageNode ${this.id}] chunk transferred: ${chunkBuffer.length} bytes → ${path.basename(filePath)}`);
      console.log(`[StorageNode ${this.id}] checksum verified: SHA-256 match (${hash.slice(0, 16)}...)`);
      console.log(`[StorageNode ${this.id}] replication successful: chunk ${docId}#${chunkIndex} stored on disk (L=${this.lamportClock.getTime()})`);

      res.json({
        success: true,
        nodeId: this.id,
        docId,
        chunkIndex: parseInt(chunkIndex),
        storedBytes: chunkBuffer.length,
        hashVerified: true,
        lamportClock: this.lamportClock.getTime()
      });
    });

    // ── Fetch Chunk API (Stream Download Reader) ──────────────────────────
    this.app.get('/api/node/fetch-chunk', (req, res) => {
      const { docId, chunkIndex } = req.query;
      const filePath = path.join(this.dataDir, `${docId}_chunk_${chunkIndex}.bin`);

      if (!fs.existsSync(filePath)) {
        return res.status(404).json({ error: `Chunk ${docId}#${chunkIndex} not found on node ${this.id}` });
      }

      this.lamportClock.tick('Fetch Chunk Read');
      const chunkBuffer = fs.readFileSync(filePath);
      res.setHeader('Content-Type', 'application/octet-stream');
      res.setHeader('X-Chunk-Index', chunkIndex);
      res.setHeader('X-Node-Id', this.id);
      res.setHeader('X-Node-Name', this.name);
      res.setHeader('X-Lamport-Clock', this.lamportClock.getTime());
      res.send(chunkBuffer);
    });

    // ── Delete Document Chunks API ────────────────────────────────────────
    this.app.delete('/api/node/delete-doc', (req, res) => {
      const { docId } = req.query;
      if (!docId) return res.status(400).json({ error: 'docId is required' });

      let deleted = 0;
      try {
        const files = fs.readdirSync(this.dataDir);
        files.forEach(file => {
          if (file.startsWith(`${docId}_chunk_`)) {
            fs.unlinkSync(path.join(this.dataDir, file));
            deleted++;
          }
        });
        this.lamportClock.tick('Delete Doc Chunks');
        console.log(`[StorageNode ${this.id}] Deleted ${deleted} chunk(s) for document '${docId}'`);
        res.json({ success: true, nodeId: this.id, docId, deletedChunks: deleted, lamportClock: this.lamportClock.getTime() });
      } catch (err) {
        console.error(`[StorageNode ${this.id}] Delete error for '${docId}':`, err.message);
        res.status(500).json({ error: err.message });
      }
    });

    // ── Node Health & Metrics API ─────────────────────────────────────────
    this.app.get('/api/node/status', (req, res) => {
      res.json({
        id: this.id,
        name: this.name,
        status: this.isKilled ? 'FAILED' : 'HEALTHY',
        storageUsedBytes: this.getStorageUsedBytes(),
        capacity: this.capacity,
        storedFiles: this.getStoredFilesList(),
        lamportClock: this.lamportClock.getTime()
      });
    });

    // ── Lamport Clock Query ───────────────────────────────────────────────
    this.app.get('/api/node/clock', (req, res) => {
      res.json({
        nodeId: this.id,
        lamportClock: this.lamportClock.getTime()
      });
    });
  }

  // ── P2P Replication ─────────────────────────────────────────────────────

  /**
   * Replicates chunks to peer nodes via P2P HTTP.
   * Returns array of { nodeId, success } results.
   */
  async replicateToPeers(peerNodes, docId, chunkIndex, chunkData, chunkHash) {
    if (!peerNodes || peerNodes.length === 0) return [];

    const results = [];
    for (const peer of peerNodes) {
      if (peer.id !== this.id) {
        const success = await this.replicationEngine.replicateChunkToPeer(
          peer, docId, chunkIndex, chunkData, chunkHash
        );
        results.push({ nodeId: peer.id, success });
      }
    }
    return results;
  }

  // ── Coordinator Registration & Heartbeat ─────────────────────────────────

  registerWithCoordinator() {
    this.sendRpcRequest('rpc.register_node', {
      id: this.id,
      name: this.name,
      host: this.host,
      httpPort: this.httpPort,
      wsPort: this.wsPort,
      capacity: this.capacity
    }).catch(err => console.error(`[StorageNode ${this.id}] Registration failed:`, err.message));
  }

  startHeartbeat() {
    this.heartbeatTimer = setInterval(() => {
      if (!this.isKilled) {
        this.sendRpcRequest('rpc.heartbeat', {
          nodeId: this.id,
          storageUsedBytes: this.getStorageUsedBytes(),
          lamportClock: this.lamportClock ? this.lamportClock.getTime() : 0
        }).catch(() => {});
      }
    }, this.globalConfig.SYSTEM.HEARTBEAT_INTERVAL);
  }

  sendRpcRequest(method, params) {
    return new Promise((resolve, reject) => {
      const payload = JSON.stringify({
        jsonrpc: '2.0',
        method,
        params,
        id: Date.now()
      });

      const req = http.request(this.coordinatorRpcUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(payload)
        }
      }, (res) => {
        let body = '';
        res.on('data', chunk => body += chunk);
        res.on('end', () => {
          try {
            const data = JSON.parse(body);
            resolve(data.result);
          } catch (e) {
            reject(e);
          }
        });
      });

      req.on('error', reject);
      req.write(payload);
      req.end();
    });
  }

  // ── Storage Metrics ──────────────────────────────────────────────────────

  getStorageUsedBytes() {
    try {
      const files = fs.readdirSync(this.dataDir);
      let total = 0;
      files.forEach(f => {
        try {
          const stats = fs.statSync(path.join(this.dataDir, f));
          total += stats.size;
        } catch (e) {}
      });
      return total;
    } catch (e) {
      return 0;
    }
  }

  getStoredFilesList() {
    try {
      return fs.readdirSync(this.dataDir);
    } catch (e) {
      return [];
    }
  }

  // ── Fault Simulation ─────────────────────────────────────────────────────

  kill() {
    this.isKilled = true;
    console.log(`[StorageNode ${this.id}] SIMULATED FAULT: Status changed HEALTHY → FAILED.`);
  }

  revive() {
    this.isKilled = false;
    console.log(`[StorageNode ${this.id}] RECOVERED: Status changed FAILED → HEALTHY.`);
    // Re-register and send immediate heartbeat so coordinator marks it HEALTHY
    this.registerWithCoordinator();
    this.sendRpcRequest('rpc.heartbeat', {
      nodeId: this.id,
      storageUsedBytes: this.getStorageUsedBytes()
    }).catch(() => {});
  }
}

module.exports = StorageNode;
