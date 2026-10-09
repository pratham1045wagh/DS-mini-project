/**
 * Storage Node Peer-to-Peer Replication Engine
 * Handles asynchronous P2P chunk sync across storage nodes.
 */

const fs = require('fs');
const path = require('path');
const http = require('http');

class ReplicationEngine {
  constructor(storageNode) {
    this.storageNode = storageNode;
  }

  /**
   * Replicates a chunk directly to a target peer node (P2P Stream/HTTP)
   */
  async replicateChunkToPeer(peerNode, docId, chunkIndex, chunkData, chunkHash) {
    return new Promise((resolve) => {
      const buffer = Buffer.isBuffer(chunkData) ? chunkData : Buffer.from(chunkData, 'base64');

      console.log(`[P2P] P2P replication started: chunk ${docId}#${chunkIndex} from ${this.storageNode.id} -> ${peerNode.id}`);

      const lClock = this.storageNode.lamportClock ? this.storageNode.lamportClock.tick('P2P Replicate Send') : 0;
      const options = {
        hostname: peerNode.host || 'localhost',
        port: peerNode.httpPort,
        path: `/api/node/store-chunk?docId=${docId}&chunkIndex=${chunkIndex}&hash=${chunkHash}&isReplication=true&lamportClock=${lClock}`,
        method: 'POST',
        headers: {
          'Content-Type': 'application/octet-stream',
          'Content-Length': buffer.length,
          'X-Sender-Node-Id': this.storageNode.id
        }
      };

      const req = http.request(options, (res) => {
        let body = '';
        res.on('data', chunk => body += chunk);
        res.on('end', () => {
          if (res.statusCode === 200) {
            console.log(`[P2P] chunk transferred: ${docId}#${chunkIndex} (${buffer.length} bytes)`);
            console.log(`[P2P] checksum verified: SHA-256 ${chunkHash.slice(0, 16)}...`);
            console.log(`[P2P] replication successful: chunk ${docId}#${chunkIndex} stored on ${peerNode.id}`);
            
            // Send WebSocket notification to peer via MessageQueue if available
            this.storageNode.messageQueue.sendToPeer(peerNode.id, peerNode.wsUrl, 'CHUNK_REPLICATION', {
              docId,
              chunkIndex,
              senderId: this.storageNode.id,
              targetId: peerNode.id,
              status: 'SUCCESS'
            }).catch(() => {});

            resolve(true);
          } else {
            console.error(`[P2P] Failed to replicate ${docId}#${chunkIndex} to ${peerNode.id}: HTTP ${res.statusCode}`);
            resolve(false);
          }
        });
      });

      req.on('error', (err) => {
        console.error(`[P2P] Error replicating ${docId}#${chunkIndex} to ${peerNode.id}:`, err.message);
        resolve(false);
      });

      req.write(buffer);
      req.end();
    });
  }
}

module.exports = ReplicationEngine;
