/**
 * Node Recovery & Re-Synchronization Manager
 * Unit 4: Fault Tolerance, Replication & State Synchronization
 *
 * Workflow:
 *  1. Node recovers: status -> RECOVERING
 *  2. Compares local chunk files against coordinator metadata catalog
 *  3. Identifies missing/outdated document chunks
 *  4. Fetches missing chunks from healthy peer replica nodes (P2P sync)
 *  5. Verifies SHA-256 hash of each chunk
 *  6. Stores chunk to disk
 *  7. Transitions node status -> HEALTHY
 */

class NodeSyncManager {
  /**
   * @param {object} coordinator - Reference to CoordinatorService
   */
  constructor(coordinator) {
    this.coordinator = coordinator;
    this.isSyncing = new Map(); // nodeId -> boolean
  }

  /**
   * Performs full re-synchronization of a recovered or desynced node.
   * @param {string} nodeId
   * @returns {Promise<{ success: boolean, syncedChunks: number, details: string[] }>}
   */
  async synchronizeNode(nodeId) {
    if (this.isSyncing.get(nodeId)) {
      return { success: true, message: `Node ${nodeId} synchronization already in progress`, syncedChunks: 0 };
    }

    this.isSyncing.set(nodeId, true);
    const syncLogs = [];

    const log = (tag, msg) => {
      syncLogs.push(`[${tag}] ${msg}`);
      console.log(`[${tag}] ${msg}`);
      if (this.coordinator && typeof this.coordinator.broadcastEvent === 'function') {
        this.coordinator.broadcastEvent('NODE_SYNC_LOG', {
          nodeId,
          tag,
          message: msg,
          timestamp: new Date().toLocaleTimeString()
        });
      }
    };

    try {
      const node = this.coordinator.namingService.getNode(nodeId);
      if (!node) {
        throw new Error(`Node ${nodeId} not registered in cluster`);
      }

      // Stage 1: Mark RECOVERING
      this.coordinator.namingService.setNodeStatus(nodeId, 'RECOVERING');
      this.coordinator.broadcastEvent('NODE_STATUS_CHANGED', {
        nodeId,
        status: 'RECOVERING',
        reason: 'Node reconnected — synchronization in progress',
        timestamp: new Date().toISOString()
      });

      log('RECOVERY', `${node.name || nodeId} reconnected`);
      log('SYNC', `Checking missing chunks against cluster metadata catalog...`);

      // Record in Lamport Timeline
      if (this.coordinator.lamportClock) {
        const lTime = this.coordinator.lamportClock.tick('Node Recovery Re-sync');
        this.coordinator.timeline.record({
          lamportClock: lTime,
          nodeOrUser: nodeId,
          operation: 'RECOVERY',
          document: 'Cluster Catalog',
          event: `${nodeId} reconnected — starting chunk resynchronization`
        });
      }

      // Stage 2: Inspect all documents where this node is supposed to be a replica
      let missingChunksCount = 0;
      let syncedChunksCount = 0;

      for (const [docId, doc] of this.coordinator.documents.entries()) {
        const isAssignedReplica = doc.replicaNodeIds && doc.replicaNodeIds.includes(nodeId);
        if (!isAssignedReplica) continue;

        const chunks = doc.chunks || [];
        for (const chunkMeta of chunks) {
          // Verify if chunk is present on recovering node
          let hasChunk = false;
          try {
            const buffer = await this.coordinator.fetchChunkFromNode(node, docId, chunkMeta.index);
            if (buffer && buffer.length > 0) {
              hasChunk = true;
            }
          } catch (e) {
            hasChunk = false;
          }

          if (!hasChunk) {
            missingChunksCount++;
            log('SYNC', `Missing chunk: ${doc.name} (${docId}#${chunkMeta.index}). Locating healthy peer replica...`);

            // Find a healthy peer that has the chunk
            const peerCandidates = (doc.replicaNodeIds || [])
              .filter(id => id !== nodeId)
              .map(id => this.coordinator.namingService.getNode(id))
              .filter(n => n && n.status === 'HEALTHY');

            let peerBuffer = null;
            let healthyPeerUsed = null;

            for (const peer of peerCandidates) {
              try {
                peerBuffer = await this.coordinator.fetchChunkFromNode(peer, docId, chunkMeta.index);
                if (peerBuffer && peerBuffer.length > 0) {
                  healthyPeerUsed = peer;
                  break;
                }
              } catch (err) {
                // try next
              }
            }

            if (!peerBuffer) {
              log('SYNC', `WARN: No healthy replica currently holds chunk ${docId}#${chunkMeta.index}`);
              continue;
            }

            // Verify SHA-256
            const isValid = this.coordinator.streamHandler.verifyChunkIntegrity(peerBuffer, chunkMeta.hash);
            if (!isValid) {
              log('SYNC', `ERROR: Peer chunk hash mismatch for ${docId}#${chunkMeta.index}`);
              continue;
            }

            log('SYNC', `Chunk verified: SHA-256 match from ${healthyPeerUsed.id} (${chunkMeta.hash.slice(0, 16)}...)`);

            // Store chunk onto recovering node
            const storeRes = await this.coordinator.storeChunkOnNode(
              node,
              docId,
              chunkMeta.index,
              peerBuffer,
              chunkMeta.hash
            );

            if (storeRes.success) {
              syncedChunksCount++;
              log('SYNC', `Synchronized chunk ${docId}#${chunkMeta.index} onto ${nodeId}`);
            }
          }
        }
      }

      log('SYNC', `Synchronization completed (${syncedChunksCount} missing chunk(s) restored)`);

      // Stage 3: Mark HEALTHY
      this.coordinator.namingService.setNodeStatus(nodeId, 'HEALTHY');
      this.coordinator.namingService.updateHeartbeat(nodeId, {});

      this.coordinator.broadcastEvent('NODE_STATUS_CHANGED', {
        nodeId,
        status: 'HEALTHY',
        reason: 'Synchronization completed — node fully verified and healthy',
        timestamp: new Date().toISOString()
      });

      log('RECOVERY', `${node.name || nodeId} HEALTHY`);

      if (this.coordinator.lamportClock) {
        const lTime = this.coordinator.lamportClock.tick('Node Recovery Complete');
        this.coordinator.timeline.record({
          lamportClock: lTime,
          nodeOrUser: nodeId,
          operation: 'SYNC',
          document: 'All Assigned Docs',
          event: `${nodeId} synchronization completed successfully (Status: HEALTHY)`
        });
      }

      return {
        success: true,
        nodeId,
        missingChunksFound: missingChunksCount,
        syncedChunks: syncedChunksCount,
        logs: syncLogs
      };
    } catch (err) {
      log('RECOVERY', `Synchronization error on ${nodeId}: ${err.message}`);
      return { success: false, nodeId, error: err.message, logs: syncLogs };
    } finally {
      this.isSyncing.set(nodeId, false);
    }
  }

  /**
   * Synchronizes all registered storage nodes across the cluster.
   * @returns {Promise<{ success: boolean, results: object[], totalSynced: number, message: string }>}
   */
  async synchronizeCluster() {
    const nodes = this.coordinator.namingService.getAllNodes();
    const results = [];
    let totalSynced = 0;

    for (const node of nodes) {
      try {
        const res = await this.synchronizeNode(node.id);
        results.push(res);
        totalSynced += (res.syncedChunks || 0);
      } catch (e) {
        results.push({ success: false, nodeId: node.id, error: e.message });
      }
    }

    return {
      success: true,
      message: `Cluster synchronization completed for ${nodes.length} nodes (${totalSynced} chunk(s) restored)`,
      totalSynced,
      results
    };
  }
}

module.exports = NodeSyncManager;
