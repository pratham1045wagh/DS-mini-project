/**
 * Master Coordinator & Middleware Service
 * Unit 1: Distributed Architecture & Middleware Layer
 * Unit 2: RPC Engine, Naming Service, Fault Detector, Document Catalog & Web Dashboard Sync
 *
 * FA-1 Improvements:
 *  - Session-based authentication (never trusts X-User-Id header)
 *  - Backend-enforced document ownership & READ/WRITE permissions
 *  - Proper 403 Forbidden responses in both HTTP and JSON-RPC 2.0 layers
 *  - Coordinator proxy for chunk download (avoids browser cross-origin issues)
 *  - Real HTTP POST to storage nodes (not internal router hack)
 *  - Replication confirmed only after all replicas return SHA-256 success
 *  - CHUNK_REPLICATION WebSocket events broadcast to dashboard
 *  - Node heartbeat recovery broadcasts NODE_STATUS_CHANGED → HEALTHY
 */

const express = require('express');
const cors = require('cors');
const http = require('http');
const https = require('https');
const path = require('path');
const multer = require('multer');
const WebSocket = require('ws');
const { v4: uuidv4 } = require('uuid');

const NamingService = require('./namingService');
const FaultDetector = require('./faultDetector');
const RPCServer = require('../communication/rpcServer');
const StreamHandler = require('../communication/streamHandler');
const AuthService = require('./authService');
const MetadataStore = require('./metadataStore');
const config = require('../config');

// FA-2: Distributed Systems Concepts (Units 3 & 4)
const { LamportClock, DistributedEventTimeline } = require('./lamportClock');
const VectorClock = require('./vectorClock');
const DeadlockDetector = require('./deadlockDetector');
const LockManager = require('./lockManager');
const BullyElection = require('./bullyElection');
const NodeSyncManager = require('./nodeSyncManager');

class CoordinatorService {
  constructor() {
    this.app = express();
    this.server = null;
    this.wss = null;

    this.namingService = new NamingService();
    this.faultDetector = new FaultDetector(
      this.namingService,
      config,
      (nodeId, status, reason) => {
        this.broadcastEvent('NODE_STATUS_CHANGED', { nodeId, status, reason, timestamp: new Date().toISOString() });
        if (this.lamportClock && this.timeline) {
          const l = this.lamportClock.tick('Node Status Changed');
          this.timeline.record({
            lamportClock: l,
            nodeOrUser: nodeId,
            operation: status === 'FAILED' ? 'NODE_FAILURE' : 'NODE_RECOVERY',
            document: 'Cluster Health',
            event: `Node ${nodeId} status changed to ${status} (${reason || 'Heartbeat timeout'})`
          });
        }
      }
    );

    this.rpcServer = new RPCServer();
    this.streamHandler = new StreamHandler(config.SYSTEM.CHUNK_SIZE);

    // Persistent metadata store (JSON file on disk)
    this.metadataStore = new MetadataStore(config.SYSTEM.METADATA_FILE);

    // Documents catalog: docId -> DocumentMetadata (loaded from disk on start)
    this.documents = new Map();

    // Historical RPC log for dashboard protocol inspector
    this.rpcLogHistory = [];

    // In-memory real-time user chat messages history
    this.chatMessages = [];

    // StorageNode instance refs for fault simulation
    this.storageNodeRefs = new Map();

    // ── FA-2: Distributed System Coordinators ─────────────────────────────
    this.lamportClock = new LamportClock('coordinator', 0);
    this.timeline = new DistributedEventTimeline(200);

    this.deadlockDetector = new DeadlockDetector((deadlockInfo) => {
      this.broadcastEvent('DEADLOCK_DETECTED', deadlockInfo);
      if (this.timeline) {
        this.timeline.record({
          lamportClock: this.lamportClock.tick('Deadlock Detected'),
          nodeOrUser: 'Coordinator',
          operation: 'DEADLOCK',
          document: 'Lock Graph',
          event: deadlockInfo.summary || 'Distributed Deadlock Detected'
        });
      }
    });

    this.lockManager = new LockManager(this.deadlockDetector, (type, payload) => {
      const allLocks = this.lockManager ? this.lockManager.getAllLocks() : [];
      this.broadcastEvent(type, Object.assign({}, payload, { locks: allLocks, activeLocks: allLocks }));
    });

    this.bullyElection = new BullyElection(this.namingService, (type, payload) => {
      const status = this.bullyElection ? this.bullyElection.getStatus() : payload;
      this.broadcastEvent(type, Object.assign({}, status, payload, { leaderState: status }));
    });

    this.syncManager = new NodeSyncManager(this);
  }

  async start(storageNodeInstances = []) {
    storageNodeInstances.forEach(node => this.storageNodeRefs.set(node.id, node));

    // ── Restore persisted document catalog before any routes are active ──
    this.documents = this.metadataStore.load();

    this.setupRpcHandlers();
    this.setupExpressRoutes();

    this.server = http.createServer(this.app);
    this.setupWebSocketServer();

    return new Promise((resolve) => {
      this.server.listen(config.COORDINATOR.PORT, async () => {
        console.log(`================================================================`);
        console.log(`🚀 MASTER COORDINATOR SERVICE running at http://localhost:${config.COORDINATOR.PORT}`);
        console.log(`📡 WebSocket Visualizer running at ws://localhost:${config.COORDINATOR.WS_PORT}`);
        console.log(`================================================================`);

        this.faultDetector.start();

        // Verify persisted documents against live storage nodes (non-blocking)
        this._verifyPersistedDocuments();

        resolve();
      });
    });
  }

  /**
   * Best-effort: after storage nodes have registered, probe each node to
   * confirm the expected chunk files are still on disk. Logs warnings for
   * any missing chunks but does NOT remove documents from the catalog —
   * the document may still be downloadable from another healthy replica.
   * Runs in the background after startup so it never blocks the server.
   */
  async _verifyPersistedDocuments() {
    if (this.documents.size === 0) return;

    // Give storage nodes a moment to register their heartbeats
    await new Promise(r => setTimeout(r, 3000));

    console.log(`[MetadataStore] Verifying ${this.documents.size} persisted document(s) against storage nodes...`);

    for (const [docId, doc] of this.documents) {
      const verifiedNodes = [];
      const missingNodes  = [];

      for (const nodeId of (doc.replicaNodeIds || [])) {
        const node = this.namingService.getNode(nodeId);
        if (!node) {
          // Node hasn't registered yet — leave it; heartbeat loop will handle it
          verifiedNodes.push(nodeId);
          continue;
        }
        const ok = await this.metadataStore.verifyChunksOnNode(node, docId, doc.totalChunks);
        if (ok) {
          verifiedNodes.push(nodeId);
        } else {
          missingNodes.push(nodeId);
          console.warn(`[MetadataStore] ⚠️  Node '${nodeId}' is missing chunks for doc '${doc.name}' (${docId})`);
        }
      }

      if (missingNodes.length > 0) {
        console.warn(`[MetadataStore] Document '${doc.name}' (${docId}): ${verifiedNodes.length}/${doc.replicaNodeIds.length} replica(s) verified.`);
      } else {
        console.log(`[MetadataStore] ✅ Document '${doc.name}' (${docId}): all ${verifiedNodes.length} replica(s) verified.`);
      }
    }
  }

  processFileAttachment(senderId, targetUserId, fileAttachment) {
    if (!fileAttachment || !fileAttachment.docId) return null;
    const docId = fileAttachment.docId;
    const doc = this.documents.get(docId);
    if (!doc) return null;

    if (doc.ownerId !== targetUserId) {
      if (!doc.shares) doc.shares = {};
      doc.shares[targetUserId] = doc.shares[targetUserId] || 'READ';
      this.metadataStore.save(this.documents);

      console.log(`[Chat Shared File] Auto-granted READ access on '${doc.name}' (${docId}) from ${senderId} to ${targetUserId}`);

      this.broadcastEvent('DOCUMENT_SHARED', {
        docId,
        docName: doc.name,
        sharedBy: senderId,
        targetUserId,
        permission: doc.shares[targetUserId]
      });

      this.broadcastEvent('SYNC_REQUEST', {
        type: 'DOC_PERMISSIONS_UPDATED',
        docId,
        targetUserId
      });
    }

    return {
      docId: doc.docId,
      fileName: doc.name,
      fileSize: doc.size,
      mimeType: doc.mimeType || 'application/octet-stream',
      totalChunks: doc.totalChunks
    };
  }

  // ─── WebSocket Dashboard Broadcaster ─────────────────────────────────────

  setupWebSocketServer() {
    this.wss = new WebSocket.Server({ port: config.COORDINATOR.WS_PORT }, () => {
      console.log(`[Coordinator] WebSocket Dashboard Broadcaster active on port ${config.COORDINATOR.WS_PORT}`);
    });

    this.wss.on('connection', (ws) => {
      // Send initial state snapshot to new dashboard client
      ws.send(JSON.stringify({
        type: 'INITIAL_STATE',
        payload: {
          nodes: this.namingService.getAllNodes(),
          documents: Array.from(this.documents.values()),
          users: AuthService.getAllUsers(),
          logs: this.rpcLogHistory.slice(-30),
          chatHistory: this.chatMessages.slice(-50),
          // FA-2: State
          leader: this.bullyElection ? this.bullyElection.getStatus() : null,
          leaderState: this.bullyElection ? this.bullyElection.getStatus() : null,
          locks: this.lockManager ? this.lockManager.getAllLocks() : [],
          activeLocks: this.lockManager ? this.lockManager.getAllLocks() : [],
          deadlock: this.deadlockDetector ? this.deadlockDetector.detectDeadlock() : { hasDeadlock: false, cycle: [] },
          deadlockState: this.deadlockDetector ? this.deadlockDetector.detectDeadlock() : { hasDeadlock: false, cycle: [] },
          logicalClock: this.lamportClock ? this.lamportClock.getTime() : 0,
          timeline: this.timeline ? this.timeline.getEvents(50) : [],
          timelineEvents: this.timeline ? this.timeline.getEvents(50) : []
        }
      }));

      ws.on('message', (messageData) => {
        try {
          const data = JSON.parse(messageData);
          if (data.type === 'SEND_CHAT_MESSAGE') {
            const { senderId, targetUserId, text, fileAttachment } = data.payload || {};
            if (senderId && targetUserId && ((text && text.trim()) || fileAttachment)) {
              const allUsers = AuthService.getAllUsers();
              const senderUser = allUsers.find(u => u.id === senderId);
              const targetUser = allUsers.find(u => u.id === targetUserId);
              if (senderUser && targetUser) {
                const processedAttachment = this.processFileAttachment(senderId, targetUserId, fileAttachment);
                const msgObj = {
                  id: `msg_${uuidv4().slice(0, 8)}`,
                  senderId,
                  senderName: senderUser.name,
                  targetUserId,
                  targetName: targetUser.name,
                  text: text && text.trim() ? text.trim() : (processedAttachment ? `Shared file: ${processedAttachment.fileName}` : ''),
                  fileAttachment: processedAttachment || undefined,
                  timestamp: new Date().toISOString()
                };
                this.chatMessages.push(msgObj);
                if (this.chatMessages.length > 500) this.chatMessages.shift();
                console.log(`[Chat WS] ${senderId} ➔ ${targetUserId}: "${msgObj.text}"${processedAttachment ? ` [File: ${processedAttachment.fileName}]` : ''}`);
                this.broadcastEvent('CHAT_MESSAGE', msgObj);
              }
            }
          }

          if (data.type === 'DELETE_CHAT_MESSAGE') {
            const { messageId, senderId } = data.payload || {};
            if (messageId) {
              const msgIndex = this.chatMessages.findIndex(m => m.id === messageId);
              if (msgIndex !== -1) {
                const msgObj = this.chatMessages[msgIndex];
                if (!senderId || msgObj.senderId === senderId) {
                  this.chatMessages.splice(msgIndex, 1);
                  console.log(`[Chat WS] Message '${messageId}' un-sent by '${senderId || msgObj.senderId}'`);
                  this.broadcastEvent('CHAT_MESSAGE_DELETED', { messageId, senderId: msgObj.senderId, targetUserId: msgObj.targetUserId });
                }
              }
            }
          }
        } catch (e) {
          console.warn('[Coordinator WS] Error processing message:', e.message);
        }
      });
    });
  }

  broadcastEvent(type, payload) {
    if (!this.wss) return;
    const packet = JSON.stringify({ type, payload });
    this.wss.clients.forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(packet);
      }
    });
  }

  logRpcMessage(direction, method, reqData, resData) {
    const entry = {
      id: uuidv4().slice(0, 8),
      timestamp: new Date().toLocaleTimeString(),
      direction,
      method,
      request: reqData,
      response: resData
    };
    this.rpcLogHistory.push(entry);
    if (this.rpcLogHistory.length > 100) this.rpcLogHistory.shift();
    this.broadcastEvent('RPC_LOG', entry);
  }

  // ─── RPC Method Handlers ──────────────────────────────────────────────────

  setupRpcHandlers() {
    // 1. Node Registration (Unit 2: Naming & Addressing)
    this.rpcServer.registerMethod('rpc.register_node', async (params) => {
      const nodeMeta = this.namingService.registerNode(params);
      this.broadcastEvent('NODE_REGISTERED', nodeMeta);
      return { success: true, node: nodeMeta };
    });

    // 2. Heartbeat (Unit 2 & 3: Fault Tolerance & Lamport Clock)
    this.rpcServer.registerMethod('rpc.heartbeat', async (params) => {
      if (params.lamportClock && this.lamportClock) {
        this.lamportClock.update(params.lamportClock, 'Heartbeat ' + params.nodeId);
      }
      const wasHealthy = this.namingService.getNode(params.nodeId)?.status;
      this.namingService.updateHeartbeat(params.nodeId, {
        storageUsedBytes: params.storageUsedBytes
      });
      // If node was FAILED and now sends a heartbeat, initiate automatic synchronization & recovery
      if (wasHealthy === 'FAILED') {
        this.syncManager.synchronizeNode(params.nodeId).catch(err => {
          console.warn(`[Coordinator] Auto-sync failed for ${params.nodeId}:`, err.message);
        });
      }
      return { success: true, acknowledgedAt: Date.now(), lamportClock: this.lamportClock ? this.lamportClock.getTime() : 0 };
    });

    // 3. Cluster Topology Query
    this.rpcServer.registerMethod('rpc.get_cluster_status', async () => {
      return {
        nodes: this.namingService.getAllNodes(),
        documents: Array.from(this.documents.values()),
        users: AuthService.getAllUsers()
      };
    });

    // 4. User Accounts RPC (public — no auth required)
    this.rpcServer.registerMethod('rpc.get_users', async () => {
      return AuthService.getAllUsers();
    });

    // 5. Document Sharing RPC (READ / WRITE Access Control)
    //    Note: caller user is validated server-side via session; params.requestingUserId is overridden
    this.rpcServer.registerMethod('rpc.share_document', async (params, requestingUser) => {
      const { docId, targetUserId, permission } = params;

      if (!requestingUser) {
        const err = new Error('403 Forbidden: Authentication required to share documents');
        err.code = -32001;
        throw err;
      }

      const doc = this.documents.get(docId);
      if (!doc) throw new Error(`Document '${docId}' not found`);

      // Enforce: only owner or WRITE-granted user can share
      const isOwner = doc.ownerId === requestingUser.id;
      const canWrite = doc.shares && doc.shares[requestingUser.id] === 'WRITE';

      if (!isOwner && !canWrite) {
        const err = new Error(`403 Forbidden: User '${requestingUser.id}' does not have permission to share document '${doc.name}'`);
        err.code = -32001;
        throw err;
      }

      // Validate target user exists
      const allUsers = AuthService.getAllUsers();
      if (!allUsers.find(u => u.id === targetUserId)) {
        throw new Error(`Target user '${targetUserId}' is not registered`);
      }

      const validPerm = (permission || 'READ').toUpperCase();
      if (!['READ', 'WRITE'].includes(validPerm)) {
        throw new Error(`Invalid permission type '${permission}'. Must be 'READ' or 'WRITE'`);
      }

      if (!doc.shares) doc.shares = {};
      doc.shares[targetUserId] = validPerm;

      // Persist updated sharing permissions
      this.metadataStore.save(this.documents);

      console.log(`[Access Control] Document '${doc.name}' (${docId}) shared by ${requestingUser.id} with ${targetUserId} (${validPerm})`);

      this.broadcastEvent('DOCUMENT_SHARED', {
        docId,
        docName: doc.name,
        sharedBy: requestingUser.id,
        targetUserId,
        permission: validPerm
      });

      this.broadcastEvent('SYNC_REQUEST', {
        type: 'DOC_PERMISSIONS_UPDATED',
        docId,
        targetUserId
      });

      return { success: true, docId, targetUserId, permission: validPerm, document: doc };
    });

    // 6. Document Location Lookup with Access Control
    this.rpcServer.registerMethod('rpc.get_doc_location', async (params, requestingUser) => {
      const { docId } = params;

      // Always require authentication first (before exposing whether a doc exists)
      if (!requestingUser) {
        const err = new Error('403 Forbidden: Authentication required to access document locations');
        err.code = -32001;
        throw err;
      }

      const doc = this.documents.get(docId);
      if (!doc) throw new Error(`Document with ID '${docId}' not found`);

      // Enforce backend access control
      const isOwner = doc.ownerId === requestingUser.id;
      const sharePermission = doc.shares ? doc.shares[requestingUser.id] : null;
      const hasAccess = isOwner || sharePermission === 'READ' || sharePermission === 'WRITE';

      if (!hasAccess) {
        console.warn(`[Access Control Denied] User '${requestingUser.id}' attempted unauthorized access to '${doc.name}'`);
        const err = new Error(`403 Forbidden: User '${requestingUser.id}' does not have access permissions for document '${doc.name}'`);
        err.code = -32001;
        throw err;
      }

      // Filter replica nodes to find HEALTHY ones
      const replicaNodes = doc.replicaNodeIds
        .map(id => this.namingService.getNode(id))
        .filter(n => n && n.status === 'HEALTHY');

      if (replicaNodes.length === 0) {
        throw new Error(`CRITICAL: All storage replica nodes containing doc '${doc.name}' are DOWN/FAILED!`);
      }

      return { doc, availableReplicaNodes: replicaNodes };
    });

    // 7. Simulated Fault Trigger (HEALTHY → FAILED / FAILED → RECOVERING → HEALTHY)
    this.rpcServer.registerMethod('rpc.simulate_fault', async (params) => {
      const { nodeId, action } = params;
      const nodeInstance = this.storageNodeRefs.get(nodeId);

      if (!nodeInstance) throw new Error(`Node ${nodeId} not found in cluster`);

      if (action === 'kill') {
        nodeInstance.kill();
        this.namingService.setNodeStatus(nodeId, 'FAILED');
        const lTime = this.lamportClock ? this.lamportClock.tick('Node Simulated Kill') : 0;
        if (this.timeline) {
          this.timeline.record({
            lamportClock: lTime,
            nodeOrUser: nodeId,
            operation: 'NODE_FAILURE',
            document: 'Cluster Health',
            event: `Node ${nodeId} killed via fault simulation`
          });
        }
        this.broadcastEvent('NODE_STATUS_CHANGED', {
          nodeId,
          status: 'FAILED',
          reason: 'User Simulated Fault (Kill)',
          timestamp: new Date().toISOString()
        });

        // If coordinator is failed, and the killed node was the current leader, trigger Bully election
        if (this.bullyElection && !this.bullyElection.isCoordinatorAlive && this.bullyElection.currentLeader?.id === nodeId) {
          await this.bullyElection.startElection();
        }
      } else {
        nodeInstance.revive();
        this.namingService.setNodeStatus(nodeId, 'HEALTHY');
        const lTime = this.lamportClock ? this.lamportClock.tick('Node Simulated Revive') : 0;
        if (this.timeline) {
          this.timeline.record({
            lamportClock: lTime,
            nodeOrUser: nodeId,
            operation: 'NODE_RECOVERY',
            document: 'Cluster Health',
            event: `Node ${nodeId} revived — initiating re-synchronization`
          });
        }

        // If coordinator is failed, and the revived node has higher rank than current leader, trigger Bully election
        if (this.bullyElection && !this.bullyElection.isCoordinatorAlive) {
          const currentRank = this.bullyElection.ranks[this.bullyElection.currentLeader?.id] || 0;
          const revivedRank = this.bullyElection.ranks[nodeId] || 0;
          if (revivedRank > currentRank) {
            await this.bullyElection.startElection(nodeId);
          }
        }

        // Trigger recovery synchronization workflow (RECOVERING -> SYNCING -> HEALTHY)
        this.syncManager.synchronizeNode(nodeId).catch(err => {
          console.warn(`[Coordinator] Sync error during revive of ${nodeId}:`, err.message);
        });
      }

      const currentStatus = action === 'kill' ? 'FAILED' : 'HEALTHY';
      return { success: true, nodeId, currentStatus };
    });

    // 8. User-to-User Real-time Chat
    this.rpcServer.registerMethod('rpc.send_chat_message', async (params, requestingUser) => {
      if (!requestingUser) throw new Error('403 Forbidden: Authentication required');
      const { targetUserId, text, fileAttachment } = params;
      if (!targetUserId || (!fileAttachment && (!text || !text.trim()))) throw new Error('targetUserId and text or fileAttachment required');

      const allUsers = AuthService.getAllUsers();
      const targetUser = allUsers.find(u => u.id === targetUserId);
      if (!targetUser) throw new Error(`Target user '${targetUserId}' not found`);

      const processedAttachment = this.processFileAttachment(requestingUser.id, targetUserId, fileAttachment);
      const message = {
        id: `msg_${uuidv4().slice(0, 8)}`,
        senderId: requestingUser.id,
        senderName: requestingUser.name,
        targetUserId,
        targetName: targetUser.name,
        text: text && text.trim() ? text.trim() : (processedAttachment ? `Shared file: ${processedAttachment.fileName}` : ''),
        fileAttachment: processedAttachment || undefined,
        timestamp: new Date().toISOString()
      };

      this.chatMessages.push(message);
      if (this.chatMessages.length > 500) this.chatMessages.shift();

      this.broadcastEvent('CHAT_MESSAGE', message);
      return { success: true, message };
    });

    this.rpcServer.registerMethod('rpc.delete_chat_message', async (params, requestingUser) => {
      if (!requestingUser) throw new Error('403 Forbidden: Authentication required');
      const { messageId } = params;
      if (!messageId) throw new Error('messageId is required');

      const msgIndex = this.chatMessages.findIndex(m => m.id === messageId);
      if (msgIndex === -1) throw new Error('Message not found');

      const msgObj = this.chatMessages[msgIndex];
      if (msgObj.senderId !== requestingUser.id) throw new Error('403 Forbidden: You can only unsend your own messages');

      this.chatMessages.splice(msgIndex, 1);
      this.broadcastEvent('CHAT_MESSAGE_DELETED', { messageId, senderId: msgObj.senderId, targetUserId: msgObj.targetUserId });
      return { success: true, messageId };
    });

    this.rpcServer.registerMethod('rpc.get_chat_history', async (params, requestingUser) => {
      if (!requestingUser) throw new Error('403 Forbidden: Authentication required');
      const userId = requestingUser.id;
      const history = this.chatMessages.filter(m => m.senderId === userId || m.targetUserId === userId);
      return { success: true, messages: history };
    });

    // ─── FA-2: Distributed Systems RPC Methods ───────────────────────────────

    // 9. Lamport Logical Clock Query
    this.rpcServer.registerMethod('rpc.get_logical_clock', async () => {
      const nodeClocks = {};
      for (const [nodeId, instance] of this.storageNodeRefs.entries()) {
        nodeClocks[nodeId] = instance.lamportClock ? instance.lamportClock.getTime() : 0;
      }
      return {
        coordinator: this.lamportClock.getTime(),
        clocks: Object.assign({ Coordinator: this.lamportClock.getTime() }, nodeClocks),
        nodes: nodeClocks,
        timeline: this.timeline.getEvents(50)
      };
    });

    // 10. Vector Clock Query
    this.rpcServer.registerMethod('rpc.get_vector_clock', async (params) => {
      if (params && params.docId) {
        const doc = this.documents.get(params.docId);
        if (!doc) throw new Error(`Document '${params.docId}' not found`);
        return {
          docId: doc.docId,
          fileName: doc.name,
          version: doc.version || 1,
          vectorClock: doc.vectorClock || {},
          conflict: doc.conflict === true,
          conflictData: doc.conflictData || null
        };
      }
      const allClocks = {};
      for (const [id, doc] of this.documents.entries()) {
        allClocks[id] = {
          docId: id,
          fileName: doc.name,
          version: doc.version || 1,
          vectorClock: doc.vectorClock || {},
          conflict: doc.conflict === true,
          conflictData: doc.conflictData || null
        };
      }
      return allClocks;
    });

    // 11. Leader Election (Bully Algorithm)
    this.rpcServer.registerMethod('rpc.start_election', async (params) => {
      const lTime = this.lamportClock.tick('Start Leader Election');
      this.timeline.record({
        lamportClock: lTime,
        nodeOrUser: params?.initiatorId || 'Cluster',
        operation: 'ELECTION',
        document: 'Cluster Topology',
        event: 'Bully leader election started'
      });
      const result = await this.bullyElection.startElection(params?.initiatorId);
      return Object.assign({ leaderId: result.currentLeader?.id, leaderName: result.currentLeader?.name }, result);
    });

    this.rpcServer.registerMethod('rpc.get_leader', async () => {
      const status = this.bullyElection.getStatus();
      return Object.assign({ leaderId: status.currentLeader?.id, leaderName: status.currentLeader?.name }, status);
    });

    this.rpcServer.registerMethod('rpc.simulate_coordinator_failure', async () => {
      const result = await this.bullyElection.simulateCoordinatorFailure();
      return Object.assign({ leaderId: result.currentLeader?.id, leaderName: result.currentLeader?.name }, result);
    });

    this.rpcServer.registerMethod('rpc.simulate_coordinator_revival', async () => {
      const result = await this.bullyElection.simulateCoordinatorRevival();
      return Object.assign({ leaderId: result.currentLeader?.id, leaderName: result.currentLeader?.name }, result);
    });

    this.rpcServer.registerMethod('rpc.kill_coordinator', async () => {
      return this.rpcServer.executeMethod('rpc.simulate_coordinator_failure');
    });

    this.rpcServer.registerMethod('rpc.revive_coordinator', async () => {
      return this.rpcServer.executeMethod('rpc.simulate_coordinator_revival');
    });

    this.rpcServer.registerMethod('rpc.trigger_election', async (params) => {
      return this.rpcServer.executeMethod('rpc.start_election', params);
    });

    // 12. Distributed Mutual Exclusion (Document Locking)
    this.rpcServer.registerMethod('rpc.acquire_document_lock', async (params, requestingUser) => {
      if (!requestingUser) {
        const err = new Error('403 Forbidden: Authentication required');
        err.code = -32001;
        throw err;
      }
      const { docId, timeoutMs } = params;
      const doc = this.documents.get(docId);
      if (!doc) throw new Error(`Document '${docId}' not found`);

      const result = this.lockManager.acquireLock(docId, doc.name, requestingUser.id, requestingUser.name, timeoutMs);
      const lTime = this.lamportClock.tick(result.success ? 'Lock Acquired' : 'Lock Denied');
      this.timeline.record({
        lamportClock: lTime,
        nodeOrUser: requestingUser.name || requestingUser.id,
        operation: 'LOCK',
        document: doc.name,
        event: result.success
          ? `Lock acquired on '${doc.name}' by ${requestingUser.name}`
          : (result.deadlock ? `DEADLOCK DETECTED! Lock denied on '${doc.name}'` : `Lock denied on '${doc.name}' (held by ${result.lockedByName})`)
      });
      return result;
    });

    this.rpcServer.registerMethod('rpc.release_document_lock', async (params, requestingUser) => {
      if (!requestingUser) {
        const err = new Error('403 Forbidden: Authentication required');
        err.code = -32001;
        throw err;
      }
      const { docId, force } = params;
      const doc = this.documents.get(docId);
      const result = this.lockManager.releaseLock(docId, requestingUser.id, force);
      if (result.released) {
        const lTime = this.lamportClock.tick('Lock Released');
        this.timeline.record({
          lamportClock: lTime,
          nodeOrUser: requestingUser.name || requestingUser.id,
          operation: 'LOCK',
          document: doc ? doc.name : docId,
          event: `Lock released on '${doc ? doc.name : docId}' by ${requestingUser.name}`
        });
      }
      return result;
    });

    this.rpcServer.registerMethod('rpc.get_document_lock', async (params) => {
      return this.lockManager.getLock(params.docId);
    });

    this.rpcServer.registerMethod('rpc.get_all_locks', async () => {
      return this.lockManager.getAllLocks();
    });

    this.rpcServer.registerMethod('rpc.acquire_lock', async (params, requestingUser) => {
      return this.rpcServer.executeMethod('rpc.acquire_document_lock', params, requestingUser);
    });

    this.rpcServer.registerMethod('rpc.release_lock', async (params, requestingUser) => {
      return this.rpcServer.executeMethod('rpc.release_document_lock', params, requestingUser);
    });

    // 13. Document Version History
    this.rpcServer.registerMethod('rpc.get_document_versions', async (params, requestingUser) => {
      if (!requestingUser) {
        const err = new Error('403 Forbidden: Authentication required');
        err.code = -32001;
        throw err;
      }
      const doc = this.documents.get(params.docId);
      if (!doc) throw new Error(`Document '${params.docId}' not found`);
      return {
        docId: doc.docId,
        fileName: doc.name,
        currentVersion: doc.version || 1,
        versions: doc.versions || []
      };
    });

    // 14. Document Update & Concurrent Version Creation
    this.rpcServer.registerMethod('rpc.update_document', async (params, requestingUser) => {
      if (!requestingUser) {
        const err = new Error('403 Forbidden: Authentication required');
        err.code = -32001;
        throw err;
      }
      const { docId, content, fileName, parentVectorClock, simulateConflict } = params;
      const buffer = Buffer.from(content || '', 'utf8');
      return await this.updateDocumentContent(docId, buffer, fileName, requestingUser, parentVectorClock, simulateConflict);
    });

    // 15. Vector Clock Conflict Resolution
    this.rpcServer.registerMethod('rpc.resolve_conflict', async (params, requestingUser) => {
      if (!requestingUser) {
        const err = new Error('403 Forbidden: Authentication required');
        err.code = -32001;
        throw err;
      }
      const { docId, resolution, mergedContent } = params;
      return await this.resolveDocumentConflict(docId, resolution, mergedContent, requestingUser);
    });

    // 16. Node Re-Synchronization
    this.rpcServer.registerMethod('rpc.sync_node', async (params) => {
      return await this.syncManager.synchronizeNode(params.nodeId);
    });

    // 17. Distributed Deadlock Detection
    this.rpcServer.registerMethod('rpc.get_deadlocks', async () => {
      return this.deadlockDetector.detectDeadlock();
    });

    this.rpcServer.registerMethod('rpc.simulate_deadlock', async () => {
      const res = this.deadlockDetector.simulateDeadlockScenario();
      const lTime = this.lamportClock.tick('Simulate Deadlock');
      this.timeline.record({
        lamportClock: lTime,
        nodeOrUser: 'Coordinator',
        operation: 'DEADLOCK',
        document: 'Lock Graph',
        event: 'Simulated circular wait: User A ⇄ User B'
      });
      return res;
    });

    this.rpcServer.registerMethod('rpc.resolve_deadlock', async () => {
      const res = this.deadlockDetector.resolveDeadlock(this.lockManager);
      const lTime = this.lamportClock.tick('Resolve Deadlock');
      this.timeline.record({
        lamportClock: lTime,
        nodeOrUser: 'Coordinator',
        operation: 'DEADLOCK',
        document: 'Lock Graph',
        event: res.message || 'Deadlock cycle resolved'
      });
      return res;
    });
  }

  // ─── HTTP Express Routes ──────────────────────────────────────────────────

  setupExpressRoutes() {
    this.app.use(cors({ origin: true, credentials: true }));
    this.app.use(express.json());
    this.app.use(AuthService.sessionMiddleware);

    // ── Authentication Routes ──────────────────────────────────────────────

    // POST /api/auth/login
    this.app.post('/api/auth/login', (req, res) => {
      const { userId, password } = req.body;
      if (!userId || !password) {
        return res.status(400).json({ error: 'userId and password are required' });
      }
      const token = AuthService.login(userId, password);
      if (!token) {
        return res.status(401).json({ error: 'Invalid credentials. Check userId and password.' });
      }
      AuthService.setSessionCookie(res, token);
      const user = AuthService.resolveSession(token);
      console.log(`[Auth] Login successful for '${userId}'`);
      res.json({ success: true, user });
    });

    // GET /api/auth/me
    this.app.get('/api/auth/me', (req, res) => {
      if (!req.user) {
        return res.status(401).json({ error: 'Not authenticated', authenticated: false });
      }
      res.json({ authenticated: true, user: req.user });
    });

    // POST /api/auth/logout
    this.app.post('/api/auth/logout', (req, res) => {
      AuthService.logout(req.sessionToken);
      AuthService.clearSessionCookie(res);
      res.json({ success: true });
    });

    // ── Serve Static Dashboard ────────────────────────────────────────────

    this.app.use(express.static(path.join(__dirname, '..', 'public')));

    // ── JSON-RPC 2.0 Endpoint ─────────────────────────────────────────────

    this.app.post(config.COORDINATOR.RPC_ENDPOINT, async (req, res) => {
      // Pass authenticated user to RPC handler
      const response = await this.rpcServer.handleRequest(req.body, req.user);
      this.logRpcMessage('RPC_CALL', req.body.method, req.body.params, response.result || response.error);
      res.json(response);
    });

    // ── File Upload Endpoint ──────────────────────────────────────────────

    const upload = multer({ storage: multer.memoryStorage() });
    this.app.post('/api/documents/upload', AuthService.requireAuth, upload.single('file'), async (req, res) => {
      try {
        if (!req.file) return res.status(400).json({ error: 'No file uploaded' });

        // Use authenticated session user — never trust client header
        const userId = req.user.id;
        const docId = `doc_${uuidv4().slice(0, 8)}`;
        const fileName = req.file.originalname;
        const fileSize = req.file.size;
        const fileBuffer = req.file.buffer;

        console.log(`[Upload] User '${userId}' uploading '${fileName}' (${fileSize} bytes)`);

        // Step 1: Select healthy nodes for replication (Replication Factor = 2)
        const targetNodes = this.namingService.selectNodesForReplication(config.SYSTEM.DEFAULT_REPLICATION_FACTOR);

        if (targetNodes.length < 2) {
          return res.status(507).json({
            error: `Insufficient healthy storage nodes. Need 2, have ${targetNodes.length}. Replication requires at least 2 healthy nodes.`
          });
        }

        const primaryNode = targetNodes[0];
        const replicaNodeIds = targetNodes.map(n => n.id);

        // Step 2: Split file into 512KB chunks (Stream-Oriented Communication)
        const chunks = this.streamHandler.splitIntoChunks(fileBuffer);
        console.log(`[Upload] '${fileName}' split into ${chunks.length} chunk(s) of ${config.SYSTEM.CHUNK_SIZE / 1024}KB`);

        const chunkMetaList = [];
        let allReplicasSucceeded = true;

        // Broadcast replication started event
        this.broadcastEvent('DOCUMENT_REPLICATION', {
          docId,
          name: fileName,
          replicaNodes: replicaNodeIds,
          status: 'STARTED',
          totalChunks: chunks.length
        });

        // Step 3: For each chunk → store on primary → P2P replicate to secondary nodes
        for (const chunk of chunks) {
          console.log(`[Upload] P2P replication started: chunk ${docId}#${chunk.index} (${chunk.size} bytes)`);

          // Store on primary node via real HTTP POST
          const primaryStoreResult = await this.storeChunkOnNode(
            primaryNode,
            docId,
            chunk.index,
            chunk.data,
            chunk.hash
          );

          if (!primaryStoreResult.success) {
            return res.status(500).json({ error: `Failed to store chunk ${chunk.index} on primary node ${primaryNode.id}` });
          }

          console.log(`[Upload] chunk transferred to primary ${primaryNode.id}: ${chunk.size} bytes`);
          console.log(`[Upload] checksum verified on primary: SHA-256 ${chunk.hash.slice(0, 16)}...`);

          // P2P replicate to secondary nodes
          const secondaryNodes = targetNodes.slice(1);
          for (const peerNode of secondaryNodes) {
            const replicaResult = await this.replicateChunkToNode(
              primaryNode,
              peerNode,
              docId,
              chunk.index,
              chunk.data,
              chunk.hash
            );

            if (!replicaResult) {
              console.error(`[Upload] WARN: Replication to ${peerNode.id} failed for chunk ${chunk.index}`);
              allReplicasSucceeded = false;
            } else {
              console.log(`[Upload] replication successful: chunk ${docId}#${chunk.index} stored on ${peerNode.id}`);

              // Broadcast CHUNK_REPLICATION event to dashboard
              this.broadcastEvent('CHUNK_REPLICATION', {
                docId,
                chunkIndex: chunk.index,
                totalChunks: chunks.length,
                senderId: primaryNode.id,
                targetId: peerNode.id,
                hash: chunk.hash.slice(0, 16) + '...',
                status: 'SUCCESS'
              });
            }
          }

          chunkMetaList.push({ index: chunk.index, size: chunk.size, hash: chunk.hash });
        }

        // Step 4: Save Document Metadata with Owner & Permissions & FA-2 Versioning
        const lTime = this.lamportClock ? this.lamportClock.tick('Document Upload') : 1;
        const initialVector = {};
        config.STORAGE_NODES.forEach(n => initialVector[n.id] = 0);
        initialVector[primaryNode.id] = 1;

        const docMetadata = {
          docId,
          name: fileName,
          size: fileSize,
          mimeType: req.file.mimetype || 'application/octet-stream',
          ownerId: userId,
          shares: {},
          totalChunks: chunks.length,
          chunks: chunkMetaList,
          primaryNodeId: primaryNode.id,
          replicaNodeIds,
          replicationSuccess: allReplicasSucceeded,
          createdAt: new Date().toISOString(),
          version: 1,
          lamportTimestamp: lTime,
          vectorClock: initialVector,
          updatedBy: userId,
          updatedAt: new Date().toISOString(),
          checksum: chunkMetaList[0]?.hash || '',
          versions: [
            {
              version: 1,
              fileName,
              size: fileSize,
              mimeType: req.file.mimetype || 'application/octet-stream',
              updatedBy: userId,
              updatedAt: new Date().toISOString(),
              lamportTimestamp: lTime,
              vectorClock: { ...initialVector },
              checksum: chunkMetaList[0]?.hash || '',
              chunks: chunkMetaList
            }
          ],
          conflict: false,
          conflictData: null
        };

        this.documents.set(docId, docMetadata);

        // Persist new document metadata immediately
        this.metadataStore.save(this.documents);

        if (this.timeline) {
          this.timeline.record({
            lamportClock: lTime,
            nodeOrUser: req.user.name || userId,
            operation: 'UPLOAD',
            document: fileName,
            event: `Uploaded '${fileName}' (v1, ${fileSize}B) — replicated to [${replicaNodeIds.join(', ')}]`
          });
        }

        console.log(`[Coordinator] Document '${fileName}' (${docId}) owned by '${userId}', replicated across [${replicaNodeIds.join(', ')}] — all replicas SHA-256 verified: ${allReplicasSucceeded}`);

        // Emit WebSocket events
        this.broadcastEvent('DOCUMENT_REPLICATION', {
          docId,
          name: fileName,
          replicaNodes: replicaNodeIds,
          status: allReplicasSucceeded ? 'SUCCESS' : 'PARTIAL',
          totalChunks: chunks.length
        });

        this.broadcastEvent('DOCUMENT_UPLOADED', docMetadata);

        res.json({ success: true, document: docMetadata });
      } catch (err) {
        console.error(`[Coordinator] Upload error:`, err);
        res.status(500).json({ error: err.message });
      }
    });

    // ── Chunk Download Proxy ───────────────────────────────────────────────
    // Coordinator proxies chunk fetches to storage nodes so the browser
    // does not need cross-origin access to node ports.

    this.app.get('/api/documents/download/:docId/chunk/:chunkIndex', AuthService.requireAuth, async (req, res) => {
      try {
        const { docId, chunkIndex } = req.params;
        const doc = this.documents.get(docId);

        if (!doc) return res.status(404).json({ error: `Document '${docId}' not found` });

        // Enforce access control
        const isOwner = doc.ownerId === req.user.id;
        const sharePermission = doc.shares ? doc.shares[req.user.id] : null;
        const hasAccess = isOwner || sharePermission === 'READ' || sharePermission === 'WRITE';

        if (!hasAccess) {
          console.warn(`[Access Control Denied] User '${req.user.id}' attempted unauthorized download of '${doc.name}'`);
          return res.status(403).json({ error: `403 Forbidden: User '${req.user.id}' does not have access to '${doc.name}'` });
        }

        // Find healthy replica nodes
        const healthyReplicas = doc.replicaNodeIds
          .map(id => this.namingService.getNode(id))
          .filter(n => n && n.status === 'HEALTHY');

        if (healthyReplicas.length === 0) {
          return res.status(503).json({ error: 'All replica nodes are DOWN. Document unavailable.' });
        }

        // Try each healthy replica until one serves the chunk (failover)
        let lastError = null;
        for (const node of healthyReplicas) {
          try {
            const chunkBuffer = await this.fetchChunkFromNode(node, docId, parseInt(chunkIndex));
            res.setHeader('Content-Type', 'application/octet-stream');
            res.setHeader('X-Chunk-Index', chunkIndex);
            res.setHeader('X-Node-Id', node.id);
            res.setHeader('X-Node-Name', node.name);
            return res.send(chunkBuffer);
          } catch (e) {
            lastError = e;
            console.warn(`[Download Proxy] Chunk ${chunkIndex} failed from ${node.id}: ${e.message}. Trying next replica...`);
          }
        }

        res.status(503).json({ error: `Failed to download chunk ${chunkIndex} from all replicas: ${lastError?.message}` });
      } catch (err) {
        console.error(`[Download Proxy] Error:`, err);
        res.status(500).json({ error: err.message });
      }
    });

    // ── Share Document HTTP API ───────────────────────────────────────────

    this.app.post('/api/documents/share', AuthService.requireAuth, (req, res) => {
      const { docId, targetUserId, permission } = req.body;
      const requestingUser = req.user;

      const doc = this.documents.get(docId);
      if (!doc) return res.status(404).json({ error: 'Document not found' });

      // Backend enforces: only owner or WRITE-granted user can share
      const isOwner = doc.ownerId === requestingUser.id;
      const canWrite = doc.shares && doc.shares[requestingUser.id] === 'WRITE';

      if (!isOwner && !canWrite) {
        return res.status(403).json({ error: `403 Forbidden: User '${requestingUser.id}' cannot share this document` });
      }

      const allUsers = AuthService.getAllUsers();
      if (!allUsers.find(u => u.id === targetUserId)) {
        return res.status(404).json({ error: `Target user '${targetUserId}' not found` });
      }

      const validPerm = (permission || 'READ').toUpperCase();
      if (!['READ', 'WRITE'].includes(validPerm)) {
        return res.status(400).json({ error: `Invalid permission '${permission}'` });
      }

      if (!doc.shares) doc.shares = {};
      doc.shares[targetUserId] = validPerm;

      // Persist updated sharing permissions
      this.metadataStore.save(this.documents);

      this.broadcastEvent('DOCUMENT_SHARED', { docId, docName: doc.name, sharedBy: requestingUser.id, targetUserId, permission: validPerm });
      res.json({ success: true, doc });
    });

    this.app.post('/api/documents/unshare', AuthService.requireAuth, (req, res) => {
      const { docId, targetUserId } = req.body;
      const requestingUser = req.user;

      const doc = this.documents.get(docId);
      if (!doc) return res.status(404).json({ error: 'Document not found' });

      const targetToRemove = targetUserId || requestingUser.id;
      const isOwner = doc.ownerId === requestingUser.id;
      const isSelf = targetToRemove === requestingUser.id;

      if (!isOwner && !isSelf) {
        return res.status(403).json({ error: '403 Forbidden: Cannot unshare this document' });
      }

      if (doc.shares && doc.shares[targetToRemove]) {
        delete doc.shares[targetToRemove];
        this.metadataStore.save(this.documents);
        console.log(`[Access Control] Unshared document '${doc.name}' (${docId}) for user ${targetToRemove}`);
        this.broadcastEvent('SYNC_REQUEST', {
          type: 'DOC_PERMISSIONS_UPDATED',
          docId,
          targetUserId: targetToRemove
        });
      }

      res.json({ success: true, docId, removedUser: targetToRemove });
    });

    // ── Document Catalog API ──────────────────────────────────────────────

    this.app.get('/api/documents', AuthService.requireAuth, (req, res) => {
      const userId = req.user.id;
      const allDocs = Array.from(this.documents.values());

      const myDocuments = allDocs.filter(d => d.ownerId === userId);
      const sharedWithMe = allDocs.filter(d => d.ownerId !== userId && d.shares && d.shares[userId]);

      res.json({ userId, myDocuments, sharedWithMe, allDocuments: allDocs });
    });

    // ── Delete Document API ───────────────────────────────────────────────

    this.app.delete('/api/documents/:docId', AuthService.requireAuth, async (req, res) => {
      const { docId } = req.params;
      const requestingUser = req.user;

      const doc = this.documents.get(docId);
      if (!doc) return res.status(404).json({ error: `Document '${docId}' not found` });

      // Only the owner can delete
      if (doc.ownerId !== requestingUser.id) {
        return res.status(403).json({ error: `403 Forbidden: Only the owner can delete document '${doc.name}'` });
      }

      // Remove from coordinator catalog and persist immediately
      this.documents.delete(docId);
      this.metadataStore.save(this.documents);

      // Best-effort: delete chunks from all replica storage nodes
      const deletePromises = (doc.replicaNodeIds || []).map(nodeId => {
        const node = this.namingService.getNode(nodeId);
        if (!node) return Promise.resolve();
        return new Promise((resolve) => {
          const options = {
            hostname: node.host || 'localhost',
            port: node.httpPort,
            path: `/api/node/delete-doc?docId=${docId}`,
            method: 'DELETE'
          };
          const req2 = http.request(options, (r) => {
            r.resume();
            r.on('end', resolve);
          });
          req2.on('error', (err) => {
            console.warn(`[Coordinator] Delete chunks from ${nodeId} failed: ${err.message}`);
            resolve();
          });
          req2.end();
        });
      });

      await Promise.all(deletePromises);

      console.log(`[Coordinator] Document '${doc.name}' (${docId}) deleted by '${requestingUser.id}'`);

      this.broadcastEvent('DOCUMENT_DELETED', { docId, name: doc.name, deletedBy: requestingUser.id });

      res.json({ success: true, docId, name: doc.name });
    });



    // ── Users API (public for sharing UI) ────────────────────────────────

    this.app.get('/api/users', (req, res) => {
      res.json(AuthService.getAllUsers());
    });

    // ── Chat API (User-to-User Real-time Messaging) ──────────────────────

    this.app.get('/api/chat/history', AuthService.requireAuth, (req, res) => {
      const userId = req.user.id;
      const history = this.chatMessages.filter(m => m.senderId === userId || m.targetUserId === userId);
      res.json({ success: true, messages: history });
    });

    this.app.post('/api/chat/send', AuthService.requireAuth, (req, res) => {
      const { targetUserId, text, fileAttachment } = req.body;
      const senderId = req.user.id;

      if (!targetUserId || (!fileAttachment && (!text || !text.trim()))) {
        return res.status(400).json({ error: 'targetUserId and non-empty text or fileAttachment are required' });
      }

      const allUsers = AuthService.getAllUsers();
      const targetUser = allUsers.find(u => u.id === targetUserId);
      if (!targetUser) {
        return res.status(404).json({ error: `Target user '${targetUserId}' not found` });
      }

      const senderUser = allUsers.find(u => u.id === senderId);
      const processedAttachment = this.processFileAttachment(senderId, targetUserId, fileAttachment);
      const message = {
        id: `msg_${uuidv4().slice(0, 8)}`,
        senderId,
        senderName: senderUser ? senderUser.name : senderId,
        targetUserId,
        targetName: targetUser.name,
        text: text && text.trim() ? text.trim() : (processedAttachment ? `Shared file: ${processedAttachment.fileName}` : ''),
        fileAttachment: processedAttachment || undefined,
        timestamp: new Date().toISOString()
      };

      this.chatMessages.push(message);
      if (this.chatMessages.length > 500) this.chatMessages.shift();

      console.log(`[Chat API] ${senderId} ➔ ${targetUserId}: "${message.text}"${processedAttachment ? ` [File: ${processedAttachment.fileName}]` : ''}`);
      this.broadcastEvent('CHAT_MESSAGE', message);

      res.json({ success: true, message });
    });

    this.app.post('/api/chat/delete', AuthService.requireAuth, (req, res) => {
      const { messageId } = req.body;
      const userId = req.user.id;

      if (!messageId) return res.status(400).json({ error: 'messageId is required' });

      const msgIndex = this.chatMessages.findIndex(m => m.id === messageId);
      if (msgIndex === -1) return res.status(404).json({ error: 'Message not found' });

      const msgObj = this.chatMessages[msgIndex];
      if (msgObj.senderId !== userId) {
        return res.status(403).json({ error: '403 Forbidden: You can only unsend your own messages' });
      }

      this.chatMessages.splice(msgIndex, 1);
      console.log(`[Chat API] Message '${messageId}' un-sent by '${userId}'`);
      this.broadcastEvent('CHAT_MESSAGE_DELETED', { messageId, senderId: msgObj.senderId, targetUserId: msgObj.targetUserId });

      res.json({ success: true, messageId });
    });

    // ── FA-2: Document Update & Versioning Endpoint ────────────────────────
    const uploadUpdate = multer({ storage: multer.memoryStorage() });
    this.app.post('/api/documents/:docId/update', AuthService.requireAuth, uploadUpdate.single('file'), async (req, res) => {
      try {
        const { docId } = req.params;
        let contentBuffer = null;
        let fileName = null;

        if (req.file) {
          contentBuffer = req.file.buffer;
          fileName = req.file.originalname;
        } else if (req.body.content !== undefined) {
          contentBuffer = Buffer.from(req.body.content, 'utf8');
          fileName = req.body.fileName;
        } else {
          return res.status(400).json({ error: 'No content or file provided for update' });
        }

        const parentVectorClock = req.body.parentVectorClock;
        const simulateConflict = req.body.simulateConflict === true || req.body.simulateConflict === 'true';

        const result = await this.updateDocumentContent(
          docId,
          contentBuffer,
          fileName,
          req.user,
          parentVectorClock,
          simulateConflict
        );

        if (result.conflict) {
          return res.status(409).json(result);
        }

        res.json(result);
      } catch (err) {
        const status = err.status || (err.code === -32001 ? 403 : (err.code === -32002 ? 423 : 500));
        res.status(status).json({ error: err.message, lock: err.lock });
      }
    });

    // ── FA-2: Conflict Resolution Endpoint ─────────────────────────────────
    this.app.post('/api/documents/:docId/resolve-conflict', AuthService.requireAuth, async (req, res) => {
      try {
        const { docId } = req.params;
        const { resolution, mergedContent } = req.body;
        const result = await this.resolveDocumentConflict(docId, resolution, mergedContent, req.user);
        res.json(result);
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });

    // ── FA-2: Simulate Concurrent Conflict Trigger ─────────────────────────
    this.app.post('/api/documents/:docId/simulate-conflict', AuthService.requireAuth, async (req, res) => {
      try {
        const { docId } = req.params;
        const result = await this.updateDocumentContent(
          docId,
          Buffer.from(`Concurrent branch update by ${req.user.name} at ${new Date().toLocaleTimeString()}`, 'utf8'),
          null,
          req.user,
          null,
          true
        );
        res.json(result);
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });

    // ── FA-2: Document Versions History ────────────────────────────────────
    this.app.get('/api/documents/:docId/versions', AuthService.requireAuth, (req, res) => {
      const { docId } = req.params;
      const doc = this.documents.get(docId);
      if (!doc) return res.status(404).json({ error: `Document '${docId}' not found` });
      res.json({
        docId: doc.docId,
        fileName: doc.name,
        currentVersion: doc.version || 1,
        versions: doc.versions || []
      });
    });

    // ── FA-2: Distributed Mutual Exclusion (Locking API) ───────────────────
    this.app.post('/api/documents/:docId/lock', AuthService.requireAuth, (req, res) => {
      const { docId } = req.params;
      const doc = this.documents.get(docId);
      if (!doc) return res.status(404).json({ error: `Document '${docId}' not found` });

      const timeoutMs = req.body.timeoutMs ? parseInt(req.body.timeoutMs) : undefined;
      const result = this.lockManager.acquireLock(docId, doc.name, req.user.id, req.user.name, timeoutMs);

      const lTime = this.lamportClock.tick(result.success ? 'Lock Acquired' : 'Lock Denied');
      this.timeline.record({
        lamportClock: lTime,
        nodeOrUser: req.user.name || req.user.id,
        operation: 'LOCK',
        document: doc.name,
        event: result.success
          ? `Lock acquired on '${doc.name}' by ${req.user.name}`
          : (result.deadlock ? `DEADLOCK DETECTED! Lock denied on '${doc.name}'` : `Lock denied on '${doc.name}' (held by ${result.lockedByName})`)
      });

      if (!result.success) {
        return res.status(result.deadlock ? 409 : 423).json(result);
      }
      res.json(result);
    });

    this.app.post('/api/documents/:docId/unlock', AuthService.requireAuth, (req, res) => {
      const { docId } = req.params;
      const force = req.body && req.body.force === true;
      const result = this.lockManager.releaseLock(docId, req.user.id, force);

      if (result.released) {
        const lTime = this.lamportClock.tick('Lock Released');
        this.timeline.record({
          lamportClock: lTime,
          nodeOrUser: req.user.name || req.user.id,
          operation: 'LOCK',
          document: result.docName || docId,
          event: `Lock released on '${result.docName || docId}' by ${req.user.name}`
        });
      }

      if (!result.success) {
        return res.status(403).json(result);
      }
      res.json(result);
    });

    this.app.get('/api/documents/:docId/lock', (req, res) => {
      const { docId } = req.params;
      res.json({ docId, lock: this.lockManager.getLock(docId) });
    });

    this.app.get('/api/locks', (req, res) => {
      res.json(this.lockManager.getAllLocks());
    });

    // ── FA-2: Deadlock Detection & Graph API ────────────────────────────────
    this.app.get('/api/cluster/deadlocks', (req, res) => {
      res.json(this.deadlockDetector.detectDeadlock());
    });

    this.app.post('/api/cluster/deadlock/simulate', (req, res) => {
      const result = this.deadlockDetector.simulateDeadlockScenario();
      const lTime = this.lamportClock.tick('Simulate Deadlock');
      this.timeline.record({
        lamportClock: lTime,
        nodeOrUser: 'Coordinator',
        operation: 'DEADLOCK',
        document: 'Lock Graph',
        event: 'Simulated circular wait: User A ⇄ User B'
      });
      res.json(result);
    });

    this.app.post('/api/cluster/deadlock/resolve', (req, res) => {
      const result = this.deadlockDetector.resolveDeadlock(this.lockManager);
      const lTime = this.lamportClock.tick('Resolve Deadlock');
      this.timeline.record({
        lamportClock: lTime,
        nodeOrUser: 'Coordinator',
        operation: 'DEADLOCK',
        document: 'Lock Graph',
        event: result.message || 'Deadlock cycle resolved'
      });
      res.json(result);
    });

    // ── FA-2: Leader Election & Bully Algorithm API ────────────────────────
    this.app.get('/api/cluster/leader', (req, res) => {
      res.json(this.bullyElection.getStatus());
    });

    this.app.post('/api/cluster/election/start', async (req, res) => {
      const { initiatorId } = req.body || {};
      const result = await this.bullyElection.startElection(initiatorId);
      const lTime = this.lamportClock.tick('Start Leader Election');
      this.timeline.record({
        lamportClock: lTime,
        nodeOrUser: initiatorId || 'Cluster',
        operation: 'ELECTION',
        document: 'Cluster Topology',
        event: `Bully leader election completed — new leader: ${result.currentLeader?.name}`
      });
      res.json(result);
    });

    this.app.post('/api/cluster/coordinator/kill', async (req, res) => {
      const result = await this.bullyElection.simulateCoordinatorFailure();
      res.json(result);
    });

    this.app.post('/api/cluster/coordinator/revive', async (req, res) => {
      const result = await this.bullyElection.simulateCoordinatorRevival();
      res.json(result);
    });

    // ── FA-2: Timeline & Logical Clock API ─────────────────────────────────
    this.app.get('/api/cluster/timeline', (req, res) => {
      res.json(this.timeline.getEvents(100));
    });

    this.app.get('/api/cluster/clocks', (req, res) => {
      const nodeClocks = {};
      for (const [nodeId, instance] of this.storageNodeRefs.entries()) {
        nodeClocks[nodeId] = instance.lamportClock ? instance.lamportClock.getTime() : 0;
      }
      res.json({
        coordinator: this.lamportClock.getTime(),
        nodes: nodeClocks,
        timeline: this.timeline.getEvents(50)
      });
    });

    // ── FA-2: Node Re-Synchronization API ──────────────────────────────────
    this.app.post('/api/cluster/nodes/:nodeId/sync', async (req, res) => {
      const { nodeId } = req.params;
      const result = await this.syncManager.synchronizeNode(nodeId);
      res.json(result);
    });

    // ── FA-2 REST Route Aliases ─────────────────────────────────────────────
    this.app.post('/api/locks/acquire', AuthService.requireAuth, (req, res) => {
      const docId = req.body.docId;
      console.log(`[LOCK] Acquire request for docId='${docId}' from user='${req.user.name || req.user.id}'`);
      const doc = this.documents.get(docId);
      if (!doc) {
        console.warn(`[LOCK] Document '${docId}' not found`);
        return res.status(404).json({ error: `Document '${docId}' not found` });
      }
      const timeoutMs = req.body.timeoutMs ? parseInt(req.body.timeoutMs) : undefined;
      const result = this.lockManager.acquireLock(docId, doc.name, req.user.id, req.user.name, timeoutMs);
      const lTime = this.lamportClock.tick(result.success ? 'Lock Acquired' : 'Lock Denied');
      this.timeline.record({
        lamportClock: lTime,
        nodeOrUser: req.user.name || req.user.id,
        operation: 'LOCK',
        document: doc.name,
        event: result.success ? `Lock acquired on '${doc.name}' by ${req.user.name}` : `Lock denied on '${doc.name}' (held by ${result.lockedByName})`
      });
      if (result.success) {
        console.log(`[LOCK] SUCCESS: Lock acquired on '${doc.name}' by ${req.user.name} (Lease: 60s)`);
      } else {
        console.log(`[LOCK] DENIED: Lock on '${doc.name}' denied for ${req.user.name} (held by ${result.lockedByName || result.lockedBy})`);
      }
      if (!result.success) return res.status(result.deadlock ? 409 : 423).json(result);
      res.json(result);
    });

    this.app.post('/api/locks/release', AuthService.requireAuth, (req, res) => {
      const docId = req.body.docId;
      const force = req.body && req.body.force === true;
      console.log(`[LOCK] Release request for docId='${docId}' by user='${req.user.name || req.user.id}'`);
      const result = this.lockManager.releaseLock(docId, req.user.id, force);
      if (result.released) {
        console.log(`[LOCK] RELEASED: Lock on '${result.docName || docId}' released by ${req.user.name}`);
        const lTime = this.lamportClock.tick('Lock Released');
        this.timeline.record({
          lamportClock: lTime,
          nodeOrUser: req.user.name || req.user.id,
          operation: 'LOCK',
          document: result.docName || docId,
          event: `Lock released on '${result.docName || docId}' by ${req.user.name}`
        });
      }
      if (!result.success) return res.status(403).json(result);
      res.json(result);
    });

    this.app.get('/api/locks/status', (req, res) => {
      res.json(this.lockManager.getAllLocks());
    });

    this.app.post('/api/documents/update', AuthService.requireAuth, async (req, res) => {
      try {
        const { docId, content, fileName, parentVectorClock, simulateConflict } = req.body;
        if (content === undefined || content === null) return res.status(400).json({ error: 'Content required' });
        const contentBuffer = Buffer.from(content, 'utf8');
        const result = await this.updateDocumentContent(
          docId,
          contentBuffer,
          fileName,
          req.user,
          parentVectorClock,
          simulateConflict
        );
        if (result.conflict) return res.status(409).json(result);
        res.json(result);
      } catch (err) {
        const status = err.status || (err.code === -32001 ? 403 : (err.code === -32002 ? 423 : 500));
        res.status(status).json({ error: err.message, lock: err.lock });
      }
    });

    this.app.post('/api/documents/resolve-conflict', AuthService.requireAuth, async (req, res) => {
      try {
        const { docId, resolutionChoice, resolution, mergedContent } = req.body;
        const choice = resolutionChoice || resolution || 'MERGE';
        const result = await this.resolveDocumentConflict(docId, choice, mergedContent, req.user);
        res.json(result);
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });

    this.app.post('/api/documents/simulate-conflict', AuthService.requireAuth, async (req, res) => {
      try {
        const { docId } = req.body;
        const result = await this.updateDocumentContent(
          docId,
          Buffer.from(`Concurrent branch update by ${req.user.name} at ${new Date().toLocaleTimeString()}`, 'utf8'),
          null,
          req.user,
          null,
          true
        );
        res.json(result);
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });

    this.app.get('/api/deadlock/status', (req, res) => {
      res.json(this.deadlockDetector.detectDeadlock());
    });

    this.app.post('/api/deadlock/simulate', (req, res) => {
      const result = this.deadlockDetector.simulateDeadlockScenario();
      const lTime = this.lamportClock.tick('Simulate Deadlock');
      this.timeline.record({
        lamportClock: lTime,
        nodeOrUser: 'Coordinator',
        operation: 'DEADLOCK',
        document: 'Lock Graph',
        event: 'Simulated circular wait: User A ⇄ User B'
      });
      res.json(result);
    });

    this.app.post('/api/deadlock/resolve', (req, res) => {
      const result = this.deadlockDetector.resolveDeadlock(this.lockManager);
      const lTime = this.lamportClock.tick('Resolve Deadlock');
      this.timeline.record({
        lamportClock: lTime,
        nodeOrUser: 'Coordinator',
        operation: 'DEADLOCK',
        document: 'Lock Graph',
        event: result.message || 'Deadlock cycle resolved'
      });
      this.broadcastEvent('DEADLOCK_RESOLVED', result);
      res.json(result);
    });

    this.app.get('/api/election/leader', (req, res) => {
      const status = this.bullyElection.getStatus();
      res.json(Object.assign({ leaderId: status.currentLeader?.id, leaderName: status.currentLeader?.name }, status));
    });
    this.app.get('/api/election/status', (req, res) => {
      const status = this.bullyElection.getStatus();
      res.json(Object.assign({ leaderId: status.currentLeader?.id, leaderName: status.currentLeader?.name }, status));
    });

    const handleStartElection = async (req, res) => {
      const { initiatorId } = req.body || {};
      const result = await this.bullyElection.startElection(initiatorId);
      const lTime = this.lamportClock.tick('Start Leader Election');
      this.timeline.record({
        lamportClock: lTime,
        nodeOrUser: initiatorId || 'Cluster',
        operation: 'ELECTION',
        document: 'Cluster Topology',
        event: `Bully leader election completed — new leader: ${result.currentLeader?.name}`
      });
      res.json(Object.assign({ leaderId: result.currentLeader?.id, leaderName: result.currentLeader?.name }, result));
    };
    this.app.post('/api/election/start', handleStartElection);
    this.app.post('/api/election/trigger', handleStartElection);

    const handleKillCoordinator = async (req, res) => {
      const result = await this.bullyElection.simulateCoordinatorFailure();
      const lTime = this.lamportClock.tick('Kill Coordinator (Crash Test)');
      this.timeline.record({
        lamportClock: lTime,
        nodeOrUser: 'Cluster',
        operation: 'ELECTION',
        document: 'Cluster Topology',
        event: `Coordinator stopped (Rank 100); Bully Election held -> Winner: ${result.currentLeader?.name}`
      });
      res.json(Object.assign({ leaderId: result.currentLeader?.id, leaderName: result.currentLeader?.name }, result));
    };
    this.app.post('/api/election/kill-coordinator', handleKillCoordinator);
    this.app.post('/api/coordinator/kill', handleKillCoordinator);

    const handleReviveCoordinator = async (req, res) => {
      const result = await this.bullyElection.simulateCoordinatorRevival();
      const lTime = this.lamportClock.tick('Revive Coordinator (Priority Assertion)');
      this.timeline.record({
        lamportClock: lTime,
        nodeOrUser: 'Coordinator',
        operation: 'ELECTION',
        document: 'Cluster Topology',
        event: `Coordinator revived; Rank 100 priority asserted -> Leadership reclaimed`
      });
      res.json(Object.assign({ leaderId: result.currentLeader?.id, leaderName: result.currentLeader?.name }, result));
    };
    this.app.post('/api/election/revive-coordinator', handleReviveCoordinator);
    this.app.post('/api/coordinator/revive', handleReviveCoordinator);

    this.app.get('/api/timeline', (req, res) => {
      res.json({ timeline: this.timeline.getEvents(100) });
    });

    this.app.get('/api/clocks', (req, res) => {
      const nodeClocks = {};
      for (const [nodeId, instance] of this.storageNodeRefs.entries()) {
        nodeClocks[nodeId] = instance.lamportClock ? instance.lamportClock.getTime() : 0;
      }
      res.json({
        coordinator: this.lamportClock.getTime(),
        clocks: Object.assign({ Coordinator: this.lamportClock.getTime() }, nodeClocks),
        nodes: nodeClocks,
        timeline: this.timeline.getEvents(50)
      });
    });

    this.app.post('/api/sync/node/:nodeId', async (req, res) => {
      const { nodeId } = req.params;
      const result = await this.syncManager.synchronizeNode(nodeId);
      res.json(result);
    });

    this.app.post('/api/sync/all', async (req, res) => {
      const result = await this.syncManager.synchronizeCluster();
      res.json(result);
    });
  }

  // ─── Helpers: FA-2 Document Updating & Conflict Resolution ───────────────

  async updateDocumentContent(docId, newContentBuffer, fileName, requestingUser, parentVectorClock = null, simulateConflict = false) {
    const doc = this.documents.get(docId);
    if (!doc) throw new Error(`Document '${docId}' not found`);

    // Check READ/WRITE permissions
    const isOwner = doc.ownerId === requestingUser.id;
    const canWrite = doc.shares && doc.shares[requestingUser.id] === 'WRITE';
    if (!isOwner && !canWrite) {
      const err = new Error(`403 Forbidden: User '${requestingUser.id}' does not have WRITE permission for '${doc.name}'`);
      err.code = -32001;
      throw err;
    }

    // Check Distributed Mutual Exclusion Lock
    if (this.lockManager.isLockedByOther(docId, requestingUser.id)) {
      const activeLock = this.lockManager.getLock(docId);
      const err = new Error(`423 Locked: Document '${doc.name}' is currently locked by ${activeLock?.lockedByName || activeLock?.lockedBy}. Acquire lock before editing.`);
      err.code = -32002;
      err.status = 423;
      err.lock = activeLock;
      throw err;
    }

    // Check Vector Clock Concurrency / Conflict
    if (parentVectorClock || simulateConflict) {
      let parsedClock = parentVectorClock;
      if (typeof parsedClock === 'string') {
        try { parsedClock = JSON.parse(parsedClock); } catch (e) {}
      }

      const isConflict = simulateConflict || (parsedClock && VectorClock.isConcurrent(parsedClock, doc.vectorClock));

      if (isConflict) {
        doc.conflict = true;
        doc.conflictData = {
          docId,
          docName: doc.name,
          versionA: {
            version: doc.version || 1,
            updatedBy: doc.updatedBy || doc.ownerId,
            updatedAt: doc.updatedAt || doc.createdAt,
            lamportTimestamp: doc.lamportTimestamp || 1,
            vectorClock: { ...(doc.vectorClock || {}) },
            contentSummary: `Current Version ${doc.version || 1} on cluster`
          },
          versionB: {
            version: (doc.version || 1) + 1,
            updatedBy: requestingUser.id,
            updatedByName: requestingUser.name,
            updatedAt: new Date().toISOString(),
            lamportTimestamp: this.lamportClock.tick('Concurrent Update Attempt'),
            vectorClock: VectorClock.increment(parsedClock || { node_1: 1, node_2: 2 }, 'node_2'),
            contentSummary: newContentBuffer ? newContentBuffer.toString('utf8').slice(0, 100) : 'Concurrent branch edit'
          },
          detectedAt: new Date().toISOString()
        };

        this.metadataStore.save(this.documents);
        this.broadcastEvent('DOCUMENT_CONFLICT', doc.conflictData);
        this.timeline.record({
          lamportClock: this.lamportClock.getTime(),
          nodeOrUser: requestingUser.name || requestingUser.id,
          operation: 'UPDATE',
          document: doc.name,
          event: `CONFLICT DETECTED: Concurrent update on '${doc.name}' detected via Vector Clock`
        });

        return {
          conflict: true,
          message: 'CONFLICT DETECTED: Concurrent update detected via Vector Clock!',
          conflictData: doc.conflictData
        };
      }
    }

    // No conflict: apply update as new version
    const buffer = Buffer.isBuffer(newContentBuffer) ? newContentBuffer : Buffer.from(newContentBuffer || '');
    const chunks = this.streamHandler.splitIntoChunks(buffer);

    let targetNodes = (doc.replicaNodeIds || [])
      .map(id => this.namingService.getNode(id))
      .filter(n => n && n.status === 'HEALTHY');

    if (targetNodes.length === 0) {
      targetNodes = this.namingService.selectNodesForReplication(config.SYSTEM.DEFAULT_REPLICATION_FACTOR);
    }
    const primaryNode = targetNodes[0] || { id: 'node_1', host: 'localhost', httpPort: 8001 };

    // Increment Lamport & Vector Clocks
    const lTime = this.lamportClock.tick('Document Update');
    const newVectorClock = VectorClock.increment(doc.vectorClock || {}, primaryNode.id);

    const chunkMetaList = [];
    for (const chunk of chunks) {
      await this.storeChunkOnNode(primaryNode, docId, chunk.index, chunk.data, chunk.hash, lTime);
      for (const peer of targetNodes.slice(1)) {
        await this.replicateChunkToNode(primaryNode, peer, docId, chunk.index, chunk.data, chunk.hash);
      }
      chunkMetaList.push({ index: chunk.index, size: chunk.size, hash: chunk.hash });
    }

    if (!doc.versions || !Array.isArray(doc.versions)) {
      doc.versions = [{
        version: doc.version || 1,
        fileName: doc.name,
        size: doc.size,
        mimeType: doc.mimeType,
        updatedBy: doc.updatedBy || doc.ownerId,
        updatedAt: doc.updatedAt || doc.createdAt,
        lamportTimestamp: doc.lamportTimestamp || 1,
        vectorClock: { ...(doc.vectorClock || {}) },
        checksum: (doc.chunks && doc.chunks[0]?.hash) || ''
      }];
    }

    const nextVersionNum = (doc.version || 1) + 1;
    doc.version = nextVersionNum;
    doc.size = buffer.length;
    doc.totalChunks = chunks.length;
    doc.chunks = chunkMetaList;
    doc.updatedBy = requestingUser.id;
    doc.updatedAt = new Date().toISOString();
    doc.lamportTimestamp = lTime;
    doc.vectorClock = newVectorClock;
    doc.conflict = false;
    doc.conflictData = null;

    const newVersionEntry = {
      version: nextVersionNum,
      fileName: fileName || doc.name,
      size: buffer.length,
      mimeType: doc.mimeType,
      updatedBy: requestingUser.id,
      updatedAt: doc.updatedAt,
      lamportTimestamp: lTime,
      vectorClock: { ...newVectorClock },
      checksum: chunkMetaList[0]?.hash || ''
    };
    doc.versions.push(newVersionEntry);

    this.metadataStore.save(this.documents);

    this.timeline.record({
      lamportClock: lTime,
      nodeOrUser: requestingUser.name || requestingUser.id,
      operation: 'UPDATE',
      document: doc.name,
      event: `Updated '${doc.name}' to Version ${nextVersionNum} (L=${lTime})`
    });

    this.broadcastEvent('DOCUMENT_UPDATED', doc);

    return { success: true, document: doc, version: newVersionEntry };
  }

  async resolveDocumentConflict(docId, resolution, mergedContent, requestingUser) {
    const doc = this.documents.get(docId);
    if (!doc) throw new Error(`Document '${docId}' not found`);

    const resType = (resolution || 'CHOOSE_A').toUpperCase();

    if (resType === 'CHOOSE_A' || resType === 'A') {
      doc.conflict = false;
      doc.conflictData = null;
      this.metadataStore.save(this.documents);
      const lTime = this.lamportClock.tick('Resolve Conflict (Kept A)');
      this.timeline.record({
        lamportClock: lTime,
        nodeOrUser: requestingUser.name || requestingUser.id,
        operation: 'UPDATE',
        document: doc.name,
        event: `Conflict resolved: Retained Version A (v${doc.version})`
      });
      this.broadcastEvent('DOCUMENT_CONFLICT_RESOLVED', { docId, resolution: 'CHOOSE_A', document: doc });
      return { success: true, document: doc, resolution: 'CHOOSE_A' };
    }

    // CHOOSE_B, B, or MERGE
    const content = mergedContent !== undefined && mergedContent !== null && mergedContent !== ''
      ? mergedContent
      : (doc.conflictData?.versionB?.contentPreview || doc.conflictData?.versionB?.contentSummary || 'Resolved Document Content');
    const buffer = Buffer.from(content, 'utf8');

    const clockA = (doc.conflictData && doc.conflictData.versionA && doc.conflictData.versionA.vectorClock) || doc.vectorClock || {};
    const clockB = (doc.conflictData && doc.conflictData.versionB && doc.conflictData.versionB.vectorClock) || {};
    let mergedClock = VectorClock.merge(clockA, clockB);
    mergedClock = VectorClock.increment(mergedClock, doc.primaryNodeId || 'node_1');

    const lTime = this.lamportClock.tick('Resolve Conflict (Merged)');
    const chunks = this.streamHandler.splitIntoChunks(buffer);

    let targetNodes = (doc.replicaNodeIds || [])
      .map(id => this.namingService.getNode(id))
      .filter(n => n && n.status === 'HEALTHY');
    if (targetNodes.length === 0) targetNodes = this.namingService.selectNodesForReplication(2);
    const primaryNode = targetNodes[0] || { id: 'node_1', host: 'localhost', httpPort: 8001 };

    const chunkMetaList = [];
    for (const chunk of chunks) {
      await this.storeChunkOnNode(primaryNode, docId, chunk.index, chunk.data, chunk.hash, lTime);
      for (const peer of targetNodes.slice(1)) {
        await this.replicateChunkToNode(primaryNode, peer, docId, chunk.index, chunk.data, chunk.hash);
      }
      chunkMetaList.push({ index: chunk.index, size: chunk.size, hash: chunk.hash });
    }

    const nextVersionNum = (doc.version || 1) + 1;
    doc.version = nextVersionNum;
    doc.size = buffer.length;
    doc.totalChunks = chunks.length;
    doc.chunks = chunkMetaList;
    doc.updatedBy = requestingUser.id;
    doc.updatedAt = new Date().toISOString();
    doc.lamportTimestamp = lTime;
    doc.vectorClock = mergedClock;
    doc.conflict = false;
    doc.conflictData = null;

    if (!doc.versions) doc.versions = [];
    const newVersionEntry = {
      version: nextVersionNum,
      fileName: doc.name,
      size: buffer.length,
      mimeType: doc.mimeType,
      updatedBy: requestingUser.id,
      updatedAt: doc.updatedAt,
      lamportTimestamp: lTime,
      vectorClock: { ...mergedClock },
      checksum: chunkMetaList[0]?.hash || ''
    };
    doc.versions.push(newVersionEntry);

    this.metadataStore.save(this.documents);

    this.timeline.record({
      lamportClock: lTime,
      nodeOrUser: requestingUser.name || requestingUser.id,
      operation: 'UPDATE',
      document: doc.name,
      event: `Conflict resolved: created Version ${nextVersionNum} with merged Vector Clock`
    });

    this.broadcastEvent('DOCUMENT_CONFLICT_RESOLVED', { docId, resolution: resType, document: doc });
    return { success: true, document: doc, resolution: resType, version: newVersionEntry };
  }

  // ─── Helpers: Storage Node HTTP Communication ─────────────────────────────

  /**
   * Stores a chunk on a storage node via HTTP POST.
   */
  storeChunkOnNode(node, docId, chunkIndex, chunkData, chunkHash, lamportClock = null) {
    return new Promise((resolve) => {
      const buffer = Buffer.isBuffer(chunkData) ? chunkData : Buffer.from(chunkData);
      const lClock = lamportClock || (this.lamportClock ? this.lamportClock.getTime() : 0);

      const options = {
        hostname: node.host || 'localhost',
        port: node.httpPort,
        path: `/api/node/store-chunk?docId=${docId}&chunkIndex=${chunkIndex}&hash=${chunkHash}&lamportClock=${lClock}`,
        method: 'POST',
        headers: {
          'Content-Type': 'application/octet-stream',
          'Content-Length': buffer.length
        }
      };

      const req = http.request(options, (res) => {
        let body = '';
        res.on('data', chunk => body += chunk);
        res.on('end', () => {
          try {
            const data = JSON.parse(body);
            resolve({ success: res.statusCode === 200, data });
          } catch (e) {
            resolve({ success: false, error: e.message });
          }
        });
      });

      req.on('error', (err) => {
        console.error(`[Coordinator] Failed to store chunk on ${node.id}:`, err.message);
        resolve({ success: false, error: err.message });
      });

      req.write(buffer);
      req.end();
    });
  }

  /**
   * Triggers P2P replication from primary to a peer via the primary node's endpoint.
   * Falls back to direct coordinator → peer if primary fails.
   */
  async replicateChunkToNode(primaryNode, peerNode, docId, chunkIndex, chunkData, chunkHash) {
    const primaryInstance = this.storageNodeRefs.get(primaryNode.id);
    if (primaryInstance && !primaryInstance.isKilled) {
      return primaryInstance.replicateToPeers([peerNode], docId, chunkIndex, chunkData, chunkHash)
        .then(results => results !== false);
    }
    // Fallback: coordinator stores directly on peer
    const result = await this.storeChunkOnNode(peerNode, docId, chunkIndex, chunkData, chunkHash);
    return result.success;
  }

  /**
   * Fetches a chunk from a storage node (for download proxy).
   */
  fetchChunkFromNode(node, docId, chunkIndex) {
    return new Promise((resolve, reject) => {
      const options = {
        hostname: node.host || 'localhost',
        port: node.httpPort,
        path: `/api/node/fetch-chunk?docId=${docId}&chunkIndex=${chunkIndex}`,
        method: 'GET'
      };

      const req = http.request(options, (res) => {
        if (res.statusCode !== 200) {
          return reject(new Error(`HTTP ${res.statusCode} from node ${node.id}`));
        }
        const chunks = [];
        res.on('data', chunk => chunks.push(chunk));
        res.on('end', () => resolve(Buffer.concat(chunks)));
      });

      req.on('error', reject);
      req.end();
    });
  }
}

module.exports = CoordinatorService;
