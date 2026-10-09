/**
 * Unit 2: Naming, Identifiers & Address Directory Service
 * Manages unique Node Identifiers, addresses, endpoints, and storage metrics.
 */

class NamingService {
  constructor() {
    this.nodes = new Map(); // nodeId -> NodeMetadata
  }

  registerNode(nodeInfo) {
    const nodeMetadata = {
      id: nodeInfo.id,
      name: nodeInfo.name || nodeInfo.id,
      host: nodeInfo.host || 'localhost',
      httpPort: nodeInfo.httpPort,
      wsPort: nodeInfo.wsPort,
      httpUrl: `http://${nodeInfo.host}:${nodeInfo.httpPort}`,
      wsUrl: `ws://${nodeInfo.host}:${nodeInfo.wsPort}`,
      capacity: nodeInfo.capacity || '10 GB',
      storageUsedBytes: 0,
      status: 'HEALTHY',
      registeredAt: new Date().toISOString(),
      lastHeartbeat: Date.now(),
      // FA-2 Extension Fields:
      lamportClock: 0,
      vectorClock: { [nodeInfo.id]: 0 }
    };

    this.nodes.set(nodeInfo.id, nodeMetadata);
    console.log(`[NamingService] Registered Node: ${nodeMetadata.id} (${nodeMetadata.httpUrl})`);
    return nodeMetadata;
  }

  updateHeartbeat(nodeId, metrics = {}) {
    const node = this.nodes.get(nodeId);
    if (node) {
      node.lastHeartbeat = Date.now();
      if (node.status === 'FAILED') {
        console.log(`[NamingService] Node ${nodeId} RECOVERED and re-joined cluster!`);
      }
      node.status = 'HEALTHY';
      if (metrics.storageUsedBytes !== undefined) {
        node.storageUsedBytes = metrics.storageUsedBytes;
      }
      if (metrics.lamportClock !== undefined) {
        node.lamportClock = metrics.lamportClock;
      }
    }
  }

  setNodeStatus(nodeId, status) {
    const node = this.nodes.get(nodeId);
    if (node) {
      node.status = status;
      console.log(`[NamingService] Node ${nodeId} status changed: HEALTHY → ${status}`);
    }
  }

  getNode(nodeId) {
    return this.nodes.get(nodeId);
  }

  getAllNodes() {
    return Array.from(this.nodes.values());
  }

  getHealthyNodes() {
    return this.getAllNodes().filter(node => node.status === 'HEALTHY');
  }

  selectNodesForReplication(replicationFactor = 2, excludeNodeId = null) {
    const healthyNodes = this.getHealthyNodes().filter(n => n.id !== excludeNodeId);
    // Sort by lowest storage used to balance load
    healthyNodes.sort((a, b) => a.storageUsedBytes - b.storageUsedBytes);
    return healthyNodes.slice(0, replicationFactor);
  }
}

module.exports = NamingService;
