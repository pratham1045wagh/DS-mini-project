/**
 * Unit 2: Fault Tolerance Engine
 * Periodically pings nodes and monitors heartbeats. Automatically detects node failures
 * and triggers failover / re-routing.
 */

class FaultDetector {
  constructor(namingService, config, onNodeStatusChange) {
    this.namingService = namingService;
    this.config = config;
    this.onNodeStatusChange = onNodeStatusChange; // Callback when a node fails or recovers
    this.timer = null;
  }

  start() {
    console.log(`[FaultDetector] Started monitoring heartbeat (Timeout: ${this.config.SYSTEM.HEARTBEAT_TIMEOUT}ms)`);
    this.timer = setInterval(() => this.checkNodeHealth(), this.config.SYSTEM.HEARTBEAT_INTERVAL);
  }

  stop() {
    if (this.timer) clearInterval(this.timer);
  }

  checkNodeHealth() {
    const now = Date.now();
    const allNodes = this.namingService.getAllNodes();

    allNodes.forEach(node => {
      const elapsed = now - node.lastHeartbeat;

      if (elapsed > this.config.SYSTEM.HEARTBEAT_TIMEOUT && node.status !== 'FAILED') {
        // Node missed heartbeats, mark as FAILED (HEALTHY → FAILED)
        this.namingService.setNodeStatus(node.id, 'FAILED');
        console.log(`[FaultDetector] Heartbeat Timeout: Node ${node.id} status transition HEALTHY → FAILED`);
        if (this.onNodeStatusChange) {
          this.onNodeStatusChange(node.id, 'FAILED', `No heartbeat received for ${(elapsed / 1000).toFixed(1)}s`);
        }
      }
    });
  }
}

module.exports = FaultDetector;
