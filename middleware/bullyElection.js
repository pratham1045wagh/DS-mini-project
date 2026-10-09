/**
 * Bully Algorithm Leader Election Engine
 * Unit 4: Distributed Coordination & Election Algorithms
 *
 * Bully Algorithm Rules:
 *  1. Process P detects coordinator/leader failure.
 *  2. P sends ELECTION message to all processes with higher IDs/ranks.
 *  3. If no higher process responds within timeout, P wins and sends COORDINATOR broadcast.
 *  4. If a higher process responds with ANSWER/OK, that higher process takes over.
 *  5. If a previously failed high-priority process revives, it starts an election
 *     and "bullies" lower nodes to reclaim leadership.
 */

class BullyElection {
  /**
   * @param {object} namingService - Reference to NamingService
   * @param {function} onEvent - Callback to broadcast WebSocket events
   */
  constructor(namingService, onEvent = null) {
    this.namingService = namingService;
    this.onEvent = onEvent;

    // Node priority / rank mapping
    this.ranks = {
      'Coordinator': 100,
      'coordinator': 100,
      'node_4': 4,
      'node_3': 3,
      'node_2': 2,
      'node_1': 1
    };

    // State
    this.isCoordinatorAlive = true;
    this.currentLeader = {
      id: 'Coordinator',
      name: 'Master Coordinator',
      rank: 100,
      role: 'COORDINATOR',
      electedAt: new Date().toISOString()
    };
    this.previousLeader = null;
    this.electionStatus = 'COMPLETED'; // 'IDLE' | 'IN_PROGRESS' | 'COMPLETED'
    this.electionLogs = [];
  }

  logElection(text) {
    const entry = {
      id: `el_${Date.now()}_${Math.random().toString(36).slice(2, 6)}`,
      timestamp: new Date().toLocaleTimeString(),
      text
    };
    this.electionLogs.unshift(entry);
    if (this.electionLogs.length > 100) this.electionLogs.pop();
    console.log(`[ELECTION] ${text}`);

    const payload = Object.assign({ log: entry }, this.getStatus());
    this._emit('LEADER_ELECTION_EVENT', payload);

    return entry;
  }

  /**
   * Returns election state summary.
   */
  getStatus() {
    return {
      leaderId: this.currentLeader ? this.currentLeader.id : 'Coordinator',
      leaderName: this.currentLeader ? this.currentLeader.name : 'Master Coordinator',
      previousLeaderId: this.previousLeader ? (this.previousLeader.name || this.previousLeader.id) : 'None',
      coordinatorStatus: this.isCoordinatorAlive ? 'HEALTHY' : 'FAILED',
      isCoordinatorAlive: this.isCoordinatorAlive,
      currentLeader: this.currentLeader,
      previousLeader: this.previousLeader,
      electionStatus: this.electionStatus,
      ranks: this.ranks,
      recentLogs: this.electionLogs.slice(0, 30)
    };
  }

  /**
   * Initiates Bully leader election.
   * Can be triggered when coordinator fails or via manual trigger.
   * @param {string} [initiatorId]
   */
  async startElection(initiatorId = null) {
    this.electionStatus = 'IN_PROGRESS';
    this.logElection('Election started');

    // Get list of currently operational storage nodes (not failed)
    const allNodes = this.namingService.getAllNodes();
    const healthyNodes = allNodes.filter(n => n.status !== 'FAILED');

    if (healthyNodes.length === 0 && !this.isCoordinatorAlive) {
      this.electionStatus = 'FAILED';
      this.logElection('ERROR: No healthy nodes available to participate in election!');
      return this.getStatus();
    }

    // Default initiator to lowest healthy node if not specified
    let initiator = healthyNodes.find(n => n.id === initiatorId);
    if (!initiator) {
      initiator = healthyNodes[0] || { id: 'node_1', name: 'Storage Node Alpha' };
    }

    this.logElection(`${initiator.name} (${initiator.id}) initiated election`);

    // Step through each participating node
    for (const node of healthyNodes) {
      this.logElection(`${node.name} (${node.id}) participating [Rank: ${this.ranks[node.id] || 0}]`);
    }

    // Find highest rank among available candidates
    let highestNode = null;
    let highestRank = -1;

    // If coordinator is alive, coordinator has rank 100
    if (this.isCoordinatorAlive) {
      highestNode = {
        id: 'Coordinator',
        name: 'Master Coordinator',
        rank: 100,
        role: 'COORDINATOR'
      };
      highestRank = 100;
    } else {
      for (const node of healthyNodes) {
        const r = this.ranks[node.id] || 0;
        if (r > highestRank) {
          highestRank = r;
          highestNode = {
            id: node.id,
            name: node.name,
            rank: r,
            role: 'PROXY COORDINATOR / LEADER'
          };
        }
      }
    }

    if (!highestNode) {
      this.electionStatus = 'FAILED';
      this.logElection('Election completed: No valid leader elected');
      return this.getStatus();
    }

    // Bully algorithm: Higher nodes answered, highest node declares victory!
    this.logElection(`${highestNode.name} (${highestNode.id}) elected as leader`);
    this.logElection(`Leader announcement broadcast from ${highestNode.name} to all cluster nodes`);

    this.previousLeader = this.currentLeader;
    this.currentLeader = {
      id: highestNode.id,
      name: highestNode.name,
      rank: highestNode.rank,
      role: highestNode.role,
      electedAt: new Date().toISOString()
    };
    this.electionStatus = 'COMPLETED';

    const payload = this.getStatus();
    this._emit('LEADER_CHANGED', payload);

    return this.getStatus();
  }

  /**
   * Simulates coordinator failure:
   * Sets isCoordinatorAlive = false and starts election among storage nodes.
   */
  async simulateCoordinatorFailure() {
    this.isCoordinatorAlive = false;
    this.logElection('Coordinator FAILED (Simulated Failure)');
    this._emit('COORDINATOR_FAILED', this.getStatus());
    return this.startElection();
  }

  /**
   * Simulates coordinator revival:
   * Reclaims leadership using Bully priority (Rank 100).
   */
  async simulateCoordinatorRevival() {
    this.isCoordinatorAlive = true;
    this.logElection('Master Coordinator REVIVED (Rank 100)');
    this.logElection('Coordinator asserting Bully priority over storage nodes...');
    this._emit('COORDINATOR_REVIVED', this.getStatus());
    return this.startElection('coordinator');
  }

  _emit(type, payload) {
    if (typeof this.onEvent === 'function') {
      try {
        this.onEvent(type, payload);
      } catch (e) {
        console.warn('[BullyElection] Failed to emit event:', e.message);
      }
    }
  }
}

module.exports = BullyElection;
