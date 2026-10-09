/**
 * Deadlock Detection & Wait-For Graph (WFG) Engine
 * Unit 4: Distributed Deadlock Detection
 *
 * Implements cycle detection in a directed Wait-For Graph:
 *  - Vertices: Users / Transactions (e.g. User A, User B)
 *  - Directed Edge: U_i -> U_j indicates that U_i is blocked waiting for
 *    a resource currently held by U_j.
 *  - Cycle condition: If there is a directed cycle (e.g., A -> B -> A),
 *    a distributed deadlock exists.
 */

class DeadlockDetector {
  constructor(onDeadlockDetected = null) {
    // waitGraph: Map<userId, Map<heldByUserId, { resourceId, resourceName, timestamp }>>
    this.waitGraph = new Map();
    // activeLocksRef: optional reference to lock map
    this.onDeadlockDetected = onDeadlockDetected;
    this.deadlockHistory = [];
  }

  /**
   * Registers that a user is waiting for a resource held by another user.
   * Creates an edge: waitingUserId -> holderUserId
   */
  addDependency(waitingUserId, waitingUserName, holderUserId, holderUserName, resourceId, resourceName) {
    if (waitingUserId === holderUserId) return null; // No self-dependency

    if (!this.waitGraph.has(waitingUserId)) {
      this.waitGraph.set(waitingUserId, new Map());
    }

    const userWaitMap = this.waitGraph.get(waitingUserId);
    userWaitMap.set(holderUserId, {
      waitingUserId,
      waitingUserName: waitingUserName || waitingUserId,
      holderUserId,
      holderUserName: holderUserName || holderUserId,
      resourceId,
      resourceName: resourceName || resourceId,
      timestamp: Date.now()
    });

    console.log(`[DeadlockDetector] Wait-For Edge added: ${waitingUserId} ➔ ${holderUserId} (Resource: ${resourceName || resourceId})`);

    // Check for cycles immediately
    return this.detectDeadlock();
  }

  /**
   * Removes dependency edge when a lock is granted, cancelled, or released.
   */
  removeDependency(waitingUserId, holderUserId, resourceId) {
    if (this.waitGraph.has(waitingUserId)) {
      const userMap = this.waitGraph.get(waitingUserId);
      if (holderUserId) {
        userMap.delete(holderUserId);
      } else {
        // Remove all edges related to this resource
        for (const [targetUser, data] of userMap.entries()) {
          if (!resourceId || data.resourceId === resourceId) {
            userMap.delete(targetUser);
          }
        }
      }
      if (userMap.size === 0) {
        this.waitGraph.delete(waitingUserId);
      }
    }
  }

  /**
   * Clears all wait edges for a user (e.g., when they disconnect or lock released).
   */
  clearUserDependencies(userId) {
    this.waitGraph.delete(userId);
    // Also remove any other users waiting on this user
    for (const [waitingId, map] of this.waitGraph.entries()) {
      map.delete(userId);
      if (map.size === 0) {
        this.waitGraph.delete(waitingId);
      }
    }
  }

  /**
   * Performs Depth-First Search (DFS) to detect directed cycles in the Wait-For Graph.
   * @returns {{ hasDeadlock: boolean, cycle: string[], cycleDetails: object[], edgeList: object[] }}
   */
  detectDeadlock() {
    const visited = new Set();
    const recStack = new Set();
    const parentMap = new Map();
    let detectedCycle = null;

    const dfs = (currUser) => {
      visited.add(currUser);
      recStack.add(currUser);

      const neighbors = this.waitGraph.get(currUser);
      if (neighbors) {
        for (const [neighborUser, edgeInfo] of neighbors.entries()) {
          if (!visited.has(neighborUser)) {
            parentMap.set(neighborUser, { from: currUser, edge: edgeInfo });
            if (dfs(neighborUser)) return true;
          } else if (recStack.has(neighborUser)) {
            // Cycle detected! Reconstruct cycle path
            const cycle = [neighborUser];
            const cycleEdges = [edgeInfo];
            let p = currUser;

            while (p && p !== neighborUser) {
              cycle.unshift(p);
              const parentInfo = parentMap.get(p);
              if (parentInfo) {
                cycleEdges.unshift(parentInfo.edge);
                p = parentInfo.from;
              } else {
                break;
              }
            }
            cycle.unshift(neighborUser); // complete visual loop

            detectedCycle = {
              cycle,
              edges: cycleEdges
            };
            return true;
          }
        }
      }

      recStack.delete(currUser);
      return false;
    };

    for (const userId of this.waitGraph.keys()) {
      if (!visited.has(userId)) {
        if (dfs(userId)) break;
      }
    }

    const edgeList = this.getAllEdges();

    if (detectedCycle) {
      const result = {
        hasDeadlock: true,
        isDeadlocked: true,
        detectedAt: new Date().toISOString(),
        cycle: detectedCycle.cycle,
        cycleEdges: detectedCycle.edges,
        edgeList,
        summary: `DEADLOCK DETECTED: Cycle [${detectedCycle.cycle.join(' ➔ ')}]`
      };

      console.warn(`[DeadlockDetector] ⚠️ ${result.summary}`);
      this.deadlockHistory.unshift(result);
      if (this.deadlockHistory.length > 20) this.deadlockHistory.pop();

      if (typeof this.onDeadlockDetected === 'function') {
        this.onDeadlockDetected(result);
      }

      return result;
    }

    return {
      hasDeadlock: false,
      isDeadlocked: false,
      cycle: [],
      cycleEdges: [],
      edgeList,
      summary: 'No deadlock detected in Wait-For Graph.'
    };
  }

  /**
   * Returns all active directed edges in the WFG.
   */
  getAllEdges() {
    const edges = [];
    for (const [fromUser, map] of this.waitGraph.entries()) {
      for (const [toUser, info] of map.entries()) {
        edges.push({
          from: fromUser,
          fromName: info.waitingUserName || fromUser,
          to: toUser,
          toName: info.holderUserName || toUser,
          resourceId: info.resourceId,
          resourceName: info.resourceName,
          timestamp: info.timestamp
        });
      }
    }
    return edges;
  }

  /**
   * Resolves an active deadlock using a predefined policy:
   * Policy: 'abort_youngest' (breaks edge with latest timestamp by releasing/cancelling wait)
   */
  resolveDeadlock(lockManager = null) {
    const status = this.detectDeadlock();
    if (!status.hasDeadlock) {
      return { resolved: false, message: 'No active deadlock to resolve.' };
    }

    const cycleEdges = status.cycleEdges || [];
    if (cycleEdges.length === 0) {
      this.waitGraph.clear();
      return { resolved: true, message: 'Wait-For Graph cleared.' };
    }

    // Select victim: edge with newest timestamp or first edge in cycle
    const victimEdge = cycleEdges[cycleEdges.length - 1];
    this.removeDependency(victimEdge.waitingUserId, victimEdge.holderUserId, victimEdge.resourceId);

    // If lock manager provided, force release or cancel wait
    if (lockManager && typeof lockManager.abortWaitingRequest === 'function') {
      lockManager.abortWaitingRequest(victimEdge.resourceId, victimEdge.waitingUserId);
    }

    const message = `Deadlock resolved by aborting wait of '${victimEdge.waitingUserName || victimEdge.waitingUserId}' for resource '${victimEdge.resourceName}' held by '${victimEdge.holderUserName || victimEdge.holderUserId}'.`;
    console.log(`[DeadlockDetector] ✅ ${message}`);

    return {
      resolved: true,
      hasDeadlock: false,
      isDeadlocked: false,
      victim: victimEdge,
      message,
      remainingStatus: this.detectDeadlock()
    };
  }

  /**
   * Simulates a classic two-resource circular wait condition for live demo:
   * User A holds Doc X and waits for Doc Y (held by User B).
   * User B holds Doc Y and waits for Doc X (held by User A).
   */
  simulateDeadlockScenario(userA = { id: 'user_a', name: 'User A' }, userB = { id: 'user_b', name: 'User B' }, docX = { id: 'doc_alpha', name: 'Project_Alpha.pdf' }, docY = { id: 'doc_beta', name: 'Design_Beta.pdf' }) {
    this.clearAll();

    // Edge 1: User A waits for Doc Y held by User B
    this.addDependency(userA.id, userA.name, userB.id, userB.name, docY.id, docY.name);

    // Edge 2: User B waits for Doc X held by User A (Forms Cycle: A -> B -> A)
    const result = this.addDependency(userB.id, userB.name, userA.id, userA.name, docX.id, docX.name);

    return result;
  }

  clearAll() {
    this.waitGraph.clear();
  }
}

module.exports = DeadlockDetector;
