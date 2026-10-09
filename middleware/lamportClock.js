/**
 * Lamport Logical Clock Implementation
 * Unit 3: Time and Global States in Distributed Systems
 *
 * Rules:
 *  1. Before generating an internal/send event, increment local clock: L = L + 1
 *  2. Attach local clock L to outgoing messages
 *  3. On receiving a message with timestamp L_msg, update: L = max(L, L_msg) + 1
 */

class LamportClock {
  /**
   * @param {string} nodeId - Identifier of the node/coordinator hosting this clock
   * @param {number} initialTime - Initial clock value (default 0)
   */
  constructor(nodeId = 'coordinator', initialTime = 0) {
    this.nodeId = nodeId;
    this.clock = initialTime;
  }

  /**
   * Increments the clock for a local event.
   * L = L + 1
   * @param {string} [eventDescription]
   * @returns {number} The updated clock value
   */
  tick(eventDescription = '') {
    this.clock += 1;
    return this.clock;
  }

  /**
   * Updates local clock upon receiving a message with a remote timestamp.
   * L = max(L, receivedTimestamp) + 1
   * @param {number} receivedTimestamp
   * @param {string} [eventDescription]
   * @returns {number} The updated clock value
   */
  update(receivedTimestamp, eventDescription = '') {
    const remote = typeof receivedTimestamp === 'number' && !isNaN(receivedTimestamp)
      ? receivedTimestamp
      : 0;
    this.clock = Math.max(this.clock, remote) + 1;
    return this.clock;
  }

  /**
   * Returns current clock value.
   * @returns {number}
   */
  getTime() {
    return this.clock;
  }

  /**
   * Resets or sets the clock to a specific value.
   * @param {number} value
   */
  setTime(value) {
    this.clock = value;
  }
}

/**
 * Distributed Event Timeline Manager
 * Maintains an in-memory chronological timeline of all Lamport-timestamped events.
 */
class DistributedEventTimeline {
  constructor(maxSize = 200) {
    this.maxSize = maxSize;
    this.events = [];
  }

  /**
   * Records an event in the distributed timeline.
   * @param {object} param
   * @param {number} param.lamportClock - L value
   * @param {string} param.nodeOrUser   - e.g. "User A", "node_1", "Coordinator"
   * @param {string} param.operation    - UPLOAD, DOWNLOAD, UPDATE, SHARE, DELETE, REPLICATION, LOCK, ELECTION, RECOVERY, SYNC
   * @param {string} param.document     - fileName or docId or "Cluster"
   * @param {string} param.event        - Detailed human-readable description
   * @returns {object} The recorded timeline entry
   */
  record({ lamportClock, nodeOrUser, operation, document = '—', event }) {
    const entry = {
      id: `ev_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`,
      lamportClock: lamportClock || 0,
      lamportTimestamp: lamportClock || 0,
      nodeOrUser: nodeOrUser || 'System',
      operation: (operation || 'OPERATION').toUpperCase(),
      document: document || '—',
      event: event || '',
      eventDetails: event || '',
      realTime: new Date().toLocaleTimeString(),
      isoTimestamp: new Date().toISOString(),
      timestamp: new Date().toISOString()
    };

    this.events.unshift(entry); // newest first for display
    if (this.events.length > this.maxSize) {
      this.events.pop();
    }

    return entry;
  }

  getEvents(limit = 100) {
    return this.events.slice(0, limit);
  }

  clear() {
    this.events = [];
  }
}

module.exports = {
  LamportClock,
  DistributedEventTimeline
};
