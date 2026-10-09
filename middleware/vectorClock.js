/**
 * Vector Clock Implementation for Document Versioning & Concurrency Detection
 * Unit 3: Time and Global States / Unit 4: Distributed Consistency
 *
 * Vector comparison rules:
 *  - V_A <= V_B iff for all k: V_A[k] <= V_B[k]
 *  - V_A < V_B  iff V_A <= V_B and exists k: V_A[k] < V_B[k] (Version A happened BEFORE Version B)
 *  - V_B < V_A  iff V_B <= V_A and exists k: V_B[k] < V_A[k] (Version B happened BEFORE Version A)
 *  - V_A == V_B iff for all k: V_A[k] == V_B[k]
 *  - Otherwise: V_A || V_B (CONCURRENT / CONFLICT DETECTED)
 */

class VectorClock {
  /**
   * Initializes a vector clock given an optional object or node ID list.
   * @param {object|string[]} initial
   */
  static create(initial = {}) {
    if (Array.isArray(initial)) {
      const clock = {};
      for (const node of initial) {
        clock[node] = 0;
      }
      return clock;
    }
    return { ...initial };
  }

  /**
   * Increments the vector clock component for a specific node/actor.
   * @param {object} clock - Existing vector clock
   * @param {string} nodeId - Node or user identifier
   * @returns {object} A new vector clock with the incremented component
   */
  static increment(clock = {}, nodeId) {
    const next = { ...clock };
    next[nodeId] = (next[nodeId] || 0) + 1;
    return next;
  }

  /**
   * Merges two vector clocks by taking the component-wise maximum.
   * @param {object} clockA
   * @param {object} clockB
   * @returns {object}
   */
  static merge(clockA = {}, clockB = {}) {
    const allKeys = new Set([...Object.keys(clockA || {}), ...Object.keys(clockB || {})]);
    const merged = {};
    for (const key of allKeys) {
      merged[key] = Math.max(clockA[key] || 0, clockB[key] || 0);
    }
    return merged;
  }

  /**
   * Compares two vector clocks.
   * @param {object} vA - Clock of Version A
   * @param {object} vB - Clock of Version B
   * @returns {'BEFORE' | 'AFTER' | 'EQUAL' | 'CONCURRENT'}
   */
  static compare(vA = {}, vB = {}) {
    const keys = new Set([...Object.keys(vA || {}), ...Object.keys(vB || {})]);

    let aLeB = true; // Is vA <= vB?
    let bLeA = true; // Is vB <= vA?

    for (const k of keys) {
      const aVal = vA[k] || 0;
      const bVal = vB[k] || 0;

      if (aVal > bVal) aLeB = false;
      if (bVal > aVal) bLeA = false;
    }

    if (aLeB && bLeA) return 'EQUAL';
    if (aLeB && !bLeA) return 'BEFORE'; // vA happened-before vB
    if (bLeA && !aLeB) return 'AFTER';  // vB happened-before vA
    return 'CONCURRENT';               // Conflict detected: vA || vB
  }

  /**
   * Determines if two clocks represent concurrent updates (conflict).
   * @param {object} vA
   * @param {object} vB
   * @returns {boolean}
   */
  static isConcurrent(vA, vB) {
    return VectorClock.compare(vA, vB) === 'CONCURRENT';
  }

  /**
   * Pretty-prints a vector clock for logs / dashboard display.
   * e.g. "{ node_1: 3, node_2: 2, node_3: 4 }"
   * @param {object} clock
   * @returns {string}
   */
  static format(clock = {}) {
    if (!clock || Object.keys(clock).length === 0) return '{}';
    return JSON.stringify(clock);
  }
}

module.exports = VectorClock;
