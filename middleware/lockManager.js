/**
 * Distributed Mutual Exclusion & Lock Manager
 * Unit 4: Distributed Mutual Exclusion
 *
 * Implements coordinator-managed distributed locks for documents:
 *  - Exclusive locking per document
 *  - Lease expiration / timeout recovery (auto-release)
 *  - Waiter tracking integrated with DeadlockDetector (Wait-For Graph)
 *  - Broadcasts lock state changes to all dashboard clients
 */

class LockManager {
  /**
   * @param {object} deadlockDetector - Instance of DeadlockDetector
   * @param {function} onEvent - Broadcast callback (type, payload)
   * @param {number} defaultTimeoutMs - Default lock lease (default 60 seconds)
   */
  constructor(deadlockDetector = null, onEvent = null, defaultTimeoutMs = 60000) {
    this.deadlockDetector = deadlockDetector;
    this.onEvent = onEvent;
    this.defaultTimeoutMs = defaultTimeoutMs;

    // docId -> { docId, docName, lockedBy, lockedByName, lockedAt, expiresAt, timeoutTimer }
    this.locks = new Map();

    // docId -> Array<{ userId, userName, requestedAt }>
    this.waitQueues = new Map();
  }

  /**
   * Attempts to acquire an exclusive lock on a document.
   * @param {string} docId
   * @param {string} docName
   * @param {string} userId
   * @param {string} userName
   * @param {number} [timeoutMs]
   * @returns {{ success: boolean, lock?: object, error?: string, deadlock?: boolean }}
   */
  acquireLock(docId, docName, userId, userName, timeoutMs = this.defaultTimeoutMs) {
    const existing = this.locks.get(docId);

    // Case 1: Already held by this user -> refresh lease
    if (existing && existing.lockedBy === userId) {
      clearTimeout(existing.timeoutTimer);
      existing.expiresAt = Date.now() + timeoutMs;
      existing.timeoutTimer = setTimeout(() => this._onLockTimeout(docId), timeoutMs);
      return { success: true, lock: this._serializeLock(existing), refreshed: true };
    }

    // Case 2: Held by another user -> check deadlock and reject with WAITING status
    if (existing) {
      // Register wait dependency with Deadlock Detector
      let deadlockResult = null;
      if (this.deadlockDetector) {
        deadlockResult = this.deadlockDetector.addDependency(
          userId,
          userName || userId,
          existing.lockedBy,
          existing.lockedByName || existing.lockedBy,
          docId,
          docName
        );
      }

      // Add to wait queue
      if (!this.waitQueues.has(docId)) {
        this.waitQueues.set(docId, []);
      }
      const queue = this.waitQueues.get(docId);
      if (!queue.find(w => w.userId === userId)) {
        queue.push({ userId, userName: userName || userId, requestedAt: Date.now() });
      }

      const hasDeadlock = deadlockResult && deadlockResult.hasDeadlock;

      this._emit('LOCK_DENIED', {
        docId,
        docName,
        attemptedBy: userId,
        attemptedByName: userName,
        heldBy: existing.lockedBy,
        heldByName: existing.lockedByName,
        deadlockDetected: hasDeadlock
      });

      return {
        success: false,
        status: 'LOCKED',
        error: hasDeadlock
          ? `DEADLOCK DETECTED! Circular wait cycle detected with ${existing.lockedByName}`
          : `ACCESS DENIED / WAITING FOR LOCK: Document '${docName}' is currently locked by ${existing.lockedByName || existing.lockedBy}`,
        currentOwner: existing.lockedBy,
        lockedBy: existing.lockedBy,
        lockedByName: existing.lockedByName,
        deadlock: hasDeadlock,
        lock: this._serializeLock(existing)
      };
    }

    // Case 3: Document is free -> grant lock
    const lockObj = {
      docId,
      docName: docName || docId,
      lockedBy: userId,
      lockedByName: userName || userId,
      lockedAt: Date.now(),
      expiresAt: Date.now() + timeoutMs,
      timeoutTimer: null
    };

    lockObj.timeoutTimer = setTimeout(() => this._onLockTimeout(docId), timeoutMs);
    this.locks.set(docId, lockObj);

    // Remove any waiting edge for this user on this document
    if (this.deadlockDetector) {
      this.deadlockDetector.removeDependency(userId, null, docId);
    }
    if (this.waitQueues.has(docId)) {
      const q = this.waitQueues.get(docId).filter(w => w.userId !== userId);
      this.waitQueues.set(docId, q);
    }

    console.log(`[LockManager] 🔒 Lock acquired on '${lockObj.docName}' (${docId}) by '${userId}' (Lease: ${timeoutMs / 1000}s)`);

    this._emit('LOCK_ACQUIRED', this._serializeLock(lockObj));

    return {
      success: true,
      lock: this._serializeLock(lockObj)
    };
  }

  /**
   * Releases an active document lock.
   * @param {string} docId
   * @param {string} userId
   * @param {boolean} [force=false]
   * @returns {{ success: boolean, released?: boolean, error?: string }}
   */
  releaseLock(docId, userId, force = false) {
    const lock = this.locks.get(docId);
    if (!lock) {
      return { success: true, released: false, message: 'Document was not locked' };
    }

    if (!force && lock.lockedBy !== userId) {
      return {
        success: false,
        error: `403 Forbidden: You cannot release a lock owned by ${lock.lockedByName || lock.lockedBy}`
      };
    }

    clearTimeout(lock.timeoutTimer);
    this.locks.delete(docId);

    // Clean up deadlock graph edges
    if (this.deadlockDetector) {
      this.deadlockDetector.clearUserDependencies(lock.lockedBy);
    }

    console.log(`[LockManager] 🔓 Lock released on '${lock.docName}' (${docId}) by '${userId}' (force=${force})`);

    this._emit('LOCK_RELEASED', {
      docId,
      docName: lock.docName,
      releasedBy: userId,
      previousOwner: lock.lockedBy
    });

    // Check if next waiter is available in queue
    const queue = this.waitQueues.get(docId);
    if (queue && queue.length > 0) {
      const nextWaiter = queue.shift();
      console.log(`[LockManager] Next in line for '${lock.docName}' is '${nextWaiter.userName}'`);
      this._emit('LOCK_AVAILABLE', {
        docId,
        docName: lock.docName,
        eligibleUser: nextWaiter.userId,
        eligibleUserName: nextWaiter.userName
      });
    }

    return { success: true, released: true, docId, docName: lock.docName };
  }

  /**
   * Checks if a document is locked by another user.
   */
  isLockedByOther(docId, userId) {
    const lock = this.locks.get(docId);
    if (!lock) return false;
    return lock.lockedBy !== userId;
  }

  /**
   * Returns current lock for a document.
   */
  getLock(docId) {
    const lock = this.locks.get(docId);
    return lock ? this._serializeLock(lock) : null;
  }

  /**
   * Returns all active locks.
   */
  getAllLocks() {
    return Array.from(this.locks.values()).map(l => this._serializeLock(l));
  }

  /**
   * Aborts a waiting request for deadlock resolution.
   */
  abortWaitingRequest(docId, userId) {
    if (this.waitQueues.has(docId)) {
      const q = this.waitQueues.get(docId).filter(w => w.userId !== userId);
      this.waitQueues.set(docId, q);
    }
    if (this.deadlockDetector) {
      this.deadlockDetector.removeDependency(userId, null, docId);
    }
  }

  _onLockTimeout(docId) {
    const lock = this.locks.get(docId);
    if (!lock) return;

    this.locks.delete(docId);
    if (this.deadlockDetector) {
      this.deadlockDetector.clearUserDependencies(lock.lockedBy);
    }

    console.warn(`[LockManager] ⏱️ Lock on '${lock.docName}' (${docId}) EXPIRED after lease timeout`);

    this._emit('LOCK_EXPIRED', {
      docId,
      docName: lock.docName,
      previousOwner: lock.lockedBy,
      reason: 'Lease timeout exceeded (Automatic recovery)'
    });
  }

  _serializeLock(lock) {
    return {
      docId: lock.docId,
      docName: lock.docName,
      lockedBy: lock.lockedBy,
      ownerId: lock.lockedBy,
      lockedByName: lock.lockedByName,
      ownerName: lock.lockedByName,
      lockedAt: lock.lockedAt,
      expiresAt: lock.expiresAt,
      remainingSec: Math.max(0, Math.round((lock.expiresAt - Date.now()) / 1000))
    };
  }

  _emit(type, payload) {
    if (typeof this.onEvent === 'function') {
      try {
        this.onEvent(type, payload);
      } catch (e) {
        console.warn('[LockManager] Failed to emit event:', e.message);
      }
    }
  }
}

module.exports = LockManager;
