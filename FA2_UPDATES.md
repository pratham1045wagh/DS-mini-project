# FA-2 Distributed Systems Updates & Architectural Reference

This document outlines all features and enhancements added to the **Distributed Collaborative Document Management & Synchronization System** for the FA-2 milestone covering **Unit 3 (Time, Global State, Synchronization)** and **Unit 4 (Coordination, Agreement, Fault Tolerance)**.

---

# 1. Lamport Logical Clock

## What was added
A scalar logical clock system maintaining a monotonically increasing integer timestamp across the Master Coordinator and all 4 storage nodes.

## Why it was added
Physical clocks in distributed systems drift and cannot guarantee a strict ordering of events without expensive synchronization protocols (like NTP). Lamport clocks provide partial ordering based on the "happened-before" relation ($\rightarrow$).

## Files changed
* `middleware/lamportClock.js`
* `middleware/coordinator.js`
* `storage_nodes/storageNode.js`
* `public/js/app.js`

## Backend implementation
Each node maintains a counter $L$. On internal events:
$$L \leftarrow L + 1$$
When sending a message, the message includes $L$. On receiving a message with timestamp $L_{\text{msg}}$:
$$L \leftarrow \max(L, L_{\text{msg}}) + 1$$

## Frontend implementation
Five dynamic clock chips display live values in the **Distributed Event Timeline** row (`clockCoordinator`, `clockNode1`, `clockNode2`, `clockNode3`, `clockNode4`).

## JSON-RPC method
* `rpc.get_logical_clock`: Returns `{ coordinator: L, nodes: { node_1: L1, ... } }`.

## WebSocket event
* `CLOCKS_UPDATED`: Broadcasts updated clock values to all connected clients.

## What the user sees
Real-time clock values ticking upwards whenever any upload, edit, lock, or replication event occurs.

## How to test it
Upload a file or click view on a document; watch the clock values increment.

## Expected result
All clocks increase monotonically without ever moving backward.

## Unit 3 / Unit 4 concept
**Unit 3:** Logical Time and Lamport's Happened-Before Relation ($e_i \rightarrow e_j$).

---

# 2. Distributed Event Timeline

## What was added
A persistent chronological ledger sequencing distributed events across the cluster using Lamport timestamps.

## Why it was added
Allows operators to observe global state transitions and verify event causality across multiple independent storage nodes.

## Files changed
* `middleware/lamportClock.js` (`DistributedEventTimeline` class)
* `middleware/coordinator.js`
* `public/index.html`, `public/js/app.js`

## Backend implementation
Ring buffer keeping the latest 200 distributed events, each containing `lamportClock`, `nodeOrUser`, `operation`, `document`, and `event` description.

## Frontend implementation
A live table displaying the sequence of events ordered by Lamport timestamp.

## JSON-RPC method
* `rpc.get_timeline`: Retrieves recorded timeline history.
* REST: `GET /api/timeline`.

## WebSocket event
* `TIMELINE_EVENT`: Dispatched whenever a significant distributed operation completes.

## What the user sees
A timeline table showing `[L=12] UPLOAD | user_a | fa2_test.txt | Replicated to [node_1, node_3]`.

## How to test it
Perform actions (upload, lock, download, election) and inspect the timeline rows.

## Expected result
Events are ordered by their logical timestamp rather than system wall-clock time.

## Unit 3 / Unit 4 concept
**Unit 3:** Global States, Distributed Event Ordering, and Observation.

---

# 3. Vector Clock

## What was added
A multi-dimensional vector clock mapping process/node IDs to integer counters (`{ node_1: c1, node_2: c2, ... }`).

## Why it was added
Scalar Lamport clocks cannot distinguish between causally ordered events ($A \rightarrow B$) and concurrent events ($A \parallel B$). Vector clocks capture causal history precisely.

## Files changed
* `middleware/vectorClock.js`
* `middleware/coordinator.js`
* `public/js/app.js`

## Backend implementation
Methods:
* `increment(clock, nodeId)`
* `compare(clockA, clockB)`: Returns `'LESS'`, `'GREATER'`, `'EQUAL'`, or `'CONCURRENT'`.
* `merge(clockA, clockB)`: Component-wise maximum:
  $$V_{\text{merged}}[i] = \max(V_A[i], V_B[i])$$

## Frontend implementation
Vector clock tags are displayed in the Document Catalog table, Version History modal, and Conflict Resolution modal.

## JSON-RPC method
* REST: `GET /api/documents/:docId/versions` returns vector clocks for each version.

## WebSocket event
* `DOCUMENT_UPDATED`: Contains updated vector clock.

## What the user sees
Badges like `{"node_1":1, "node_2":2}` displayed next to version tags.

## How to test it
Edit a document and save; view the vector clock incrementing for the primary storage node.

## Expected result
The vector clock updates accurately reflecting the node that accepted the update.

## Unit 3 / Unit 4 concept
**Unit 3:** Vector Clocks and Causal Consistency.

---

# 4. Concurrent Update Detection (Vector Clocks)

## What was added
Automatic detection of branching document modifications without causal relationship ($V_A \parallel V_B$).

## Why it was added
When multiple collaborators work in disconnected or concurrent sessions, updates can diverge. The system must detect this rather than silently overwriting data.

## Files changed
* `middleware/vectorClock.js`
* `middleware/coordinator.js`
* `public/js/app.js`

## Backend implementation
When an update is submitted with `parentVectorClock`, the coordinator checks:
```javascript
VectorClock.isConcurrent(parentVectorClock, doc.vectorClock)
```
If true, the document enters a `conflict = true` state with both branches preserved.

## Frontend implementation
A pulsing red alert banner appears: `⚠️ CONFLICT DETECTED`, and a red `CONFLICT` tag appears in the document catalog.

## JSON-RPC method
* REST: `POST /api/documents/simulate-conflict` or `POST /api/documents/:docId/update`.

## WebSocket event
* `DOCUMENT_CONFLICT`: Notifies all connected dashboards of the concurrent branches.

## What the user sees
Prominent red notification alerting the operator that two branches exist concurrently.

## How to test it
Open Edit Document modal, click **Simulate Concurrent Update (Trigger Conflict)**.

## Expected result
System halts auto-overwriting and prompts operator for conflict resolution.

## Unit 3 / Unit 4 concept
**Unit 3:** Concurrency Detection and Causal Precedence.

---

# 5. Document Versioning

## What was added
An immutable historical version store tracking every revision of a document.

## Why it was added
Preserves full audit trails, allows historical rollback, and prevents data loss during concurrent collaborative sessions.

## Files changed
* `middleware/coordinator.js`
* `public/index.html`, `public/js/app.js`

## Backend implementation
`doc.versions` array stores metadata objects containing:
* `version`: Incremental integer ($1, 2, 3, \dots$).
* `updatedBy`: User ID who authored the version.
* `lamportTimestamp`: Logical time of update.
* `vectorClock`: Vector clock at time of commit.
* `checksum`: SHA-256 digest of chunk data.
* `updatedAt`: ISO timestamp.

## Frontend implementation
Clicking the **Version History** button (<i class="fa-solid fa-clock-rotate-left"></i>) opens a modal listing all historical versions.

## JSON-RPC method
* `rpc.get_document_versions`
* REST: `GET /api/documents/:docId/versions`

## WebSocket event
* `DOCUMENT_UPDATED`

## What the user sees
A modal table showing Version number, Author, Lamport timestamp, Vector clock, and SHA-256 hash.

## How to test it
Update a document, then click the Version History button.

## Expected result
All previous versions remain listed and intact.

## Unit 3 / Unit 4 concept
**Unit 3 & 4:** Immutable Logs and State Reification.

---

# 6. Conflict Detection & Conflict Resolution

## What was added
Interactive multi-branch comparison and reconciliation interface for concurrent edits.

## Why it was added
Provides distributed systems reconciliation: choosing Version A, choosing Version B, or merging changes with vector clock component-wise max.

## Files changed
* `middleware/coordinator.js`
* `public/index.html`, `public/js/app.js`

## Backend implementation
`resolveDocumentConflict(docId, choice, mergedContent, user)`:
* Choice `CHOOSE_A` / `A`: Discards Branch B, keeps Branch A.
* Choice `CHOOSE_B` / `B`: Adopts Branch B content and increments version.
* Choice `MERGE`: Saves combined text, increments version, and merges clocks:
  $$V_{\text{new}} = \text{VectorClock.merge}(V_A, V_B)$$

## Frontend implementation
Split-view modal showing Branch A (cyan) and Branch B (purple) side-by-side with an editable merge textarea.

## JSON-RPC method
* `rpc.resolve_conflict`
* REST: `POST /api/documents/resolve-conflict`

## WebSocket event
* `DOCUMENT_CONFLICT_RESOLVED`

## What the user sees
Side-by-side comparison of conflicting versions with one-click resolution buttons.

## How to test it
Trigger conflict via edit modal $\rightarrow$ click Resolve Conflict $\rightarrow$ click Save Merged Version.

## Expected result
Conflict flag clears, catalog updates to version 3, and merged vector clock is committed.

## Unit 3 / Unit 4 concept
**Unit 3 & 4:** Distributed Consensus and Divergence Reconciliation.

---

# 7. Distributed Mutual Exclusion & Document Locks

## What was added
Centralized leased lock manager providing exclusive access to documents during editing.

## Why it was added
Guarantees safety (no two users modify the same document concurrently under lock) and liveness (no starvation).

## Files changed
* `middleware/lockManager.js`
* `middleware/coordinator.js`
* `public/index.html`, `public/js/app.js`

## Backend implementation
`LockManager` class assigns exclusive ownership with a 60-second lease timeout. When a user requests a held lock, the attempt is rejected with HTTP 423 Locked.

## Frontend implementation
* Lock status pill in document table (`LOCKED` or `FREE`).
* Active Document Locks table in Mutual Exclusion panel.
* Acquire / Release buttons in Edit modal.

## JSON-RPC method
* `rpc.acquire_lock`
* `rpc.release_lock`
* `rpc.get_locks`
* REST: `POST /api/locks/acquire`, `POST /api/locks/release`, `GET /api/locks/status`.

## WebSocket event
* `LOCK_ACQUIRED`
* `LOCK_RELEASED`
* `LOCK_EXPIRED`
* `LOCK_DENIED`

## What the user sees
Document displays locked badge; other users see alert `⛔ ACCESS DENIED (HTTP 423)`.

## How to test it
User A acquires lock; User B tries to acquire same lock and is blocked.

## Expected result
Strict mutual exclusion enforced across different sessions.

## Unit 4 concept
**Unit 4:** Distributed Mutual Exclusion and Leased Locks.

---

# 8. Deadlock Detection via Wait-For Graph (WFG)

## What was added
Directed Wait-For Graph engine with cycle detection using Depth-First Search (DFS) and victim abort resolution.

## Why it was added
When multiple users request locks across multiple documents in different orders, circular dependencies ($U_A \rightarrow U_B \rightarrow U_A$) can cause permanent deadlock.

## Files changed
* `middleware/deadlockDetector.js`
* `middleware/coordinator.js`
* `public/index.html`, `public/js/app.js`

## Backend implementation
Maintains directed graph $G = (V, E)$ where vertices are users and edges represent blocked lock requests. DFS traverses the graph:
* If a back-edge to an active stack node is found, a cycle exists.
* Resolution policy: aborts the youngest waiting request to break the cycle.

## Frontend implementation
Live Wait-For Graph visualizer showing circular wait dependency pills (`user_a ➔ user_b ➔ user_a`) and red `DEADLOCK DETECTED` badge.

## JSON-RPC method
* `rpc.get_deadlocks`
* `rpc.simulate_deadlock`
* `rpc.resolve_deadlock`
* REST: `GET /api/deadlock/status`, `POST /api/deadlock/simulate`, `POST /api/deadlock/resolve`.

## WebSocket event
* `DEADLOCK_DETECTED`
* `DEADLOCK_RESOLVED`

## What the user sees
Circular wait pills visually connecting the deadlocked users; turns to green `NO DEADLOCK` upon resolution.

## How to test it
Click **Simulate Deadlock** $\rightarrow$ observe cycle $\rightarrow$ click **Resolve Deadlock**.

## Expected result
Cycle is detected immediately, and resolved cleanly by aborting the victim edge.

## Unit 4 concept
**Unit 4:** Distributed Deadlock Detection, Wait-For Graphs, and Cycle Detection.

---

# 9. Bully Algorithm Leader Election

## What was added
Implementation of the Bully Leader Election algorithm with priority ranks:
* Master Coordinator: Rank 100
* Storage Node 4: Rank 4
* Storage Node 3: Rank 3
* Storage Node 2: Rank 2
* Storage Node 1: Rank 1

## Why it was added
In a distributed system, if the coordinator fails, surviving nodes must elect a new leader without human intervention.

## Files changed
* `middleware/bullyElection.js`
* `middleware/coordinator.js`
* `public/index.html`, `public/js/app.js`

## Backend implementation
When coordinator failure is simulated:
1. Surviving storage nodes hold an election.
2. The surviving node with the highest priority rank (`node_4`) wins.
3. When Coordinator revives, it asserts its superior Rank 100 priority and reclaims leadership.

## Frontend implementation
* **Leader & Cluster Coordination Banner** at the top of the dashboard.
* Live election logs terminal displaying algorithm message exchanges.
* Buttons: Trigger Bully Election, Kill Coordinator, Revive Coordinator.

## JSON-RPC method
* `rpc.get_leader`
* `rpc.trigger_election`
* REST: `POST /api/election/start`, `POST /api/election/kill-coordinator`, `POST /api/election/revive-coordinator`.

## WebSocket event
* `LEADER_CHANGED`
* `LEADER_ELECTION_EVENT`

## What the user sees
Current leader changes from Master Coordinator $\rightarrow$ Storage Node Delta $\rightarrow$ Master Coordinator.

## How to test it
Click **Kill Coordinator** $\rightarrow$ watch Node 4 become leader $\rightarrow$ click **Revive Coordinator** $\rightarrow$ Coordinator reclaims leadership.

## Expected result
Real election transitions occur based on the rank hierarchy.

## Unit 4 concept
**Unit 4:** Leader Election and the Bully Algorithm.

---

# 10. Node Recovery & Re-Synchronization (P2P)

## What was added
A 4-stage recovery state machine for storage nodes:
$$\text{FAILED} \longrightarrow \text{RECOVERING} \longrightarrow \text{SYNCING} \longrightarrow \text{HEALTHY}$$

## Why it was added
When a failed node restarts, its local disk may be missing chunks created or updated during its downtime. It must re-synchronize missing chunks from healthy peer replicas.

## Files changed
* `middleware/nodeSyncManager.js`
* `middleware/coordinator.js`
* `public/index.html`, `public/js/app.js`

## Backend implementation
1. When node reconnects, status changes to `RECOVERING`.
2. `NodeSyncManager` compares the node's disk chunks against the Coordinator's catalog.
3. Missing chunks are streamed peer-to-peer (P2P) from surviving healthy replicas.
4. SHA-256 hash is validated before writing to disk.
5. Status transitions to `HEALTHY`.

## Frontend implementation
* Node Recovery status cards with animated status tags.
* **Sync Nodes** button to trigger cluster-wide reconciliation on demand.

## JSON-RPC method
* `rpc.synchronize_node`
* REST: `POST /api/sync/all`, `POST /api/sync/node/:nodeId`.

## WebSocket event
* `NODE_STATUS_CHANGED`
* `NODE_SYNC_LOG`

## What the user sees
Node status progression: red `FAILED` $\rightarrow$ amber `RECOVERING` $\rightarrow$ green `HEALTHY`.

## How to test it
Kill Node 4 $\rightarrow$ upload a document $\rightarrow$ revive Node 4 $\rightarrow$ click Sync Nodes.

## Expected result
Node 4 downloads missing chunks from Node 2 via P2P and becomes `HEALTHY`.

## Unit 4 concept
**Unit 4:** Fault Tolerance, Replica Recovery, and Anti-Entropy Synchronization.

---

# 11. Categorized Distributed Event Monitor

## What was added
A categorized real-time protocol monitor with 9 category filter tabs:
`ALL`, `[RPC]`, `[P2P]`, `[CLOCK]`, `[VECTOR]`, `[LOCK]`, `[ELECTION]`, `[DEADLOCK]`, `[RECOVERY]`.

## Why it was added
Provides transparent visibility into internal distributed systems communications without cluttering the screen.

## Files changed
* `public/index.html`
* `public/js/app.js`

## Backend implementation
Coordinator and storage nodes broadcast categorized messages tagged with category identifiers.

## Frontend implementation
Filter button tabs filter the log stream instantly without page reload.

## What the user sees
Terminal log box with clean colored lines matching the active category.

## How to test it
Click each tab in the monitor to see category-specific logs.

## Expected result
Only logs matching the selected category are displayed.

## Unit 3 & 4 concept
Distributed Observability and Protocol Tracing.
