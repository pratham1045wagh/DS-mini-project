# Distributed Systems FA-1 & FA-2 Viva Examination Guide

This comprehensive guide prepares you to explain every theoretical concept, architectural decision, and live demonstration for the **Distributed Collaborative Document Management & Synchronization System**.

---

## Table of Contents
1. [Lamport Logical Clock](#1-lamport-logical-clock)
2. [Vector Clock](#2-vector-clock)
3. [Concurrent Updates](#3-concurrent-updates)
4. [Conflict Detection](#4-conflict-detection)
5. [Document Versioning](#5-document-versioning)
6. [Distributed Mutual Exclusion](#6-distributed-mutual-exclusion)
7. [Document Locks](#7-document-locks)
8. [Lock Expiration & Leases](#8-lock-expiration--leases)
9. [Wait-For Graph (WFG)](#9-wait-for-graph-wfg)
10. [Deadlock Detection](#10-deadlock-detection)
11. [Deadlock Resolution](#11-deadlock-resolution)
12. [Bully Algorithm](#12-bully-algorithm)
13. [Leader Election](#13-leader-election)
14. [Coordinator Failure](#14-coordinator-failure)
15. [Failover & High Availability](#15-failover--high-availability)
16. [Node Recovery](#16-node-recovery)
17. [P2P Re-Synchronization](#17-p2p-re-synchronization)
18. [SHA-256 Data Integrity](#18-sha-256-data-integrity)
19. [JSON-RPC 2.0 Protocol](#19-json-rpc-20-protocol)
20. [WebSocket Real-Time Communication](#20-websocket-real-time-communication)
21. [P2P Communication & Event Monitor](#21-p2p-communication--event-monitor)

---

## 1. Lamport Logical Clock

### 1. What is the concept?
Physical clocks across independent machines drift due to quartz crystal variance and network latency (clock skew), making physical timestamps unreliable for ordering distributed events. Leslie Lamport proposed **Logical Clocks**: monotonically increasing integer counters that capture the **happened-before relation ($a \rightarrow b$)**:
* Before a process executes an event, it increments its local clock: $L = L + 1$.
* When sending a message $m$, the process attaches timestamp $L_m = L$.
* Upon receiving message $m$ with timestamp $L_m$, the receiving process updates:
  $$L_{\text{receiver}} = \max(L_{\text{receiver}}, L_m) + 1$$
This guarantees that if event $A$ caused event $B$, then $L(A) < L(B)$.

### 2. Why did we add it?
To establish a causal timeline of operations (file uploads, version updates, locks, peer replications) across the Coordinator and the 4 Storage Nodes without needing an external NTP server or synchronized wall-clocks.

### 3. Where is it implemented?
* Backend: `middleware/lamportClock.js` (Defines class `LamportClock` with `tick()` and `update(receivedTime)`).
* Attached to: `middleware/coordinator.js` (`this.lamportClock`) and each `storage_nodes/storageNode.js` (`this.lamportClock`).
* Frontend: `public/js/app.js` (`renderClocks()` and `renderTimeline()`).

### 4. How do I demonstrate it?
1. Open the dashboard at `http://localhost:3000`.
2. Note the initial Lamport clock badges at the top panel (`COORDINATOR L=...`, `NODE 1 L=...`).
3. Upload a file or click any action (e.g. acquire lock).
4. Watch the clock counter advance and observe the entry in the **Distributed Event Timeline**.

### 5. What should I see?
The clock badge increments and the timeline renders a new card showing `[L=X]` with the event type, node ID, and timestamp.

### 6. What should I say to the examiner?
> *"Examiner, physical clocks drift across distributed nodes, making wall-clock time unsuitable for event ordering. We implemented Lamport Logical Clocks where every local event increments the counter, and every distributed message carries its clock. When a node receives a message, it sets its clock to $\max(L_{\text{local}}, L_{\text{msg}}) + 1$. This guarantees that causally related events are correctly sequenced across our entire cluster."*

---

## 2. Vector Clock

### 1. What is the concept?
While Lamport clocks guarantee that $A \rightarrow B \implies L(A) < L(B)$, the converse is **not** true (i.e., $L(A) < L(B)$ does not necessarily mean $A \rightarrow B$; they might be concurrent). **Vector Clocks** solve this by maintaining an array (or map) of logical clocks representing every node in the system:
$$V = [V[1], V[2], \dots, V[n]]$$
* Node $i$ increments its own component $V[i]$ on local events.
* When sending a message, node $i$ includes vector $V$.
* When node $i$ receives vector $V'$, it updates:
  $$V[j] = \max(V[j], V'[j]) \quad \forall j \neq i, \quad V[i] = V[i] + 1$$
Vector clocks allow exact causal comparison:
* $V_A \le V_B$ if every element $V_A[k] \le V_B[k]$.
* $V_A$ and $V_B$ are **concurrent** ($V_A \parallel V_B$) if neither $V_A \le V_B$ nor $V_B \le V_A$.

### 2. Why did we add it?
In our collaborative document system, two users might edit document replicas concurrently on different nodes during network latency or partitions. Vector clocks enable the system to detect when two document versions diverge concurrently without relying on arbitrary timestamps.

### 3. Where is it implemented?
* Backend: `middleware/vectorClock.js` (Defines `compare(v1, v2)`, `merge(v1, v2)`, and `tick(nodeId)`).
* Integrated in: `middleware/coordinator.js` (tracks vector clock on each document update and version history).
* Frontend: `public/js/app.js` (renders vector clocks in table and version history modal).

### 4. How do I demonstrate it?
1. Open Document Version History on any document.
2. View the Vector Clock string (e.g. `{"node_1":1, "node_2":2, "node_4":3}`).
3. Trigger a concurrent update using the **Simulate Concurrent Update** button.

### 5. What should I see?
The vector clocks of the two branches show divergent values where neither is strictly greater than the other, causing a `CONFLICT DETECTED` alert.

### 6. What should I say to the examiner?
> *"Examiner, Lamport clocks cannot distinguish between causally ordered events and concurrent events. We implemented Vector Clocks, where each document version carries a vector of integers representing operations across all nodes. By computing component-wise inequalities, the system mathematically identifies whether one version caused another or if they are concurrent ($V_A \parallel V_B$), which triggers our conflict resolution workflow."*

---

## 3. Concurrent Updates

### 1. What is the concept?
Concurrent updates occur when two or more distributed processes submit modifications to the same shared state (document) without having observed each other's changes. Mathematically, updates $U_1$ and $U_2$ are concurrent if:
$$U_1 \not\rightarrow U_2 \quad \text{and} \quad U_2 \not\rightarrow U_1$$

### 2. Why did we add it?
Distributed collaborative editing systems must tolerate parallel work without data loss. Detecting concurrency is the prerequisite for safe branch merging.

### 3. Where is it implemented?
* Backend: `middleware/coordinator.js` in `/api/documents/concurrent-update` and `/api/documents/update`.
* Evaluation: `middleware/vectorClock.js` method `compare(v1, v2) === 'CONCURRENT'`.

### 4. How do I demonstrate it?
Click **Simulate Concurrent Update (Trigger Conflict)** on the document or in the Fault/Distributed panel.

### 5. What should I see?
The system creates Branch A and Branch B with concurrent vector clocks, displays a red pulsing notification, and updates the document status to `CONFLICT`.

### 6. What should I say to the examiner?
> *"Examiner, concurrent updates occur when two users modify a document without knowing about each other's changes. In our project, rather than using a simplistic 'last-write-wins' strategy that silently overwrites data, we capture the vector clocks from both paths and flag a concurrent branch state for explicit reconciliation."*

---

## 4. Conflict Detection

### 1. What is the concept?
Conflict detection is the deterministic algorithmic process of inspecting the vector clocks of two competing versions. If the comparison returns `'CONCURRENT'` ($V_A \parallel V_B$), the coordinator marks the document in conflict.

### 2. Why did we add it?
To prevent silent data corruption and ensure strong eventual consistency across all replicas.

### 3. Where is it implemented?
* Backend: `middleware/coordinator.js` checks `VectorClock.compare(incomingVC, currentVC)`.
* Notification: WebSocket broadcast event `DOCUMENT_CONFLICT`.
* Frontend: `public/js/app.js` displays the red conflict banner and activates the **Resolve Conflict** button.

### 4. How do I demonstrate it?
1. Click **Simulate Concurrent Update**.
2. Observe the red banner: `⚠️ CONFLICT DETECTED (Concurrent Updates via Vector Clock)`.
3. Click the **Resolve Conflict** button to view the side-by-side branch comparison modal.

### 5. What should I see?
A side-by-side visual comparison showing Branch Version A with its vector clock and Branch Version B with its vector clock.

### 6. What should I say to the examiner?
> *"Examiner, conflict detection uses our Vector Clock comparison logic. When an incoming version has elements that are both smaller and larger than the existing version's clock, the system knows neither preceded the other. It immediately broadcasts a `DOCUMENT_CONFLICT` WebSocket message to alert all connected clients."*

---

## 5. Document Versioning

### 1. What is the concept?
Document versioning ensures that every saved modification creates a new, immutable historical snapshot ($v_1, v_2, v_3, \dots$) rather than mutating files in place. Each version snapshot stores:
* Unique version identifier (`v1`, `v2`, etc.)
* Author / `updatedBy`
* Lamport logical timestamp
* Vector clock state
* SHA-256 content checksum
* Creation timestamp (`updatedAt`)

### 2. Why did we add it?
To provide auditability, rollbacks, and historical inspection in a collaborative environment.

### 3. Where is it implemented?
* Backend: `middleware/coordinator.js` in `doc.versions` array and `storageNode.saveChunk()` snapshot storage.
* Frontend: `public/index.html` modal `#versionHistoryModal` and `public/js/app.js` function `showVersionHistory()`.

### 4. How do I demonstrate it?
1. Edit a document, save changes as `v2`.
2. Click the **Version History** button (<i class="fa-solid fa-clock-rotate-left"></i>) on the document.

### 5. What should I see?
A modal listing all historical versions in reverse chronological order, displaying author `user_a`, vector clock, Lamport timestamp, and full SHA-256 checksum for each revision.

### 6. What should I say to the examiner?
> *"Examiner, our versioning system ensures immutability. Every update creates a new numbered snapshot containing the full SHA-256 hash, the Lamport timestamp, and the full vector clock. Historical versions remain permanently accessible and can be inspected or restored at any time."*

---

## 6. Distributed Mutual Exclusion

### 1. What is the concept?
In distributed systems, concurrent uncoordinated writes to a shared resource cause race conditions and inconsistency. **Distributed Mutual Exclusion** guarantees that at most one process can enter the critical section (modify a document) at any given moment.

### 2. Why did we add it?
To coordinate collaborative editing between User A and User B, ensuring only the lock-holder can commit new versions.

### 3. Where is it implemented?
* Backend: `middleware/lockManager.js` (Class `DistributedLockManager`).
* Endpoints: `POST /api/documents/:docId/lock/acquire` and `POST /api/documents/:docId/lock/release`.
* Status code: Returns `HTTP 423 Locked` if another user attempts to acquire an active lock.

### 4. How do I demonstrate it?
1. User A acquires lock on a document.
2. User B in another browser window attempts to acquire lock or edit the document.
3. Show User B receives an `HTTP 423 Locked` rejection.
4. User A releases the lock; User B now successfully acquires it.

### 5. What should I see?
Lock state changes to `LOCKED - Held by user_a (You)` for User A, and `LOCKED - Held by user_a` (disabled) for User B.

### 6. What should I say to the examiner?
> *"Examiner, we implement Distributed Mutual Exclusion via a centralized Coordinator Lock Manager. When a user acquires a lock, the critical section is locked exclusively for that user. If another client tries to acquire the lock concurrently, the system rejects the request with HTTP 423 Locked."*

---

## 7. Document Locks

### 1. What is the concept?
An explicit token granted to a user granting exclusive write privileges over a specific document identifier.

### 2. Why did we add it?
To prevent interleaved chunk uploads and inconsistent replica writes during editing.

### 3. Where is it implemented?
* Backend: `middleware/lockManager.js`.
* State maintained: `{ docId, holder, acquiredAt, expiresAt, token }`.
* Frontend: Lock badge in document table and lock controls in the Edit Document modal.

### 4. How do I demonstrate it?
Click the **Acquire Lock** button inside the Edit Document modal.

### 5. What should I see?
The badge switches from green `FREE` to red `🔒 LOCKED - Held by user_a`.

### 6. What should I say to the examiner?
> *"Examiner, document locks are granted as structured leases with unique tokens. The Coordinator validates the lock token on every write operation, rejecting unauthenticated edits."*

---

## 8. Lock Expiration & Leases

### 1. What is the concept?
If a client acquires a distributed lock and subsequently crashes or loses network connectivity, an indefinite lock would cause permanent starvation. A **Lease** assigns a time-to-live (e.g. 60 seconds). If not renewed or released, the lock automatically expires and is reclaimed by the system.

### 2. Why did we add it?
Fault tolerance: to ensure that dead clients cannot freeze document access indefinitely.

### 3. Where is it implemented?
* Backend: `middleware/lockManager.js` in `checkExpiredLocks()` and `leaseDurationMs = 60000`.
* Periodic cleanup: Timer checks every 5 seconds for expired leases.

### 4. How do I demonstrate it?
Acquire a lock and wait 60 seconds, or observe the lease countdown timer in the lock modal.

### 5. What should I see?
The lock expires automatically and transitions back to `FREE`.

### 6. What should I say to the examiner?
> *"Examiner, in distributed systems, infinite locks are dangerous because client crashes cause permanent lock starvation. We solved this using distributed leases: every lock has a 60-second lease with automatic background expiration, guaranteeing liveness."*

---

## 9. Wait-For Graph (WFG)

### 1. What is the concept?
A **Wait-For Graph** is a directed graph $G = (V, E)$ used for deadlock modeling:
* Vertices $V$ represent processes/users.
* A directed edge $(P_i \rightarrow P_j)$ indicates that process $P_i$ is waiting for a resource currently held by process $P_j$.

### 2. Why did we add it?
To dynamically track resource allocation and dependencies across multiple users and documents.

### 3. Where is it implemented?
* Backend: `middleware/deadlockDetector.js` (Maintains adjacency list `this.graph = new Map()`).
* Frontend: Rendered visually in the **Wait-For Graph (WFG)** container on the dashboard.

### 4. How do I demonstrate it?
Click **Simulate Deadlock** on the dashboard.

### 5. What should I see?
The WFG visualizer displays directed nodes: `user_a ➔ user_b ➔ user_a`.

### 6. What should I say to the examiner?
> *"Examiner, a Wait-For Graph models resource contention. Each node is a process, and each directed edge represents a process waiting for a lock held by another. If and only if there is a directed cycle in this graph, a distributed deadlock exists."*

---

## 10. Deadlock Detection

### 1. What is the concept?
Deadlock is a state where a set of processes are blocked because each is holding a resource and waiting for another resource held by another process in the set (Coffman conditions). In a Wait-For Graph, deadlock is equivalent to a **directed cycle**. The cycle is detected using **Depth-First Search (DFS)** with recursion-stack cycle tracking.

### 2. Why did we add it?
To automatically detect circular wait conditions in multi-resource collaborative workflows.

### 3. Where is it implemented?
* Backend: `middleware/deadlockDetector.js` method `detectDeadlock()` using standard DFS with `visited` and `recursionStack` sets.
* WebSocket: Broadcasts `DEADLOCK_DETECTED` event with cycle path details.

### 4. How do I demonstrate it?
Click **Simulate Deadlock (User A ⇄ User B)** in the Deadlock section.

### 5. What should I see?
The badge turns red: `⚠️ DEADLOCK DETECTED! Cycle: user_a ➔ user_b ➔ user_a`.

### 6. What should I say to the examiner?
> *"Examiner, deadlock detection executes a Depth-First Search over the dynamic Wait-For Graph. When a back-edge is encountered during DFS traversal, a circular wait cycle is proven to exist, and the coordinator raises an immediate system alert."*

---

## 11. Deadlock Resolution

### 1. What is the concept?
Once a cycle is detected, the deadlock must be broken by selecting a **victim process** and aborting or preempting its request to eliminate the circular dependency. Common policies include aborting the youngest transaction or releasing the contested lock.

### 2. Why did we add it?
To restore cluster liveness without restarting servers or losing uncommitted work.

### 3. Where is it implemented?
* Backend: `middleware/deadlockDetector.js` method `resolveDeadlock(policy)` and `coordinator.js` endpoint `POST /api/deadlock/resolve`.
* Frontend: Button `#btnResolveDeadlock` and `resolveDeadlock()` function.

### 4. How do I demonstrate it?
While deadlock is active, click the **Resolve Deadlock** button (<i class="fa-solid fa-wand-magic-sparkles"></i>).

### 5. What should I see?
The circular edge is severed, the notification displays `Deadlock resolved by aborting wait of User B`, and the WFG badge returns to green `NO DEADLOCK`.

### 6. What should I say to the examiner?
> *"Examiner, to resolve deadlock, our engine selects a victim process—specifically the youngest requesting transaction—and preempts its wait dependency. This breaks the cycle in the Wait-For Graph, returning the system to normal operations."*

---

## 12. Bully Algorithm

### 1. What is the concept?
The **Bully Algorithm** (Garcia-Molina) is an election algorithm for synchronous distributed systems with crash-stop failures:
* Every process has a unique, known priority rank.
* In our cluster:
  * Master Coordinator = **Rank 100**
  * Storage Node 4 (Delta) = **Rank 4**
  * Storage Node 3 (Gamma) = **Rank 3**
  * Storage Node 2 (Beta) = **Rank 2**
  * Storage Node 1 (Alpha) = **Rank 1**
* When a node detects the leader has failed, it sends an `ELECTION` message to all nodes with higher rank.
* If no higher node responds within a timeout, the node declares itself the winner by sending a `COORDINATOR` message to all lower nodes.
* If a higher-ranked node revives, it immediately bullies out any lower-ranked leader and reclaims leadership.

### 2. Why did we add it?
To elect a temporary coordination leader among surviving storage nodes if the Master Coordinator crashes.

### 3. Where is it implemented?
* Backend: `middleware/bullyElection.js` (Class `BullyElection`).
* Coordinator revival hook: `middleware/coordinator.js` method `reviveCoordinator()`.
* Logs: Visualized in real time in the **Leader Election Logs** terminal.

### 4. How do I demonstrate it?
1. Note Current Leader = `Master Coordinator (Rank 100)`.
2. Click **Kill Coordinator**.
3. Watch the election terminal: Node 1 initiates election $\rightarrow$ Node 4 bullies lower nodes $\rightarrow$ Node 4 becomes leader.
4. Click **Revive Coordinator** $\rightarrow$ Coordinator asserts Rank 100 $\rightarrow$ Coordinator reclaims leadership.

### 5. What should I see?
Current Leader updates to `Storage Node Delta (node_4)` on kill, then returns to `Master Coordinator` on revive.

### 6. What should I say to the examiner?
> *"Examiner, the Bully Algorithm assigns static priority ranks to all nodes. In our system, Coordinator is Rank 100 and Nodes 1 to 4 have ranks 1 through 4. When Coordinator fails, Node 4 wins the election because it has the highest surviving rank. When Coordinator revives, its Rank 100 supersedes Node 4, immediately reclaiming cluster leadership."*

---

## 13. Leader Election

### 1. What is the concept?
The protocol by which distributed nodes agree on a single coordinator responsible for global decisions (lock management, chunk placement, membership).

### 2. Why did we add it?
A single active leader prevents split-brain scenarios and coordinates consensus during cluster operations.

### 3. Where is it implemented?
* Backend: `middleware/bullyElection.js` and `storage_nodes/storageNode.js`.
* Status API: `GET /api/cluster/leader`.

### 4. How do I demonstrate it?
Observe the top navbar banner: `Current Leader: Master Coordinator` with green `COMPLETED` election badge.

### 5. What should I see?
Clear identification of the active leader ID and its priority rank.

### 6. What should I say to the examiner?
> *"Examiner, leader election ensures there is always exactly one authoritative node managing metadata and locks, preventing split-brain inconsistency."*

---

## 14. Coordinator Failure

### 1. What is the concept?
Simulating the abrupt crash of the central coordinator process (crash-stop model) to test cluster survivability and election failover.

### 2. Why did we add it?
To demonstrate high availability and ensure the system does not enter an unrecoverable failure state.

### 3. Where is it implemented?
* Backend: `middleware/coordinator.js` method `killCoordinator()` sets status `CRASHED` and suspends heartbeat monitors.
* Frontend: Button `#btnKillCoordinator`.

### 4. How do I demonstrate it?
Click **Kill Coordinator**.

### 5. What should I see?
Coordinator status turns red `CRASHED`, and Bully election triggers immediately.

### 6. What should I say to the examiner?
> *"Examiner, we simulate a crash-stop failure of the master coordinator. The heartbeat stoppage is detected by storage nodes, which autonomously self-organize to elect a new leader."*

---

## 15. Failover & High Availability

### 1. What is the concept?
**Failover** is the automated redirection of requests to redundant replicas when a primary node fails, ensuring uninterrupted service availability ($R \ge 2$).

### 2. Why did we add it?
To guarantee that document downloads succeed even if one of the storage nodes holding chunks is dead.

### 3. Where is it implemented?
* Backend: `middleware/coordinator.js` download chunk handler loops over `chunk.nodes` array: if `node.status !== 'HEALTHY'`, it automatically redirects the stream to the next healthy replica.

### 4. How do I demonstrate it?
1. Upload a document replicated to Node 4 and Node 2.
2. Click **Kill Node 4**. Node 4 turns red `FAILED`.
3. Click **Download** on the document.

### 5. What should I see?
Download completes successfully (HTTP 200) from Node 2 with zero errors.

### 6. What should I say to the examiner?
> *"Examiner, because we store chunks with replication factor $R = 2$, killing a replica node does not cause downtime. The Coordinator automatically fails over to the alternate healthy replica, verifying fault tolerance."*

---

## 16. Node Recovery

### 1. What is the concept?
When a failed node returns online, it must safely transition through a disciplined recovery lifecycle before accepting live read/write traffic:
$$\text{FAILED} \longrightarrow \text{RECOVERING} \longrightarrow \text{SYNCING} \longrightarrow \text{HEALTHY}$$

### 2. Why did we add it?
To prevent a stale or corrupted node from serving outdated data to clients.

### 3. Where is it implemented?
* Backend: `middleware/coordinator.js` method `reviveNode()` and `syncClusterNodes()`.
* Panel: Visualized in the **Node Recovery & Re-Synchronization** panel.

### 4. How do I demonstrate it?
Click **Revive Node** on any killed storage node.

### 5. What should I see?
The node badge transitions from `FAILED` $\rightarrow$ `RECOVERING` $\rightarrow$ `SYNCING` $\rightarrow$ `HEALTHY`.

### 6. What should I say to the examiner?
> *"Examiner, a revived node cannot immediately be marked healthy because its disk may be stale. Our recovery protocol moves it through RECOVERING and SYNCING states while it verifies its chunks against peer nodes."*

---

## 17. P2P Re-Synchronization

### 1. What is the concept?
Peer-to-Peer chunk synchronization: rather than placing the entire bandwidth burden on the Coordinator, recovering nodes fetch missing chunks directly from healthy peer storage nodes via HTTP streaming.

### 2. Why did we add it?
To distribute network bandwidth across storage nodes and restore replica count $R = 2$ after a crash.

### 3. Where is it implemented?
* Backend: `storage_nodes/storageNode.js` methods `pullChunkFromPeer(peerUrl, chunkId)` and `GET /api/node/fetch-chunk`.
* Coordinator orchestration: `POST /api/nodes/sync`.

### 4. How do I demonstrate it?
Click the cyan **Sync Nodes** button.

### 5. What should I see?
Log displays P2P chunk transfers between storage node ports (e.g. `8002 ➔ 8004`), followed by `HEALTHY` verification.

### 6. What should I say to the examiner?
> *"Examiner, during re-synchronization, storage nodes use direct P2P connections to transfer missing chunks from healthy peers without routing bulky payload data through the coordinator."*

---

## 18. SHA-256 Data Integrity

### 1. What is the concept?
A cryptographic hash function that produces a unique 256-bit (64 hex character) digest of file content. Even a 1-bit alteration results in an entirely different digest (avalanche effect).

### 2. Why did we add it?
To detect chunk corruption during network transit, disk failure, or replica synchronization.

### 3. Where is it implemented?
* Chunk hashing: `crypto.createHash('sha256')` in `middleware/coordinator.js` and `storage_nodes/storageNode.js`.
* Verification: Computed before saving and verified upon re-assembly and peer sync.

### 4. How do I demonstrate it?
1. Open Document Version History.
2. View the complete SHA-256 hash displayed next to each version.

### 5. What should I see?
A full 64-character hexadecimal checksum (e.g. `e6fda1cd9ad84141...`).

### 6. What should I say to the examiner?
> *"Examiner, every 512KB chunk is hashed using SHA-256 upon creation. During downloads and peer-to-peer sync, the received chunk is re-hashed. If the hash does not match, the chunk is rejected as corrupted."*

---

## 19. JSON-RPC 2.0 Protocol

### 1. What is the concept?
A stateless, lightweight remote procedure call specification using JSON payloads:
```json
{
  "jsonrpc": "2.0",
  "method": "rpc.get_cluster_status",
  "params": {},
  "id": 1
}
```
Responses return either `{ "jsonrpc": "2.0", "result": ..., "id": 1 }` or `{ "jsonrpc": "2.0", "error": { "code": -32601, "message": "..." }, "id": 1 }`.

### 2. Why did we add it?
Standardized, structured communication between Coordinator, browser clients, and storage nodes.

### 3. Where is it implemented?
* Client: `public/js/rpcClient.js` (Class `RPCClient`).
* Server: `middleware/coordinator.js` endpoint `POST /api/rpc`.

### 4. How do I demonstrate it?
Open browser console or PowerShell and call `POST /api/rpc` with `rpc.get_cluster_status`.

### 5. What should I see?
A strict JSON-RPC 2.0 envelope containing `jsonrpc: "2.0"`, matching `id`, and payload `result`.

### 6. What should I say to the examiner?
> *"Examiner, we implement the JSON-RPC 2.0 specification for internal procedure calls. It provides a standardized request/response structure with explicit error codes and request IDs for asynchronous correlation."*

---

## 20. WebSocket Real-Time Communication

### 1. What is the concept?
Full-duplex, persistent TCP connection enabling bi-directional, event-driven communication between server and clients without HTTP polling.

### 2. Why did we add it?
To update cluster topology, election results, lock state, and event monitors across all open browser sessions in real time.

### 3. Where is it implemented?
* Server: `middleware/coordinator.js` runs WebSocket server on port `3001` via `ws` library.
* Client: `public/js/streamClient.js` and `public/js/app.js`.

### 4. How do I demonstrate it?
Perform an action in one window (e.g. acquire lock or kill node) and observe the second browser window update instantly without refreshing.

### 5. What should I see?
Instant UI changes and logs in the browser console under `[WS Received]`.

### 6. What should I say to the examiner?
> *"Examiner, we use WebSockets on port 3001 for real-time push events. Whenever a node crashes, a leader is elected, or a lock is acquired, the Coordinator broadcasts a typed event to all connected dashboards with zero polling delay."*

---

## 21. P2P Communication & Event Monitor

### 1. What is the concept?
* **P2P Communication:** Direct storage node-to-node HTTP data plane for chunk replication and recovery.
* **Distributed Event Monitor:** A unified audit log that captures and categorizes distributed events (RPC, P2P, CLOCK, VECTOR, LOCK, ELECTION, DEADLOCK, RECOVERY).

### 2. Why did we add it?
To visualize the complex interactions of a distributed system in an intuitive, filterable interface for observability.

### 3. Where is it implemented?
* Frontend: `public/js/app.js` filter tabs and `logEvent()` pipeline.
* Backend: Broadcasts via WebSocket `TIMELINE_EVENT` and `LOG_ENTRY`.

### 4. How do I demonstrate it?
Click the category filter buttons: `[RPC]`, `[P2P]`, `[CLOCK]`, `[VECTOR]`, `[LOCK]`, `[ELECTION]`, `[DEADLOCK]`, `[RECOVERY]`.

### 5. What should I see?
The log display immediately filters to only show events matching the selected distributed subsystem.

### 6. What should I say to the examiner?
> *"Examiner, observability is critical in distributed systems. Our Event Monitor aggregates events across 9 subsystems—including Lamport clocks, vector clocks, Bully elections, and P2P transfers—allowing operators to filter and inspect distributed traces in real time."*

---
