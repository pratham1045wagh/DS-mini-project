# Distributed Collaborative Document Management & Synchronization System (FA-1 + FA-2)

A production-grade, local distributed system combining **Unit 1 & 2 (FA-1)** foundations with **Unit 3 & 4 (FA-2)** advanced concepts: Lamport Logical Clocks, Vector Clocks with concurrent conflict detection, Bully Algorithm Leader Election, Distributed Mutual Exclusion, Deadlock Detection via Wait-For Graphs, Document Versioning, and Node Recovery with Re-Synchronization.

---

## Technology Stack

| Layer | Technology |
|---|---|
| **Runtime** | Node.js (v18+ / v22.x, multi-service architecture) |
| **HTTP Framework** | Express.js |
| **RPC Protocol** | JSON-RPC 2.0 (over HTTP POST with 27 registered methods) |
| **Real-Time Sync** | WebSocket (`ws` library) for real-time dashboard events and node message queues |
| **P2P Replication** | Direct Node-to-Node HTTP (no external message brokers) |
| **Chunk Streaming** | 512 KB chunk splitter with SHA-256 integrity verification |
| **Logical Clocks** | Lamport Logical Clock ($L = \max(L, L_{msg}) + 1$) + Distributed Event Timeline |
| **Vector Clocks** | Multi-node vector clock ($V_A < V_B$, $V_B < V_A$, or $V_A \parallel V_B$) for document versions |
| **Leader Election** | Bully Algorithm with dynamic rank assignment and priority revival |
| **Mutual Exclusion** | Leased exclusive document locking with auto-timeout recovery |
| **Deadlock Detection** | Wait-For Graph (WFG) with Depth-First Search (DFS) cycle detection and victim abort |
| **Node Recovery** | Missing chunk discovery and peer re-synchronization (`RECOVERING` → `SYNCING` → `HEALTHY`) |
| **Authentication** | Session-based authentication via cryptographic tokens in `HttpOnly` cookies |
| **Local Persistence** | Local JSON metadata store (`storage_nodes/data/metadata.json`) preserving state across restarts |
| **Frontend** | Vanilla HTML5 + CSS3 + Modern JavaScript (no heavy external build frameworks) |

---

## Distributed Architecture Overview

```
╔═════════════════════════════════════════════════════════════════════════════════════╗
║                                 Interactive Dashboard                               ║
║           Topology • Event Timeline • Vector Clocks • Locks • WFG • Bully Logs      ║
╚══════════════════════════════════════════════╦══════════════════════════════════════╝
                                               ║ HTTP (REST + JSON-RPC 2.0)
                                               ║ WebSocket (ws://localhost:3001)
                             ╔═════════════════╩═════════════════╗
                             ║        MASTER COORDINATOR         ║ Port 3000
                             ║        (LEADER: RANK 100)         ║
                             ╠═══════════════════════════════════╣
                             ║ • AuthService (Session Cookies)   ║
                             ║ • NamingService (Registry)        ║
                             ║ • LamportClock (Event Timeline)   ║
                             ║ • LockManager (Mutual Exclusion)  ║
                             ║ • DeadlockDetector (Wait-For Graph║
                             ║ • BullyElection (Leader Election) ║
                             ║ • NodeSyncManager (Re-sync)       ║
                             ║ • StreamHandler (512KB + SHA-256) ║
                             ╚═════════════════╦═════════════════╝
             ┌───────────────────┬─────────────┴───────┬───────────────────┐
             ▼                   ▼                     ▼                   ▼
     ┌──────────────┐    ┌──────────────┐      ┌──────────────┐    ┌──────────────┐
     │ Storage Node │    │ Storage Node │      │ Storage Node │    │ Storage Node │
     │  1 (Alpha)   │    │   2 (Beta)   │      │  3 (Gamma)   │    │  4 (Delta)   │
     │ HTTP: 8001   │    │ HTTP: 8002   │      │ HTTP: 8003   │    │ HTTP: 8004   │
     │  WS: 9001    │    │  WS: 9002    │      │  WS: 9003    │    │  WS: 9004    │
     │  Rank: 1     │    │  Rank: 2     │      │  Rank: 3     │    │  Rank: 4     │
     └───────┬──────┘    └──────┬───────┘      └──────┬───────┘    └──────┬───────┘
             │                  │                     │                   │
             └──────────────────┴── Direct HTTP P2P ──┴───────────────────┘
```

---

## Distributed Systems Concepts Demonstrated

| Unit | Theoretical Concept | Implementation Module | Demonstrated Capability |
|---|---|---|---|
| **Unit 1** | Distributed Architecture & Middleware | `server.js`, `middleware/coordinator.js` | Master-worker topology, coordinator middleware, independent storage nodes on local ports |
| **Unit 1** | Security & Access Control | `middleware/authService.js` | Session-based authentication, `HttpOnly` cookies, User A/User B permission isolation |
| **Unit 2** | Remote Procedure Call (RPC) | `communication/rpcServer.js`, `public/js/rpcClient.js` | Strict JSON-RPC 2.0 protocol over HTTP POST with 27 methods |
| **Unit 2** | Stream-Oriented Communication | `communication/streamHandler.js` | File splitting into 512KB chunks, SHA-256 integrity verification |
| **Unit 2** | P2P Replication & Failover | `storage_nodes/replicationEngine.js`, `storage_nodes/storageNode.js` | Replication factor = 2, peer chunk forwarding, transparent failover download |
| **Unit 3** | Lamport Logical Clocks | `middleware/lamportClock.js` | Clock tick ($L = L + 1$) and message update ($L = \max(L, L_{msg}) + 1$), Distributed Event Timeline |
| **Unit 3** | Vector Clocks & Concurrency | `middleware/vectorClock.js` | Vector comparison, causal partial ordering, concurrent branch detection ($V_A \parallel V_B$) |
| **Unit 4** | Leader Election | `middleware/bullyElection.js` | Bully Algorithm election upon coordinator crash, highest rank win, coordinator priority revival |
| **Unit 4** | Distributed Mutual Exclusion | `middleware/lockManager.js` | Leased document locking, lease countdown, access denial for concurrent editors |
| **Unit 4** | Deadlock Detection & Resolution | `middleware/deadlockDetector.js` | Wait-For Graph (WFG) maintenance, DFS cycle detection, resolution via victim abort |
| **Unit 4** | Node Recovery & Re-Synchronization | `middleware/nodeSyncManager.js` | Recovery state machine (`RECOVERING` → `SYNCING` → `HEALTHY`), missing chunk peer pull |
| **All** | Local State Persistence | `middleware/metadataStore.js` | Persistent catalog (`metadata.json`) preserving documents, versions, replicas, and ACL across restarts |

---

## Detailed Features

### 1. Lamport Logical Clock & Distributed Event Timeline (Unit 3)
- Every storage node and coordinator maintains an independent logical clock.
- **Rules Enforced**:
  - Local event: $L = L + 1$ before generating the event.
  - Outgoing message: Clock timestamp attached to headers/payloads.
  - Incoming message: $L = \max(L, L_{msg}) + 1$.
- **Tracked Operations**:
  - `UPLOAD`, `DOWNLOAD`, `UPDATE`, `SHARE`, `DELETE`, `REPLICATION`, `REPLICATION_ACK`, `LOCK_ACQUIRED`, `LOCK_RELEASED`, `NODE_FAILURE`, `NODE_RECOVERY`, `ELECTION`, `DEADLOCK`.
- **Distributed Event Timeline UI**:
  - Shows Lamport Timestamp ($L=x$), Node/User, Operation badge, Document name, Event Details, and Real Time.

### 2. Vector Clocks & Concurrent Update Detection (Unit 3)
- Every document version tracks a vector clock mapping each storage node to its counter:
  ```json
  {
    "node_1": 3,
    "node_2": 2,
    "node_3": 4,
    "node_4": 1
  }
  ```
- **Causal Ordering Determination**:
  - $V_A < V_B$: Version A happened before Version B.
  - $V_B < V_A$: Version B happened before Version A.
  - $V_A \parallel V_B$: Versions are concurrent.
- **Conflict Handling**:
  - When concurrent modifications occur, the system flags **"CONFLICT DETECTED"**.
  - Both branches (Version A and Version B) are preserved.
  - An interactive **Conflict Resolution Modal** allows choosing Version A, choosing Version B, or merging changes with a component-wise maximum vector clock.

### 3. Bully Algorithm Leader Election (Unit 4)
- **Rank Hierarchy**:
  - `Coordinator`: Rank 100
  - `Node 4 (Delta)`: Rank 4
  - `Node 3 (Gamma)`: Rank 3
  - `Node 2 (Beta)`: Rank 2
  - `Node 1 (Alpha)`: Rank 1
- **Election Protocol**:
  - If Coordinator fails, surviving storage nodes detect the failure and initiate an election.
  - Higher-ranked nodes respond to lower-ranked nodes.
  - The highest available node declares victory and broadcasts its leadership.
  - When Coordinator revives, its Rank 100 asserts Bully priority to reclaim leadership.
- **Dashboard Leader Display**:
  - Shows Current Leader, Previous Leader, Election Status, and real-time Bully election logs.

### 4. Distributed Mutual Exclusion (Unit 4)
- Coordinator-managed exclusive locks for document editing.
- When User A locks a document:
  - Document status becomes `LOCKED` by User A.
  - User B attempting to edit or acquire the lock receives `ACCESS DENIED / WAITING FOR LOCK (HTTP 423)`.
- **Lease Timeout Recovery**:
  - Locks include an automatic lease expiration timer (default 60 seconds) to prevent permanent resource starvation if a client disconnects.

### 5. Deadlock Detection via Wait-For Graph (Unit 4)
- Maintains a directed **Wait-For Graph (WFG)**:
  - Directed edge $U_1 \rightarrow U_2$: User 1 is waiting for a document locked by User 2.
- **Cycle Detection**:
  - Depth-First Search (DFS) detects directed cycles (e.g. $U_A \rightarrow U_B \rightarrow U_A$).
  - When a cycle is detected, the dashboard alerts **"DEADLOCK DETECTED"**.
- **Cycle Resolution Policy**:
  - Aborts the youngest waiting lock request to break the circular dependency.

### 6. Document Versioning & Consistency
- Previous versions are never destroyed on update.
- Each document version contains:
  - `version`, `fileName`, `author`/`updatedBy`, `lamportTimestamp`, `vectorClock`, `checksum` (SHA-256), `updatedAt`, `chunks`.
- Version history modal displays the complete audit log of changes.

### 7. Node Recovery & Re-Synchronization (Unit 4)
- **Lifecycle**:
  - `Node FAILED` $\rightarrow$ Remaining healthy replicas serve downloads seamlessly.
  - `Node RECOVERING` $\rightarrow$ Node re-registers with coordinator.
  - `Node SYNCING` $\rightarrow$ Compares local chunks against coordinator metadata, pulls missing chunks from healthy peers via P2P HTTP, and verifies SHA-256 checksums.
  - `Node HEALTHY` $\rightarrow$ Restored to active cluster rotation.

---

## JSON-RPC 2.0 API Methods

| Method | Parameters | Description |
|---|---|---|
| `rpc.register_node` | `{ id, name, httpPort, wsPort }` | Registers a storage node with coordinator |
| `rpc.heartbeat` | `{ nodeId, storageUsedBytes, lamportClock }` | Periodic health heartbeat from storage node |
| `rpc.get_cluster_status` | `{}` | Returns all nodes, documents, and cluster metrics |
| `rpc.get_users` | `{}` | Lists all system users |
| `rpc.share_document` | `{ docId, targetUserId, permission }` | Shares document access (READ or WRITE) |
| `rpc.get_doc_location` | `{ docId }` | Resolves healthy replica node endpoints for download |
| `rpc.simulate_fault` | `{ nodeId, action }` | Simulates node crash (`kill`) or revival (`revive`) |
| `rpc.get_logical_clock` | `{}` | Returns logical clocks of coordinator and all nodes |
| `rpc.get_vector_clock` | `{ docId? }` | Queries vector clock metadata for documents |
| `rpc.start_election` | `{ initiatorId? }` | Triggers a Bully Algorithm leader election |
| `rpc.get_leader` | `{}` | Returns current leader, previous leader, and election status |
| `rpc.simulate_coordinator_failure` | `{}` | Crashes coordinator and triggers election among nodes |
| `rpc.simulate_coordinator_revival` | `{}` | Revives coordinator and reclaims leadership |
| `rpc.acquire_document_lock` | `{ docId, timeoutMs? }` | Requests exclusive distributed document lock |
| `rpc.release_document_lock` | `{ docId, force? }` | Releases exclusive document lock |
| `rpc.get_document_lock` | `{ docId }` | Returns current lock status of a document |
| `rpc.get_all_locks` | `{}` | Lists all active distributed locks |
| `rpc.get_document_versions` | `{ docId }` | Returns complete version history of a document |
| `rpc.update_document` | `{ docId, content, fileName? }` | Creates a new version under mutual exclusion |
| `rpc.resolve_conflict` | `{ docId, resolution, mergedContent? }` | Resolves concurrent vector clock conflict |
| `rpc.sync_node` | `{ nodeId }` | Triggers chunk synchronization for a node |
| `rpc.get_deadlocks` | `{}` | Evaluates Wait-For Graph for deadlocks |
| `rpc.simulate_deadlock` | `{}` | Injects circular wait dependency ($U_A \rightleftarrows U_B$) |
| `rpc.resolve_deadlock` | `{}` | Resolves circular wait by aborting victim request |
| `rpc.send_chat_message` | `{ targetUserId, text, fileAttachment? }` | Sends peer-to-peer user chat message |
| `rpc.delete_chat_message` | `{ messageId }` | Unsends a previously sent chat message |
| `rpc.get_chat_history` | `{}` | Retrieves user's message history |

---

## How to Run

### Prerequisites
- Node.js (v18.0.0 or later, tested on Node v22)
- npm

### Installation & Startup
```bash
# Clone or navigate to the project directory
cd "DS Mini Project"

# Install dependencies (only express, ws, multer, uuid)
npm install

# Start the complete distributed cluster
npm start
# or
node server.js
```

Open your browser to: **`http://localhost:3000`**

### Pre-Seeded User Accounts
| User ID | Name | Password | Role |
|---|---|---|---|
| `user_a` | User A (Alice) | `passA` | Administrator / Primary Owner |
| `user_b` | User B (Bob) | `passB` | Collaborator / Engineer |
| `user_c` | User C (Charlie) | `passC` | Team Member |
| `user_d` | User D (Diana) | `passD` | Reviewer |
| `user_e` | User E (Evan) | `passE` | Auditor |

---

## Step-by-Step Viva Demonstration Script

Follow these steps to demonstrate every required distributed systems feature:

### Step 1: System Boot & Topology
1. Open `http://localhost:3000` and sign in as `user_a` with password `passA`.
2. Notice the **Cluster Topology Visualizer** displaying the Coordinator (Port 3000) and 4 Distributed Storage Nodes (Alpha, Beta, Gamma, Delta).
3. Observe live pulse dots and healthy storage capacities.

### Step 2: Document Upload & Replication (Factor = 2)
1. In the **Upload & Storage** panel, drop a document or type text into Quick Text Creator and click **Create & Upload Document**.
2. Notice the replication banner: the file is split into 512KB chunks with SHA-256 checksums verified across 2 replicas.
3. Observe the document appearing in the **Document Catalog** with Version `v1` and its initial vector clock.

### Step 3: Lamport Logical Clocks & Timeline
1. Look at the **Distributed Event Timeline** section.
2. Notice the logical clock badges (`L=...`) incrementing across Coordinator and Storage Nodes.
3. Review the timeline table entries tracking `UPLOAD`, `REPLICATION`, and `REPLICATION_ACK`.

### Step 4: Access Control & Sharing
1. In the document row, click the **Share** button (`<i class="fa-solid fa-share-nodes"></i>`).
2. Select `user_b` (Bob) and choose **WRITE** permission.
3. Open an Incognito window or second browser tab at `http://localhost:3000`, sign in as `user_b` (`passB`), and open **Shared With Me** to see the document.

### Step 5: Distributed Mutual Exclusion (Locking)
1. As `user_a`, click **Edit** (`<i class="fa-solid fa-pen-to-square"></i>`) on the document.
2. In the modal, click **Acquire Lock**. The badge turns red: `LOCKED by user_a`.
3. In the second browser window as `user_b`, click **Edit** on the same document and attempt to acquire the lock.
4. An alert pops up: `ACCESS DENIED / WAITING FOR LOCK: Document is currently locked by user_a`.
5. Return to `user_a`, make edits, click **Save New Version**, and release the lock.

### Step 6: Vector Clocks & Concurrent Update Conflict
1. In the Edit modal, click **Simulate Concurrent Update (Trigger Conflict)**.
2. Notice the red banner: **"CONFLICT DETECTED (Concurrent Updates via Vector Clock)"**.
3. Click **Resolve Conflict** to open the side-by-side modal displaying Branch Version A and Branch Version B with their concurrent vector clocks ($V_A \parallel V_B$).
4. Choose **Merge Changes** to establish a unified Version 3 with the component-wise maximum vector clock.

### Step 7: Bully Algorithm Leader Election
1. In the top Leader Banner, click **Kill Coordinator**.
2. Watch the election terminal: storage nodes detect the failure and run the Bully algorithm.
3. The highest available storage node (`node_4` / Delta) wins the election and becomes the **NEW LEADER**.
4. Click **Revive Coordinator**: the Coordinator reboots with Rank 100, asserts priority, and reclaims leadership.

### Step 8: Node Failure, Failover & Recovery Re-Sync
1. In the Topology panel, click **Kill Node** under Node 4. The node turns red (`FAILED`).
2. Click **Download** on a document stored on Node 4: the Coordinator proxy detects the failure and seamlessly routes the download to surviving replica Node 2 (failover verified via `X-Node-Id` header).
3. Click **Revive Node** under Node 4:
   - Node 4 transitions: `FAILED` $\rightarrow$ `RECOVERING` $\rightarrow$ `SYNCING` $\rightarrow$ `HEALTHY`.
   - Node 4 scans its catalog, pulls missing chunks from Node 2, verifies SHA-256 checksums, and restores full consistency.

### Step 9: Deadlock Detection (Wait-For Graph)
1. In the Coordination panel, click **Simulate Deadlock (User A ⇄ User B)**.
2. The Wait-For Graph displays a circular dependency: `user_a ➔ user_b` and `user_b ➔ user_a`.
3. The status badge turns red: **"DEADLOCK DETECTED"**.
4. Click **Resolve Deadlock**: the cycle is resolved by aborting the victim request and releasing the blocking resource.

---

## Running the Automated Test Suite

The project includes automated integration test suites for both FA-1 and FA-2:

```bash
# 1. Run the FA-2 Full Verification Suite (All 18 requirements & 51 assertions)
node test_fa2.js

# 2. Run the FA-1 Integration Suite (Ensures backward compatibility)
node test_fa1.js

# 3. Run Real-Time Chat & Unsend Tests
node test_chat.js
node test_chat_actions.js

# 4. Run Multi-User Permission Tests
node test_new_users.js
```

---

## File Structure

```
├── server.js                        # Master launcher: boots coordinator + 4 storage nodes
├── config.js                        # Network topology, ports, chunk size, replication factor
├── package.json
│
├── middleware/
│   ├── authService.js               # Session auth: login, logout, requireAuth middleware
│   ├── coordinator.js               # Master coordinator: HTTP/RPC routes + event dispatching
│   ├── lamportClock.js              # [FA-2] Lamport Clock + Distributed Event Timeline ring buffer
│   ├── vectorClock.js               # [FA-2] Vector Clock engine: compare, merge, concurrent detection
│   ├── bullyElection.js             # [FA-2] Bully Algorithm leader election with priority assertion
│   ├── lockManager.js               # [FA-2] Distributed Mutual Exclusion exclusive lock manager
│   ├── deadlockDetector.js          # [FA-2] Wait-For Graph (WFG) with DFS cycle detection & victim abort
│   ├── nodeSyncManager.js           # [FA-2] Node recovery state machine & peer chunk resynchronization
│   ├── namingService.js             # Storage node registry & load-balanced node selection
│   ├── metadataStore.js             # Local JSON file persistent catalog (metadata.json)
│   └── faultDetector.js             # Heartbeat monitor & node failure detector
│
├── communication/
│   ├── rpcServer.js                 # JSON-RPC 2.0 dispatcher supporting 27 RPC methods
│   ├── messageQueue.js              # Inter-node WebSocket message brokers
│   └── streamHandler.js             # 512KB chunk splitter & SHA-256 integrity verifier
│
├── storage_nodes/
│   ├── storageNode.js               # Storage node HTTP server, Lamport clock & chunk store
│   ├── replicationEngine.js         # Peer-to-peer HTTP chunk replication engine
│   └── data/                        # Physical storage directories for nodes 1-4 and metadata.json
│
├── public/                          # Web dashboard (vanilla HTML/CSS/JS)
│   ├── index.html                   # Dashboard layout: Leader Banner, Timeline, Clocks, Modals
│   ├── css/styles.css               # Modern glassmorphism UI styles, badges & modals
│   └── js/
│       ├── app.js                   # Client controller: WebSockets, Timeline, Locks, Bully & Modals
│       ├── rpcClient.js             # Client JSON-RPC 2.0 wrapper
│       └── streamClient.js          # Client-side chunk reconstructor & downloader
│
├── test_fa1.js                      # FA-1 test suite
├── test_fa2.js                      # FA-2 18-step verification test suite (51 assertions)
├── test_chat.js                     # User chat test suite
├── test_chat_actions.js             # Chat unsend, forward & file sharing tests
└── test_new_users.js                # Multi-user accounts test suite
```

---

## Assumptions & Limitations

1. **Local Simulation**: All 4 storage nodes and coordinator run as simulated distributed processes on independent local ports on a single host machine, avoiding cloud or Docker dependencies.
2. **Deterministic Bully Priority**: The Master Coordinator is assigned the highest rank (100) so that reviving it predictably reclaims leadership for presentation demos.
3. **Lock Lease Recovery**: Document locks have an automatic 60-second lease timeout to safeguard against abandoned locks if an editor closes their browser.
4. **Local Disk Persistence**: All metadata and document chunks are persisted to `storage_nodes/data/`, surviving server restarts without needing external database servers (MongoDB/PostgreSQL).
