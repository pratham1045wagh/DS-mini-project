# FA-1 + FA-2 Complete Manual Testing Guide

A step-by-step, beginner-friendly testing guide for the **Distributed Collaborative Document Management & Synchronization System**. Follow these instructions sequentially from Step 1 through Step 18 to demonstrate and verify every feature.

---

## Quick Reference Summary

| Test Step | Feature Under Test | Expected Action | Viva Topic |
|---|---|---|---|
| **1. Start App** | Node & Server Initialization | 1 Coordinator + 4 Storage Nodes online | Distributed Cluster Architecture |
| **2. Login** | User Authentication & Session | User A logged in with session cookie | Access Control & Security |
| **3. Verify Cluster** | Node Health & Topology | Coordinator + 4 Nodes HEALTHY, P2P active | Heartbeat Monitoring & Membership |
| **4. Upload Test** | Chunking & SHA-256 Storage | File chunked into 512KB blocks with hash | Distributed Storage & Integrity |
| **5. Replication Test** | P2P Replica Placement | Replication Factor $R = 2$ on two nodes | Fault Tolerance & Data Redundancy |
| **6. Lamport Clock** | Logical Timestamping | Clock ticks monotonically on each event | Causal Ordering & Logical Time |
| **7. Sharing Test** | Multi-User Authorization | User A grants WRITE access to User B | Access Control Lists (ACLs) |
| **8. Lock Test** | Distributed Mutual Exclusion | User B blocked with HTTP 423 while locked | Critical Sections & Mutual Exclusion |
| **9. Versioning Test** | Immutable History Snapshots | Creates v2, preserves v1 on disk | Event Sourcing & Auditability |
| **10. Vector Clock** | Concurrency & Conflict Merge | Detects $V_A \parallel V_B$, merges max clock | Vector Clocks & Reconciliation |
| **11. Bully Election** | Failover Leader Election | Node 4 wins election; Coord reclaims | Bully Election Algorithm |
| **12. Node Failure** | Replica Failover Download | Document served from healthy replica | High Availability & Zero Downtime |
| **13. Node Recovery** | P2P Re-Synchronization | FAILED $\rightarrow$ RECOVERING $\rightarrow$ HEALTHY | Dynamic Cluster Re-synchronization |
| **14. Deadlock Test** | Wait-For Graph (WFG) | DFS detects cycle; aborts victim request | Distributed Deadlock Detection |
| **15. Event Monitor** | Subsystem Categorization | Dynamic filter across 9 subsystems | Observability & Distributed Tracing |
| **16. JSON-RPC Test** | Protocol Specification | Standardized JSON-RPC 2.0 calls | RPC vs REST Communication |
| **17. WebSocket Test** | Real-Time Push Events | Instant updates across all clients | Push Notifications & Event Streaming |
| **18. Health Check** | Cluster Steady-State | All nodes HEALTHY, no deadlocks | Distributed System Reliability |

---

## 1. Start the Application

### WHAT TO CLICK
Open your PowerShell or terminal in the project directory and run:
```powershell
node server.js
```

### WHAT SHOULD HAPPEN
1. The server starts the **Master Coordinator** on port `3000` (WebSocket on port `3001`).
2. The server initializes all 4 Storage Nodes on independent ports:
   * **Node 1 (Alpha):** Port `8001` (WebSocket: `9001`)
   * **Node 2 (Beta):** Port `8002` (WebSocket: `9002`)
   * **Node 3 (Gamma):** Port `8003` (WebSocket: `9003`)
   * **Node 4 (Delta):** Port `8004` (WebSocket: `9004`)
3. Storage nodes send initial heartbeat registrations to the Coordinator.

### WHAT TO LOOK FOR
Look at the terminal output for:
* `[Coordinator] HTTP server listening on port 3000`
* `[Coordinator] WebSocket server listening on port 3001`
* `[StorageNode node_1] Listening on port 8001`
* `[Cluster] Node node_1 registered (HEALTHY)` for all 4 nodes.

### WHAT TO SAY IN VIVA
> *"Examiner, our system starts as a distributed cluster comprising a Master Coordinator and 4 autonomous Storage Nodes. Each node runs as an independent HTTP and WebSocket server on distinct ports, communicating via JSON-RPC 2.0 and P2P HTTP streaming."*

---

## 2. Login

### WHAT TO CLICK
1. Open your browser and navigate to:
   ```
   http://localhost:3000
   ```
2. In the **Sign In** modal:
   * Select User: `user_a (User A)`
   * Enter Password: `passA`
3. Click the **Sign In** button.

### WHAT SHOULD HAPPEN
The client authenticates via `POST /api/auth/login`. An authenticated session cookie is created, and the full interactive dashboard loads.

### WHAT TO LOOK FOR
* In the top-right navbar: `Logged in as: User A (user_a)`.
* Green status badge: `Coordinator ONLINE`.
* Current Leader banner: `Master Coordinator` with green `COMPLETED` badge.

### WHAT TO SAY IN VIVA
> *"Examiner, we enforce role-based access control. User A authenticates with the Coordinator, which establishes a secure HTTP session used by our JSON-RPC and REST client calls."*

---

## 3. Verify Cluster

### WHAT TO CLICK
1. Look at the **Cluster Topology & Live Node Status** panel on the left side of the dashboard.
2. Observe the interactive node status cards for Node 1, Node 2, Node 3, and Node 4.

### WHAT SHOULD HAPPEN
The dashboard communicates with the Coordinator via WebSocket to receive live heartbeat statuses from all 4 storage nodes.

### WHAT TO LOOK FOR
* All 4 nodes show green glowing borders and `HEALTHY` badges.
* The topology badge displays `P2P Sync Active`.
* Heartbeat indicators update every 5 seconds.

### WHAT TO SAY IN VIVA
> *"Examiner, the Coordinator maintains cluster membership using continuous heartbeat pings. Every 5 seconds, storage nodes report their status and disk capacity. If a node misses 3 consecutive heartbeats, it is marked as FAILED."*

---

## 4. Upload Test

### WHAT TO CLICK
1. Scroll to the **Create Quick Text Document** section.
2. In **Document Title**, type: `FA2_Demo_Test.txt`
3. In **Content**, type: `Distributed Systems FA-2 Demonstration`
4. Click the blue **Create & Sync** button (`#quickCreateBtn`).

### WHAT SHOULD HAPPEN
1. The client splits the document into 512KB binary chunks.
2. A SHA-256 hash is calculated for each chunk.
3. The Coordinator assigns 2 replica storage nodes based on round-robin and storage capacity.
4. Chunks are uploaded and replicated.

### WHAT TO LOOK FOR
* Progress bar animates to 100%.
* Notification popup: `✅ Success! Document created and synced across replicas`.
* Document appears in the **My Documents** table showing Version `v1` and Lock Status `FREE`.

### WHAT TO SAY IN VIVA
> *"Examiner, document upload demonstrates file chunking and cryptographic integrity. Large files are segmented into fixed-size chunks, each verified with a SHA-256 hash before being dispatched to storage nodes."*

---

## 5. Replication Test

### WHAT TO CLICK
1. In the **My Documents** table, locate `FA2_Demo_Test.txt`.
2. Inspect the **Replicas** column (e.g., `node_4 ✓`, `node_2 ✓`).
3. Scroll to the **Distributed Event / Message Monitor** at the bottom.

### WHAT SHOULD HAPPEN
The primary storage node automatically initiates peer-to-peer (P2P) replication to the second assigned storage node to satisfy the system's replication factor $R = 2$.

### WHAT TO LOOK FOR
* Two distinct green node pills appear under the Replicas column.
* In the Event Monitor, look for the entry:
  `[P2P] Chunk #0/0 replicated: node_X → node_Y | SHA-256: ... | Status: 200 OK`.

### WHAT TO SAY IN VIVA
> *"Examiner, our system enforces a replication factor of R = 2. When the primary node receives chunk 0, it directly streams a copy to the secondary replica over a peer-to-peer HTTP connection, guaranteeing data redundancy."*

---

## 6. Lamport Clock Test

### WHAT TO CLICK
1. Look at the **Distributed Event Timeline & Lamport Logical Clocks** panel.
2. Note the numerical clock values inside the chips:
   `COORDINATOR L=...`, `NODE 1 L=...`, `NODE 2 L=...`.
3. In the document list, click the view icon (👁️) or upload another quick file.
4. Observe the Lamport clock chips update.

### WHAT SHOULD HAPPEN
Every local event and distributed message exchange causes the logical clock to increment following Lamport's algorithm:
$$L = \max(L_{\text{local}}, L_{\text{msg}}) + 1$$

### WHAT TO LOOK FOR
* The clock counter chips advance sequentially.
* The timeline renders a new event card tagged with `[L=X]`, identifying the exact causal sequence number.

### WHAT TO SAY IN VIVA
> *"Examiner, physical wall clocks suffer from clock skew. We use Lamport Logical Clocks to establish partial ordering across all nodes. Every message carries the sender's clock, and receivers advance their clock to the maximum plus one."*

---

## 7. Sharing Test

### WHAT TO CLICK
1. In the document row for `FA2_Demo_Test.txt`, click the cyan **Share** button (<i class="fa-solid fa-share-nodes"></i>).
2. In the modal:
   * Target User: Select `user_b (User B)`
   * Permission Level: Select `WRITE (View, Download & Re-share)`
3. Click **Grant Permission**.
4. Open an Incognito window (or second browser) at `http://localhost:3000`.
5. Log in as `user_b` with password `passB`.
6. Click the **Shared With Me** tab in the document catalog.

### WHAT SHOULD HAPPEN
The Coordinator updates the document's Access Control List (ACL) and records the sharing event in the distributed timeline.

### WHAT TO LOOK FOR
* In User A's browser: Success alert confirms sharing.
* In User B's browser: `FA2_Demo_Test.txt` appears in the **Shared With Me** view with a purple `WRITE` badge.

### WHAT TO SAY IN VIVA
> *"Examiner, access control is enforced at the Coordinator level. When User A grants WRITE access to User B, the permission is saved in metadata, allowing multi-user collaboration while restricting unauthorized edits."*

---

## 8. Lock Test (Distributed Mutual Exclusion)

### WHAT TO CLICK
1. In **User A's** window:
   * Click the purple **Edit** button (<i class="fa-solid fa-pen-to-square"></i>) on `FA2_Demo_Test.txt`.
   * Click the **Acquire Lock** button.
2. In **User B's** window:
   * Click the **Edit** button on the same document.
   * Observe lock status. Click **Acquire Lock** as User B.
3. In **User A's** window:
   * Click **Release Lock**.
4. In **User B's** window:
   * Click **Acquire Lock** again.

### WHAT SHOULD HAPPEN
* User A acquires the exclusive 60-second lease.
* User B is blocked with `HTTP 423 Locked`.
* When User A releases the lock, User B can acquire it.

### WHAT TO LOOK FOR
* User A sees: `🔒 LOCKED - Held by user_a (You)`.
* User B sees: `🔒 LOCKED - Held by user_a` and error message: `⛔ ACCESS DENIED / WAITING FOR LOCK (HTTP 423 Locked)`.
* Upon release, User B's status changes to `🔒 LOCKED - Held by user_b (You)`.

### WHAT TO SAY IN VIVA
> *"Examiner, Distributed Mutual Exclusion guarantees that only one collaborator can modify a document at a time. The Coordinator grants an exclusive leased lock for 60 seconds. Any concurrent attempt returns HTTP 423 Locked, preventing write-write conflicts."*

---

## 9. Versioning Test

### WHAT TO CLICK
1. With the lock held, open the Edit Document modal on `FA2_Demo_Test.txt`.
2. In the textarea, append:
   `\nVersion 2 update by User A.`
3. Click **Save New Version & Replicate**.
4. In the document row, click the cyan **History** button (<i class="fa-solid fa-clock-rotate-left"></i>).

### WHAT SHOULD HAPPEN
The Coordinator increments the version number from `v1` to `v2`, creates an immutable historical snapshot, re-chunks the file, and broadcasts the update.

### WHAT TO LOOK FOR
* In the table: Document tag shows `v2`.
* In the Version History modal: Two versions are listed:
  * `v2` (Current): Author `user_a`, vector clock, timestamp, SHA-256 hash.
  * `v1` (Previous): Author `user_a`, original hash and Lamport timestamp.

### WHAT TO SAY IN VIVA
> *"Examiner, our versioning system ensures immutability. Rather than overwriting files in place, each update creates a new immutable revision snapshot with its own SHA-256 checksum and vector clock, enabling historical auditing."*

---

## 10. Vector Clock Conflict Test

### WHAT TO CLICK
1. In the document row or Edit modal, click the red button:
   **Simulate Concurrent Update (Trigger Conflict)**.
2. Observe the dashboard.
3. When the red pulsing banner appears:
   `⚠️ CONFLICT DETECTED (Concurrent Updates via Vector Clock)`,
   click **Resolve Conflict**.
4. In the conflict comparison modal:
   * Inspect **Branch Version A** vs **Branch Version B** vector clocks.
5. In the Merged Content box, make your edit and click:
   **Save Merged Version (Vector Clock Component-Wise Max)**.

### WHAT SHOULD HAPPEN
1. Concurrent vector clocks ($V_A \parallel V_B$) are generated without a causal happened-before relationship.
2. Conflict detection triggers a system-wide alert.
3. Merging computes the component-wise maximum vector clock:
   $$V_{\text{merged}}[i] = \max(V_A[i], V_B[i]) + 1$$
4. A clean reconciled Version `v3` is saved.

### WHAT TO LOOK FOR
* Red banner appears with concurrent clock details.
* Modal clearly shows divergence between Branch A and Branch B.
* Clicking "Save Merged Version" clears the banner and creates version `v3`.

### WHAT TO SAY IN VIVA
> *"Examiner, when network partitions or asynchronous edits occur, two branches may diverge. Vector clocks detect this because neither vector dominates the other ($V_A \parallel V_B$). Our conflict resolution merges the branches by computing the component-wise maximum of both vectors plus one."*

---

## 11. Bully Election Test

### WHAT TO CLICK
1. Look at the top **Leader & Cluster Coordination Banner**:
   `Current Leader: Master Coordinator (Rank 100)`.
2. Click the red button: **Kill Coordinator** (<i class="fa-solid fa-power-off"></i>).
3. Wait 2 seconds for the election to execute.
4. Next, click the green button: **Revive Coordinator** (<i class="fa-solid fa-rotate"></i>).

### WHAT SHOULD HAPPEN
1. Killing the coordinator halts its heartbeat.
2. Storage nodes detect the failure and run the Bully Algorithm. Node 4 (Delta, Rank 4) bullies lower nodes and becomes leader.
3. Reviving the Coordinator causes it to assert its static Rank 100 priority, reclaiming leadership.

### WHAT TO LOOK FOR
* After kill: Current Leader changes to `Storage Node Delta (node_4)` (Rank 4).
* In the Leader Election terminal: Shows messages `ELECTION sent by node_1` $\rightarrow$ `COORDINATOR declared by node_4`.
* After revive: Current Leader immediately returns to `Master Coordinator (Rank 100)`.

### WHAT TO SAY IN VIVA
> *"Examiner, the Bully Algorithm selects the operational process with the highest priority rank. The Coordinator has Rank 100, while storage nodes have ranks 1 through 4. When Coordinator dies, Node 4 wins the election. When Coordinator revives, its Rank 100 asserts dominance and reclaims cluster leadership."*

---

## 12. Node Failure Test

### WHAT TO CLICK
1. Identify which two nodes hold replicas for `FA2_Demo_Test.txt` (e.g., `node_4` and `node_2`).
2. Under **Fault Injection Control Panel**, click **Kill Node 4 (Delta)**.
3. Observe Node 4's status card.
4. Click the green **Download** button (<i class="fa-solid fa-cloud-arrow-down"></i>) on `FA2_Demo_Test.txt`.

### WHAT SHOULD HAPPEN
Node 4's process is stopped. When the user requests a download, the Coordinator detects Node 4 is unavailable and automatically redirects chunk retrieval to surviving replica Node 2.

### WHAT TO LOOK FOR
* Node 4 status card turns red: `FAILED`.
* The download completes with `HTTP 200` without any error message or delay.
* The downloaded file content matches the original text.

### WHAT TO SAY IN VIVA
> *"Examiner, this demonstrates fault tolerance. Because chunks are replicated across two nodes, the crash of Node 4 does not cause downtime. The Coordinator automatically performs failover to the surviving replica Node 2."*

---

## 13. Node Recovery Test

### WHAT TO CLICK
1. Under **Fault Injection Control Panel**, click **Revive & Sync Node 4**.
2. Observe the node status transition.
3. Click the cyan **Sync Nodes** button.

### WHAT SHOULD HAPPEN
Node 4 is restarted. It enters a recovery pipeline, verifies its local chunks against the Coordinator's catalog, requests any missing chunks from healthy peer nodes via P2P stream, verifies SHA-256 hashes, and returns to `HEALTHY`.

### WHAT TO LOOK FOR
* Node 4 transitions through:
  `FAILED` $\longrightarrow$ `RECOVERING` $\longrightarrow$ `SYNCING` $\longrightarrow$ `HEALTHY`.
* Status card border turns green with `HEALTHY` badge.
* Terminal logs: `Cluster synchronization completed for 4 nodes (0 chunk(s) restored)`.

### WHAT TO SAY IN VIVA
> *"Examiner, when a failed node recovers, it cannot immediately serve traffic because its state may be stale. Our protocol transitions it through RECOVERING and SYNCING phases, pulling missing chunks from peers via P2P HTTP streaming and validating SHA-256 integrity before declaring it HEALTHY."*

---

## 14. Deadlock Test

### WHAT TO CLICK
1. Scroll to the **Mutual Exclusion & Deadlock Detection** section.
2. Click **Simulate Deadlock (User A ⇄ User B)**.
3. Observe the **Wait-For Graph (WFG)** container.
4. Click the **Resolve Deadlock** button (<i class="fa-solid fa-wand-magic-sparkles"></i>).

### WHAT SHOULD HAPPEN
1. A circular dependency is injected: User A holds Doc 1 and waits for Doc 2 (held by User B); User B holds Doc 2 and waits for Doc 1.
2. The deadlock engine runs Depth-First Search (DFS), detects the directed cycle, and displays an alert.
3. Resolving deadlocks aborts the youngest waiting request, severing the cycle.

### WHAT TO LOOK FOR
* While deadlocked: Red badge `⚠️ DEADLOCK DETECTED!` and cycle visualizer:
  `user_a ➔ user_b ➔ user_a`.
* After clicking Resolve: Badge turns green `NO DEADLOCK` and cycle disappears.

### WHAT TO SAY IN VIVA
> *"Examiner, deadlock occurs when processes form a circular wait dependency. We maintain a dynamic Wait-For Graph and execute DFS cycle detection. When a cycle is detected, our resolution policy aborts the youngest waiting request, safely eliminating the cycle."*

---

## 15. Event Monitor Test

### WHAT TO CLICK
1. Scroll to the **Distributed Event / Message Monitor** at the bottom of the page.
2. Click each filter tab one by one:
   * **[RPC]**
   * **[P2P]**
   * **[CLOCK]**
   * **[VECTOR]**
   * **[LOCK]**
   * **[ELECTION]**
   * **[DEADLOCK]**
   * **[RECOVERY]**
   * **All Events**

### WHAT SHOULD HAPPEN
The log terminal filters dynamically to show only the messages matching the selected distributed subsystem tag.

### WHAT TO LOOK FOR
* Clicking `[LOCK]` shows only lock acquire/release messages.
* Clicking `[ELECTION]` shows Bully election messages.
* Clicking `[DEADLOCK]` shows Wait-For Graph cycle events.
* Clicking `All Events` restores the full chronological feed.

### WHAT TO SAY IN VIVA
> *"Examiner, observability is essential in distributed systems. Our Event Monitor categorizes real-time distributed traces across 9 subsystems, allowing administrators to isolate RPC calls, P2P replications, or election sequences on demand."*

---

## 16. JSON-RPC Test

### WHAT TO CLICK
Open your terminal and execute:
```powershell
$body = '{"jsonrpc":"2.0","id":1,"method":"rpc.get_cluster_status","params":{}}'
Invoke-RestMethod -Uri "http://localhost:3000/api/rpc" -Method Post -Body $body -ContentType "application/json"
```

### WHAT SHOULD HAPPEN
The Coordinator processes the JSON-RPC 2.0 payload and returns a compliant JSON response with matching request ID.

### WHAT TO LOOK FOR
Terminal prints:
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "coordinator": { "status": "ONLINE", "port": 3000 },
    "nodes": [ ... ]
  }
}
```

### WHAT TO SAY IN VIVA
> *"Examiner, our frontend and nodes use the JSON-RPC 2.0 specification for remote procedure calls, providing structured error envelopes and correlation IDs over standard HTTP POST endpoints."*

---

## 17. WebSocket Test

### WHAT TO CLICK
1. In your browser, press **F12** to open Developer Tools.
2. Navigate to the **Network** tab and select the **WS** filter.
3. Refresh the page or click any button (e.g. acquire lock or upload file).
4. Inspect the frames exchanged with `ws://localhost:3001`.

### WHAT SHOULD HAPPEN
The browser maintains a persistent full-duplex WebSocket connection receiving broadcast events.

### WHAT TO LOOK FOR
Look at the WebSocket frames for event types:
* `INITIAL_STATE`
* `LEADER_CHANGED`
* `LOCK_ACQUIRED`
* `TIMELINE_EVENT`
* `CLOCKS_UPDATED`

### WHAT TO SAY IN VIVA
> *"Examiner, WebSockets provide bi-directional event push on port 3001. Whenever state changes in the cluster—such as a node failing or a lock being acquired—the Coordinator immediately pushes the update to all connected clients without polling."*

---

## 18. Final Health Check

### WHAT TO LOOK FOR
Verify that the cluster has returned to a completely healthy steady state:
* **Master Coordinator:** `ONLINE` (Rank 100)
* **Current Leader:** `Master Coordinator` with green `COMPLETED` badge.
* **Storage Nodes:** Node 1, Node 2, Node 3, and Node 4 are all `HEALTHY`.
* **Topology:** `P2P Sync Active`.
* **Deadlock Status:** `NO DEADLOCK`.
* **Conflict Status:** No active conflict warnings.

### WHAT TO SAY IN VIVA
> *"Examiner, all 18 distributed systems tests have passed. The cluster is operating in a healthy, fault-tolerant state with active logical clocks, multi-user concurrency control, automated failover, and peer-to-peer re-synchronization."*

---
