# Presentation & Viva Demonstration Script

**Project Title:** Distributed Collaborative Document Management & Synchronization System  
**Scope:** FA-1 (P2P Streaming, Chunking, SHA-256, Access Control) + FA-2 (Lamport Clocks, Vector Clocks, Bully Election, Distributed Mutual Exclusion, Deadlock Detection, Anti-Entropy Recovery)

---

## Presentation Walkthrough (23 Sequential Steps)

### Step 1: Start Application
* **WHAT I CLICK:** Open PowerShell terminal in project root and run:
  ```powershell
  node server.js
  ```
* **WHAT SHOULD HAPPEN:** Terminal logs show Master Coordinator starting on port 3000 (WebSocket: 3001) and all 4 Storage Nodes starting on ports 8001–8004.
* **WHAT I SHOULD SAY IN VIVA:**  
  *"Good morning/afternoon examiners. Our application architecture consists of a Master Coordinator and 4 distributed storage nodes running on independent ports. The Coordinator manages cluster metadata and distributed coordination, while storage nodes handle chunk-based storage and peer-to-peer data replication."*

---

### Step 2: Login
* **WHAT I CLICK:** Open `http://localhost:3000` in browser. Select `user_a (User A)` from dropdown, enter `passA`, and click **Sign In**.
* **WHAT SHOULD HAPPEN:** Successful sign-in; navbar shows `Logged in as: User A (user_a)` and main dashboard appears.
* **WHAT I SHOULD SAY IN VIVA:**  
  *"User sessions use HTTP-only cryptographic session cookies to establish identity and prevent header impersonation. All subsequent operations are validated against this authenticated session."*

---

### Step 3: Show Architecture / Cluster Topology
* **WHAT I CLICK:** Point to the **Cluster Topology & Live Node Status** panel on the left.
* **WHAT SHOULD HAPPEN:** SVG visualizer displays the Coordinator in the center connected to all 4 storage nodes (Alpha, Beta, Gamma, Delta), all showing green `HEALTHY` badges with live heartbeat monitoring.
* **WHAT I SHOULD SAY IN VIVA:**  
  *"Here we see our dynamic topology visualizer. The Coordinator monitors storage node health using continuous 2-second heartbeats. All 4 storage nodes are registered and currently healthy."*

---

### Step 4: Upload Document
* **WHAT I CLICK:** In the **Create Quick Text Document** box, enter title `FA2_Lamport_Test.txt`, enter sample text, and click **Create & Sync**.
* **WHAT SHOULD HAPPEN:** The document is split into 512KB chunks, SHA-256 hashes are calculated, and the document appears in the **My Documents** table with version `v1`.
* **WHAT I SHOULD SAY IN VIVA:**  
  *"When a file is uploaded, the coordinator chunks it into 512KB blocks and computes a SHA-256 cryptographic checksum for each block. This ensures data integrity throughout storage and transfer."*

---

### Step 5: Show Replication
* **WHAT I CLICK:** Point to the **Replicas** column of the newly uploaded document in the table.
* **WHAT SHOULD HAPPEN:** Displays 2 replica nodes (for example `node_4 ✓` and `node_2 ✓`).
* **WHAT I SHOULD SAY IN VIVA:**  
  *"Our system enforces a replication factor of $R = 2$. The primary node streams the chunk to a designated peer using direct peer-to-peer HTTP POST streaming, and both nodes verify the SHA-256 hash before committing to disk."*

---

### Step 6: Show Lamport Logical Clock
* **WHAT I CLICK:** Point to the **Distributed Event Timeline & Lamport Logical Clocks** panel.
* **WHAT SHOULD HAPPEN:** Displays clock values for Coordinator and Nodes (e.g. `L=...`), and the timeline table lists the recent `UPLOAD` event with its Lamport timestamp.
* **WHAT I SHOULD SAY IN VIVA:**  
  *"Physical clocks drift in distributed networks. We implemented Lamport Logical Clocks where every node updates its counter according to $L = \max(L_{\text{local}}, L_{\text{msg}}) + 1$. This provides a partial ordering of events across the distributed cluster without relying on NTP or physical wall clocks."*

---

### Step 7: Show Multi-User Sharing & Access Control
* **WHAT I CLICK:** Click the cyan **Share** button (<i class="fa-solid fa-share-nodes"></i>) on `FA2_Lamport_Test.txt`. Select `user_b`, choose `WRITE (View, Download & Re-share)`, and click **Grant Permission**.
* **WHAT SHOULD HAPPEN:** Modal shows success. In a second browser window logged in as `user_b`, the file appears under the **Shared With Me** tab.
* **WHAT I SHOULD SAY IN VIVA:**  
  *"Our access control model supports OWNER, READ, and WRITE permissions. Here, User A shared the document with User B granting WRITE permission, which is dynamically persisted in metadata and broadcast to User B via WebSocket."*

---

### Step 8: Show Distributed Locking (Mutual Exclusion)
* **WHAT I CLICK:** As User A, click **Edit** (<i class="fa-solid fa-pen-to-square"></i>) on the document $\rightarrow$ click **Acquire Lock**. Then switch to User B's window and click **Edit** on the same document $\rightarrow$ click **Acquire Lock**.
* **WHAT SHOULD HAPPEN:** User A receives the lock (`🔒 LOCKED - Held by user_a`). User B is blocked with alert: `⛔ ACCESS DENIED / WAITING FOR LOCK (HTTP 423 Locked)`.
* **WHAT I SHOULD SAY IN VIVA:**  
  *"To guarantee safety during collaborative editing, we implemented Distributed Mutual Exclusion with leased locks. While User A holds the active lease, User B is blocked from modifying the document, preventing race conditions."*

---

### Step 9: Show Version History
* **WHAT I CLICK:** As User A, edit the text, click **Save New Version & Replicate**, release lock, and click the cyan **Version History** button (<i class="fa-solid fa-clock-rotate-left"></i>).
* **WHAT SHOULD HAPPEN:** The modal opens showing both `v2` (new version) and `v1` (historical version) with author, Lamport time, vector clock, and SHA-256 checksum.
* **WHAT I SHOULD SAY IN VIVA:**  
  *"Versions are immutable. Each update increments the version counter, recalculates the SHA-256 checksum, and preserves previous versions on disk so historical states remain accessible."*

---

### Step 10: Show Vector Clocks
* **WHAT I CLICK:** Point to the **Version & Vector Clock** column in the document catalog and inside the Version History modal.
* **WHAT SHOULD HAPPEN:** Displays vector clock JSON tag: `{"node_1":1, "node_2":2, ...}`.
* **WHAT I SHOULD SAY IN VIVA:**  
  *"While Lamport clocks give partial ordering, they cannot distinguish concurrent events from causal ones. Vector clocks maintain an array of clocks per process, capturing causal dependencies ($V_A \rightarrow V_B$) and concurrency ($V_A \parallel V_B$) accurately."*

---

### Step 11: Show Conflict Detection
* **WHAT I CLICK:** Click **Edit** on the document $\rightarrow$ click the red button **Simulate Concurrent Update (Trigger Conflict)**.
* **WHAT SHOULD HAPPEN:** A red pulsing banner appears: `⚠️ CONFLICT DETECTED (Concurrent Updates via Vector Clock)`, and the document in the table is flagged with a red `CONFLICT` tag.
* **WHAT I SHOULD SAY IN VIVA:**  
  *"When concurrent updates occur without a causal 'happened-before' relationship ($V_A \parallel V_B$), the system detects the divergence using our Vector Clock comparison algorithm and prevents accidental overwrites."*

---

### Step 12: Resolve Conflict
* **WHAT I CLICK:** Click **Resolve Conflict** on the banner $\rightarrow$ review Branch A vs Branch B side-by-side $\rightarrow$ click **Save Merged Version (Vector Clock Component-Wise Max)**.
* **WHAT SHOULD HAPPEN:** Version advances to `v3`, the conflict alert clears, and the vector clock merges using component-wise maximum:
  $$V_{\text{new}} = \max(V_A, V_B) + 1$$
* **WHAT I SHOULD SAY IN VIVA:**  
  *"Conflict resolution reconciles the diverging branches. We merge the changes and update the vector clock to the component-wise maximum of both branches, restoring a consistent global state."*

---

### Step 13: Kill Coordinator
* **WHAT I CLICK:** In the top **Leader & Cluster Coordination Banner**, click **Kill Coordinator** (<i class="fa-solid fa-power-off"></i>).
* **WHAT SHOULD HAPPEN:** The Coordinator state changes to `CRASHED / OFFLINE (Simulated)`.
* **WHAT I SHOULD SAY IN VIVA:**  
  *"In distributed systems, the leader can crash. When the Coordinator fails, storage nodes must elect a new leader using the Bully Election Algorithm."*

---

### Step 14: Bully Algorithm Leader Election
* **WHAT I CLICK:** Watch the **Leader Election Logs** terminal.
* **WHAT SHOULD HAPPEN:** Storage nodesAlpha, Beta, Gamma, and Delta run the election. Storage Node Delta (`node_4`) wins because it has the highest surviving priority (Rank 4). Current Leader updates to `Storage Node Delta`.
* **WHAT I SHOULD SAY IN VIVA:**  
  *"In the Bully Algorithm, any node can initiate an election. Higher-ranked processes answer lower ones. Because Node 4 has Rank 4, which is higher than Nodes 1, 2, and 3, it bullies the lower nodes and takes over as the new leader."*

---

### Step 15: Revive Coordinator
* **WHAT I CLICK:** Click **Revive Coordinator** (<i class="fa-solid fa-rotate"></i>).
* **WHAT SHOULD HAPPEN:** Master Coordinator restarts, asserts its superior priority (Rank 100), and reclaims leadership. Current Leader banner switches back to `Master Coordinator`.
* **WHAT I SHOULD SAY IN VIVA:**  
  *"When a higher-priority process recovers, the Bully algorithm dictates that it immediately asserts its priority. Because the Coordinator has Rank 100, it reclaims leadership from Node 4."*

---

### Step 16: Kill Storage Node
* **WHAT I CLICK:** Identify the replica node of a document (e.g. `node_4`). In the Fault Injection panel, click **Kill Node 4 (Delta)**.
* **WHAT SHOULD HAPPEN:** Node 4 circle in topology turns red with status `FAILED`.
* **WHAT I SHOULD SAY IN VIVA:**  
  *"We now simulate a physical storage node failure. Node 4 is now offline."*

---

### Step 17: Download Through Healthy Replica (Failover)
* **WHAT I CLICK:** Click the green **Download** button (<i class="fa-solid fa-cloud-arrow-down"></i>) on the document.
* **WHAT SHOULD HAPPEN:** The download succeeds seamlessly with status HTTP 200, served by surviving replica `node_2`.
* **WHAT I SHOULD SAY IN VIVA:**  
  *"The Coordinator automatically detects that Node 4 is unreachable and redirects the download request to the surviving replica Node 2. This proves high availability and fault-tolerant failover with zero downtime."*

---

### Step 18: Revive Node
* **WHAT I CLICK:** In the Fault Injection panel, click **Revive & Sync Node 4**.
* **WHAT SHOULD HAPPEN:** Node status transitions to `RECOVERING`.
* **WHAT I SHOULD SAY IN VIVA:**  
  *"Node 4 is now revived. However, before it can serve client requests, it enters the RECOVERING state to verify if any chunks are missing."*

---

### Step 19: Node Recovery & Re-Synchronization
* **WHAT I CLICK:** Point to the **Node Recovery & Re-Synchronization** panel and click **Sync Nodes**.
* **WHAT SHOULD HAPPEN:** Node transitions: `RECOVERING` $\rightarrow$ `SYNCING` $\rightarrow$ `HEALTHY`. The terminal logs P2P peer chunk verification and SHA-256 validation.
* **WHAT I SHOULD SAY IN VIVA:**  
  *"Our Anti-Entropy synchronization manager compares the recovering node's local files against the cluster metadata catalog. Any missing chunks are pulled directly from healthy peers via P2P streaming and validated against their SHA-256 hash before the node returns to HEALTHY status."*

---

### Step 20: Simulate Deadlock
* **WHAT I CLICK:** In the **Mutual Exclusion & Deadlock Detection** panel, click **Simulate Deadlock (User A ⇄ User B)**.
* **WHAT SHOULD HAPPEN:** The **Wait-For Graph (WFG)** visualizer displays a cycle: `user_a ➔ user_b ➔ user_a`, with red status `DEADLOCK DETECTED`.
* **WHAT I SHOULD SAY IN VIVA:**  
  *"Here we simulate a classic circular wait: User A holds Document X and waits for Document Y, while User B holds Document Y and waits for Document X. Our Deadlock Detector runs Depth-First Search over the directed Wait-For Graph and flags the cycle."*

---

### Step 21: Resolve Deadlock
* **WHAT I CLICK:** Click **Resolve Deadlock** (<i class="fa-solid fa-wand-magic-sparkles"></i>).
* **WHAT SHOULD HAPPEN:** Deadlock cycle is broken; status badge returns to green `NO DEADLOCK`.
* **WHAT I SHOULD SAY IN VIVA:**  
  *"To resolve the deadlock, the coordinator selects a victim using the 'abort youngest request' policy. It cancels the blocked dependency, breaking the cycle and allowing the system to resume normal operation."*

---

### Step 22: Show Event / Message Monitor
* **WHAT I CLICK:** Scroll to the **Distributed Event / Message Monitor** at the bottom and click through the category tabs: `[RPC]`, `[P2P]`, `[CLOCK]`, `[VECTOR]`, `[LOCK]`, `[ELECTION]`, `[DEADLOCK]`, `[RECOVERY]`.
* **WHAT SHOULD HAPPEN:** The terminal dynamically filters the logs in real time to show only messages belonging to the selected category.
* **WHAT I SHOULD SAY IN VIVA:**  
  *"Our Protocol Monitor provides complete observability. We can filter the real-time message stream by JSON-RPC calls, peer-to-peer chunk transfers, Lamport ticks, vector clocks, mutex locks, elections, deadlocks, and recovery events."*

---

### Step 23: Final Healthy Cluster Overview
* **WHAT I CLICK:** Return to the top of the dashboard and show the overall system.
* **WHAT SHOULD HAPPEN:**
  * Coordinator: `ONLINE` (Rank 100, Leader)
  * All 4 Storage Nodes: `HEALTHY`
  * Replication: Factor 2
  * Deadlock: `NO DEADLOCK`
  * Conflicts: Cleared / Consistent
* **WHAT I SHOULD SAY IN VIVA:**  
  *"In conclusion, all FA-1 and FA-2 requirements are fully verified and operational: P2P streaming, Lamport clocks, vector clocks, versioning, mutual exclusion, deadlock detection, Bully leader elections, and fault-tolerant replica recovery. Thank you."*
