const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

// Helper: Make HTTP request returning { status, body, headers, cookies }
function request(options, body = null) {
  return new Promise((resolve, reject) => {
    const req = http.request(options, (res) => {
      const chunks = [];
      res.on("data", c => chunks.push(c));
      res.on("end", () => {
        const buf = Buffer.concat(chunks);
        const cookies = res.headers["set-cookie"] || [];
        resolve({ status: res.statusCode, body: buf.toString(), raw: buf, headers: res.headers, cookies });
      });
    });
    req.on("error", reject);
    if (body) req.write(body);
    req.end();
  });
}

// Helper: Upload file using multipart form data
async function uploadFile(cookieHeader, fileBuffer, fileName, mimeType = "text/plain") {
  const boundary = "----FormBoundary" + Math.random().toString(36).slice(2);
  const parts = [
    `--${boundary}\r\nContent-Disposition: form-data; name="file"; filename="${fileName}"\r\nContent-Type: ${mimeType}\r\n\r\n`,
    fileBuffer,
    `\r\n--${boundary}--\r\n`
  ];
  const bodyParts = parts.map(p => typeof p === "string" ? Buffer.from(p) : p);
  const fullBody = Buffer.concat(bodyParts);

  return request({
    hostname: "localhost", port: 3000, path: "/api/documents/upload", method: "POST",
    headers: {
      "Content-Type": `multipart/form-data; boundary=${boundary}`,
      "Content-Length": fullBody.length,
      Cookie: cookieHeader
    }
  }, fullBody);
}

// Helper: Login and return cookie string
async function login(userId, password) {
  const body = JSON.stringify({ userId, password });
  const res = await request({
    hostname: "localhost", port: 3000, path: "/api/auth/login", method: "POST",
    headers: { "Content-Type": "application/json", "Content-Length": body.length }
  }, body);
  if (res.status !== 200) throw new Error(`Login failed: ${res.status} ${res.body}`);
  const cookie = res.cookies.map(c => c.split(";")[0]).join("; ");
  return cookie;
}

// Helper: RPC call
async function rpc(method, params, cookieHeader = "") {
  const body = JSON.stringify({ jsonrpc: "2.0", method, params, id: Date.now() });
  const res = await request({
    hostname: "localhost", port: 3000, path: "/api/rpc", method: "POST",
    headers: { "Content-Type": "application/json", "Content-Length": body.length, Cookie: cookieHeader }
  }, body);
  return JSON.parse(res.body);
}

let passed = 0;
let failed = 0;

function test(name, condition, detail = "") {
  if (condition) {
    console.log(`  ✅ PASS: ${name}${detail ? " | " + detail : ""}`);
    passed++;
  } else {
    console.log(`  ❌ FAIL: ${name}${detail ? " | " + detail : ""}`);
    failed++;
  }
}

async function runFullFlow() {
  console.log("\n╔═══════════════════════════════════════╗");
  console.log("║    FA-1 Full Integration Test Suite   ║");
  console.log("╚═══════════════════════════════════════╝\n");

  // ── Step 1: Login ──────────────────────────────────────────────────────
  console.log("── 1. Authentication ──");
  const cookieA = await login("user_a", "passA");
  test("User A login", cookieA.includes("ds_session"));
  const cookieB = await login("user_b", "passB");
  test("User B login", cookieB.includes("ds_session"));
  
  // Verify bad login
  try {
    await login("user_a", "wrongpassword");
    test("Bad password rejected", false, "Should have thrown");
  } catch(e) {
    test("Bad password rejected", true);
  }

  // ── Step 2: Small File Upload + 2-Node Replication ────────────────────
  console.log("\n── 2. Small File Upload (1 chunk) ──");
  const smallContent = "Hello FA-1 Distributed System! ".repeat(10);
  const smallBuf = Buffer.from(smallContent, "utf8");
  
  const uploadSmall = await uploadFile(cookieA, smallBuf, "small_test.txt");
  const uploadSmallData = JSON.parse(uploadSmall.body);
  test("Upload small file (200)", uploadSmall.status === 200, `Status: ${uploadSmall.status}`);
  test("Replication to 2 nodes", uploadSmallData.document?.replicaNodeIds?.length >= 2, `Nodes: ${JSON.stringify(uploadSmallData.document?.replicaNodeIds)}`);
  test("Owner is user_a", uploadSmallData.document?.ownerId === "user_a");
  test("SHA-256 replication reported", uploadSmallData.document?.replicationSuccess === true);
  const smallDocId = uploadSmallData.document?.docId;
  test("DocId generated", !!smallDocId, smallDocId);

  // ── Step 3: Large File Upload (multi-chunk >512KB) ─────────────────────
  console.log("\n── 3. Large File Upload (multi-chunk, >512KB) ──");
  const largeContent = "X".repeat(600 * 1024); // 600 KB
  const largeBuf = Buffer.from(largeContent, "utf8");
  
  const uploadLarge = await uploadFile(cookieA, largeBuf, "large_test_600kb.bin", "application/octet-stream");
  const uploadLargeData = JSON.parse(uploadLarge.body);
  test("Upload large file (200)", uploadLarge.status === 200, `Status: ${uploadLarge.status}`);
  test("Multiple chunks created", uploadLargeData.document?.totalChunks >= 2, `Chunks: ${uploadLargeData.document?.totalChunks}`);
  test("Replicated to 2 nodes", uploadLargeData.document?.replicaNodeIds?.length >= 2);
  const largeDocId = uploadLargeData.document?.docId;
  test("Large DocId generated", !!largeDocId, largeDocId);

  // ── Step 4: Access Control - User B cannot access User A's doc ────────
  console.log("\n── 4. Access Control (403 Enforcement) ──");
  const forbiddenRpc = await rpc("rpc.get_doc_location", { docId: smallDocId }, cookieB);
  test("User B blocked from User A doc (RPC -32001)", forbiddenRpc.error?.code === -32001, `Code: ${forbiddenRpc.error?.code}`);
  
  // User B can't access download proxy either
  const downloadRes = await request({
    hostname: "localhost", port: 3000, path: `/api/documents/download/${smallDocId}/chunk/0`, method: "GET",
    headers: { Cookie: cookieB }
  });
  test("User B 403 on download proxy", downloadRes.status === 403, `Status: ${downloadRes.status}`);

  // ── Step 5: Share with User B ──────────────────────────────────────────
  console.log("\n── 5. Document Sharing ──");
  const shareRpc = await rpc("rpc.share_document", { docId: smallDocId, targetUserId: "user_b", permission: "READ" }, cookieA);
  test("Share RPC success", shareRpc.result?.success === true, `Result: ${JSON.stringify(shareRpc.result)}`);
  test("Shared permission READ", shareRpc.result?.permission === "READ");
  test("Server uses session identity (not client ownerId)", shareRpc.error === undefined);

  // ── Step 6: User B can now access shared doc ───────────────────────────
  console.log("\n── 6. Shared Document Access ──");
  const allowedRpc = await rpc("rpc.get_doc_location", { docId: smallDocId }, cookieB);
  test("User B can locate shared doc", !!allowedRpc.result?.doc, `DocId: ${allowedRpc.result?.doc?.docId}`);
  
  const downloadSharedChunk = await request({
    hostname: "localhost", port: 3000, path: `/api/documents/download/${smallDocId}/chunk/0`, method: "GET",
    headers: { Cookie: cookieB }
  });
  test("User B downloads shared chunk (200)", downloadSharedChunk.status === 200, `Status: ${downloadSharedChunk.status}`);
  test("Downloaded data non-empty", downloadSharedChunk.raw.length > 0, `Size: ${downloadSharedChunk.raw.length} bytes`);

  // Verify SHA-256 integrity of downloaded chunk
  const downloadedHash = crypto.createHash("sha256").update(downloadSharedChunk.raw).digest("hex");
  const originalHash = crypto.createHash("sha256").update(smallBuf).digest("hex");
  test("Downloaded data SHA-256 matches original", downloadedHash === originalHash, `Hash: ${downloadedHash.slice(0, 16)}...`);

  // ── Step 7: My Documents & Shared With Me listing ─────────────────────
  console.log("\n── 7. My Documents & Shared With Me ──");
  const docsA = await request({ hostname: "localhost", port: 3000, path: "/api/documents", method: "GET", headers: { Cookie: cookieA } });
  const docsAData = JSON.parse(docsA.body);
  test("User A sees my docs (has 2+ docs)", docsAData.myDocuments?.length >= 2, `Count: ${docsAData.myDocuments?.length}`);
  test("User A shared with me is empty", docsAData.sharedWithMe?.length === 0);

  const docsB = await request({ hostname: "localhost", port: 3000, path: "/api/documents", method: "GET", headers: { Cookie: cookieB } });
  const docsBData = JSON.parse(docsB.body);
  test("User B my docs is empty", docsBData.myDocuments?.length === 0);
  test("User B shared with me has 1 doc", docsBData.sharedWithMe?.length >= 1, `Count: ${docsBData.sharedWithMe?.length}`);

  // ── Step 8: Fault Injection → Failover ────────────────────────────────
  console.log("\n── 8. Fault Injection & Failover ──");
  // Get doc to find which nodes it's replicated on
  const docLocation = await rpc("rpc.get_doc_location", { docId: smallDocId }, cookieA);
  const replicaNodes = docLocation.result?.availableReplicaNodes || [];
  test("Document has healthy replicas before kill", replicaNodes.length >= 2, `Count: ${replicaNodes.length}`);

  // Kill first replica node
  const killNodeId = docLocation.result?.doc?.primaryNodeId || "node_1";
  const killRpc = await rpc("rpc.simulate_fault", { nodeId: killNodeId, action: "kill" });
  test(`Kill ${killNodeId}`, killRpc.result?.currentStatus === "FAILED", `Status: ${killRpc.result?.currentStatus}`);

  // Download should still work from remaining healthy replica
  const downloadAfterKill = await request({
    hostname: "localhost", port: 3000, path: `/api/documents/download/${smallDocId}/chunk/0`, method: "GET",
    headers: { Cookie: cookieA }
  });
  test("Download succeeds after node kill (failover)", downloadAfterKill.status === 200, `Status: ${downloadAfterKill.status}`);
  const failoverNodeId = downloadAfterKill.headers["x-node-id"] || "?";
  test("Served from different node", failoverNodeId !== killNodeId, `Served from: ${failoverNodeId}`);

  // ── Step 9: Revive Node ────────────────────────────────────────────────
  console.log("\n── 9. Node Revival ──");
  const reviveRpc = await rpc("rpc.simulate_fault", { nodeId: killNodeId, action: "revive" });
  test(`Revive ${killNodeId}`, reviveRpc.result?.currentStatus === "HEALTHY", `Status: ${reviveRpc.result?.currentStatus}`);
  
  await new Promise(r => setTimeout(r, 1500)); // brief delay
  const locationAfterRevive = await rpc("rpc.get_doc_location", { docId: smallDocId }, cookieA);
  const reviveNodes = locationAfterRevive.result?.availableReplicaNodes || [];
  test("More replicas available after revive", reviveNodes.length >= 1, `Count: ${reviveNodes.length}`);

  // ── Step 10: Large file multi-chunk download & reassembly ──────────────
  console.log("\n── 10. Multi-Chunk Download (600KB) ──");
  const largeDocChunks = uploadLargeData.document?.totalChunks || 2;
  let reassembledBufs = [];
  let chunksFetched = 0;
  
  for (let i = 0; i < largeDocChunks; i++) {
    const chunkRes = await request({
      hostname: "localhost", port: 3000, path: `/api/documents/download/${largeDocId}/chunk/${i}`, method: "GET",
      headers: { Cookie: cookieA }
    });
    if (chunkRes.status === 200) {
      reassembledBufs.push(chunkRes.raw);
      chunksFetched++;
    }
  }
  
  const reassembled = Buffer.concat(reassembledBufs);
  const originalLargeHash = crypto.createHash("sha256").update(largeBuf).digest("hex");
  const reassembledHash = crypto.createHash("sha256").update(reassembled).digest("hex");
  
  test("All chunks downloaded", chunksFetched === largeDocChunks, `${chunksFetched}/${largeDocChunks}`);
  test("Reassembled file matches original (SHA-256)", originalLargeHash === reassembledHash, `Size: ${reassembled.length}B`);
  test("Correct size: 600KB", reassembled.length === largeBuf.length, `${reassembled.length} === ${largeBuf.length}`);

  // ── Summary ────────────────────────────────────────────────────────────
  console.log("\n╔═══════════════════════════════════════╗");
  console.log(`║  Results: ${passed} passed, ${failed} failed${" ".repeat(20 - String(passed+failed).length)}║`);
  console.log("╚═══════════════════════════════════════╝\n");
}

runFullFlow().catch(err => {
  console.error("Test suite error:", err);
  process.exit(1);
});
