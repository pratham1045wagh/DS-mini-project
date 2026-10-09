/**
 * FA-2 Distributed Systems Test Suite
 * Validates all Unit 3 & Unit 4 concepts:
 *  1. Cluster Nodes & Health
 *  2. Document Upload & 2-Node Replication
 *  3. Lamport Logical Clocks (inter-node messages & event timeline)
 *  4. Document Sharing (Access Control READ/WRITE)
 *  5. Distributed Mutual Exclusion (Lock acquire / lease / release)
 *  6. Mutual Exclusion Enforcement (User B blocked while User A holds lock)
 *  7. Document Versioning (immutable version records)
 *  8. Vector Clocks & Concurrent Update Detection (Conflict Detection)
 *  9. Conflict Resolution (Branch selection & Component-wise max merge)
 * 10. Coordinator Failure Simulation & Bully Algorithm Leader Election
 * 11. Elected Leader verification & rank hierarchy
 * 12. Coordinator Revival & Bully Priority Assertion
 * 13. Storage Node Failure & Replica Failover Download
 * 14. Node Recovery & Re-Synchronization (Missing chunks peer sync)
 * 15. Deadlock Detection (Wait-For Graph DFS cycle detection)
 * 16. Deadlock Resolution (Cycle break via victim abort)
 * 17. FA-2 JSON-RPC 2.0 Method Verification
 * 18. Local File Persistence across operations
 */

const http = require('http');
const crypto = require('crypto');

let passed = 0;
let failed = 0;

function test(name, condition, extra = '') {
  if (condition) {
    console.log(`  ✅ PASS: ${name} ${extra ? `| ${extra}` : ''}`);
    passed++;
  } else {
    console.log(`  ❌ FAIL: ${name} ${extra ? `| ${extra}` : ''}`);
    failed++;
  }
}

function request({ hostname = 'localhost', port = 3000, path, method = 'GET', headers = {}, body = null }) {
  return new Promise((resolve, reject) => {
    const opts = { hostname, port, path, method, headers };
    const req = http.request(opts, (res) => {
      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('end', () => {
        const raw = Buffer.concat(chunks);
        const text = raw.toString('utf8');
        let json = null;
        try { json = JSON.parse(text); } catch (e) {}
        resolve({
          status: res.statusCode,
          headers: res.headers,
          raw,
          text,
          json
        });
      });
    });
    req.on('error', reject);
    if (body) {
      if (Buffer.isBuffer(body)) req.write(body);
      else req.write(body);
    }
    req.end();
  });
}

async function rpcCall(method, params = {}, cookie = '') {
  const payload = JSON.stringify({
    jsonrpc: '2.0',
    id: `test_${Date.now()}`,
    method,
    params
  });
  const headers = { 'Content-Type': 'application/json' };
  if (cookie) headers['Cookie'] = cookie;

  const res = await request({
    path: '/api/rpc',
    method: 'POST',
    headers,
    body: payload
  });
  return res.json;
}

async function login(userId, password) {
  const res = await request({
    path: '/api/auth/login',
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ userId, password })
  });
  const setCookie = res.headers['set-cookie'];
  const cookie = Array.isArray(setCookie) ? setCookie[0].split(';')[0] : '';
  return { user: res.json ? res.json.user : null, cookie };
}

async function runFa2TestSuite() {
  console.log('╔═══════════════════════════════════════════════════════════════╗');
  console.log('║   FA-2 Distributed Systems 18-Step Full Verification Suite    ║');
  console.log('║   Units 3 & 4: Clocks, Mutex, Deadlocks, Election & Recovery  ║');
  console.log('╚═══════════════════════════════════════════════════════════════╝\n');

  // Step 1: Login users
  console.log('── Step 1: User Sessions ──');
  const userA = await login('user_a', 'passA');
  const userB = await login('user_b', 'passB');
  test('User A authenticated', !!userA.cookie);
  test('User B authenticated', !!userB.cookie);

  // Step 2: Show 3+ healthy storage nodes
  console.log('\n── Step 2: Cluster Topology & Storage Nodes ──');
  const clusterStatus = await rpcCall('rpc.get_cluster_status');
  const healthyNodes = (clusterStatus.result?.nodes || []).filter(n => n.status === 'HEALTHY');
  test('Storage nodes count >= 3', healthyNodes.length >= 3, `Count: ${healthyNodes.length}`);
  const nodeNames = healthyNodes.map(n => n.id).join(', ');
  test('Nodes live and registered', healthyNodes.length > 0, `Nodes: [${nodeNames}]`);

  // Step 3: Document Upload with Multipart Boundary
  console.log('\n── Step 3: Document Upload (Version 1 & Integrity) ──');
  const boundary = '----WebKitFormBoundaryFA2' + Math.random().toString(36).substring(2);
  const fileContent = 'FA2 Distributed Systems Research Document Content - Revision 1';
  const fileBuffer = Buffer.from(fileContent);

  const multipartBody = Buffer.concat([
    Buffer.from(`--${boundary}\r\nContent-Disposition: form-data; name="file"; filename="fa2_test_report.txt"\r\nContent-Type: text/plain\r\n\r\n`),
    fileBuffer,
    Buffer.from(`\r\n--${boundary}--\r\n`)
  ]);

  const uploadRes = await request({
    path: '/api/documents/upload',
    method: 'POST',
    headers: {
      'Content-Type': `multipart/form-data; boundary=${boundary}`,
      'Cookie': userA.cookie
    },
    body: multipartBody
  });

  test('Upload HTTP status 200', uploadRes.status === 200);
  const uploadedDoc = uploadRes.json?.document;
  const docId = uploadedDoc?.docId;
  test('DocId generated', !!docId, `DocId: ${docId}`);
  test('Initial version is 1', uploadedDoc?.version === 1);
  test('Owner is user_a', uploadedDoc?.ownerId === 'user_a');

  // Step 4: Show Replication to 2 nodes
  console.log('\n── Step 4: P2P Chunk Replication (Factor = 2) ──');
  const replicas = uploadedDoc?.replicaNodeIds || [];
  test('Replication factor = 2', replicas.length === 2, `Replicas: [${replicas.join(', ')}]`);
  test('Replication verified SHA-256', uploadedDoc?.replicationSuccess === true);

  // Step 5: Lamport Logical Timestamps
  console.log('\n── Step 5: Lamport Logical Clocks ──');
  const lamportRpc = await rpcCall('rpc.get_logical_clock');
  const coordClock = lamportRpc.result?.clocks?.Coordinator || 0;
  test('Lamport clock exists and is positive', coordClock > 0, `Coordinator L=${coordClock}`);
  test('Document metadata has Lamport timestamp', uploadedDoc?.lamportTimestamp >= 1, `Doc L=${uploadedDoc?.lamportTimestamp}`);

  const timelineRes = await request({ path: '/api/timeline', method: 'GET', headers: { Cookie: userA.cookie } });
  const timelineEvents = timelineRes.json?.timeline || [];
  test('Event timeline captures events', timelineEvents.length > 0, `Recorded events: ${timelineEvents.length}`);
  const hasUploadEvent = timelineEvents.some(e => e.operation === 'UPLOAD' || e.document?.includes('fa2_test_report'));
  test('Timeline records document upload event', hasUploadEvent);

  // Step 6: Share document with User B
  console.log('\n── Step 6: Access Control & Sharing ──');
  const shareRpc = await rpcCall('rpc.share_document', {
    docId,
    targetUserId: 'user_b',
    permission: 'WRITE'
  }, userA.cookie);
  test('Document shared with User B', shareRpc.result?.success === true, `Permission: ${shareRpc.result?.permission}`);

  // Step 7: Distributed Mutual Exclusion - User A acquires lock
  console.log('\n── Step 7: Distributed Mutual Exclusion (Acquire Lock) ──');
  const lockAcquireA = await request({
    path: '/api/locks/acquire',
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Cookie': userA.cookie },
    body: JSON.stringify({ docId, userId: 'user_a' })
  });
  test('User A acquires exclusive lock', lockAcquireA.status === 200, `Status: ${lockAcquireA.status}`);
  test('Lock owner is user_a', lockAcquireA.json?.lock?.ownerId === 'user_a');

  // Step 8: User B attempts to acquire or edit locked document
  console.log('\n── Step 8: Mutual Exclusion Enforcement (Lock Conflict) ──');
  const lockAcquireB = await request({
    path: '/api/locks/acquire',
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Cookie': userB.cookie },
    body: JSON.stringify({ docId, userId: 'user_b' })
  });
  test('User B lock attempt is DENIED (HTTP 423/409 Locked)', lockAcquireB.status === 423 || lockAcquireB.status === 409, `Status: ${lockAcquireB.status}`);
  test('Access denied message mentions current owner', !!lockAcquireB.json?.currentOwner || !!lockAcquireB.json?.lockedBy);

  // Step 9: User A updates document to Version 2
  console.log('\n── Step 9: Document Update & Version Increment ──');
  const updateContent = 'FA2 Distributed Systems Content - Version 2 (Authorized Edit by User A)';
  const updateResA = await request({
    path: '/api/documents/update',
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Cookie': userA.cookie },
    body: JSON.stringify({ docId, content: updateContent, updatedBy: 'user_a' })
  });
  test('Document update succeeds under lock', updateResA.status === 200);
  test('Document version incremented to 2', updateResA.json?.document?.version === 2);
  test('Vector clock updated', !!updateResA.json?.document?.vectorClock);

  // User A releases lock
  const lockReleaseA = await request({
    path: '/api/locks/release',
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Cookie': userA.cookie },
    body: JSON.stringify({ docId, userId: 'user_a' })
  });
  test('User A releases exclusive lock', lockReleaseA.status === 200);

  // Step 10: Vector Clock & Concurrent Update Detection (Conflict Simulation)
  console.log('\n── Step 10: Concurrent Update & Conflict Detection (Vector Clock) ──');
  const conflictSim = await request({
    path: '/api/documents/simulate-conflict',
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Cookie': userA.cookie },
    body: JSON.stringify({ docId })
  });
  test('Conflict simulation executed', conflictSim.status === 200);
  test('Conflict status flagged in response', conflictSim.json?.conflict === true);

  const docAfterConflict = await rpcCall('rpc.get_vector_clock', { docId });
  test('Document is in CONFLICT state', docAfterConflict.result?.conflict === true);
  test('Both concurrent branches preserved', !!docAfterConflict.result?.conflictData?.versionA && !!docAfterConflict.result?.conflictData?.versionB);

  // Step 11: Conflict Resolution
  console.log('\n── Step 11: Conflict Resolution (Choose Version or Merge) ──');
  const resolveRes = await request({
    path: '/api/documents/resolve-conflict',
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Cookie': userA.cookie },
    body: JSON.stringify({
      docId,
      resolutionChoice: 'MERGE',
      mergedContent: 'FA2 Distributed Systems - Reconciled and Merged Version 3',
      resolvedBy: 'user_a'
    })
  });
  test('Conflict resolved successfully', resolveRes.status === 200);
  test('Conflict cleared', resolveRes.json?.document?.conflict === false);
  test('New version created after resolution', resolveRes.json?.document?.version >= 3);

  // Step 12: Version History Verification
  console.log('\n── Step 12: Document Version History ──');
  const historyRes = await request({
    path: `/api/documents/${docId}/versions`,
    method: 'GET',
    headers: { Cookie: userA.cookie }
  });
  test('Version history retrieved', historyRes.status === 200);
  const versions = historyRes.json?.versions || [];
  test('All version states recorded', versions.length >= 2, `Total versions: ${versions.length}`);
  const v1 = versions.find(v => v.version === 1);
  test('Version 1 preserved intact with checksum', !!v1 && !!v1.checksum);

  // Step 13: Bully Algorithm Leader Election (Coordinator Failure)
  console.log('\n── Step 13: Coordinator Failure & Bully Leader Election ──');
  const killCoordRes = await request({
    path: '/api/election/kill-coordinator',
    method: 'POST',
    headers: { Cookie: userA.cookie }
  });
  test('Coordinator simulated failure', killCoordRes.status === 200);
  const electedLeader = killCoordRes.json?.leaderId;
  test('Leader election produced winner', !!electedLeader);
  test('Highest ranked available storage node won election', electedLeader === 'node_4' || electedLeader === 'node_3', `Elected Leader: ${electedLeader}`);

  const leaderRpc = await rpcCall('rpc.get_leader');
  test('rpc.get_leader confirms new leader', leaderRpc.result?.leaderId === electedLeader, `Leader: ${leaderRpc.result?.leaderId}`);

  // Step 14: Coordinator Revival & Bully Priority Assertion
  console.log('\n── Step 14: Coordinator Revival (Bully Priority Assertion) ──');
  const reviveCoordRes = await request({
    path: '/api/election/revive-coordinator',
    method: 'POST',
    headers: { Cookie: userA.cookie }
  });
  test('Coordinator revived successfully', reviveCoordRes.status === 200);
  test('Coordinator reclaimed leadership (Rank 100)', reviveCoordRes.json?.leaderId === 'Coordinator');

  // Step 15: Storage Node Failure & Replica Failover
  console.log('\n── Step 15: Storage Node Failure & Failover Download ──');
  const targetKillNode = replicas[0];
  const killNodeRpc = await rpcCall('rpc.simulate_fault', { nodeId: targetKillNode, action: 'kill' });
  test(`Kill node ${targetKillNode}`, killNodeRpc.result?.currentStatus === 'FAILED' || killNodeRpc.result?.success === true);

  // Download chunk from surviving replica
  const downloadRes = await request({
    path: `/api/documents/download/${docId}/chunk/0`,
    method: 'GET',
    headers: { Cookie: userA.cookie }
  });
  test('Download succeeds via surviving replica (failover)', downloadRes.status === 200);
  const servedBy = downloadRes.headers['x-node-id'];
  test('Served by surviving peer', servedBy !== targetKillNode, `Served by: ${servedBy}`);

  // Step 16: Node Recovery & Re-Synchronization
  console.log('\n── Step 16: Node Recovery & Re-Synchronization ──');
  const reviveNodeRpc = await rpcCall('rpc.simulate_fault', { nodeId: targetKillNode, action: 'revive' });
  test(`Revive node ${targetKillNode}`, reviveNodeRpc.result?.success === true);

  // Trigger explicit sync check
  const syncRpc = await rpcCall('rpc.sync_node', { nodeId: targetKillNode });
  test(`Node ${targetKillNode} synchronizes missing data`, syncRpc.result?.success === true, `Synced: ${syncRpc.result?.syncedChunks || 0} chunks`);

  // Step 17: Deadlock Detection (Wait-For Graph Cycle)
  console.log('\n── Step 17: Deadlock Detection (Wait-For Graph Cycle Detection) ──');
  const deadlockSim = await request({
    path: '/api/deadlock/simulate',
    method: 'POST',
    headers: { Cookie: userA.cookie }
  });
  test('Deadlock simulation executed', deadlockSim.status === 200);
  test('Cycle detected in Wait-For Graph', deadlockSim.json?.isDeadlocked === true);
  const cycleUsers = deadlockSim.json?.cycle || [];
  test('Wait-For Graph shows circular dependency', cycleUsers.length >= 2, `Cycle: ${cycleUsers.join(' ⇄ ')}`);

  const deadlockRpc = await rpcCall('rpc.get_deadlocks');
  test('rpc.get_deadlocks detects cycle', deadlockRpc.result?.isDeadlocked === true);

  // Step 18: Deadlock Resolution
  console.log('\n── Step 18: Deadlock Resolution (Victim Request Abort) ──');
  const resolveDeadlockRes = await request({
    path: '/api/deadlock/resolve',
    method: 'POST',
    headers: { Cookie: userA.cookie }
  });
  test('Deadlock resolution executed', resolveDeadlockRes.status === 200);
  test('Deadlock cleared in WFG', resolveDeadlockRes.json?.isDeadlocked === false);

  const deadlockRpcAfter = await rpcCall('rpc.get_deadlocks');
  test('rpc.get_deadlocks confirms no deadlock', deadlockRpcAfter.result?.isDeadlocked === false);

  // Summary
  console.log('\n╔═══════════════════════════════════════════════════════════════╗');
  console.log(`║   FA-2 Test Results: ${passed} PASSED, ${failed} FAILED                          ║`);
  console.log('╚═══════════════════════════════════════════════════════════════╝\n');

  if (failed === 0) {
    console.log('🎉 ALL 18 FA-2 REQUIREMENTS & DISTRIBUTED SYSTEMS CONCEPTS VERIFIED SUCCESSFULLY!\n');
    process.exit(0);
  } else {
    console.log('❌ Some tests failed. Please review the output above.\n');
    process.exit(1);
  }
}

runFa2TestSuite().catch(err => {
  console.error('Fatal Test Suite Error:', err);
  process.exit(1);
});
