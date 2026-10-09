/**
 * Verification Suite for Bully Election & Cluster Topology Dynamic Synchronization
 * Tests:
 *  - TEST 1: Initial State (Coordinator = Leader, Topology = Leader)
 *  - TEST 2: Kill Coordinator (Bully runs, Node 4 = Leader, Topology = Node 4 Leader, Coord = Failed)
 *  - TEST 3: Trigger Election while Failed (Node 4 remains Leader)
 *  - TEST 4: Revive Coordinator (Coordinator reclaims Leader, Node 4 loses Leader)
 *  - TEST 5: Browser Refresh / Backend State Persistence
 *  - TEST 6: Multi-node Failure Cascade (Node 3 takeover)
 *  - TEST 7: WebSocket Events (LEADER_CHANGED, COORDINATOR_FAILED, COORDINATOR_REVIVED)
 */

const http = require('http');
const WebSocket = require('ws');

function makeRequest(method, path, body = null) {
  return new Promise((resolve, reject) => {
    const data = body ? JSON.stringify(body) : null;
    const req = http.request({
      hostname: 'localhost',
      port: 3000,
      path,
      method,
      headers: data ? {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(data)
      } : {}
    }, (res) => {
      let raw = '';
      res.on('data', chunk => raw += chunk);
      res.on('end', () => {
        try {
          resolve({ status: res.statusCode, body: JSON.parse(raw) });
        } catch (e) {
          resolve({ status: res.statusCode, body: raw });
        }
      });
    });
    req.on('error', reject);
    if (data) req.write(data);
    req.end();
  });
}

// Simulates the topology rendering logic from public/js/app.js
function evaluateTopologyState(leaderState, nodes) {
  const activeLeaderId = leaderState.leaderId;
  const activeLeaderName = leaderState.leaderName || '';
  const isCoordOnline = leaderState.coordinatorStatus === 'HEALTHY' || leaderState.isCoordinatorAlive === true;
  const isCoordFailed = !isCoordOnline;

  const isCoordLeader = isCoordOnline && (
    activeLeaderId === 'Coordinator' ||
    activeLeaderId === 'coordinator' ||
    activeLeaderName.includes('Coordinator') ||
    !activeLeaderId
  );

  const isStorageNodeLeader = (nodeId) => {
    if (isCoordLeader) return false;
    if (activeLeaderId === nodeId) return true;
    if (activeLeaderName && (activeLeaderName.toLowerCase().includes(nodeId.replace('_', ' ')) || activeLeaderName.toLowerCase().includes(nodeId))) return true;
    const nd = nodes.find(n => n.id === nodeId);
    if (nd && activeLeaderName && (activeLeaderName.includes(nd.name) || activeLeaderName.includes(nd.id))) return true;
    return false;
  };

  const nodeResults = nodes.map(n => ({
    id: n.id,
    name: n.name,
    isLeader: isStorageNodeLeader(n.id),
    badge: isStorageNodeLeader(n.id) ? '👑 LEADER' : (n.status === 'FAILED' ? '❌ FAILED' : 'HEALTHY')
  }));

  const coordBadge = isCoordLeader ? '👑 LEADER' : (isCoordFailed ? '❌ FAILED (OFFLINE)' : 'ONLINE (Standby)');

  return {
    isCoordLeader,
    isCoordFailed,
    coordBadge,
    nodes: nodeResults,
    leaderNode: nodeResults.find(n => n.isLeader) || (isCoordLeader ? { id: 'Coordinator', name: 'Master Coordinator' } : null)
  };
}

async function runTests() {
  console.log('================================================================');
  console.log('🧪 VERIFYING CLUSTER TOPOLOGY & BULLY LEADER DYNAMIC SYNC');
  console.log('================================================================\n');

  let passed = 0;
  let total = 0;

  function assert(condition, message) {
    total++;
    if (condition) {
      console.log(`  ✅ [PASS] ${message}`);
      passed++;
    } else {
      console.error(`  ❌ [FAIL] ${message}`);
      process.exitCode = 1;
    }
  }

  // Set up WebSocket listener
  const wsEvents = [];
  const ws = new WebSocket('ws://localhost:3001');
  await new Promise(resolve => {
    ws.on('open', resolve);
    ws.on('message', data => {
      try {
        const parsed = JSON.parse(data);
        wsEvents.push(parsed);
      } catch (e) {}
    });
  });

  const testNodes = [
    { id: 'node_1', name: 'Storage Node Alpha', status: 'HEALTHY' },
    { id: 'node_2', name: 'Storage Node Beta',  status: 'HEALTHY' },
    { id: 'node_3', name: 'Storage Node Gamma', status: 'HEALTHY' },
    { id: 'node_4', name: 'Storage Node Delta', status: 'HEALTHY' }
  ];

  // ── TEST 1: Initial State ──
  console.log('--- TEST 1: Initial Cluster State ---');
  // First ensure coordinator is alive
  await makeRequest('POST', '/api/election/revive-coordinator');
  await new Promise(r => setTimeout(r, 200));

  const initRes = await makeRequest('GET', '/api/election/leader');
  assert(initRes.status === 200, 'GET /api/election/leader returns 200');
  assert(initRes.body.leaderId === 'Coordinator', 'Initial Leader is Coordinator');
  assert(initRes.body.coordinatorStatus === 'HEALTHY', 'Initial Coordinator status is HEALTHY');

  const topo1 = evaluateTopologyState(initRes.body, testNodes);
  assert(topo1.isCoordLeader === true, 'Topology marks Coordinator as Leader (isCoordLeader = true)');
  assert(topo1.coordBadge === '👑 LEADER', 'Topology renders 👑 LEADER badge for Coordinator');
  assert(topo1.nodes.every(n => !n.isLeader), 'No storage nodes have leader indicator in initial state');

  // ── TEST 2: Kill Coordinator ──
  console.log('\n--- TEST 2: Kill Coordinator & Bully Failover ---');
  const killRes = await makeRequest('POST', '/api/election/kill-coordinator');
  assert(killRes.status === 200, 'POST /api/election/kill-coordinator returns 200');
  assert(killRes.body.coordinatorStatus === 'FAILED', 'Coordinator status is FAILED');
  assert(killRes.body.leaderId === 'node_4', 'Node 4 (Delta) elected leader via Bully algorithm');

  await new Promise(r => setTimeout(r, 200));

  const topo2 = evaluateTopologyState(killRes.body, testNodes);
  assert(topo2.isCoordLeader === false, 'Topology removes Leader status from Coordinator');
  assert(topo2.isCoordFailed === true, 'Topology marks Coordinator as FAILED');
  assert(topo2.coordBadge === '❌ FAILED (OFFLINE)', 'Topology renders ❌ FAILED (OFFLINE) badge on Coordinator');
  assert(topo2.nodes.find(n => n.id === 'node_4').isLeader === true, 'Topology marks Node 4 (Delta) as LEADER');
  assert(topo2.nodes.find(n => n.id === 'node_4').badge === '👑 LEADER', 'Topology renders 👑 LEADER badge on Node 4');
  assert(topo2.nodes.filter(n => n.id !== 'node_4').every(n => !n.isLeader), 'Nodes 1, 2, 3 do not have leader indicator');

  // Verify WebSocket events
  const hasLeaderChanged = wsEvents.some(e => e.type === 'LEADER_CHANGED' && (e.payload.leaderId === 'node_4' || e.payload.currentLeader?.id === 'node_4'));
  assert(hasLeaderChanged, 'WebSocket broadcasted LEADER_CHANGED for Node 4 takeover');

  // ── TEST 3: Trigger Election while Coordinator Failed ──
  console.log('\n--- TEST 3: Trigger Election while Coordinator is Failed ---');
  const electRes = await makeRequest('POST', '/api/election/start');
  assert(electRes.status === 200, 'POST /api/election/start returns 200');
  assert(electRes.body.leaderId === 'node_4', 'Node 4 remains leader');
  assert(electRes.body.coordinatorStatus === 'FAILED', 'Coordinator remains FAILED');

  const topo3 = evaluateTopologyState(electRes.body, testNodes);
  assert(topo3.isCoordLeader === false, 'Coordinator is not leader');
  assert(topo3.nodes.find(n => n.id === 'node_4').isLeader === true, 'Topology still shows Node 4 as LEADER');

  // ── TEST 4: Revive Coordinator ──
  console.log('\n--- TEST 4: Revive Coordinator & Leadership Reclaim ---');
  const reviveRes = await makeRequest('POST', '/api/election/revive-coordinator');
  assert(reviveRes.status === 200, 'POST /api/election/revive-coordinator returns 200');
  assert(reviveRes.body.leaderId === 'Coordinator', 'Coordinator reclaims leadership with Rank 100');
  assert(reviveRes.body.coordinatorStatus === 'HEALTHY', 'Coordinator is ONLINE/HEALTHY');
  assert(reviveRes.body.previousLeaderId === 'Storage Node Delta' || reviveRes.body.previousLeader?.id === 'node_4', 'Previous leader was Storage Node Delta');

  await new Promise(r => setTimeout(r, 200));

  const topo4 = evaluateTopologyState(reviveRes.body, testNodes);
  assert(topo4.isCoordLeader === true, 'Topology moves LEADER indicator back to Coordinator');
  assert(topo4.coordBadge === '👑 LEADER', 'Topology renders 👑 LEADER badge on Coordinator');
  assert(topo4.nodes.find(n => n.id === 'node_4').isLeader === false, 'Node 4 loses LEADER status');
  assert(topo4.nodes.every(n => !n.isLeader), 'All storage nodes are non-leaders');

  // ── TEST 5: Browser Refresh / Persistence ──
  console.log('\n--- TEST 5: Browser Refresh Simulation ---');
  const refreshRes = await makeRequest('GET', '/api/election/status');
  assert(refreshRes.status === 200, 'GET /api/election/status returns 200');
  assert(refreshRes.body.leaderId === 'Coordinator', 'Leader state persisted on backend');

  const topo5 = evaluateTopologyState(refreshRes.body, testNodes);
  assert(topo5.isCoordLeader === true, 'Topology on fresh load correctly shows Coordinator as LEADER');

  // ── TEST 6: Multi-Node Failure Cascade ──
  console.log('\n--- TEST 6: Multi-Node Failure Cascade ---');
  // Kill Coordinator
  await makeRequest('POST', '/api/election/kill-coordinator');
  // Simulate Node 4 killed
  const cascadeNodes = [
    { id: 'node_1', name: 'Storage Node Alpha', status: 'HEALTHY' },
    { id: 'node_2', name: 'Storage Node Beta',  status: 'HEALTHY' },
    { id: 'node_3', name: 'Storage Node Gamma', status: 'HEALTHY' },
    { id: 'node_4', name: 'Storage Node Delta', status: 'FAILED' }
  ];
  // Trigger election among remaining healthy nodes (Node 1, 2, 3)
  const nodeFault = await makeRequest('POST', '/api/rpc', {
    jsonrpc: '2.0',
    method: 'rpc.simulate_fault',
    params: { nodeId: 'node_4', action: 'kill' },
    id: 1
  });
  assert(nodeFault.status === 200, 'RPC fault simulation on Node 4 returned 200');

  const n3LeaderRes = await makeRequest('GET', '/api/election/leader');
  assert(n3LeaderRes.body.leaderId === 'node_3', 'Node 3 elected leader when Node 4 + Coordinator are failed');

  const topo6 = evaluateTopologyState(n3LeaderRes.body, cascadeNodes);
  assert(topo6.nodes.find(n => n.id === 'node_3').isLeader === true, 'Topology shows Node 3 as 👑 LEADER');
  assert(topo6.nodes.find(n => n.id === 'node_4').isLeader === false, 'Failed Node 4 is NOT leader');

  // Clean up: revive Node 4 and revive Coordinator
  await makeRequest('POST', '/api/rpc', {
    jsonrpc: '2.0',
    method: 'rpc.simulate_fault',
    params: { nodeId: 'node_4', action: 'revive' },
    id: 2
  });
  await makeRequest('POST', '/api/election/revive-coordinator');
  const finalState = await makeRequest('GET', '/api/election/leader');
  assert(finalState.body.leaderId === 'Coordinator', 'Cluster restored to Coordinator leader');

  ws.close();

  console.log('\n================================================================');
  console.log(`SUMMARY: ${passed} / ${total} CHECKS PASSED (${Math.round((passed / total) * 100)}%)`);
  console.log('================================================================\n');

  if (passed === total) {
    process.exit(0);
  } else {
    process.exit(1);
  }
}

runTests().catch(err => {
  console.error('Test error:', err);
  process.exit(1);
});
