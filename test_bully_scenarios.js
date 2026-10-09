const http = require('http');

function makeRequest(options, postData = null) {
  return new Promise((resolve, reject) => {
    const headers = options.headers || {};
    if (postData) {
      headers['Content-Type'] = 'application/json';
      headers['Content-Length'] = Buffer.byteLength(postData);
    }
    const req = http.request({ ...options, headers }, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        let json = null;
        try { json = JSON.parse(data); } catch (e) {}
        resolve({ status: res.statusCode, headers: res.headers, body: data, json });
      });
    });
    req.on('error', reject);
    if (postData) req.write(postData);
    req.end();
  });
}

function rpcCall(method, params = {}) {
  const payload = JSON.stringify({ jsonrpc: '2.0', id: Date.now(), method, params });
  return makeRequest({
    hostname: 'localhost',
    port: 3000,
    path: '/api/rpc',
    method: 'POST'
  }, payload);
}

async function runBullyTests() {
  console.log('================================================================');
  console.log('👑 VERIFYING BULLY ALGORITHM LEADER ELECTION & SCENARIOS');
  console.log('================================================================\n');

  // TEST A: Initial State
  console.log('── TEST A: Initial Cluster State ──');
  const initRes = await makeRequest({
    hostname: 'localhost',
    port: 3000,
    path: '/api/election/leader',
    method: 'GET'
  });
  console.log(`   Leader ID        : ${initRes.json?.leaderId}`);
  console.log(`   Leader Name      : ${initRes.json?.leaderName}`);
  console.log(`   Election Status  : ${initRes.json?.electionStatus}`);
  console.log(`   Coordinator State: ${initRes.json?.coordinatorStatus} (Alive: ${initRes.json?.isCoordinatorAlive})`);

  if (initRes.json?.leaderId !== 'Coordinator' || initRes.json?.coordinatorStatus !== 'HEALTHY') {
    throw new Error('TEST A FAILED: Initial leader is not Coordinator');
  }
  console.log('   ✔ TEST A PASSED: Coordinator is Leader (ONLINE, Rank 100)\n');

  // TEST B: Trigger Election with Coordinator healthy
  console.log('── TEST B: Trigger Bully Election with Coordinator Healthy ──');
  const electRes = await makeRequest({
    hostname: 'localhost',
    port: 3000,
    path: '/api/election/start',
    method: 'POST'
  });
  console.log(`   Leader ID        : ${electRes.json?.leaderId}`);
  console.log(`   Election Status  : ${electRes.json?.electionStatus}`);
  console.log(`   Coordinator State: ${electRes.json?.coordinatorStatus}`);

  if (electRes.json?.leaderId !== 'Coordinator' || electRes.json?.electionStatus !== 'COMPLETED') {
    throw new Error('TEST B FAILED: Coordinator should remain leader');
  }
  console.log('   ✔ TEST B PASSED: Coordinator remains leader due to Rank 100 priority\n');

  // TEST C: Kill Coordinator
  console.log('── TEST C: Kill Coordinator (Simulated Crash) ──');
  const killRes = await makeRequest({
    hostname: 'localhost',
    port: 3000,
    path: '/api/election/kill-coordinator',
    method: 'POST'
  });
  console.log(`   Leader ID        : ${killRes.json?.leaderId}`);
  console.log(`   Leader Name      : ${killRes.json?.leaderName}`);
  console.log(`   Previous Leader  : ${killRes.json?.previousLeaderId}`);
  console.log(`   Coordinator State: ${killRes.json?.coordinatorStatus} (Alive: ${killRes.json?.isCoordinatorAlive})`);

  if (killRes.json?.leaderId !== 'node_4' || killRes.json?.coordinatorStatus !== 'FAILED') {
    throw new Error(`TEST C FAILED: Expected node_4 as leader and FAILED coordinator, got ${killRes.json?.leaderId}`);
  }
  console.log('   ✔ TEST C PASSED: Node 4 (Delta, Rank 4) elected Leader; Coordinator FAILED\n');

  // TEST D: Trigger Election while Coordinator is failed
  console.log('── TEST D: Trigger Bully Election while Coordinator is FAILED ──');
  const electWhileDead = await makeRequest({
    hostname: 'localhost',
    port: 3000,
    path: '/api/election/start',
    method: 'POST'
  });
  console.log(`   Leader ID        : ${electWhileDead.json?.leaderId}`);
  console.log(`   Coordinator State: ${electWhileDead.json?.coordinatorStatus}`);

  if (electWhileDead.json?.leaderId !== 'node_4' || electWhileDead.json?.coordinatorStatus !== 'FAILED') {
    throw new Error('TEST D FAILED: Failed coordinator should not win');
  }
  console.log('   ✔ TEST D PASSED: Node 4 remains leader; dead coordinator is ineligible\n');

  // TEST E: Kill Node 4 while Coordinator is failed -> Node 3 becomes leader
  console.log('── TEST E: Kill Node 4 while Coordinator is FAILED ──');
  await rpcCall('rpc.simulate_fault', { nodeId: 'node_4', action: 'kill' });
  const checkLeader3 = await makeRequest({
    hostname: 'localhost',
    port: 3000,
    path: '/api/election/leader',
    method: 'GET'
  });
  console.log(`   Leader ID: ${checkLeader3.json?.leaderId} (${checkLeader3.json?.leaderName})`);
  if (checkLeader3.json?.leaderId !== 'node_3') {
    throw new Error(`TEST E FAILED: Expected node_3, got ${checkLeader3.json?.leaderId}`);
  }
  console.log('   ✔ TEST E PASSED: Node 3 (Gamma, Rank 3) elected Leader after Node 4 crashed\n');

  // TEST F: Kill Node 3 while Coordinator is failed -> Node 2 becomes leader
  console.log('── TEST F: Kill Node 3 while Coordinator is FAILED ──');
  await rpcCall('rpc.simulate_fault', { nodeId: 'node_3', action: 'kill' });
  const checkLeader2 = await makeRequest({
    hostname: 'localhost',
    port: 3000,
    path: '/api/election/leader',
    method: 'GET'
  });
  console.log(`   Leader ID: ${checkLeader2.json?.leaderId} (${checkLeader2.json?.leaderName})`);
  if (checkLeader2.json?.leaderId !== 'node_2') {
    throw new Error(`TEST F FAILED: Expected node_2, got ${checkLeader2.json?.leaderId}`);
  }
  console.log('   ✔ TEST F PASSED: Node 2 (Beta, Rank 2) elected Leader after Node 3 crashed\n');

  // TEST G: Kill Node 2 while Coordinator is failed -> Node 1 becomes leader
  console.log('── TEST G: Kill Node 2 while Coordinator is FAILED ──');
  await rpcCall('rpc.simulate_fault', { nodeId: 'node_2', action: 'kill' });
  const checkLeader1 = await makeRequest({
    hostname: 'localhost',
    port: 3000,
    path: '/api/election/leader',
    method: 'GET'
  });
  console.log(`   Leader ID: ${checkLeader1.json?.leaderId} (${checkLeader1.json?.leaderName})`);
  if (checkLeader1.json?.leaderId !== 'node_1') {
    throw new Error(`TEST G FAILED: Expected node_1, got ${checkLeader1.json?.leaderId}`);
  }
  console.log('   ✔ TEST G PASSED: Node 1 (Alpha, Rank 1) elected Leader as sole surviving node\n');

  // TEST H: Revive Node 3 (Rank 3 > Rank 1) -> Node 3 bullies Node 1 and becomes leader
  console.log('── TEST H: Revive Node 3 (Rank 3 > Rank 1) ──');
  await rpcCall('rpc.simulate_fault', { nodeId: 'node_3', action: 'revive' });
  const checkRevived3 = await makeRequest({
    hostname: 'localhost',
    port: 3000,
    path: '/api/election/leader',
    method: 'GET'
  });
  console.log(`   Leader ID: ${checkRevived3.json?.leaderId} (${checkRevived3.json?.leaderName})`);
  if (checkRevived3.json?.leaderId !== 'node_3') {
    throw new Error(`TEST H FAILED: Expected node_3, got ${checkRevived3.json?.leaderId}`);
  }
  console.log('   ✔ TEST H PASSED: Revived Node 3 bullied Node 1 and took leadership\n');

  // Revive remaining storage nodes
  await rpcCall('rpc.simulate_fault', { nodeId: 'node_4', action: 'revive' });
  await rpcCall('rpc.simulate_fault', { nodeId: 'node_2', action: 'revive' });
  const checkRevived4 = await makeRequest({
    hostname: 'localhost',
    port: 3000,
    path: '/api/election/leader',
    method: 'GET'
  });
  console.log(`   Leader ID after Node 4 revived: ${checkRevived4.json?.leaderId} (${checkRevived4.json?.leaderName})`);

  // TEST I: Revive Coordinator
  console.log('\n── TEST I: Revive Coordinator (Assert Rank 100) ──');
  const reviveRes = await makeRequest({
    hostname: 'localhost',
    port: 3000,
    path: '/api/election/revive-coordinator',
    method: 'POST'
  });
  console.log(`   Leader ID        : ${reviveRes.json?.leaderId}`);
  console.log(`   Leader Name      : ${reviveRes.json?.leaderName}`);
  console.log(`   Previous Leader  : ${reviveRes.json?.previousLeaderId}`);
  console.log(`   Coordinator State: ${reviveRes.json?.coordinatorStatus} (Alive: ${reviveRes.json?.isCoordinatorAlive})`);

  if (reviveRes.json?.leaderId !== 'Coordinator' || reviveRes.json?.coordinatorStatus !== 'HEALTHY') {
    throw new Error('TEST I FAILED: Coordinator did not reclaim leadership');
  }
  console.log('   ✔ TEST I PASSED: Coordinator reclaimed leadership via Bully priority (Rank 100)\n');

  // TEST J: Final Trigger Election check
  console.log('── TEST J: Trigger Bully Election after full recovery ──');
  const finalElect = await makeRequest({
    hostname: 'localhost',
    port: 3000,
    path: '/api/election/start',
    method: 'POST'
  });
  console.log(`   Leader ID        : ${finalElect.json?.leaderId}`);
  console.log(`   Election Status  : ${finalElect.json?.electionStatus}`);
  console.log(`   Coordinator State: ${finalElect.json?.coordinatorStatus}`);
  if (finalElect.json?.leaderId !== 'Coordinator') {
    throw new Error('TEST J FAILED: Coordinator should remain leader');
  }
  console.log('   ✔ TEST J PASSED: Steady-state leadership preserved\n');

  console.log('================================================================');
  console.log('🎉 ALL BULLY ALGORITHM & MULTI-NODE FAILURE TESTS PASSED!');
  console.log('================================================================\n');
}

runBullyTests().catch(err => {
  console.error('\n❌ ERROR:', err);
  process.exit(1);
});
