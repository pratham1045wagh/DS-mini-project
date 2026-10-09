const http = require('http');

function makeRequest(options, postData = null, cookie = null) {
  return new Promise((resolve, reject) => {
    const headers = options.headers || {};
    if (cookie) headers['Cookie'] = cookie;
    if (postData) {
      headers['Content-Type'] = 'application/json';
      headers['Content-Length'] = Buffer.byteLength(postData);
    }
    const req = http.request({ ...options, headers }, (res) => {
      let data = '';
      const setCookie = res.headers['set-cookie'] ? res.headers['set-cookie'][0] : null;
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        let json = null;
        try { json = JSON.parse(data); } catch (e) {}
        resolve({ status: res.statusCode, headers: res.headers, cookie: setCookie, body: data, json });
      });
    });
    req.on('error', reject);
    if (postData) req.write(postData);
    req.end();
  });
}

async function runLockVerification() {
  console.log('================================================================');
  console.log('🔒 VERIFYING COMPLETE DISTRIBUTED MUTUAL EXCLUSION LOCK FLOW');
  console.log('================================================================');

  // 1. Login User A
  console.log('\n[1] Logging in as user_a (User A)...');
  const loginA = await makeRequest({
    hostname: 'localhost',
    port: 3000,
    path: '/api/auth/login',
    method: 'POST'
  }, JSON.stringify({ userId: 'user_a', password: 'passA' }));
  const cookieA = loginA.cookie ? loginA.cookie.split(';')[0] : null;
  console.log(`   User A login status: ${loginA.status} (Cookie: ${cookieA})`);

  // 2. Login User B
  console.log('\n[2] Logging in as user_b (User B)...');
  const loginB = await makeRequest({
    hostname: 'localhost',
    port: 3000,
    path: '/api/auth/login',
    method: 'POST'
  }, JSON.stringify({ userId: 'user_b', password: 'passB' }));
  const cookieB = loginB.cookie ? loginB.cookie.split(';')[0] : null;
  console.log(`   User B login status: ${loginB.status} (Cookie: ${cookieB})`);

  // 3. Get Documents
  console.log('\n[3] Fetching document catalog...');
  const docsRes = await makeRequest({
    hostname: 'localhost',
    port: 3000,
    path: '/api/documents',
    method: 'GET'
  }, null, cookieA);
  const docs = docsRes.json?.allDocuments || docsRes.json?.myDocuments || [];
  if (docs.length === 0) throw new Error('No documents found in catalog');
  const targetDoc = docs[0];
  const docId = targetDoc.docId;
  console.log(`   Target document: '${targetDoc.name}' (docId: ${docId})`);

  // Release any existing lock on targetDoc first
  await makeRequest({
    hostname: 'localhost',
    port: 3000,
    path: '/api/locks/release',
    method: 'POST'
  }, JSON.stringify({ docId, force: true }), cookieA);

  // Clear any existing deadlock dependencies
  await makeRequest({
    hostname: 'localhost',
    port: 3000,
    path: '/api/deadlock/resolve',
    method: 'POST'
  }, null, cookieA);

  // 4. TEST 1: User A clicks Acquire Lock
  console.log('\n── TEST 1: User A clicks "Acquire Lock" ──');
  const acquireResA = await makeRequest({
    hostname: 'localhost',
    port: 3000,
    path: '/api/locks/acquire',
    method: 'POST'
  }, JSON.stringify({ docId, userId: 'user_a' }), cookieA);

  console.log(`   Status Code  : ${acquireResA.status} (Expected: 200)`);
  console.log(`   Lock Success : ${acquireResA.json?.success}`);
  console.log(`   Lock Owner   : ${acquireResA.json?.lock?.ownerId} (${acquireResA.json?.lock?.ownerName})`);
  console.log(`   Remaining Sec: ${acquireResA.json?.lock?.remainingSec}s`);

  if (acquireResA.status !== 200 || acquireResA.json?.lock?.ownerId !== 'user_a') {
    throw new Error('TEST 1 FAILED: User A could not acquire lock');
  }
  console.log('   ✔ TEST 1 PASSED: Document successfully LOCKED by User A');

  // Verify GET /api/locks/status
  const statusRes1 = await makeRequest({
    hostname: 'localhost',
    port: 3000,
    path: '/api/locks/status',
    method: 'GET'
  }, null, cookieA);
  const activeLocks1 = statusRes1.json || [];
  console.log(`   Active Locks count in backend: ${activeLocks1.length}`);
  const foundLock = activeLocks1.find(l => l.docId === docId);
  console.log(`   Found lock in backend list: ${foundLock ? foundLock.ownerId : 'NONE'}`);

  // 5. TEST 2: User B clicks Acquire Lock on same document
  console.log('\n── TEST 2: User B attempts to Acquire Lock on same document ──');
  const acquireResB = await makeRequest({
    hostname: 'localhost',
    port: 3000,
    path: '/api/locks/acquire',
    method: 'POST'
  }, JSON.stringify({ docId, userId: 'user_b' }), cookieB);

  console.log(`   Status Code  : ${acquireResB.status} (Expected: 423 or 409 Locked)`);
  console.log(`   Success      : ${acquireResB.json?.success} (Expected: false)`);
  console.log(`   Error Msg    : ${acquireResB.json?.error}`);
  console.log(`   Current Owner: ${acquireResB.json?.lockedBy || acquireResB.json?.currentOwner}`);

  if ((acquireResB.status !== 423 && acquireResB.status !== 409) || acquireResB.json?.success === true) {
    throw new Error('TEST 2 FAILED: User B was not rejected with HTTP 423/409');
  }
  console.log('   ✔ TEST 2 PASSED: Contention blocked with HTTP 423/409 Locked');

  // 6. TEST 3: User A clicks Release Lock
  console.log('\n── TEST 3: User A clicks "Release Lock" ──');
  const releaseResA = await makeRequest({
    hostname: 'localhost',
    port: 3000,
    path: '/api/locks/release',
    method: 'POST'
  }, JSON.stringify({ docId, userId: 'user_a' }), cookieA);

  console.log(`   Status Code   : ${releaseResA.status} (Expected: 200)`);
  console.log(`   Release Status: ${releaseResA.json?.released}`);

  if (releaseResA.status !== 200 || !releaseResA.json?.released) {
    throw new Error('TEST 3 FAILED: User A could not release lock');
  }

  // Verify GET /api/locks/status
  const statusRes2 = await makeRequest({
    hostname: 'localhost',
    port: 3000,
    path: '/api/locks/status',
    method: 'GET'
  }, null, cookieA);
  const activeLocks2 = statusRes2.json || [];
  console.log(`   Active Locks count after release: ${activeLocks2.length}`);
  console.log('   ✔ TEST 3 PASSED: Document UNLOCKED and free to acquire');

  // 7. TEST 4: User B clicks Acquire Lock after release
  console.log('\n── TEST 4: User B clicks "Acquire Lock" after release ──');
  const acquireResB2 = await makeRequest({
    hostname: 'localhost',
    port: 3000,
    path: '/api/locks/acquire',
    method: 'POST'
  }, JSON.stringify({ docId, userId: 'user_b' }), cookieB);

  console.log(`   Status Code  : ${acquireResB2.status} (Expected: 200)`);
  console.log(`   Lock Success : ${acquireResB2.json?.success}`);
  console.log(`   Lock Owner   : ${acquireResB2.json?.lock?.ownerId} (${acquireResB2.json?.lock?.ownerName})`);

  if (acquireResB2.status !== 200 || acquireResB2.json?.lock?.ownerId !== 'user_b') {
    throw new Error('TEST 4 FAILED: User B could not acquire free lock');
  }
  console.log('   ✔ TEST 4 PASSED: Document successfully LOCKED by User B');

  // Clean up: User B releases lock
  await makeRequest({
    hostname: 'localhost',
    port: 3000,
    path: '/api/locks/release',
    method: 'POST'
  }, JSON.stringify({ docId, userId: 'user_b' }), cookieB);
  console.log('\n   Cleaned up test lock. All tests passed!');
  console.log('\n================================================================');
  console.log('🎉 ALL 4 LOCK TESTS PASSED PERFECTLY!');
  console.log('================================================================\n');
}

runLockVerification().catch(err => {
  console.error('\n❌ ERROR:', err);
  process.exit(1);
});
