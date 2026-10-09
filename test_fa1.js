const http = require('http');
const fs = require('fs');
const path = require('path');

function makeRequest(options, body = null) {
  return new Promise((resolve, reject) => {
    const req = http.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        const cookies = res.headers['set-cookie'] || [];
        resolve({ status: res.statusCode, body: data, headers: res.headers, cookies });
      });
    });
    req.on('error', reject);
    if (body) req.write(body);
    req.end();
  });
}

async function runTests() {
  console.log('\n=== FA-1 Test Suite ===\n');

  // Test 1: Login as User A
  const loginBody = JSON.stringify({ userId: 'user_a', password: 'passA' });
  const loginRes = await makeRequest({
    hostname: 'localhost', port: 3000, path: '/api/auth/login', method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Content-Length': loginBody.length }
  }, loginBody);
  
  const loginData = JSON.parse(loginRes.body);
  const cookieA = loginRes.cookies.map(c => c.split(';')[0]).join('; ');
  console.log('TEST 1 - User A Login:', loginRes.status === 200 ? 'PASS' : 'FAIL', loginData.user?.name);

  // Test 2: /api/auth/me with session
  const meRes = await makeRequest({
    hostname: 'localhost', port: 3000, path: '/api/auth/me', method: 'GET',
    headers: { Cookie: cookieA }
  });
  const meData = JSON.parse(meRes.body);
  console.log('TEST 2 - Session Me:', meRes.status === 200 && meData.authenticated ? 'PASS' : 'FAIL', meData.user?.id);

  // Test 3: Upload blocked without auth
  const uploadNoAuth = await makeRequest({
    hostname: 'localhost', port: 3000, path: '/api/documents/upload', method: 'POST',
    headers: { 'Content-Type': 'multipart/form-data' }
  });
  console.log('TEST 3 - Upload blocks without auth (expect 401):', uploadNoAuth.status === 401 ? 'PASS' : 'FAIL', 'Status:', uploadNoAuth.status);

  // Test 4: Docs endpoint blocked without auth
  const docsNoAuth = await makeRequest({
    hostname: 'localhost', port: 3000, path: '/api/documents', method: 'GET'
  });
  console.log('TEST 4 - Docs blocks without auth (expect 401):', docsNoAuth.status === 401 ? 'PASS' : 'FAIL', 'Status:', docsNoAuth.status);

  // Test 5: Wrong password
  const wrongPwBody = JSON.stringify({ userId: 'user_a', password: 'wrongpass' });
  const wrongPw = await makeRequest({
    hostname: 'localhost', port: 3000, path: '/api/auth/login', method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Content-Length': wrongPwBody.length }
  }, wrongPwBody);
  console.log('TEST 5 - Wrong password (expect 401):', wrongPw.status === 401 ? 'PASS' : 'FAIL', 'Status:', wrongPw.status);

  // Test 6: Get docs with valid auth
  const docsRes = await makeRequest({
    hostname: 'localhost', port: 3000, path: '/api/documents', method: 'GET',
    headers: { Cookie: cookieA }
  });
  const docsData = JSON.parse(docsRes.body);
  console.log('TEST 6 - Docs with auth (expect 200):', docsRes.status === 200 ? 'PASS' : 'FAIL', 'userId:', docsData.userId);

  // Test 7: RPC without session - get_doc_location should return 403
  const rpcNoAuthBody = JSON.stringify({ jsonrpc: '2.0', method: 'rpc.get_doc_location', params: { docId: 'doc_fake' }, id: 1 });
  const rpcNoAuth = await makeRequest({
    hostname: 'localhost', port: 3000, path: '/api/rpc', method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Content-Length': rpcNoAuthBody.length }
  }, rpcNoAuthBody);
  const rpcNoAuthData = JSON.parse(rpcNoAuth.body);
  console.log('TEST 7 - RPC get_doc_location without auth (expect error -32001):', 
    rpcNoAuthData.error?.code === -32001 ? 'PASS' : 'FAIL', 'Code:', rpcNoAuthData.error?.code);

  // Test 8: Login as User B  
  const loginBBody = JSON.stringify({ userId: 'user_b', password: 'passB' });
  const loginBRes = await makeRequest({
    hostname: 'localhost', port: 3000, path: '/api/auth/login', method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Content-Length': loginBBody.length }
  }, loginBBody);
  const loginBData = JSON.parse(loginBRes.body);
  const cookieB = loginBRes.cookies.map(c => c.split(';')[0]).join('; ');
  console.log('TEST 8 - User B Login:', loginBRes.status === 200 ? 'PASS' : 'FAIL', loginBData.user?.name);

  // Test 9: Users API
  const usersRes = await makeRequest({
    hostname: 'localhost', port: 3000, path: '/api/users', method: 'GET'
  });
  const usersData = JSON.parse(usersRes.body);
  console.log('TEST 9 - Users API (public):', usersRes.status === 200 && Array.isArray(usersData) ? 'PASS' : 'FAIL', 'Count:', usersData.length);

  // Test 10: Cluster status RPC
  const clusterBody = JSON.stringify({ jsonrpc: '2.0', method: 'rpc.get_cluster_status', params: {}, id: 2 });
  const clusterRes = await makeRequest({
    hostname: 'localhost', port: 3000, path: '/api/rpc', method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Content-Length': clusterBody.length }
  }, clusterBody);
  const clusterData = JSON.parse(clusterRes.body);
  console.log('TEST 10 - Cluster Status RPC:', clusterData.result?.nodes ? 'PASS' : 'FAIL', 'Nodes:', clusterData.result?.nodes?.length);

  console.log('\n=== Tests Complete ===\n');
}

runTests().catch(console.error);
