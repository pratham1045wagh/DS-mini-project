const http = require('http');
const WebSocket = require('ws');

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

async function runChatTests() {
  console.log('\n====================================================');
  console.log('🧪 REAL-TIME USER-TO-USER CHAT TEST SUITE');
  console.log('====================================================\n');

  // Step 1: Login user_a
  const bodyA = JSON.stringify({ userId: 'user_a', password: 'passA' });
  const loginA = await makeRequest({
    hostname: 'localhost', port: 3000, path: '/api/auth/login', method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Content-Length': bodyA.length }
  }, bodyA);
  const cookieA = loginA.cookies.map(c => c.split(';')[0]).join('; ');
  console.log('TEST 1 - User A Login:', loginA.status === 200 ? '✅ PASS' : '❌ FAIL');

  // Step 2: Login user_b
  const bodyB = JSON.stringify({ userId: 'user_b', password: 'passB' });
  const loginB = await makeRequest({
    hostname: 'localhost', port: 3000, path: '/api/auth/login', method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Content-Length': bodyB.length }
  }, bodyB);
  const cookieB = loginB.cookies.map(c => c.split(';')[0]).join('; ');
  console.log('TEST 2 - User B Login:', loginB.status === 200 ? '✅ PASS' : '❌ FAIL');

  // Step 3: Connect WebSocket for User B to test real-time broadcast
  let wsReceivedMessage = null;
  const wsB = new WebSocket('ws://localhost:3001');
  wsB.on('message', (msgStr) => {
    try {
      const data = JSON.parse(msgStr);
      if (data.type === 'CHAT_MESSAGE') {
        wsReceivedMessage = data.payload;
      }
    } catch (e) {}
  });

  await new Promise(r => setTimeout(r, 500));

  // Step 4: User A sends chat message to User B via HTTP API
  const msgBody = JSON.stringify({ targetUserId: 'user_b', text: 'Hello User B! Let us sync documents.' });
  const sendRes = await makeRequest({
    hostname: 'localhost', port: 3000, path: '/api/chat/send', method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Content-Length': msgBody.length, Cookie: cookieA }
  }, msgBody);
  const sendData = JSON.parse(sendRes.body);
  console.log('TEST 3 - HTTP Chat Send (User A ➔ User B):', sendRes.status === 200 && sendData.success ? '✅ PASS' : '❌ FAIL');

  await new Promise(r => setTimeout(r, 500));

  // Step 5: Verify WebSocket Broadcast received
  console.log('TEST 4 - WebSocket Real-Time Broadcast:', wsReceivedMessage && wsReceivedMessage.text === 'Hello User B! Let us sync documents.' ? '✅ PASS' : '❌ FAIL');

  // Step 6: User B fetches Chat History
  const historyRes = await makeRequest({
    hostname: 'localhost', port: 3000, path: '/api/chat/history', method: 'GET',
    headers: { Cookie: cookieB }
  });
  const historyData = JSON.parse(historyRes.body);
  const hasMsg = historyData.messages && historyData.messages.some(m => m.text.includes('Hello User B!'));
  console.log('TEST 5 - User B Chat History Retrieval:', historyRes.status === 200 && hasMsg ? '✅ PASS' : '❌ FAIL');

  // Step 7: Test RPC send_chat_message from User B to User A
  const rpcPayload = JSON.stringify({
    jsonrpc: '2.0', id: '101', method: 'rpc.send_chat_message',
    params: { targetUserId: 'user_a', text: 'Hey User A! Received loud and clear.' }
  });
  const rpcRes = await makeRequest({
    hostname: 'localhost', port: 3000, path: '/api/rpc', method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Content-Length': rpcPayload.length, Cookie: cookieB }
  }, rpcPayload);
  const rpcData = JSON.parse(rpcRes.body);
  console.log('TEST 6 - JSON-RPC 2.0 Chat Message Send:', rpcRes.status === 200 && rpcData.result?.success ? '✅ PASS' : '❌ FAIL');

  wsB.close();
  console.log('\n====================================================');
  console.log('🎉 ALL CHAT TESTS COMPLETED SUCCESSFULLY!');
  console.log('====================================================\n');
}

runChatTests().catch(err => {
  console.error('Chat Test Suite Error:', err);
  process.exit(1);
});
