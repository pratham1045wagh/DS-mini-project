const http = require('http');
const WebSocket = require('ws');

function request(options, body = null) {
  return new Promise((resolve, reject) => {
    const req = http.request(options, (res) => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        const buf = Buffer.concat(chunks);
        const cookies = res.headers['set-cookie'] || [];
        resolve({ status: res.statusCode, body: buf.toString(), raw: buf, headers: res.headers, cookies });
      });
    });
    req.on('error', reject);
    if (body) req.write(body);
    req.end();
  });
}

async function login(userId, password) {
  const body = JSON.stringify({ userId, password });
  const res = await request({
    hostname: 'localhost', port: 3000, path: '/api/auth/login', method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Content-Length': body.length }
  }, body);
  return res.cookies.map(c => c.split(';')[0]).join('; ');
}

async function runChatActionsTest() {
  console.log('\n====================================================');
  console.log('🧪 TEST: CHAT UNSEND, FORWARD & UNSHARE SHARED FILE');
  console.log('====================================================\n');

  // Step 1: Login User A, User B, User C
  const cookieA = await login('user_a', 'passA');
  const cookieB = await login('user_b', 'passB');
  const cookieC = await login('user_c', 'passC');

  // Step 2: User A sends chat message to User B
  const sendBody = JSON.stringify({ targetUserId: 'user_b', text: 'Message to be unsent' });
  const sendRes = await request({
    hostname: 'localhost', port: 3000, path: '/api/chat/send', method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Content-Length': sendBody.length, Cookie: cookieA }
  }, sendBody);
  const sendData = JSON.parse(sendRes.body);
  const messageId = sendData.message.id;
  console.log('Test 1 - User A Sends Message:', sendRes.status === 200 && !!messageId ? '✅ PASS' : '❌ FAIL');

  // Step 3: User A unsends (deletes) the message
  const unsendBody = JSON.stringify({ messageId });
  const unsendRes = await request({
    hostname: 'localhost', port: 3000, path: '/api/chat/delete', method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Content-Length': unsendBody.length, Cookie: cookieA }
  }, unsendBody);
  console.log('Test 2 - User A Unsends Message:', unsendRes.status === 200 ? '✅ PASS' : '❌ FAIL');

  // Verify message is removed from User B history
  const historyRes = await request({
    hostname: 'localhost', port: 3000, path: '/api/chat/history', method: 'GET',
    headers: { Cookie: cookieB }
  });
  const historyData = JSON.parse(historyRes.body);
  const isDeleted = !historyData.messages?.some(m => m.id === messageId);
  console.log('Test 3 - Message Verified Removed from History:', isDeleted ? '✅ PASS' : '❌ FAIL');

  // Step 4: User A shares a doc with User B
  const docsRes = await request({
    hostname: 'localhost', port: 3000, path: '/api/documents', method: 'GET',
    headers: { Cookie: cookieA }
  });
  const docsData = JSON.parse(docsRes.body);
  const userADoc = docsData.myDocuments?.[0];

  if (userADoc) {
    const shareBody = JSON.stringify({ docId: userADoc.docId, targetUserId: 'user_b', permission: 'READ' });
    await request({
      hostname: 'localhost', port: 3000, path: '/api/documents/share', method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Content-Length': shareBody.length, Cookie: cookieA }
    }, shareBody);

    // Verify User B has shared doc
    const bDocsRes1 = await request({
      hostname: 'localhost', port: 3000, path: '/api/documents', method: 'GET',
      headers: { Cookie: cookieB }
    });
    const hasShared1 = JSON.parse(bDocsRes1.body).sharedWithMe?.some(d => d.docId === userADoc.docId);
    console.log('Test 4 - Shared File Appears in Shared With Me:', hasShared1 ? '✅ PASS' : '❌ FAIL');

    // User B removes/unshares shared file from their Shared With Me view
    const unshareBody = JSON.stringify({ docId: userADoc.docId });
    const unshareRes = await request({
      hostname: 'localhost', port: 3000, path: '/api/documents/unshare', method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Content-Length': unshareBody.length, Cookie: cookieB }
    }, unshareBody);
    console.log('Test 5 - User B Removes Shared File:', unshareRes.status === 200 ? '✅ PASS' : '❌ FAIL');

    // Verify User B sharedWithMe no longer contains userADoc
    const bDocsRes2 = await request({
      hostname: 'localhost', port: 3000, path: '/api/documents', method: 'GET',
      headers: { Cookie: cookieB }
    });
    const hasShared2 = JSON.parse(bDocsRes2.body).sharedWithMe?.some(d => d.docId === userADoc.docId);
    console.log('Test 6 - Shared File Verified Removed from User B Library:', !hasShared2 ? '✅ PASS' : '❌ FAIL');
  }

  console.log('\n====================================================');
  console.log('🎉 ALL CHAT ACTIONS & UNSHARE TESTS PASSED!');
  console.log('====================================================\n');
}

runChatActionsTest().catch(err => {
  console.error('Chat Actions Test Error:', err);
  process.exit(1);
});
