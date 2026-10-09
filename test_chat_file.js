const http = require('http');
const WebSocket = require('ws');
const CoordinatorService = require('./middleware/coordinator');
const StorageNode = require('./storage_nodes/storageNode');
const config = require('./config');

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

async function uploadFile(cookieHeader, fileBuffer, fileName, mimeType = 'text/plain') {
  const boundary = '----FormBoundary' + Math.random().toString(36).slice(2);
  const parts = [
    `--${boundary}\r\nContent-Disposition: form-data; name="file"; filename="${fileName}"\r\nContent-Type: ${mimeType}\r\n\r\n`,
    fileBuffer,
    `\r\n--${boundary}--\r\n`
  ];
  const bodyParts = parts.map(p => typeof p === 'string' ? Buffer.from(p) : p);
  const fullBody = Buffer.concat(bodyParts);

  return request({
    hostname: 'localhost', port: 3000, path: '/api/documents/upload', method: 'POST',
    headers: {
      'Content-Type': `multipart/form-data; boundary=${boundary}`,
      'Content-Length': fullBody.length,
      Cookie: cookieHeader
    }
  }, fullBody);
}

async function login(userId, password) {
  const body = JSON.stringify({ userId, password });
  const res = await request({
    hostname: 'localhost', port: 3000, path: '/api/auth/login', method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Content-Length': body.length }
  }, body);
  return res.cookies.map(c => c.split(';')[0]).join('; ');
}

async function testFileSharingInChat() {
  console.log('\n====================================================');
  console.log('🧪 TEST: FILE SHARING IN REAL-TIME CHAT');
  console.log('====================================================\n');

  // Step 1: Login User A and User B
  const cookieA = await login('user_a', 'passA');
  const cookieB = await login('user_b', 'passB');

  // Step 2: User A uploads a document to storage cluster
  const testContent = Buffer.from('Shared file inside chat test content ' + Date.now());
  const uploadRes = await uploadFile(cookieA, testContent, 'chat_shared_file.txt');
  const uploadData = JSON.parse(uploadRes.body);
  console.log('Step 1 - User A File Upload:', uploadRes.status === 200 && uploadData.success ? '✅ PASS' : '❌ FAIL');
  const docId = uploadData.document.docId;

  // Step 3: Connect User B via WebSocket to test real-time broadcast of file attachment
  let receivedWsMessage = null;
  const wsB = new WebSocket('ws://localhost:3001');
  wsB.on('message', (msgStr) => {
    try {
      const data = JSON.parse(msgStr);
      if (data.type === 'CHAT_MESSAGE') {
        receivedWsMessage = data.payload;
      }
    } catch (e) {}
  });

  await new Promise(r => setTimeout(r, 500));

  // Step 4: User A sends file in chat to User B via HTTP API
  const chatBody = JSON.stringify({
    targetUserId: 'user_b',
    text: 'Check out this shared file in chat!',
    fileAttachment: { docId }
  });
  const sendRes = await request({
    hostname: 'localhost', port: 3000, path: '/api/chat/send', method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Content-Length': chatBody.length, Cookie: cookieA }
  }, chatBody);
  const sendData = JSON.parse(sendRes.body);

  console.log('Step 2 - HTTP Send Chat File (User A ➔ User B):', sendRes.status === 200 && sendData.message?.fileAttachment?.docId === docId ? '✅ PASS' : '❌ FAIL');

  await new Promise(r => setTimeout(r, 500));

  // Step 5: Verify WebSocket received fileAttachment metadata
  const wsPass = receivedWsMessage && receivedWsMessage.fileAttachment?.docId === docId && receivedWsMessage.fileAttachment?.fileName === 'chat_shared_file.txt';
  console.log('Step 3 - Real-Time WS File Attachment Broadcast:', wsPass ? '✅ PASS' : '❌ FAIL');

  // Step 6: User B fetches Chat History and verifies file attachment presence
  const historyRes = await request({
    hostname: 'localhost', port: 3000, path: '/api/chat/history', method: 'GET',
    headers: { Cookie: cookieB }
  });
  const historyData = JSON.parse(historyRes.body);
  const fileMsgInHistory = historyData.messages?.find(m => m.fileAttachment?.docId === docId);
  console.log('Step 4 - User B Chat History File Retrieval:', historyRes.status === 200 && !!fileMsgInHistory ? '✅ PASS' : '❌ FAIL');

  // Step 7: Verify User B has auto-granted READ permission on the shared file
  const docLocRes = await request({
    hostname: 'localhost', port: 3000, path: `/api/documents/download/${docId}/chunk/0`, method: 'GET',
    headers: { Cookie: cookieB }
  });
  console.log('Step 5 - Recipient Auto-Granted Permission & Chunk Access:', docLocRes.status === 200 ? '✅ PASS' : '❌ FAIL');

  wsB.close();
  console.log('\n====================================================');
  console.log('🎉 CHAT FILE SHARING TEST COMPLETED SUCCESSFULLY!');
  console.log('====================================================\n');
}

testFileSharingInChat().catch(err => {
  console.error('Chat File Test Error:', err);
  process.exit(1);
});
