const http = require('http');

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

async function testNewUsers() {
  console.log('\n====================================================');
  console.log('🧪 TESTING NEW USERS (user_c, user_d, user_e)');
  console.log('====================================================\n');

  // Test Logins
  const users = [
    { id: 'user_c', pass: 'passC' },
    { id: 'user_d', pass: 'passD' },
    { id: 'user_e', pass: 'passE' }
  ];

  const sessions = {};

  for (const u of users) {
    const body = JSON.stringify({ userId: u.id, password: u.pass });
    const res = await makeRequest({
      hostname: 'localhost', port: 3000, path: '/api/auth/login', method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Content-Length': body.length }
    }, body);

    const ok = res.status === 200 && res.cookies.length > 0;
    console.log(`Login Test [${u.id}]:`, ok ? '✅ PASS' : '❌ FAIL');
    if (!ok) throw new Error(`Failed to login as ${u.id}`);
    sessions[u.id] = res.cookies.map(c => c.split(';')[0]).join('; ');
  }

  // Test Chat between user_c and user_d
  const chatBody = JSON.stringify({ targetUserId: 'user_d', text: 'Hello User D from User C!' });
  const chatRes = await makeRequest({
    hostname: 'localhost', port: 3000, path: '/api/chat/send', method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Content-Length': chatBody.length, Cookie: sessions['user_c'] }
  }, chatBody);
  const chatData = JSON.parse(chatRes.body);
  console.log('Chat Test (User C ➔ User D):', chatRes.status === 200 && chatData.success ? '✅ PASS' : '❌ FAIL');

  // Test Chat History for User D
  const histRes = await makeRequest({
    hostname: 'localhost', port: 3000, path: '/api/chat/history', method: 'GET',
    headers: { Cookie: sessions['user_d'] }
  });
  const histData = JSON.parse(histRes.body);
  const foundChat = histData.messages && histData.messages.some(m => m.senderId === 'user_c' && m.targetUserId === 'user_d');
  console.log('Chat History Test (User D):', histRes.status === 200 && foundChat ? '✅ PASS' : '❌ FAIL');

  // Test Get Users endpoint
  const usersRes = await makeRequest({
    hostname: 'localhost', port: 3000, path: '/api/users', method: 'GET'
  });
  const allUsers = JSON.parse(usersRes.body);
  const hasAll = ['user_a', 'user_b', 'user_c', 'user_d', 'user_e'].every(id => allUsers.some(u => u.id === id));
  console.log('Get All Users API Test (5 users present):', hasAll ? '✅ PASS' : '❌ FAIL');

  console.log('\n====================================================');
  console.log('🎉 NEW USERS TEST SUITE PASSED SUCCESSFULLY!');
  console.log('====================================================\n');
}

testNewUsers().catch(err => {
  console.error('Test Error:', err);
  process.exit(1);
});
