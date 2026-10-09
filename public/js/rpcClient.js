/**
 * JSON-RPC 2.0 Browser Client Wrapper
 *
 * FA-1 Improvement: Always sends session cookie (credentials: 'include')
 * so the coordinator can identify the authenticated user from the session.
 */
class RPCClient {
  constructor(endpoint = '/api/rpc') {
    this.endpoint = endpoint;
    this.requestId = 1;
  }

  async call(method, params = {}) {
    const id = this.requestId++;
    const payload = {
      jsonrpc: '2.0',
      method,
      params,
      id
    };

    const response = await fetch(this.endpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      credentials: 'include', // Send session cookie for server-side auth
      body: JSON.stringify(payload)
    });

    if (!response.ok) {
      throw new Error(`HTTP Error ${response.status}: ${response.statusText}`);
    }

    const data = await response.json();

    if (data.error) {
      // Structured error: include error code in message for caller to distinguish 403 vs 500
      const code = data.error.code;
      const msg  = data.error.message || 'RPC Error';
      throw new Error(`RPC Error [${code}]: ${msg}`);
    }

    return data.result;
  }
}

window.RPCClient = RPCClient;
