/**
 * Unit 2: Remote Procedure Call (RPC) Server Module
 * Implements standard JSON-RPC 2.0 specification for Client-Server and Node-Coordinator calls.
 *
 * FA-1 Improvements:
 *  - Pass authenticated user to each handler (2nd argument)
 *  - Structured 403 Forbidden error code (-32001) distinct from internal errors (-32603)
 */

class RPCServer {
  constructor() {
    this.methods = new Map();
  }

  registerMethod(name, handler) {
    this.methods.set(name, handler);
    console.log(`[RPCServer] Registered RPC Method: '${name}'`);
  }

  /**
   * Handle a JSON-RPC 2.0 request.
   * @param {object} rpcBody - The parsed request body
   * @param {object|null} requestingUser - The authenticated user (from session), or null if unauthenticated
   */
  async handleRequest(rpcBody, requestingUser = null) {
    // Validate JSON-RPC 2.0 format
    if (!rpcBody || rpcBody.jsonrpc !== '2.0' || !rpcBody.method) {
      return {
        jsonrpc: '2.0',
        error: { code: -32600, message: 'Invalid Request: Must follow JSON-RPC 2.0 format' },
        id: rpcBody ? rpcBody.id : null
      };
    }

    const { method, params, id } = rpcBody;
    const handler = this.methods.get(method);

    if (!handler) {
      return {
        jsonrpc: '2.0',
        error: { code: -32601, message: `Method '${method}' not found` },
        id
      };
    }

    try {
      // Pass requestingUser as second argument so handlers can enforce permissions server-side
      const result = await handler(params || {}, requestingUser);
      return {
        jsonrpc: '2.0',
        result,
        id
      };
    } catch (err) {
      console.error(`[RPCServer] Error executing method '${method}':`, err.message);

      // Distinguish 403 Forbidden errors from general internal errors
      const is403 = err.code === -32001 ||
        (err.message && (err.message.includes('403') || err.message.toLowerCase().includes('forbidden')));

      return {
        jsonrpc: '2.0',
        error: {
          code: is403 ? -32001 : -32603,
          message: err.message || 'Internal RPC Error',
          // -32001 = Application-level 403 Forbidden (not in standard, used per spec extension rules)
          // -32603 = Internal error (JSON-RPC 2.0 standard)
          data: is403 ? { httpEquivalent: 403, type: 'FORBIDDEN' } : { httpEquivalent: 500, type: 'INTERNAL_ERROR' }
        },
        id
      };
    }
  }
}

module.exports = RPCServer;
