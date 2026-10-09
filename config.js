const path = require('path');

module.exports = {
  // Coordinator Configuration
  COORDINATOR: {
    PORT: 3000,
    HOST: 'localhost',
    RPC_ENDPOINT: '/api/rpc',
    WS_PORT: 3001
  },
  
  // Storage Nodes Configuration
  STORAGE_NODES: [
    { id: 'node_1', name: 'Storage Node Alpha', host: 'localhost', httpPort: 8001, wsPort: 9001, capacity: '10 GB' },
    { id: 'node_2', name: 'Storage Node Beta',  host: 'localhost', httpPort: 8002, wsPort: 9002, capacity: '10 GB' },
    { id: 'node_3', name: 'Storage Node Gamma', host: 'localhost', httpPort: 8003, wsPort: 9003, capacity: '10 GB' },
    { id: 'node_4', name: 'Storage Node Delta', host: 'localhost', httpPort: 8004, wsPort: 9004, capacity: '10 GB' }
  ],

  // System Protocols & Settings
  SYSTEM: {
    CHUNK_SIZE: 512 * 1024, // 512 KB per chunk for streaming
    DEFAULT_REPLICATION_FACTOR: 2, // Every doc replicated on at least 2 nodes
    HEARTBEAT_INTERVAL: 2000, // Heartbeat sent every 2 seconds
    HEARTBEAT_TIMEOUT: 6000, // Marked dead if no heartbeat for 6 seconds
    DATA_DIR: path.join(__dirname, 'storage_nodes', 'data'),
    METADATA_FILE: path.join(__dirname, 'storage_nodes', 'data', 'metadata.json')
  }
};
