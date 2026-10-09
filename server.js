/**
 * Master Launcher Entrypoint
 * Spawns the Master Coordinator Service and 3 Distributed Storage Nodes in a single process loop.
 */

const config = require('./config');
const CoordinatorService = require('./middleware/coordinator');
const StorageNode = require('./storage_nodes/storageNode');

async function bootstrap() {
  console.log(`================================================================`);
  console.log(`  DISTRIBUTED COLLABORATIVE DOCUMENT MANAGEMENT & SYNC SYSTEM   `);
  console.log(`  Units 1, 2, 3 & 4 Implementation (FA-1 + FA-2 Scope)          `);
  console.log(`================================================================\n`);

  // Step 1: Initialize Storage Nodes
  const storageNodes = config.STORAGE_NODES.map(nodeCfg => new StorageNode(nodeCfg, config.COORDINATOR, config));

  // Step 2: Initialize Coordinator Service
  const coordinator = new CoordinatorService();
  await coordinator.start(storageNodes);

  // Step 3: Start Storage Nodes
  console.log(`\n[Cluster] Bootstrapping ${storageNodes.length} Distributed Storage Nodes...`);
  for (const node of storageNodes) {
    await node.start();
  }

  console.log(`\n================================================================`);
  console.log(`✅ ALL CLUSTER SERVICES STARTED SUCCESSFULLY!`);
  console.log(`🌐 Open Interactive Dashboard: http://localhost:${config.COORDINATOR.PORT}`);
  console.log(`================================================================\n`);
}

bootstrap().catch(err => {
  console.error(`❌ Cluster Boot Failure:`, err);
  process.exit(1);
});
