/**
 * MetadataStore
 * Persists the coordinator's document catalog (this.documents Map) to a local
 * JSON file so documents survive server restarts without any external database.
 *
 * Design decisions:
 *  - Atomic write: data is written to a .tmp file then renamed so a crash
 *    mid-write never corrupts the live metadata file.
 *  - Synchronous rename (fs.renameSync) is safe here because the OS atomic
 *    rename guarantee applies on the same filesystem volume (which is always
 *    true for local files on Windows/Linux/macOS).
 *  - verifyChunksOnNode is best-effort and non-blocking: it does not throw
 *    on failure; it just returns false so the caller can log a warning.
 */

const fs   = require('fs');
const path = require('path');
const http = require('http');

class MetadataStore {
  /**
   * @param {string} filePath - Absolute path to metadata.json
   */
  constructor(filePath) {
    this.filePath = filePath;
    this.tmpPath  = filePath + '.tmp';

    // Ensure parent directory exists
    const dir = path.dirname(filePath);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
  }

  // ─── Load ──────────────────────────────────────────────────────────────────

  /**
   * Reads the persisted metadata file and returns a Map<docId, docMetadata>.
   * Returns an empty Map if the file does not exist or is malformed.
   */
  load() {
    if (!fs.existsSync(this.filePath)) {
      console.log(`[MetadataStore] No existing metadata file found — starting fresh.`);
      return new Map();
    }

    try {
      const raw  = fs.readFileSync(this.filePath, 'utf8');
      const list = JSON.parse(raw); // stored as array of docMetadata objects

      if (!Array.isArray(list)) {
        console.warn(`[MetadataStore] Unexpected format in metadata file — starting fresh.`);
        return new Map();
      }

      const map = new Map();
      for (const doc of list) {
        if (doc && doc.docId) {
          map.set(doc.docId, doc);
        }
      }

      console.log(`[MetadataStore] ✅ Loaded ${map.size} document(s) from persistent storage.`);
      return map;
    } catch (err) {
      console.error(`[MetadataStore] Failed to load metadata file: ${err.message} — starting fresh.`);
      return new Map();
    }
  }

  // ─── Save ──────────────────────────────────────────────────────────────────

  /**
   * Atomically saves the full documents Map to disk.
   * @param {Map<string, object>} documentsMap
   */
  save(documentsMap) {
    try {
      const list = Array.from(documentsMap.values());
      const json = JSON.stringify(list, null, 2);

      // Write to .tmp then rename for atomicity
      fs.writeFileSync(this.tmpPath, json, 'utf8');
      fs.renameSync(this.tmpPath, this.filePath);
    } catch (err) {
      console.error(`[MetadataStore] ⚠️  Failed to save metadata: ${err.message}`);
    }
  }

  // ─── Startup Verification ──────────────────────────────────────────────────

  /**
   * Probes a storage node's /api/node/status endpoint to check whether all
   * expected chunks for a document are present on that node.
   *
   * @param {object}  node        - Node descriptor { host, httpPort, id }
   * @param {string}  docId
   * @param {number}  totalChunks
   * @returns {Promise<boolean>}  true if every chunk file is found on the node
   */
  verifyChunksOnNode(node, docId, totalChunks) {
    return new Promise((resolve) => {
      const options = {
        hostname: node.host || 'localhost',
        port:     node.httpPort,
        path:     '/api/node/status',
        method:   'GET'
      };

      const req = http.request(options, (res) => {
        let body = '';
        res.on('data', chunk => body += chunk);
        res.on('end', () => {
          try {
            const data = JSON.parse(body);
            // data.storedFiles is an array of filenames like "doc_abc_chunk_0.bin"
            const storedFiles = Array.isArray(data.storedFiles) ? data.storedFiles : [];

            let allPresent = true;
            for (let i = 0; i < totalChunks; i++) {
              if (!storedFiles.includes(`${docId}_chunk_${i}.bin`)) {
                allPresent = false;
                break;
              }
            }
            resolve(allPresent);
          } catch {
            resolve(false);
          }
        });
      });

      req.setTimeout(2000, () => { req.destroy(); resolve(false); });
      req.on('error', () => resolve(false));
      req.end();
    });
  }
}

module.exports = MetadataStore;
