/**
 * Stream Client Engine (Unit 2: Stream-Oriented & Fault Tolerant Downloads)
 *
 * FA-1 Improvements:
 *  - Downloads chunks through coordinator proxy (/api/documents/download/:docId/chunk/:index)
 *    instead of directly hitting storage node ports (avoids browser cross-origin errors)
 *  - Coordinator proxy handles: session auth check, 403 enforcement, failover across replicas
 *  - No need to pass userId — session cookie is sent automatically with credentials:'include'
 */
class StreamClient {
  constructor(rpcClient) {
    this.rpcClient = rpcClient;
  }

  /**
   * Downloads a document using the coordinator proxy endpoint.
   * Coordinator handles: auth, access control, replica selection, failover.
   *
   * @param {string} docId - Document ID
   * @param {function} onProgress - Progress callback({ chunkIndex, totalChunks, percent, usedNodeId })
   * @returns {{ blob, fileName, usedReplicaNodes }}
   */
  async downloadDocument(docId, onProgress) {
    // Handle backward-compatible call where second arg was userId (now ignored — session handles it)
    if (typeof onProgress !== 'function') {
      onProgress = null;
    }

    // Step 1: Query Coordinator for document metadata & access check via RPC
    // The RPC call passes the session cookie automatically (browser sends cookies for same-origin)
    const locationInfo = await this.rpcClient.call('rpc.get_doc_location', { docId });
    const { doc, availableReplicaNodes } = locationInfo;

    if (!availableReplicaNodes || availableReplicaNodes.length === 0) {
      throw new Error(`CRITICAL FAULT: All storage replica nodes containing '${doc.name}' are DOWN!`);
    }

    // Authoritative chunk count from existing document metadata
    const totalChunks = (typeof doc.totalChunks === 'number' && doc.totalChunks > 0)
      ? doc.totalChunks
      : (Array.isArray(doc.chunks) && doc.chunks.length > 0)
        ? doc.chunks.length
        : (typeof doc.chunkCount === 'number' && doc.chunkCount > 0)
          ? doc.chunkCount
          : (doc.size ? Math.max(1, Math.ceil(doc.size / (512 * 1024))) : 1);

    // Step 2: Download chunks concurrently via coordinator proxy (with automatic failover)
    let completedChunks = 0;
    const chunkPromises = [];

    for (let chunkIndex = 0; chunkIndex < totalChunks; chunkIndex++) {
      const chunkUrl = `/api/documents/download/${docId}/chunk/${chunkIndex}`;
      chunkPromises.push((async (idx) => {
        const res = await fetch(chunkUrl, {
          credentials: 'include' // Send session cookie for auth
        });

        if (res.status === 403) {
          const body = await res.json().catch(() => ({}));
          throw new Error(`403 Forbidden: ${body.error || 'Access denied'}`);
        }

        if (res.status === 401) {
          throw new Error('Not authenticated. Please login again.');
        }

        if (res.status === 503) {
          throw new Error(`All replicas are unavailable for chunk ${idx}. Try again later.`);
        }

        if (!res.ok) {
          throw new Error(`Failed to download chunk ${idx}: HTTP ${res.status}`);
        }

        const arrayBuffer = await res.arrayBuffer();
        const usedNodeId  = res.headers.get('X-Node-Id') || 'replica';

        completedChunks++;
        if (onProgress) {
          onProgress({
            chunkIndex: idx,
            totalChunks,
            percent: Math.round((completedChunks / totalChunks) * 100),
            usedNodeId
          });
        }

        return { index: idx, data: arrayBuffer };
      })(chunkIndex));
    }

    const downloadedChunks = await Promise.all(chunkPromises);

    // Step 3: Reassemble chunks in strict order into Blob & trigger browser save
    downloadedChunks.sort((a, b) => a.index - b.index);
    const blobParts = downloadedChunks.map(c => c.data);
    const blob = new Blob(blobParts, { type: doc.mimeType || 'application/octet-stream' });

    return {
      blob,
      fileName: doc.name,
      usedReplicaNodes: availableReplicaNodes // coordinator already filtered to healthy
    };
  }
}

window.StreamClient = StreamClient;
