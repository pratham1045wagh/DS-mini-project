/**
 * Unit 2: Stream-Oriented Communication Module
 * Handles chunked streaming of large documents for uploads and downloads.
 * Computes SHA-256 hash integrity per chunk and supports P2P stream replication.
 */

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

class StreamHandler {
  constructor(chunkSize = 512 * 1024) {
    this.chunkSize = chunkSize;
  }

  /**
   * Splits a buffer or file into chunks with metadata
   */
  splitIntoChunks(fileBuffer) {
    const chunks = [];
    const totalSize = fileBuffer.length;
    let offset = 0;
    let chunkIndex = 0;

    while (offset < totalSize) {
      const end = Math.min(offset + this.chunkSize, totalSize);
      const chunkData = fileBuffer.slice(offset, end);
      const hash = crypto.createHash('sha256').update(chunkData).digest('hex');

      chunks.push({
        index: chunkIndex,
        size: chunkData.length,
        hash,
        data: chunkData
      });

      offset = end;
      chunkIndex++;
    }

    return chunks;
  }

  /**
   * Reassembles array of binary chunks into a single document buffer
   */
  reassembleChunks(chunks) {
    // Sort chunks by index to guarantee correct ordering
    chunks.sort((a, b) => a.index - b.index);
    const buffers = chunks.map(c => Buffer.isBuffer(c.data) ? c.data : Buffer.from(c.data, 'base64'));
    return Buffer.concat(buffers);
  }

  /**
   * Verifies hash integrity of a chunk using SHA-256
   */
  verifyChunkIntegrity(chunkData, expectedHash) {
    const buffer = Buffer.isBuffer(chunkData) ? chunkData : Buffer.from(chunkData, 'base64');
    const computedHash = crypto.createHash('sha256').update(buffer).digest('hex');
    return computedHash === expectedHash;
  }
}

module.exports = StreamHandler;
