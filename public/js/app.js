/**
 * Main Application Dashboard & Access Control Controller
 * FA-1 + FA-2 Distributed Systems Scope • Units 1, 2, 3 & 4 Implementation
 *
 * FA-1 Features preserved:
 *  - Session-based login (/api/auth/me, /api/auth/login, /api/auth/logout)
 *  - Real-time P2P chunk streaming & download
 *  - Dynamic SVG cluster topology visualization & heartbeat monitoring
 *  - User-to-user real-time chat with document sharing & attachments
 *  - Document permissions (READ / WRITE / OWNER)
 *
 * FA-2 Features added:
 *  - Lamport Logical Clock: Distributed Event Timeline & clock chips
 *  - Vector Clock: Versioning, concurrency & conflict resolution modal
 *  - Bully Algorithm Leader Election: Failures, rank-based elections & revival
 *  - Distributed Mutual Exclusion: Leased exclusive document locking
 *  - Deadlock Detection: Wait-For Graph (WFG) cycle detection & victim resolution
 *  - Node Recovery & Re-Synchronization: FAILED -> RECOVERING -> HEALTHY with chunk sync
 *  - Categorized Distributed Event Monitor with category filter tabs
 */

document.addEventListener('DOMContentLoaded', () => {
  const rpc = new window.RPCClient('/api/rpc');
  const streamClient = new window.StreamClient(rpc);

  // ── Application State ──────────────────────────────────────────────────
  let currentUser = null;   // { id, name, email } from session
  let activeTab = 'mydocs'; // 'mydocs' | 'shared'
  let nodes = [];
  let documents = [];
  let allUsers = [];
  let logs = [];
  let ws = null;

  // Real-time Chat State
  let chatMessages = [];
  let unreadCount = 0;
  let isChatOpen = false;

  // FA-2 Distributed State
  let leaderState = {
    leaderId: 'Coordinator',
    leaderName: 'Coordinator (Port 3000)',
    previousLeaderId: null,
    electionStatus: 'COMPLETED',
    coordinatorStatus: 'HEALTHY'
  };
  let activeLocks = [];
  let deadlockState = { isDeadlocked: false, cycle: [], detectedAt: null };
  let logicalClocks = { Coordinator: 0, node_1: 0, node_2: 0, node_3: 0, node_4: 0 };
  let timelineEvents = [];
  let electionLogs = [];
  let currentLogFilter = 'all';
  let activeConflictDoc = null;
  let currentEditingDocId = null;

  // ── DOM Elements ────────────────────────────────────────────────────────

  // Login screen elements
  const loginScreen       = document.getElementById('loginScreen');
  const mainApp           = document.getElementById('mainApp');
  const loginUserId       = document.getElementById('loginUserId');
  const loginPassword     = document.getElementById('loginPassword');
  const loginBtn          = document.getElementById('loginBtn');
  const loginError        = document.getElementById('loginError');

  // Navbar elements
  const loggedInUserName  = document.getElementById('loggedInUserName');
  const logoutBtn         = document.getElementById('logoutBtn');
  const openChatNavBtn    = document.getElementById('openChatNavBtn');
  const chatNavBadge      = document.getElementById('chatNavBadge');
  const activeUserUploadLabel = document.getElementById('activeUserUploadLabel');

  // Leader & Conflict Banners (FA-2)
  const currentLeaderDisplay   = document.getElementById('currentLeaderDisplay');
  const currentLeaderText      = document.getElementById('currentLeaderText');
  const prevLeaderText         = document.getElementById('prevLeaderText');
  const electionStatusBadge    = document.getElementById('electionStatusBadge');
  const coordinatorStatusBadge = document.getElementById('coordinatorStatusBadge');
  const triggerElectionBtn     = document.getElementById('triggerElectionBtn');
  const killCoordinatorBtn     = document.getElementById('killCoordinatorBtn');
  const reviveCoordinatorBtn   = document.getElementById('reviveCoordinatorBtn');
  const conflictAlertBox       = document.getElementById('conflictAlertBox');
  const conflictAlertSummary   = document.getElementById('conflictAlertSummary');
  const openConflictResolveBtn = document.getElementById('openConflictResolveBtn');

  // Chat Elements
  const floatingChatBtn      = document.getElementById('floatingChatBtn');
  const floatingChatBadge    = document.getElementById('floatingChatBadge');
  const chatDrawer           = document.getElementById('chatDrawer');
  const closeChatBtn         = document.getElementById('closeChatBtn');
  const chatTargetUserSelect = document.getElementById('chatTargetUserSelect');
  const chatMessageList      = document.getElementById('chatMessageList');
  const chatForm             = document.getElementById('chatForm');
  const chatInput            = document.getElementById('chatInput');
  const chatAttachBtn        = document.getElementById('chatAttachBtn');
  const chatFileInput        = document.getElementById('chatFileInput');
  const chatShareDocBtn      = document.getElementById('chatShareDocBtn');
  const chatSelectDocModal   = document.getElementById('chatSelectDocModal');
  const chatSelectDocCloseBtn = document.getElementById('chatSelectDocCloseBtn');
  const cancelChatSelectDocBtn = document.getElementById('cancelChatSelectDocBtn');
  const confirmChatSelectDocBtn = document.getElementById('confirmChatSelectDocBtn');
  const chatSelectDocDropdown = document.getElementById('chatSelectDocDropdown');

  // Document manager
  const fileInput         = document.getElementById('fileInput');
  const dropZone          = document.getElementById('dropZone');
  const docTableBody      = document.getElementById('docTableBody');
  const logTerminal       = document.getElementById('logTerminal');
  const progressWrapper   = document.getElementById('progressWrapper');
  const progressBar       = document.getElementById('progressBar');
  const progressText      = document.getElementById('progressText');

  // Tabs
  const tabMyDocs         = document.getElementById('tabMyDocs');
  const tabSharedWithMe   = document.getElementById('tabSharedWithMe');
  const myDocsCount       = document.getElementById('myDocsCount');
  const sharedCount       = document.getElementById('sharedCount');

  // Quick Text Creator
  const quickDocTitle     = document.getElementById('quickDocTitle');
  const quickDocContent   = document.getElementById('quickDocContent');
  const createQuickDocBtn = document.getElementById('createQuickDocBtn');

  // Document Search & Refresh
  const docSearchInput    = document.getElementById('docSearchInput');
  const refreshDocsBtn    = document.getElementById('refreshDocsBtn');
  const refreshDocsIcon   = document.getElementById('refreshDocsIcon');

  // Timeline & Logical Clocks (FA-2)
  const clockCoordinator   = document.getElementById('clockCoordinator');
  const clockNode1         = document.getElementById('clockNode1');
  const clockNode2         = document.getElementById('clockNode2');
  const clockNode3         = document.getElementById('clockNode3');
  const clockNode4         = document.getElementById('clockNode4');
  const timelineTableBody  = document.getElementById('timelineTableBody');

  // Coordination & Deadlock (FA-2)
  const activeLocksCount     = document.getElementById('activeLocksCount');
  const activeLocksTableBody = document.getElementById('activeLocksTableBody');
  const deadlockStatusBadge  = document.getElementById('deadlockStatusBadge');
  const wfgContainer         = document.getElementById('wfgContainer');
  const simulateDeadlockBtn  = document.getElementById('simulateDeadlockBtn');
  const resolveDeadlockBtn   = document.getElementById('resolveDeadlockBtn');

  // Bully Election & Recovery (FA-2)
  const nodeRecoveryCards   = document.getElementById('nodeRecoveryCards');
  const manualSyncAllBtn     = document.getElementById('manualSyncAllBtn');
  const electionLogTerminal  = document.getElementById('electionLogTerminal');

  // Log filter tabs
  const logFilterTabs = document.getElementById('logFilterTabs');

  // Share Modal
  const shareModal            = document.getElementById('shareModal');
  const modalCloseBtn         = document.getElementById('modalCloseBtn');
  const cancelShareBtn        = document.getElementById('cancelShareBtn');
  const confirmShareBtn       = document.getElementById('confirmShareBtn');
  const shareDocNameInput     = document.getElementById('shareDocNameInput');
  const shareDocIdInput       = document.getElementById('shareDocIdInput');
  const shareTargetUserSelect = document.getElementById('shareTargetUserSelect');
  const sharePermissionSelect = document.getElementById('sharePermissionSelect');

  // File View / Preview Modal
  const viewModal           = document.getElementById('viewModal');
  const viewModalCloseBtn   = document.getElementById('viewModalCloseBtn');
  const viewCloseBtn        = document.getElementById('viewCloseBtn');
  const viewDocTitle        = document.getElementById('viewDocTitle');
  const copyToast           = document.getElementById('copyToast');
  const viewMetaSize        = document.getElementById('viewMetaSize');
  const viewMetaOwner       = document.getElementById('viewMetaOwner');
  const viewMetaAccess      = document.getElementById('viewMetaAccess');
  const viewMetaReplicas    = document.getElementById('viewMetaReplicas');
  const viewModalBody       = document.getElementById('viewModalBody');
  const viewCopyBtn         = document.getElementById('viewCopyBtn');
  const viewDownloadBtn     = document.getElementById('viewDownloadBtn');
  let currentPreviewUrl     = null;
  let currentPreviewText    = '';

  // FA-2: Edit Document Modal
  const editDocModal               = document.getElementById('editDocModal');
  const editDocModalCloseBtn       = document.getElementById('editDocModalCloseBtn');
  const cancelEditDocBtn           = document.getElementById('cancelEditDocBtn');
  const editDocModalTitle          = document.getElementById('editDocModalTitle');
  const editDocLockBadge           = document.getElementById('editDocLockBadge');
  const editDocLockOwner           = document.getElementById('editDocLockOwner');
  const editDocAcquireLockBtn       = document.getElementById('editDocAcquireLockBtn');
  const editDocReleaseLockBtn       = document.getElementById('editDocReleaseLockBtn');
  const editDocVersionBadge        = document.getElementById('editDocVersionBadge');
  const editDocTextarea            = document.getElementById('editDocTextarea');
  const editDocSimulateConflictBtn = document.getElementById('editDocSimulateConflictBtn');
  const saveEditDocBtn             = document.getElementById('saveEditDocBtn');

  // FA-2: Version History Modal
  const versionHistoryModal      = document.getElementById('versionHistoryModal');
  const versionHistoryCloseBtn   = document.getElementById('versionHistoryCloseBtn');
  const closeVersionHistoryBtn   = document.getElementById('closeVersionHistoryBtn');
  const historyDocTitle          = document.getElementById('historyDocTitle');
  const historyDocSubtitle       = document.getElementById('historyDocSubtitle');
  const versionHistoryTableBody  = document.getElementById('versionHistoryTableBody');

  // FA-2: Conflict Resolution Modal
  const conflictModal           = document.getElementById('conflictModal');
  const conflictModalCloseBtn   = document.getElementById('conflictModalCloseBtn');
  const conflictCancelBtn       = document.getElementById('conflictCancelBtn');
  const conflictVerALabel       = document.getElementById('conflictVerALabel');
  const conflictVerAMeta        = document.getElementById('conflictVerAMeta');
  const conflictVerAVector      = document.getElementById('conflictVerAVector');
  const conflictVerAPreview     = document.getElementById('conflictVerAPreview');
  const chooseVerABtn           = document.getElementById('chooseVerABtn');
  const conflictVerBLabel       = document.getElementById('conflictVerBLabel');
  const conflictVerBMeta        = document.getElementById('conflictVerBMeta');
  const conflictVerBVector      = document.getElementById('conflictVerBVector');
  const conflictVerBPreview     = document.getElementById('conflictVerBPreview');
  const chooseVerBBtn           = document.getElementById('chooseVerBBtn');
  const conflictMergeTextarea   = document.getElementById('conflictMergeTextarea');
  const confirmMergeBtn         = document.getElementById('confirmMergeBtn');

  // ── Session Check ────────────────────────────────────────────────────────

  async function checkSession() {
    try {
      const res = await fetch('/api/auth/me', { credentials: 'include' });
      const data = await res.json();
      if (data.authenticated && data.user) {
        onLoginSuccess(data.user);
      } else {
        showLoginScreen();
      }
    } catch (e) {
      showLoginScreen();
    }
  }

  function showLoginScreen() {
    loginScreen.style.display = 'flex';
    mainApp.style.display = 'none';
    currentUser = null;
    fetchUsers();
  }

  function showMainApp() {
    loginScreen.style.display = 'none';
    mainApp.style.display = 'block';
  }

  function onLoginSuccess(user) {
    currentUser = user;
    if (loggedInUserName) loggedInUserName.textContent = `${user.name} (${user.id})`;
    if (activeUserUploadLabel) activeUserUploadLabel.textContent = user.name;
    showMainApp();
    initWebSocket();
    Promise.all([fetchDocuments(), fetchUsers(), fetchLeaderState()]).catch(() => {});
    appendLog(`[AUTH] Logged in as ${user.name} (${user.id}) — session established`, 'log-success');
  }

  // ── Login Form ──────────────────────────────────────────────────────────

  if (loginBtn) loginBtn.addEventListener('click', handleLogin);
  if (loginPassword) {
    loginPassword.addEventListener('keypress', (e) => {
      if (e.key === 'Enter') handleLogin();
    });
  }

  async function handleLogin() {
    const userId   = loginUserId ? loginUserId.value : '';
    const password = loginPassword ? loginPassword.value : '';

    if (!userId || !password) {
      showLoginError('Please enter your user ID and password.');
      return;
    }

    if (loginBtn) {
      loginBtn.disabled = true;
      loginBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Signing in...';
    }

    try {
      const res = await fetch('/api/auth/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({ userId, password })
      });

      const data = await res.json();

      if (res.ok && data.user) {
        hideLoginError();
        onLoginSuccess(data.user);
      } else {
        showLoginError(data.error || 'Login failed. Check your credentials.');
      }
    } catch (err) {
      showLoginError(`Login error: ${err.message}`);
    } finally {
      if (loginBtn) {
        loginBtn.disabled = false;
        loginBtn.innerHTML = '<i class="fa-solid fa-right-to-bracket"></i> Sign In';
      }
    }
  }

  function showLoginError(msg) {
    if (loginError) {
      loginError.textContent = msg;
      loginError.style.display = 'block';
    }
  }

  function hideLoginError() {
    if (loginError) loginError.style.display = 'none';
  }

  // ── Logout ──────────────────────────────────────────────────────────────

  if (logoutBtn) {
    logoutBtn.addEventListener('click', async () => {
      try {
        await fetch('/api/auth/logout', { method: 'POST', credentials: 'include' });
      } catch (e) {}
      if (ws) { ws.close(); ws = null; }
      documents = [];
      nodes = [];
      showLoginScreen();
      appendLog('[AUTH] Logged out — session destroyed', 'log-info');
    });
  }

  // ── Render Schedulers (RequestAnimationFrame Batching) ──────────────────
  let pendingTopologyRender = false;
  function scheduleTopologyRender() {
    if (pendingTopologyRender) return;
    pendingTopologyRender = true;
    requestAnimationFrame(() => {
      pendingTopologyRender = false;
      renderTopology();
    });
  }

  let pendingRecoveryRender = false;
  function scheduleNodeRecoveryRender() {
    if (pendingRecoveryRender) return;
    pendingRecoveryRender = true;
    requestAnimationFrame(() => {
      pendingRecoveryRender = false;
      renderNodeRecovery();
    });
  }

  // ── WebSocket Handler ────────────────────────────────────────────────────

  function initWebSocket() {
    if (ws) {
      try { ws.onclose = null; ws.close(); } catch (e) {}
      ws = null;
    }

    const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${wsProtocol}//${window.location.hostname}:3001`;

    ws = new WebSocket(wsUrl);

    ws.onmessage = (event) => {
      try {
        const { type, payload } = JSON.parse(event.data);

        if (type === 'INITIAL_STATE') {
          nodes          = payload.nodes          || [];
          documents      = payload.documents      || [];
          allUsers       = payload.users          || [];
          logs           = payload.logs           || [];
          chatMessages   = payload.chatHistory    || [];
          if (payload.leaderState || payload.leader) {
            updateLeaderState(payload.leaderState || payload.leader);
          }
          activeLocks    = payload.activeLocks    || payload.locks  || [];
          deadlockState  = payload.deadlockState  || payload.deadlock || deadlockState;
          logicalClocks  = payload.logicalClocks  || logicalClocks;
          timelineEvents = payload.timelineEvents || payload.timeline || [];
          electionLogs   = payload.electionLogs   || (payload.leader && payload.leader.recentLogs ? payload.leader.recentLogs.map(l => l.text) : []);

          checkDocumentConflicts();
          populateChatUserSelect();
          renderAll();
          if (isChatOpen) renderChatMessages();
          updateEditModalLockState();

        } else if (type === 'CHAT_MESSAGE') {
          chatMessages.push(payload);
          appendLog(`[CHAT] ${payload.senderName} (${payload.senderId}) ➔ ${payload.targetName} (${payload.targetUserId}): "${payload.text}"`, 'log-info');

          if (currentUser && (payload.senderId === currentUser.id || payload.targetUserId === currentUser.id)) {
            if (isChatOpen) {
              renderChatMessages();
            } else if (payload.senderId !== currentUser.id) {
              unreadCount++;
              updateChatBadges();
            }
          }

        } else if (type === 'CHAT_MESSAGE_DELETED') {
          const { messageId } = payload;
          chatMessages = chatMessages.filter(m => m.id !== messageId);
          if (isChatOpen) renderChatMessages();

        } else if (type === 'NODE_STATUS_CHANGED') {
          const node = nodes.find(n => n.id === payload.nodeId);
          if (node) {
            node.status = payload.status;
            node.storageUsedBytes = payload.storageUsedBytes !== undefined ? payload.storageUsedBytes : node.storageUsedBytes;
          }
          const styleClass = payload.status === 'HEALTHY' ? 'log-success' : 'log-error';
          appendLog(`[NODE MONITOR] Node ${payload.nodeId} → ${payload.status} (${payload.reason || ''})`, styleClass);
          renderClusterBadges();
          scheduleTopologyRender();
          scheduleNodeRecoveryRender();

        } else if (type === 'DOCUMENT_UPLOADED') {
          const idx = documents.findIndex(d => d.docId === payload.docId);
          if (idx >= 0) documents[idx] = payload;
          else documents.push(payload);
          appendLog(`[CLUSTER] Document uploaded: '${payload.name}' (v${payload.version || 1}) by ${payload.ownerId} (SHA-256 verified)`, 'log-success');
          renderDocuments();

        } else if (type === 'DOCUMENT_UPDATED') {
          const idx = documents.findIndex(d => d.docId === payload.docId);
          if (idx >= 0) documents[idx] = payload.document;
          appendLog(`[VECTOR] Document '${payload.document.name}' updated to v${payload.document.version} by ${payload.document.updatedBy} | Clock: ${JSON.stringify(payload.document.vectorClock)}`, 'log-success');
          renderDocuments();

        } else if (type === 'DOCUMENT_SHARED') {
          const doc = documents.find(d => d.docId === payload.docId);
          if (doc) {
            if (!doc.shares) doc.shares = {};
            doc.shares[payload.targetUserId] = payload.permission;
          }
          appendLog(`[ACCESS CONTROL] '${payload.docName || payload.docId}' shared with ${payload.targetUserId} (${payload.permission})`, 'log-info');
          renderDocuments();

        } else if (type === 'DOCUMENT_REPLICATION') {
          if (payload.status === 'STARTED') {
            appendLog(`[P2P] Document replication started: ${payload.name} → nodes [${(payload.replicaNodes || []).join(', ')}]`, 'log-info');
          } else if (payload.status === 'SUCCESS') {
            appendLog(`[P2P] Replication SUCCESS: ${payload.name} — all replicas SHA-256 verified on [${(payload.replicaNodes || []).join(', ')}]`, 'log-success');
          } else if (payload.status === 'PARTIAL') {
            appendLog(`[P2P] Replication PARTIAL: ${payload.name} — some replicas may have failed`, 'log-error');
          }

        } else if (type === 'CHUNK_REPLICATION') {
          appendLog(`[P2P] Chunk #${payload.chunkIndex}/${payload.totalChunks - 1} replicated: ${payload.senderId} → ${payload.targetId} | SHA-256: ${payload.hash} | Status: ${payload.status}`, 'log-info');

        } else if (type === 'DOCUMENT_DELETED') {
          documents = documents.filter(d => d.docId !== payload.docId);
          appendLog(`[DELETE] Document '${payload.name}' removed from cluster by ${payload.deletedBy}`, 'log-error');
          renderDocuments();

        } else if (type === 'RPC_LOG') {
          logs.push(payload);
          if (logs.length > 200) logs.shift();
          appendRpcLogEntry(payload);

        } else if (type === 'NODE_REGISTERED') {
          const existing = nodes.find(n => n.id === payload.id);
          if (existing) {
            Object.assign(existing, payload);
          } else {
            nodes.push(payload);
          }
          appendLog(`[CLUSTER] Node registered: ${payload.name} (${payload.id}) @ ${payload.httpUrl}`, 'log-success');
          renderAll();

        } else if (type === 'LEADER_CHANGED' || type === 'LEADER_ELECTION_EVENT' || type === 'COORDINATOR_FAILED' || type === 'COORDINATOR_REVIVED') {
          updateLeaderState(payload.leaderState || payload);
          if (payload.log) {
            const logText = typeof payload.log === 'string' ? payload.log : (payload.log.text || JSON.stringify(payload.log));
            appendElectionLog(logText);
          } else if (payload.electionLogs) {
            electionLogs = payload.electionLogs;
            renderElectionLogs();
          }

        } else if (type === 'LOCK_ACQUIRED') {
          if (payload.activeLocks) {
            activeLocks = payload.activeLocks;
          } else if (payload.locks) {
            activeLocks = payload.locks;
          } else if (payload.docId) {
            const idx = activeLocks.findIndex(l => l.docId === payload.docId);
            if (idx >= 0) activeLocks[idx] = payload;
            else activeLocks.push(payload);
          }
          appendLog(`[LOCK] Exclusive lock acquired on '${payload.docName || payload.docId}' by ${payload.ownerName || payload.lockedByName || payload.lockedBy}`, 'log-success');
          renderLocks();
          updateSingleDocumentLockUI(payload.docId);
          updateEditModalLockState();

        } else if (type === 'LOCK_RELEASED') {
          if (payload.activeLocks) {
            activeLocks = payload.activeLocks;
          } else if (payload.locks) {
            activeLocks = payload.locks;
          } else if (payload.docId) {
            activeLocks = activeLocks.filter(l => l.docId !== payload.docId);
          }
          appendLog(`[LOCK] Lock released on '${payload.docName || payload.docId}' by ${payload.releasedBy}`, 'log-info');
          renderLocks();
          updateSingleDocumentLockUI(payload.docId);
          updateEditModalLockState();

        } else if (type === 'LOCK_EXPIRED') {
          if (payload.activeLocks) {
            activeLocks = payload.activeLocks;
          } else if (payload.locks) {
            activeLocks = payload.locks;
          } else if (payload.docId) {
            activeLocks = activeLocks.filter(l => l.docId !== payload.docId);
          }
          appendLog(`[LOCK] Lock expired on '${payload.docName || payload.docId}' (automatic recovery)`, 'log-warning');
          renderLocks();
          updateSingleDocumentLockUI(payload.docId);
          updateEditModalLockState();

        } else if (type === 'LOCK_DENIED') {
          appendLog(`[LOCK] Access Denied: '${payload.docName || payload.docId}' is currently locked by ${payload.heldByName || payload.heldBy || payload.currentOwner}`, 'log-error');

        } else if (type === 'DEADLOCK_DETECTED') {
          deadlockState = {
            isDeadlocked: true,
            cycle: payload.cycle || [],
            wfg: payload.wfg || {},
            detectedAt: new Date().toISOString()
          };
          renderDeadlock();
          appendLog(`[DEADLOCK] DEADLOCK DETECTED! Cycle: ${(payload.cycle || []).join(' ⇄ ')}`, 'log-error');

        } else if (type === 'DEADLOCK_RESOLVED') {
          deadlockState = {
            isDeadlocked: false,
            cycle: [],
            wfg: payload.remainingWfg || {},
            detectedAt: null
          };
          renderDeadlock();
          appendLog(`[DEADLOCK] Resolved: ${payload.message || 'Victim request aborted'}`, 'log-success');

        } else if (type === 'DOCUMENT_CONFLICT') {
          const doc = documents.find(d => d.docId === payload.docId);
          if (doc) {
            doc.conflict = true;
            doc.conflictData = { versionA: payload.versionA, versionB: payload.versionB };
          }
          activeConflictDoc = { docId: payload.docId, versionA: payload.versionA, versionB: payload.versionB };
          renderConflictBanner();
          renderDocuments();
          appendLog(`[VECTOR] CONFLICT DETECTED on document '${payload.docId}'! Concurrent vector clocks: ${JSON.stringify(payload.versionA.vectorClock)} || ${JSON.stringify(payload.versionB.vectorClock)}`, 'log-error');

        } else if (type === 'DOCUMENT_CONFLICT_RESOLVED') {
          const doc = documents.find(d => d.docId === payload.docId);
          if (doc) {
            doc.conflict = false;
            doc.conflictData = null;
            if (payload.resolvedVersion) Object.assign(doc, payload.resolvedVersion);
          }
          if (activeConflictDoc && activeConflictDoc.docId === payload.docId) {
            activeConflictDoc = null;
          }
          renderConflictBanner();
          renderDocuments();
          closeConflictModal();
          appendLog(`[VECTOR] Conflict resolved on '${payload.docId}'. New vector clock established.`, 'log-success');

        } else if (type === 'TIMELINE_EVENT') {
          if (payload.event) {
            timelineEvents.unshift(payload.event);
            if (timelineEvents.length > 60) timelineEvents.pop();
            prependTimelineRow(payload.event);
          }

        } else if (type === 'CLOCKS_UPDATED') {
          if (payload.clocks) {
            logicalClocks = Object.assign({}, logicalClocks, payload.clocks);
            renderClocks();
          }

        } else if (type === 'NODE_SYNC_LOG') {
          appendLog(`[RECOVERY] [Node ${payload.nodeId}] ${payload.message}`, payload.status === 'FAILED' ? 'log-error' : 'log-success');
          scheduleNodeRecoveryRender();
        }

      } catch (e) {
        console.error('WS Message parsing error:', e);
      }
    };

    ws.onclose = () => {
      if (currentUser) {
        setTimeout(() => {
          if (currentUser && (!ws || ws.readyState === WebSocket.CLOSED)) {
            initWebSocket();
          }
        }, 2000);
      }
    };

    ws.onerror = (e) => {
      console.warn('[WS] Connection error:', e);
    };
  }

  // ── Fetch Documents & Initial State ──────────────────────────────────────

  async function fetchDocuments() {
    if (!currentUser) return;
    try {
      const res = await fetch('/api/documents', { credentials: 'include' });
      if (res.ok) {
        const data = await res.json();
        if (data.allDocuments) documents = data.allDocuments;
        else if (Array.isArray(data)) documents = data;
        checkDocumentConflicts();
        renderDocuments();
      } else if (res.status === 401) {
        showLoginScreen();
      }
    } catch (e) {}
  }

  async function fetchLeaderState() {
    try {
      const res = await fetch('/api/election/leader', { credentials: 'include' });
      if (res.ok) {
        const data = await res.json();
        if (data) updateLeaderState(data);
      }
    } catch (e) {}
  }

  async function fetchUsers() {
    try {
      const res = await fetch('/api/users', { credentials: 'include' });
      if (res.ok) {
        allUsers = await res.json();
        if (loginUserId && allUsers.length > 0) {
          const currentVal = loginUserId.value;
          loginUserId.innerHTML = allUsers.map(u => `<option value="${u.id}">${u.id} (${u.name})</option>`).join('');
          if (currentVal && allUsers.some(u => u.id === currentVal)) {
            loginUserId.value = currentVal;
          }
        }
        populateChatUserSelect();
      }
    } catch (e) {}
  }

  function checkDocumentConflicts() {
    const conflicted = documents.find(d => d.conflict && d.conflictData);
    activeConflictDoc = conflicted ? { docId: conflicted.docId, versionA: conflicted.conflictData.versionA, versionB: conflicted.conflictData.versionB } : null;
    renderConflictBanner();
  }

  // ── Tab Switcher ──────────────────────────────────────────────────────────

  if (tabMyDocs) {
    tabMyDocs.addEventListener('click', () => {
      activeTab = 'mydocs';
      tabMyDocs.classList.add('active');
      tabSharedWithMe.classList.remove('active');
      renderDocuments();
    });
  }

  if (tabSharedWithMe) {
    tabSharedWithMe.addEventListener('click', () => {
      activeTab = 'shared';
      tabSharedWithMe.classList.add('active');
      tabMyDocs.classList.remove('active');
      renderDocuments();
    });
  }

  let searchDebounceTimer = null;
  if (docSearchInput) {
    docSearchInput.addEventListener('input', () => {
      if (searchDebounceTimer) clearTimeout(searchDebounceTimer);
      searchDebounceTimer = setTimeout(() => {
        renderDocuments();
      }, 80);
    });
  }

  if (refreshDocsBtn) {
    refreshDocsBtn.addEventListener('click', async () => {
      if (refreshDocsBtn.disabled) return;
      refreshDocsBtn.disabled = true;
      if (refreshDocsIcon) refreshDocsIcon.classList.add('fa-spin');
      appendLog('[DOCS] Refreshing document catalog from Master Coordinator...', 'log-info');
      try {
        await fetchDocuments();
      } finally {
        if (refreshDocsIcon) refreshDocsIcon.classList.remove('fa-spin');
        refreshDocsBtn.disabled = false;
      }
    });
  }

  // ── Render All Master ─────────────────────────────────────────────────────

  function renderAll() {
    renderTopology();
    renderDocuments();
    renderLogs();
    renderClusterBadges();
    renderLeaderBanner();
    renderClocks();
    renderTimeline();
    renderLocks();
    renderDeadlock();
    renderNodeRecovery();
    renderElectionLogs();
    renderConflictBanner();
  }

  function renderClusterBadges() {
    nodes.forEach(node => {
      const dot = document.getElementById(`dot-${node.id}`);
      if (dot) {
        dot.className = node.status === 'HEALTHY' ? 'pulse-dot' : 'pulse-dot dead';
      }
    });
  }

  // ── FA-2: Leader Election Banner & Controls ───────────────────────────────

  function updateLeaderState(state) {
    if (!state) return;
    const lId = state.leaderId || (state.currentLeader ? state.currentLeader.id : null);
    const lName = state.leaderName || (state.currentLeader ? state.currentLeader.name : null);
    const pId = state.previousLeaderId !== undefined ? state.previousLeaderId : (state.previousLeader ? (state.previousLeader.name || state.previousLeader.id) : null);
    const eStatus = state.electionStatus || 'COMPLETED';
    let cStatus = state.coordinatorStatus;
    if (!cStatus && state.isCoordinatorAlive !== undefined) {
      cStatus = state.isCoordinatorAlive ? 'HEALTHY' : 'FAILED';
    }

    leaderState = {
      leaderId: lId || leaderState.leaderId || 'Coordinator',
      leaderName: lName || leaderState.leaderName || 'Master Coordinator',
      previousLeaderId: pId !== null && pId !== undefined ? pId : (leaderState.previousLeaderId || 'None'),
      electionStatus: eStatus,
      coordinatorStatus: cStatus || leaderState.coordinatorStatus || 'HEALTHY',
      isCoordinatorAlive: state.isCoordinatorAlive !== undefined ? state.isCoordinatorAlive : (cStatus === 'HEALTHY')
    };

    renderLeaderBanner();
    scheduleTopologyRender();
    scheduleNodeRecoveryRender();
  }

  function renderLeaderBanner() {
    if (currentLeaderText) {
      currentLeaderText.textContent = leaderState.leaderName || leaderState.leaderId || 'Master Coordinator';
    }
    if (prevLeaderText) {
      prevLeaderText.textContent = leaderState.previousLeaderId || 'None';
    }
    if (electionStatusBadge) {
      electionStatusBadge.textContent = leaderState.electionStatus || 'COMPLETED';
      if (leaderState.electionStatus === 'ELECTING' || leaderState.electionStatus === 'IN_PROGRESS') {
        electionStatusBadge.style.color = 'var(--accent-yellow)';
        electionStatusBadge.style.background = 'rgba(217, 119, 6, 0.1)';
        electionStatusBadge.style.borderColor = 'rgba(217, 119, 6, 0.3)';
      } else {
        electionStatusBadge.style.color = 'var(--accent-green)';
        electionStatusBadge.style.background = 'rgba(5, 150, 105, 0.1)';
        electionStatusBadge.style.borderColor = 'rgba(5, 150, 105, 0.3)';
      }
    }
    if (coordinatorStatusBadge) {
      const isOnline = leaderState.coordinatorStatus === 'HEALTHY' || leaderState.isCoordinatorAlive === true;
      coordinatorStatusBadge.textContent = isOnline ? 'ONLINE (Rank 100)' : 'FAILED (Simulated)';
      coordinatorStatusBadge.style.color = isOnline ? 'var(--accent-cyan)' : 'var(--accent-red)';
      coordinatorStatusBadge.style.background = isOnline ? 'rgba(2, 132, 199, 0.1)' : 'rgba(239, 68, 68, 0.1)';
      coordinatorStatusBadge.style.borderColor = isOnline ? 'rgba(2, 132, 199, 0.3)' : 'rgba(239, 68, 68, 0.3)';
    }
  }

  if (triggerElectionBtn) {
    triggerElectionBtn.addEventListener('click', async () => {
      triggerElectionBtn.disabled = true;
      triggerElectionBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Electing...';
      try {
        appendLog('[ELECTION] Triggering Bully Algorithm election across storage nodes...', 'log-info');
        const res = await fetch('/api/election/start', { method: 'POST', credentials: 'include' });
        const data = await res.json();
        updateLeaderState(data);
        appendLog(`[ELECTION] Bully Election finished: Winner is ${data.leaderName || data.leaderId}`, 'log-success');
      } catch (e) {
        alert(`Election trigger failed: ${e.message}`);
      } finally {
        triggerElectionBtn.disabled = false;
        triggerElectionBtn.innerHTML = '<i class="fa-solid fa-bolt"></i> Trigger Bully Election';
      }
    });
  }

  if (killCoordinatorBtn) {
    killCoordinatorBtn.addEventListener('click', async () => {
      killCoordinatorBtn.disabled = true;
      killCoordinatorBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Stopping...';
      try {
        appendLog('[ELECTION] Simulating Coordinator Crash... Highest available node will be elected leader.', 'log-error');
        const res = await fetch('/api/election/kill-coordinator', { method: 'POST', credentials: 'include' });
        const data = await res.json();
        updateLeaderState(data);
        appendLog(`[ELECTION] Coordinator stopped! New leader elected: ${data.leaderName || data.leaderId}`, 'log-success');
      } catch (e) {
        alert(`Failed to simulate Coordinator crash: ${e.message}`);
      } finally {
        killCoordinatorBtn.disabled = false;
        killCoordinatorBtn.innerHTML = '<i class="fa-solid fa-power-off"></i> Kill Coordinator';
      }
    });
  }

  if (reviveCoordinatorBtn) {
    reviveCoordinatorBtn.addEventListener('click', async () => {
      reviveCoordinatorBtn.disabled = true;
      reviveCoordinatorBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Reviving...';
      try {
        appendLog('[ELECTION] Reviving Coordinator (Rank 100). Bully priority assertion will reclaim leadership.', 'log-info');
        const res = await fetch('/api/election/revive-coordinator', { method: 'POST', credentials: 'include' });
        const data = await res.json();
        updateLeaderState(data);
        appendLog(`[ELECTION] Coordinator revived: Reclaimed leadership as ${data.leaderName || data.leaderId}`, 'log-success');
      } catch (e) {
        alert(`Failed to revive Coordinator: ${e.message}`);
      } finally {
        reviveCoordinatorBtn.disabled = false;
        reviveCoordinatorBtn.innerHTML = '<i class="fa-solid fa-rotate"></i> Revive Coordinator';
      }
    });
  }

  function renderElectionLogs() {
    if (!electionLogTerminal) return;
    if (electionLogs.length === 0) {
      electionLogTerminal.innerHTML = '<div class="log-line log-info">System stable. Coordinator (Rank 100) is active leader.</div>';
      return;
    }
    electionLogTerminal.innerHTML = electionLogs.map(l => {
      const isWinner = l.includes('elected as leader') || l.includes('RECLAIMED');
      const isFail = l.includes('FAILED') || l.includes('unreachable');
      const cls = isWinner ? 'log-success' : (isFail ? 'log-error' : 'log-info');
      return `<div class="log-line ${cls}">${escapeHtml(l)}</div>`;
    }).join('');
    electionLogTerminal.scrollTop = electionLogTerminal.scrollHeight;
  }

  function appendElectionLog(logText) {
    electionLogs.unshift(logText);
    if (electionLogs.length > 50) electionLogs.pop();

    if (!electionLogTerminal) return;
    const isWinner = logText.includes('elected as leader') || logText.includes('RECLAIMED');
    const isFail = logText.includes('FAILED') || logText.includes('unreachable');
    const cls = isWinner ? 'log-success' : (isFail ? 'log-error' : 'log-info');

    if (electionLogs.length <= 1 && electionLogTerminal.children.length <= 1) {
      const firstChild = electionLogTerminal.firstElementChild;
      if (firstChild && firstChild.textContent.includes('System stable')) {
        electionLogTerminal.innerHTML = '';
      }
    }

    const div = document.createElement('div');
    div.className = `log-line ${cls}`;
    div.textContent = logText;
    if (electionLogTerminal.firstChild) {
      electionLogTerminal.insertBefore(div, electionLogTerminal.firstChild);
    } else {
      electionLogTerminal.appendChild(div);
    }
    if (electionLogTerminal.children.length > 50) {
      electionLogTerminal.removeChild(electionLogTerminal.lastChild);
    }
    electionLogTerminal.scrollTop = 0;
  }

  // ── FA-2: Lamport Logical Clocks & Timeline ───────────────────────────────

  function renderClocks() {
    if (clockCoordinator) clockCoordinator.textContent = `L=${logicalClocks.Coordinator || 0}`;
    if (clockNode1) clockNode1.textContent = `L=${logicalClocks.node_1 || 0}`;
    if (clockNode2) clockNode2.textContent = `L=${logicalClocks.node_2 || 0}`;
    if (clockNode3) clockNode3.textContent = `L=${logicalClocks.node_3 || 0}`;
    if (clockNode4) clockNode4.textContent = `L=${logicalClocks.node_4 || 0}`;
  }

  function renderTimeline() {
    if (!timelineTableBody) return;
    if (timelineEvents.length === 0) {
      timelineTableBody.innerHTML = `<tr><td colspan="6" style="text-align:center; color: var(--text-muted); padding: 1rem;">No distributed events captured yet. Upload or update a document to advance clocks!</td></tr>`;
      return;
    }
    timelineTableBody.innerHTML = timelineEvents.map(evt => {
      const opBadgeClass = getOpBadgeClass(evt.operation);
      return `
        <tr>
          <td style="font-weight: 700; color: var(--accent-cyan); font-family: monospace;">L=${evt.lamportTimestamp}</td>
          <td style="font-weight: 600;">${escapeHtml(evt.nodeOrUser)}</td>
          <td><span class="badge ${opBadgeClass}">${escapeHtml(evt.operation)}</span></td>
          <td style="font-family: monospace; font-size: 0.8rem;">${escapeHtml(evt.document || '—')}</td>
          <td style="font-size: 0.8rem; color: #475569;">${escapeHtml(evt.eventDetails)}</td>
          <td style="font-size: 0.75rem; color: var(--text-muted);">${formatTime(evt.timestamp)}</td>
        </tr>
      `;
    }).join('');
  }

  function prependTimelineRow(evt) {
    if (!timelineTableBody) return;
    if (timelineEvents.length <= 1) {
      timelineTableBody.innerHTML = '';
    }
    const opBadgeClass = getOpBadgeClass(evt.operation);
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td style="font-weight: 700; color: var(--accent-cyan); font-family: monospace;">L=${evt.lamportTimestamp}</td>
      <td style="font-weight: 600;">${escapeHtml(evt.nodeOrUser)}</td>
      <td><span class="badge ${opBadgeClass}">${escapeHtml(evt.operation)}</span></td>
      <td style="font-family: monospace; font-size: 0.8rem;">${escapeHtml(evt.document || '—')}</td>
      <td style="font-size: 0.8rem; color: #475569;">${escapeHtml(evt.eventDetails)}</td>
      <td style="font-size: 0.75rem; color: var(--text-muted);">${formatTime(evt.timestamp)}</td>
    `;
    timelineTableBody.insertBefore(tr, timelineTableBody.firstChild);
    if (timelineTableBody.children.length > 60) {
      timelineTableBody.removeChild(timelineTableBody.lastChild);
    }
  }

  function getOpBadgeClass(op) {
    if (!op) return 'badge-cyan';
    if (op.includes('UPLOAD') || op.includes('CREATE')) return 'badge-green';
    if (op.includes('UPDATE') || op.includes('VERSION')) return 'badge-purple';
    if (op.includes('LOCK')) return 'badge-yellow';
    if (op.includes('FAIL') || op.includes('DEADLOCK') || op.includes('DELETE')) return 'badge-red';
    if (op.includes('RECOVERY') || op.includes('SYNC')) return 'badge-green';
    return 'badge-cyan';
  }

  function formatTime(isoStr) {
    if (!isoStr) return '';
    try {
      const d = new Date(isoStr);
      return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    } catch (e) {
      return '';
    }
  }

  // ── FA-2: Distributed Mutual Exclusion (Locks) ────────────────────────────

  function updateSingleDocumentLockUI(docId) {
    if (!docTableBody || !docId) return;
    const row = docTableBody.querySelector(`tr[data-docid="${docId}"]`);
    if (!row) return;
    const lockCell = row.querySelector('.doc-lock-cell');
    if (!lockCell) return;

    const lock = activeLocks.find(l => l.docId === docId);
    if (lock) {
      lockCell.innerHTML = `<span class="lock-pill locked"><i class="fa-solid fa-lock"></i> ${escapeHtml(lock.ownerId || lock.lockedBy)}</span>`;
    } else {
      lockCell.innerHTML = `<span class="lock-pill unlocked"><i class="fa-solid fa-lock-open"></i> FREE</span>`;
    }
  }

  function renderLocks() {
    if (activeLocksCount) {
      activeLocksCount.textContent = `${activeLocks.length} Active`;
    }
    if (!activeLocksTableBody) return;

    if (activeLocks.length === 0) {
      activeLocksTableBody.innerHTML = `<tr><td colspan="4" style="text-align:center; color: var(--text-muted); padding: 0.8rem; font-size: 0.8rem;">No active distributed locks. All documents are available for editing.</td></tr>`;
      return;
    }

    const now = Date.now();
    activeLocksTableBody.innerHTML = activeLocks.map(l => {
      const remainingSec = Math.max(0, Math.round((new Date(l.expiresAt).getTime() - now) / 1000));
      const isOwner = currentUser && (l.ownerId === currentUser.id || l.lockedBy === currentUser.id);
      return `
        <tr>
          <td style="font-weight: 600; font-size: 0.82rem;">${escapeHtml(l.docName || l.docId)}</td>
          <td><span class="badge" style="background: rgba(139, 92, 246, 0.1); color: var(--accent-purple); font-size: 0.75rem;"><i class="fa-solid fa-user"></i> ${escapeHtml(l.ownerId || l.lockedBy)}</span></td>
          <td style="font-size: 0.8rem; color: ${remainingSec < 10 ? '#ef4444' : '#0284c7'}; font-weight: 600;">${remainingSec}s</td>
          <td>
            ${isOwner ? `
              <button class="btn btn-red btn-release-lock" data-docid="${l.docId}" style="padding: 0.2rem 0.5rem; font-size: 0.72rem;">
                <i class="fa-solid fa-unlock"></i> Release
              </button>
            ` : `<span style="font-size:0.75rem; color:var(--text-muted);">Protected</span>`}
          </td>
        </tr>
      `;
    }).join('');
  }

  // Delegated click listener for Active Locks Table (prevents per-render listener bindings)
  if (activeLocksTableBody) {
    activeLocksTableBody.addEventListener('click', async (e) => {
      const btn = e.target.closest('.btn-release-lock');
      if (!btn) return;
      const docId = btn.getAttribute('data-docid');
      if (!docId || btn.disabled) return;
      btn.disabled = true;
      const origHtml = btn.innerHTML;
      btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Releasing...';
      try {
        await releaseLock(docId);
      } finally {
        if (btn && btn.parentNode) {
          btn.disabled = false;
          btn.innerHTML = origHtml;
        }
      }
    });
  }

  async function acquireLock(docId) {
    if (!currentUser) {
      alert('Please sign in first to acquire a document lock.');
      return false;
    }
    try {
      const res = await fetch('/api/locks/acquire', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({ docId, userId: currentUser.id })
      });
      const data = await res.json();
      if (res.ok) {
        if (data.lock) {
          const idx = activeLocks.findIndex(l => l.docId === data.lock.docId);
          if (idx >= 0) activeLocks[idx] = data.lock;
          else activeLocks.push(data.lock);
        }
        appendLog(`[LOCK] SUCCESS: Acquired exclusive lock on '${data.lock ? (data.lock.docName || docId) : docId}' by ${currentUser.name} (Lease: 60s)`, 'log-success');
        renderLocks();
        updateSingleDocumentLockUI(docId);
        updateEditModalLockState();
        return true;
      } else {
        if (data.lock) {
          const idx = activeLocks.findIndex(l => l.docId === data.lock.docId);
          if (idx >= 0) activeLocks[idx] = data.lock;
          else activeLocks.push(data.lock);
          renderLocks();
          updateSingleDocumentLockUI(docId);
          updateEditModalLockState();
        }
        alert(`⛔ ACCESS DENIED / WAITING FOR LOCK (HTTP ${res.status})\n\n${data.error || 'Document is currently locked by another user.'}`);
        appendLog(`[LOCK] Lock request DENIED for ${currentUser.name} on '${docId}': ${data.error}`, 'log-error');
        return false;
      }
    } catch (e) {
      alert(`Lock error: ${e.message}`);
      return false;
    }
  }

  async function releaseLock(docId) {
    if (!currentUser) return false;
    try {
      const res = await fetch('/api/locks/release', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({ docId, userId: currentUser.id })
      });
      const data = await res.json();
      if (res.ok) {
        activeLocks = activeLocks.filter(l => l.docId !== docId);
        appendLog(`[LOCK] SUCCESS: Released lock on '${docId}' by ${currentUser.name}`, 'log-info');
        renderLocks();
        updateSingleDocumentLockUI(docId);
        updateEditModalLockState();
        return true;
      } else {
        alert(`Release lock failed: ${data.error}`);
        return false;
      }
    } catch (e) {
      alert(`Release lock error: ${e.message}`);
      return false;
    }
  }

  // ── FA-2: Deadlock Detection & Resolution ─────────────────────────────────

  function renderDeadlock() {
    if (deadlockStatusBadge) {
      if (deadlockState.isDeadlocked) {
        deadlockStatusBadge.textContent = 'DEADLOCK DETECTED';
        deadlockStatusBadge.style.color = '#ef4444';
        deadlockStatusBadge.style.background = 'rgba(239, 68, 68, 0.15)';
      } else {
        deadlockStatusBadge.textContent = 'NO DEADLOCK';
        deadlockStatusBadge.style.color = 'var(--accent-green)';
        deadlockStatusBadge.style.background = 'rgba(5, 150, 105, 0.1)';
      }
    }

    if (!wfgContainer) return;
    if (!deadlockState.isDeadlocked) {
      wfgContainer.innerHTML = `
        <div style="color: var(--text-muted); font-size: 0.82rem; text-align: center;">
          <i class="fa-solid fa-circle-check" style="color: var(--accent-green);"></i> No cyclic wait dependencies detected in Wait-For Graph.
        </div>
      `;
      return;
    }

    const cycleNodes = deadlockState.cycle || [];
    const pills = cycleNodes.map((u, i) => {
      const nextU = cycleNodes[(i + 1) % cycleNodes.length];
      return `
        <div class="wfg-edge-pill">
          <span style="font-weight: 700; color: var(--accent-purple);">${escapeHtml(u)}</span>
          <i class="fa-solid fa-arrow-right" style="color: #ef4444;"></i>
          <span style="font-weight: 700; color: var(--accent-cyan);">${escapeHtml(nextU)}</span>
        </div>
      `;
    }).join(' ');

    wfgContainer.innerHTML = `
      <div style="width: 100%;">
        <div style="color: #ef4444; font-weight: 700; font-size: 0.82rem; margin-bottom: 0.5rem;">
          <i class="fa-solid fa-triangle-exclamation"></i> Circular Wait Cycle Detected in Wait-For Graph:
        </div>
        <div style="display: flex; gap: 0.5rem; flex-wrap: wrap; justify-content: center;">
          ${pills}
        </div>
        <div style="font-size: 0.75rem; color: #64748b; margin-top: 0.4rem; text-align: center;">
          Policy: Cycle resolution via youngest lock request abort.
        </div>
      </div>
    `;
  }

  if (simulateDeadlockBtn) {
    simulateDeadlockBtn.addEventListener('click', async () => {
      if (simulateDeadlockBtn.disabled) return;
      simulateDeadlockBtn.disabled = true;
      const origHtml = simulateDeadlockBtn.innerHTML;
      simulateDeadlockBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Simulating...';
      try {
        appendLog('[DEADLOCK] Triggering cross-document circular wait simulation: User A holds Doc X & waits for Doc Y; User B holds Doc Y & waits for Doc X...', 'log-info');
        const res = await fetch('/api/deadlock/simulate', { method: 'POST', credentials: 'include' });
        const data = await res.json();
        appendLog(`[DEADLOCK] Simulation completed: ${data.message}`, 'log-error');
      } catch (e) {
        alert(`Deadlock simulation failed: ${e.message}`);
      } finally {
        simulateDeadlockBtn.disabled = false;
        simulateDeadlockBtn.innerHTML = origHtml;
      }
    });
  }

  if (resolveDeadlockBtn) {
    resolveDeadlockBtn.addEventListener('click', async () => {
      if (resolveDeadlockBtn.disabled) return;
      resolveDeadlockBtn.disabled = true;
      const origHtml = resolveDeadlockBtn.innerHTML;
      resolveDeadlockBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Resolving...';
      try {
        appendLog('[DEADLOCK] Executing cycle break: aborting youngest waiting lock request...', 'log-info');
        const res = await fetch('/api/deadlock/resolve', { method: 'POST', credentials: 'include' });
        const data = await res.json();
        deadlockState = { isDeadlocked: false, cycle: [], wfg: {}, detectedAt: null };
        renderDeadlock();
        appendLog(`[DEADLOCK] ${data.message}`, 'log-success');
      } catch (e) {
        alert(`Deadlock resolution failed: ${e.message}`);
      } finally {
        resolveDeadlockBtn.disabled = false;
        resolveDeadlockBtn.innerHTML = origHtml;
      }
    });
  }

  // ── FA-2: Node Recovery & Re-Synchronization ──────────────────────────────

  function renderNodeRecovery() {
    if (!nodeRecoveryCards) return;
    const storageNodes = nodes.filter(n => n.id.startsWith('node_'));

    nodeRecoveryCards.innerHTML = storageNodes.map(node => {
      const isFailed = node.status === 'FAILED' || node.status === 'DEAD';
      const isSyncing = node.status === 'RECOVERING' || node.status === 'SYNCING';
      const isLeader = leaderState.leaderId === node.id || (leaderState.leaderName && (leaderState.leaderName.includes(node.name) || leaderState.leaderName.includes(node.id)));
      const statusColor = isFailed ? '#ef4444' : (isSyncing ? '#d97706' : (isLeader ? '#f59e0b' : '#059669'));
      const statusBg = isFailed ? 'rgba(239,68,68,0.1)' : (isSyncing ? 'rgba(217,119,6,0.1)' : (isLeader ? 'rgba(245,158,11,0.15)' : 'rgba(5,150,105,0.1)'));
      const statusText = isLeader ? '👑 LEADER' : (node.status || 'HEALTHY');
      const usedKb = Math.round((node.storageUsedBytes || 0) / 1024);

      return `
        <div class="recovery-node-card" style="border-left: 3px solid ${statusColor}; ${isLeader ? 'box-shadow: 0 0 10px rgba(245,158,11,0.25);' : ''}">
          <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom: 0.3rem;">
            <span style="font-weight: 700; font-size: 0.8rem; color: #0f172a;">${escapeHtml(node.name || node.id)}</span>
            <span class="badge" style="font-size: 0.68rem; color: ${isLeader ? '#b45309' : statusColor}; background: ${statusBg}; font-weight:700; ${isLeader ? 'border: 1px solid #f59e0b;' : ''}">${statusText}</span>
          </div>
          <div style="font-size: 0.72rem; color: var(--text-muted); margin-bottom: 0.4rem;">
            Storage: ${usedKb} KB | Lamport: L=${logicalClocks[node.id] || 0}
          </div>
          <button class="btn btn-cyan btn-sync-single" data-nodeid="${node.id}" style="width: 100%; padding: 0.2rem 0.4rem; font-size: 0.7rem;">
            <i class="fa-solid fa-arrows-rotate"></i> Synchronize Missing Chunks
          </button>
        </div>
      `;
    }).join('');
  }

  // Event delegation for single node synchronization buttons
  if (nodeRecoveryCards) {
    nodeRecoveryCards.addEventListener('click', async (e) => {
      const btn = e.target.closest('.btn-sync-single');
      if (btn && !btn.disabled) {
        const nodeId = btn.getAttribute('data-nodeid');
        if (!nodeId) return;
        btn.disabled = true;
        const origHtml = btn.innerHTML;
        btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Syncing...';
        try {
          appendLog(`[RECOVERY] Initiating peer re-synchronization for ${nodeId}...`, 'log-info');
          const res = await fetch(`/api/sync/node/${nodeId}`, { method: 'POST', credentials: 'include' });
          const data = await res.json();
          appendLog(`[SYNC] Node ${nodeId} sync complete: ${data.message}`, 'log-success');
        } catch (err) {
          alert(`Sync failed: ${err.message}`);
        } finally {
          btn.disabled = false;
          btn.innerHTML = origHtml;
        }
      }
    });
  }

  if (manualSyncAllBtn) {
    manualSyncAllBtn.addEventListener('click', async () => {
      if (manualSyncAllBtn.disabled) return;
      manualSyncAllBtn.disabled = true;
      const origHtml = manualSyncAllBtn.innerHTML;
      manualSyncAllBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Synchronizing Cluster...';
      try {
        appendLog('[SYNC] Requesting full cluster-wide missing chunk synchronization...', 'log-info');
        const res = await fetch('/api/sync/all', { method: 'POST', credentials: 'include' });
        const data = await res.json();
        appendLog(`[SYNC] Cluster synchronization complete: ${data.message}`, 'log-success');
      } catch (e) {
        alert(`Cluster sync failed: ${e.message}`);
      } finally {
        manualSyncAllBtn.disabled = false;
        manualSyncAllBtn.innerHTML = origHtml;
      }
    });
  }

  // ── FA-2: Vector Clock Conflict Alert Banner ──────────────────────────────

  function renderConflictBanner() {
    if (!conflictAlertBox) return;
    if (activeConflictDoc) {
      conflictAlertBox.style.display = 'flex';
      if (conflictAlertSummary) {
        conflictAlertSummary.textContent = `Document '${activeConflictDoc.docId}' has concurrent branch updates. Select a resolution branch or merge changes.`;
      }
    } else {
      conflictAlertBox.style.display = 'none';
    }
  }

  if (openConflictResolveBtn) {
    openConflictResolveBtn.addEventListener('click', () => {
      if (activeConflictDoc) {
        openConflictModal(activeConflictDoc.docId);
      }
    });
  }

  // ── SVG Topology Visualizer ───────────────────────────────────────────────

  function renderTopology() {
    const svg = document.getElementById('topologySvg');
    if (!svg) return;
    const panel = document.getElementById('topologyPanel');
    if (panel && panel.classList.contains('panel-collapsed')) return;
    if (svg.offsetParent === null && svg.clientWidth === 0) return;
    svg.innerHTML = '';

    const width  = svg.clientWidth  || 700;
    const height = Math.max(svg.clientHeight || 380, 360);
    svg.setAttribute('height', height);

    // Setup SVG Filters
    const defs = document.createElementNS('http://www.w3.org/2000/svg', 'defs');
    defs.innerHTML = `
      <filter id="leaderGlow" x="-30%" y="-30%" width="160%" height="160%">
        <feDropShadow dx="0" dy="2" stdDeviation="4" flood-color="#f59e0b" flood-opacity="0.45" />
      </filter>
    `;
    svg.appendChild(defs);

    const coordPos = { x: width / 2, y: 78 };

    const labelMap = {
      node_1: 'Node 1 (Alpha)',
      node_2: 'Node 2 (Beta)',
      node_3: 'Node 3 (Gamma)',
      node_4: 'Node 4 (Delta)',
      node_5: 'Node 5 (Epsilon)'
    };

    const defaultStorageNodes = [
      { id: 'node_1', name: 'Storage Node Alpha', status: 'HEALTHY' },
      { id: 'node_2', name: 'Storage Node Beta',  status: 'HEALTHY' },
      { id: 'node_3', name: 'Storage Node Gamma', status: 'HEALTHY' },
      { id: 'node_4', name: 'Storage Node Delta', status: 'HEALTHY' }
    ];

    const effectiveNodes = (nodes && nodes.length > 0) ? nodes : defaultStorageNodes;
    const storageNodesList = effectiveNodes.map((n, i, arr) => {
      const fraction = arr.length <= 1 ? 0.5 : i / (arr.length - 1);
      const margin   = width < 480 ? 0.14 : 0.12;
      const x = width * (margin + fraction * (1 - 2 * margin));
      return {
        x,
        y: 265,
        id: n.id,
        name: n.name,
        label: labelMap[n.id] || n.name || n.id
      };
    });

    // Determine Leader and Coordinator States
    const activeLeaderId = leaderState.leaderId;
    const activeLeaderName = leaderState.leaderName || '';
    const isCoordOnline = leaderState.coordinatorStatus === 'HEALTHY' || leaderState.isCoordinatorAlive === true;
    const isCoordFailed = !isCoordOnline;

    const isCoordLeader = isCoordOnline && (
      activeLeaderId === 'Coordinator' ||
      activeLeaderId === 'coordinator' ||
      activeLeaderName.includes('Coordinator') ||
      !activeLeaderId
    );

    const isStorageNodeLeader = (nodeId) => {
      if (isCoordLeader) return false;
      if (activeLeaderId === nodeId) return true;
      if (activeLeaderName && (activeLeaderName.toLowerCase().includes(nodeId.replace('_', ' ')) || activeLeaderName.toLowerCase().includes(nodeId))) return true;
      const nd = effectiveNodes.find(n => n.id === nodeId);
      if (nd && activeLeaderName && (activeLeaderName.includes(nd.name) || activeLeaderName.includes(nd.id))) return true;
      return false;
    };

    // 1. Connection Lines from Coordinator -> Storage Nodes
    storageNodesList.forEach(st => {
      const nodeData = effectiveNodes.find(n => n.id === st.id);
      const isNodeFailed = nodeData && (nodeData.status === 'FAILED' || nodeData.status === 'DEAD');

      const line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
      line.setAttribute('x1', coordPos.x);
      line.setAttribute('y1', coordPos.y + 26);
      line.setAttribute('x2', st.x);
      line.setAttribute('y2', st.y - 26);

      if (isCoordFailed) {
        line.setAttribute('stroke', '#ef4444');
        line.setAttribute('stroke-width', '1.5');
        line.setAttribute('stroke-dasharray', '5,5');
        line.setAttribute('stroke-opacity', '0.35');
      } else if (isNodeFailed) {
        line.setAttribute('stroke', '#ef4444');
        line.setAttribute('stroke-width', '1.5');
        line.setAttribute('stroke-dasharray', '5,5');
      } else {
        line.setAttribute('stroke', 'rgba(2, 132, 199, 0.45)');
        line.setAttribute('stroke-width', '2');
      }
      svg.appendChild(line);
    });

    // 2. Dynamic Leadership Coordination Lines from Elected Storage Node Leader
    const electedLeaderSt = storageNodesList.find(st => isStorageNodeLeader(st.id));
    if (electedLeaderSt) {
      storageNodesList.forEach(otherSt => {
        if (otherSt.id === electedLeaderSt.id) return;
        const otherData = effectiveNodes.find(n => n.id === otherSt.id);
        const otherFailed = otherData && (otherData.status === 'FAILED' || otherData.status === 'DEAD');
        if (otherFailed) return;

        const leaderLine = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        const midX = (electedLeaderSt.x + otherSt.x) / 2;
        const curveY = 175;
        leaderLine.setAttribute('d', `M ${electedLeaderSt.x} ${electedLeaderSt.y - 24} Q ${midX} ${curveY} ${otherSt.x} ${otherSt.y - 24}`);
        leaderLine.setAttribute('fill', 'none');
        leaderLine.setAttribute('stroke', '#8b5cf6');
        leaderLine.setAttribute('stroke-width', '2.5');
        leaderLine.setAttribute('stroke-dasharray', '5,3');
        leaderLine.setAttribute('filter', 'url(#leaderGlow)');
        svg.appendChild(leaderLine);
      });
    }

    // 3. P2P Inter-node Arc Lines
    for (let i = 0; i < storageNodesList.length - 1; i++) {
      const a = storageNodesList[i];
      const b = storageNodesList[i + 1];
      const arc = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      arc.setAttribute('d', `M ${a.x + 20} ${a.y} Q ${(a.x + b.x) / 2} ${a.y + 40} ${b.x - 20} ${b.y}`);
      arc.setAttribute('fill', 'none');
      arc.setAttribute('stroke', 'rgba(124, 58, 237, 0.25)');
      arc.setAttribute('stroke-width', '1.5');
      arc.setAttribute('stroke-dasharray', '4,4');
      svg.appendChild(arc);
    }

    // 4. Render Master Coordinator Node
    let coordTitle = 'Master Coordinator';
    let coordSubtitle = '';
    let coordColor = '#0284c7';
    let coordRank = 'Rank 100';

    if (isCoordFailed) {
      coordColor = '#ef4444';
      coordSubtitle = 'STATUS: FAILED (Simulated)';
      coordRank = 'Rank 100 (Dead)';
    } else if (isCoordLeader) {
      coordColor = '#f59e0b';
      coordSubtitle = '👑 ACTIVE LEADER | Rank 100 (Port 3000)';
      coordRank = 'Rank 100 (Leader)';
    } else {
      coordColor = '#0284c7';
      coordSubtitle = 'ONLINE (Standby) | Rank 100';
      coordRank = 'Rank 100';
    }

    drawNodeSvg(
      svg,
      coordPos.x,
      coordPos.y,
      coordTitle,
      coordSubtitle,
      coordColor,
      isCoordFailed,
      isCoordLeader,
      coordRank,
      width
    );

    // 5. Render Storage Nodes
    const ranks = { node_4: 4, node_3: 3, node_2: 2, node_1: 1 };

    storageNodesList.forEach(st => {
      const nodeData = effectiveNodes.find(n => n.id === st.id);
      const isFailed = nodeData && (nodeData.status === 'FAILED' || nodeData.status === 'DEAD');
      const isSyncing = nodeData && (nodeData.status === 'RECOVERING' || nodeData.status === 'SYNCING');
      const isLeader = isStorageNodeLeader(st.id);
      const usedKb = nodeData ? Math.round((nodeData.storageUsedBytes || 0) / 1024) : 0;
      const r = ranks[st.id] || 0;

      let color = '#0284c7';
      let subtitle = `HEALTHY | ${usedKb} KB`;
      let rankText = `Rank ${r}`;

      if (isFailed) {
        color = '#ef4444';
        subtitle = 'STATUS: FAILED';
        rankText = `Rank ${r} (Dead)`;
      } else if (isSyncing) {
        color = '#d97706';
        subtitle = 'STATUS: SYNCING';
        rankText = `Rank ${r}`;
      } else if (isLeader) {
        color = '#f59e0b';
        subtitle = `👑 ELECTED LEADER | ${usedKb} KB`;
        rankText = `Rank ${r} (Leader)`;
      }

      drawNodeSvg(
        svg,
        st.x,
        st.y,
        st.label,
        subtitle,
        color,
        isFailed,
        isLeader,
        rankText,
        width
      );
    });

    // 6. Update Topology Panel Header Badge
    const panelBadge = document.querySelector('#topologyPanel .panel-header-badge');
    if (panelBadge) {
      if (isCoordLeader) {
        panelBadge.textContent = '👑 Leader: Master Coordinator (Rank 100)';
        panelBadge.style.color = 'var(--accent-yellow)';
        panelBadge.style.background = 'rgba(217, 119, 6, 0.12)';
        panelBadge.style.borderColor = 'rgba(217, 119, 6, 0.35)';
      } else {
        const leaderSt = storageNodesList.find(st => isStorageNodeLeader(st.id));
        const leaderName = leaderSt ? leaderSt.label : (leaderState.leaderName || 'Storage Node');
        panelBadge.textContent = `👑 Proxy Leader: ${leaderName}`;
        panelBadge.style.color = 'var(--accent-purple)';
        panelBadge.style.background = 'rgba(124, 58, 237, 0.12)';
        panelBadge.style.borderColor = 'rgba(124, 58, 237, 0.35)';
      }
    }
  }

  function drawNodeSvg(svg, x, y, title, subtitle, color, isFailed, isLeader, rankText, canvasWidth) {
    const group = document.createElementNS('http://www.w3.org/2000/svg', 'g');
    if (isLeader) {
      group.setAttribute('class', 'leader-node-group');
    }

    const wRef = canvasWidth || 450;

    // 1. Leader or Failure Badge Pill Above Node
    if (isLeader) {
      const pillW = Math.min(104, Math.max(76, wRef * 0.22));
      const pillGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');
      pillGroup.setAttribute('class', 'svg-leader-crown');

      const pillRect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
      pillRect.setAttribute('x', x - (pillW / 2));
      pillRect.setAttribute('y', y - 56);
      pillRect.setAttribute('width', pillW);
      pillRect.setAttribute('height', '24');
      pillRect.setAttribute('rx', '12');
      pillRect.setAttribute('ry', '12');
      pillRect.setAttribute('fill', '#fffbeb');
      pillRect.setAttribute('stroke', '#f59e0b');
      pillRect.setAttribute('stroke-width', '2');
      pillRect.setAttribute('filter', 'url(#leaderGlow)');
      pillGroup.appendChild(pillRect);

      const pillText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
      pillText.setAttribute('x', x);
      pillText.setAttribute('y', y - 40);
      pillText.setAttribute('text-anchor', 'middle');
      pillText.setAttribute('font-size', '11');
      pillText.setAttribute('font-weight', '800');
      pillText.setAttribute('fill', '#b45309');
      pillText.setAttribute('font-family', "'Outfit', sans-serif");
      pillText.textContent = '👑 LEADER';
      pillGroup.appendChild(pillText);

      group.appendChild(pillGroup);

      // Outer Pulsing Aura Ring
      const aura = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
      aura.setAttribute('cx', x);
      aura.setAttribute('cy', y);
      aura.setAttribute('r', '34');
      aura.setAttribute('fill', 'rgba(245, 158, 11, 0.12)');
      aura.setAttribute('stroke', '#f59e0b');
      aura.setAttribute('stroke-width', '2');
      aura.setAttribute('stroke-dasharray', '4,3');
      aura.setAttribute('class', 'svg-leader-pulse');
      group.appendChild(aura);

    } else if (isFailed) {
      const failW = Math.min(112, Math.max(86, wRef * 0.24));
      const failGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');

      const failRect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
      failRect.setAttribute('x', x - (failW / 2));
      failRect.setAttribute('y', y - 56);
      failRect.setAttribute('width', failW);
      failRect.setAttribute('height', '24');
      failRect.setAttribute('rx', '12');
      failRect.setAttribute('ry', '12');
      failRect.setAttribute('fill', '#fef2f2');
      failRect.setAttribute('stroke', '#ef4444');
      failRect.setAttribute('stroke-width', '1.5');
      failGroup.appendChild(failRect);

      const failText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
      failText.setAttribute('x', x);
      failText.setAttribute('y', y - 40);
      failText.setAttribute('text-anchor', 'middle');
      failText.setAttribute('font-size', '10');
      failText.setAttribute('font-weight', '700');
      failText.setAttribute('fill', '#dc2626');
      failText.setAttribute('font-family', "'Outfit', sans-serif");
      failText.textContent = '❌ FAILED (OFFLINE)';
      failGroup.appendChild(failText);

      group.appendChild(failGroup);
    }

    // 2. Main Node Circle
    const circle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
    circle.setAttribute('cx', x);
    circle.setAttribute('cy', y);
    circle.setAttribute('r', isLeader ? '27' : '24');
    circle.setAttribute('fill', isLeader ? '#f59e0b' : color);
    circle.setAttribute('fill-opacity', isLeader ? '0.22' : (isFailed ? '0.08' : '0.14'));
    circle.setAttribute('stroke', isLeader ? '#f59e0b' : color);
    circle.setAttribute('stroke-width', isLeader ? '3' : '2');
    if (isFailed) circle.setAttribute('stroke-dasharray', '4,4');
    group.appendChild(circle);

    // 3. Center Core Dot
    const dot = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
    dot.setAttribute('cx', x);
    dot.setAttribute('cy', y);
    dot.setAttribute('r', isLeader ? '9' : '8');
    dot.setAttribute('fill', isLeader ? '#f59e0b' : color);
    group.appendChild(dot);

    // 4. Title Text
    const textTitle = document.createElementNS('http://www.w3.org/2000/svg', 'text');
    textTitle.setAttribute('x', x);
    textTitle.setAttribute('y', y + 43);
    textTitle.setAttribute('text-anchor', 'middle');
    textTitle.setAttribute('fill', isFailed ? '#64748b' : '#0f172a');
    textTitle.setAttribute('font-size', '12');
    textTitle.setAttribute('font-weight', isLeader ? '700' : '600');
    textTitle.setAttribute('font-family', "'Outfit', sans-serif");
    textTitle.textContent = title;
    group.appendChild(textTitle);

    // 5. Subtitle Text
    const textSub = document.createElementNS('http://www.w3.org/2000/svg', 'text');
    textSub.setAttribute('x', x);
    textSub.setAttribute('y', y + 58);
    textSub.setAttribute('text-anchor', 'middle');
    textSub.setAttribute('fill', isLeader ? '#b45309' : (isFailed ? '#ef4444' : '#64748b'));
    textSub.setAttribute('font-size', '10');
    textSub.setAttribute('font-weight', isLeader ? '700' : '500');
    textSub.setAttribute('font-family', "'Outfit', sans-serif");
    textSub.textContent = subtitle;
    group.appendChild(textSub);

    // 6. Rank Badge Chip Below Node
    if (rankText) {
      const rankGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');
      const badgeW = isLeader ? 68 : 58;
      const rankRect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
      rankRect.setAttribute('x', x - (badgeW / 2));
      rankRect.setAttribute('y', y + 67);
      rankRect.setAttribute('width', badgeW);
      rankRect.setAttribute('height', '16');
      rankRect.setAttribute('rx', '8');
      rankRect.setAttribute('ry', '8');
      rankRect.setAttribute('fill', isLeader ? 'rgba(245, 158, 11, 0.15)' : (isFailed ? 'rgba(239, 68, 68, 0.1)' : 'rgba(2, 132, 199, 0.08)'));
      rankRect.setAttribute('stroke', isLeader ? 'rgba(245, 158, 11, 0.5)' : (isFailed ? 'rgba(239, 68, 68, 0.3)' : 'rgba(2, 132, 199, 0.25)'));
      rankRect.setAttribute('stroke-width', '1');
      rankGroup.appendChild(rankRect);

      const rankT = document.createElementNS('http://www.w3.org/2000/svg', 'text');
      rankT.setAttribute('x', x);
      rankT.setAttribute('y', y + 79);
      rankT.setAttribute('text-anchor', 'middle');
      rankT.setAttribute('fill', isLeader ? '#b45309' : (isFailed ? '#dc2626' : '#0284c7'));
      rankT.setAttribute('font-size', '9');
      rankT.setAttribute('font-weight', '700');
      rankT.setAttribute('font-family', "'Outfit', sans-serif");
      rankT.textContent = rankText;
      rankGroup.appendChild(rankT);

      group.appendChild(rankGroup);
    }

    svg.appendChild(group);
  }

  // ── Render Document Catalog (6 Columns: FA-1 + FA-2) ─────────────────────

  function renderDocuments() {
    if (!docTableBody || !currentUser) return;

    const userId  = currentUser.id;
    const myDocs  = documents.filter(d => d.ownerId === userId);
    const shared  = documents.filter(d => d.ownerId !== userId && d.shares && d.shares[userId]);

    if (myDocsCount) myDocsCount.textContent = myDocs.length;
    if (sharedCount)  sharedCount.textContent = shared.length;

    const searchFilter = docSearchInput ? docSearchInput.value.trim().toLowerCase() : '';
    let displayList = activeTab === 'mydocs' ? myDocs : shared;
    if (searchFilter) {
      displayList = displayList.filter(d => (d.name && d.name.toLowerCase().includes(searchFilter)) || (d.docId && d.docId.toLowerCase().includes(searchFilter)));
    }

    if (displayList.length === 0) {
      const msg = searchFilter
        ? `No documents matching "${searchFilter}".`
        : (activeTab === 'mydocs'
            ? `No documents owned by ${currentUser.name}. Drag & drop a file to upload!`
            : `No documents shared with ${currentUser.name} yet.`);
      docTableBody.innerHTML = `<tr><td colspan="6" style="text-align:center; color: var(--text-muted); padding: 1.5rem;">${msg}</td></tr>`;
      return;
    }

    const rowsHtml = displayList.map(doc => {
      const isOwner        = doc.ownerId === userId;
      const sharePermission = doc.shares ? doc.shares[userId] : null;

      const accessBadge = isOwner
        ? `<span class="perm-tag owner">OWNER</span>`
        : `<span class="perm-tag ${sharePermission === 'WRITE' ? 'write' : 'read'}">${sharePermission || 'SHARED'}</span>`;

      const ownerLabel = allUsers.find(u => u.id === doc.ownerId)?.name || doc.ownerId || 'Unknown';

      // Lock status badge
      const lock = activeLocks.find(l => l.docId === doc.docId);
      let lockBadgeHtml = '';
      if (lock) {
        lockBadgeHtml = `<span class="lock-pill locked"><i class="fa-solid fa-lock"></i> ${escapeHtml(lock.ownerId || lock.lockedBy)}</span>`;
      } else {
        lockBadgeHtml = `<span class="lock-pill unlocked"><i class="fa-solid fa-lock-open"></i> FREE</span>`;
      }

      // Version & Vector Clock badge
      const vectorClockStr = JSON.stringify(doc.vectorClock || {});
      const conflictBadge = doc.conflict
        ? `<div style="margin-top: 0.25rem;"><span class="badge" style="background:#ef4444; color:#fff; font-size:0.68rem; animation: pulse 1s infinite;"><i class="fa-solid fa-triangle-exclamation"></i> CONFLICT</span></div>`
        : '';

      const replicasHtml = (doc.replicaNodeIds || []).map(nodeId => {
        const nodeData = nodes.find(n => n.id === nodeId);
        const isFailed = nodeData && (nodeData.status === 'FAILED' || nodeData.status === 'DEAD');
        return `<span class="replica-tag" style="${isFailed ? 'background: rgba(239, 68, 68, 0.2); color: #fca5a5; border-color: rgba(239,68,68,0.4);' : ''}">${nodeId} ${isFailed ? '⚠ FAILED' : '✓'}</span>`;
      }).join(' ');

      const formattedSize = (doc.size / 1024).toFixed(1) + ' KB';
      const canWrite = isOwner || sharePermission === 'WRITE';

      const shareBtnHtml = canWrite
        ? `<button class="btn btn-cyan btn-share" data-docid="${doc.docId}" data-docname="${doc.name}" title="Share Access">
             <i class="fa-solid fa-share-nodes"></i>
           </button>`
        : '';

      const editBtnHtml = canWrite
        ? `<button class="btn btn-purple btn-edit-doc" data-docid="${doc.docId}" data-docname="${doc.name}" title="Edit Document & Mutual Exclusion">
             <i class="fa-solid fa-pen-to-square"></i>
           </button>`
        : '';

      const conflictBtnHtml = doc.conflict
        ? `<button class="btn btn-red btn-resolve-conflict" data-docid="${doc.docId}" title="Resolve Concurrent Conflict">
             <i class="fa-solid fa-code-merge"></i>
           </button>`
        : '';

      const deleteBtnHtml = isOwner
        ? `<button class="btn btn-red btn-delete" data-docid="${doc.docId}" data-docname="${doc.name}" title="Delete Document">
             <i class="fa-solid fa-trash"></i>
           </button>`
        : '';

      const removeSharedBtnHtml = !isOwner
        ? `<button class="btn btn-red btn-remove-shared" data-docid="${doc.docId}" data-docname="${doc.name}" title="Remove / Delete Shared File">
             <i class="fa-solid fa-trash"></i>
           </button>`
        : '';

      return `
        <tr data-docid="${doc.docId}">
          <td style="font-weight: 600;">
            <span class="doc-name-clickable" data-docid="${doc.docId}" title="Click to view file contents">📄 ${escapeHtml(doc.name)}</span>
            <div style="font-size: 0.75rem; color: var(--text-muted);">${formattedSize} • ${doc.totalChunks} chunk(s)</div>
          </td>
          <td>
            <span class="badge" style="background: rgba(139, 92, 246, 0.15); color: var(--accent-purple); font-weight:700;">v${doc.version || 1}</span>
            <span style="font-size: 0.72rem; color: var(--accent-cyan); font-weight: 600; margin-left: 0.3rem;">L=${doc.lamportTimestamp || 0}</span>
            <div style="font-size: 0.7rem; color: var(--text-muted); font-family: monospace; margin-top: 2px;">${escapeHtml(vectorClockStr)}</div>
            ${conflictBadge}
          </td>
          <td><div>${accessBadge}</div><div style="font-size: 0.75rem; color: var(--text-muted); margin-top: 0.2rem;">Owner: ${escapeHtml(ownerLabel)}</div></td>
          <td class="doc-lock-cell">${lockBadgeHtml}</td>
          <td>${replicasHtml}</td>
          <td class="doc-actions-cell">
            <div class="doc-actions-group">
              <button class="btn btn-purple btn-view" data-docid="${doc.docId}" title="View Content">
                <i class="fa-solid fa-eye"></i>
              </button>
              ${editBtnHtml}
              <button class="btn btn-cyan btn-history" data-docid="${doc.docId}" title="Version History">
                <i class="fa-solid fa-clock-rotate-left"></i>
              </button>
              ${conflictBtnHtml}
              <button class="btn btn-cyan btn-share-chat" data-docid="${doc.docId}" data-docname="${doc.name}" title="Share in Chat">
                <i class="fa-solid fa-comments"></i>
              </button>
              ${shareBtnHtml}
              <button class="btn btn-green btn-download" data-docid="${doc.docId}" title="Download File">
                <i class="fa-solid fa-cloud-arrow-down"></i>
              </button>
              ${deleteBtnHtml}
              ${removeSharedBtnHtml}
            </div>
          </td>
        </tr>
      `;
    }).join('');

    docTableBody.innerHTML = rowsHtml;
  }

  // Event delegation on docTableBody (avoids re-attaching hundreds of listeners on every render)
  if (docTableBody) {
    docTableBody.addEventListener('click', (e) => {
      const target = e.target;

      const viewEl = target.closest('.doc-name-clickable, .btn-view');
      if (viewEl) {
        const docId = viewEl.getAttribute('data-docid') || viewEl.closest('tr')?.getAttribute('data-docid');
        if (docId) handleViewDocument(docId);
        return;
      }

      const editBtn = target.closest('.btn-edit-doc');
      if (editBtn) {
        const docId = editBtn.getAttribute('data-docid') || editBtn.closest('tr')?.getAttribute('data-docid');
        if (docId) openEditDocModal(docId);
        return;
      }

      const histBtn = target.closest('.btn-history');
      if (histBtn) {
        const docId = histBtn.getAttribute('data-docid') || histBtn.closest('tr')?.getAttribute('data-docid');
        if (docId) openVersionHistoryModal(docId);
        return;
      }

      const conflictBtn = target.closest('.btn-resolve-conflict');
      if (conflictBtn) {
        const docId = conflictBtn.getAttribute('data-docid') || conflictBtn.closest('tr')?.getAttribute('data-docid');
        if (docId) openConflictModal(docId);
        return;
      }

      const dlBtn = target.closest('.btn-download');
      if (dlBtn) {
        const docId = dlBtn.getAttribute('data-docid') || dlBtn.closest('tr')?.getAttribute('data-docid');
        if (docId) handleStreamDownload(docId);
        return;
      }

      const shareBtn = target.closest('.btn-share');
      if (shareBtn) {
        const docId = shareBtn.getAttribute('data-docid') || shareBtn.closest('tr')?.getAttribute('data-docid');
        const docName = shareBtn.getAttribute('data-docname') || '';
        if (docId) openShareModal(docId, docName);
        return;
      }

      const shareChatBtn = target.closest('.btn-share-chat');
      if (shareChatBtn) {
        const docId = shareChatBtn.getAttribute('data-docid') || shareChatBtn.closest('tr')?.getAttribute('data-docid');
        const docName = shareChatBtn.getAttribute('data-docname') || '';
        if (docId) handleShareDocInChat(docId, docName);
        return;
      }

      const delBtn = target.closest('.btn-delete');
      if (delBtn) {
        const docId = delBtn.getAttribute('data-docid') || delBtn.closest('tr')?.getAttribute('data-docid');
        const docName = delBtn.getAttribute('data-docname') || '';
        if (docId) handleDeleteDocument(docId, docName);
        return;
      }

      const remBtn = target.closest('.btn-remove-shared');
      if (remBtn) {
        const docId = remBtn.getAttribute('data-docid') || remBtn.closest('tr')?.getAttribute('data-docid');
        const docName = remBtn.getAttribute('data-docname') || '';
        if (docId) handleRemoveSharedDoc(docId, docName);
        return;
      }
    });
  }

  let editDocLockTimer = null;

  async function openEditDocModal(docId) {
    currentEditingDocId = docId;
    const doc = documents.find(d => d.docId === docId);
    if (!doc) return;

    if (editDocModalTitle) editDocModalTitle.textContent = `Edit Document: ${doc.name}`;
    if (editDocVersionBadge) editDocVersionBadge.textContent = `Current: Version ${doc.version || 1}`;
    if (editDocTextarea) editDocTextarea.value = 'Loading contents from storage node...';

    // Show modal immediately for instant UI response
    updateEditModalLockState();
    if (editDocModal) {
      editDocModal.classList.add('active');
      editDocModal.style.display = 'flex';
    }

    // Refresh locks in background without blocking modal display
    fetch('/api/locks/status', { credentials: 'include' })
      .then(res => res.ok ? res.json() : null)
      .then(freshLocks => {
        if (Array.isArray(freshLocks) && currentEditingDocId === docId) {
          activeLocks = freshLocks;
          renderLocks();
          updateEditModalLockState();
          updateSingleDocumentLockUI(docId);
        }
      })
      .catch(e => console.warn('[LOCK] Could not refresh locks on modal open:', e));

    // Start 1-second countdown lease updater while modal is open
    if (editDocLockTimer) clearInterval(editDocLockTimer);
    editDocLockTimer = setInterval(() => {
      if (currentEditingDocId && editDocModal && editDocModal.classList.contains('active')) {
        updateEditModalLockState();
      } else {
        clearInterval(editDocLockTimer);
        editDocLockTimer = null;
      }
    }, 1000);

    try {
      const result = await streamClient.downloadDocument(docId);
      const text = await result.blob.text();
      if (editDocTextarea && currentEditingDocId === docId) editDocTextarea.value = text;
    } catch (e) {
      if (editDocTextarea && currentEditingDocId === docId) editDocTextarea.value = 'Failed to load plaintext content: ' + e.message;
    }
  }

  function updateEditModalLockState() {
    if (!currentEditingDocId) return;
    const lock = activeLocks.find(l => l.docId === currentEditingDocId);

    if (lock) {
      const isMine = currentUser && (lock.ownerId === currentUser.id || lock.lockedBy === currentUser.id);
      const remainingSec = lock.expiresAt ? Math.max(0, Math.round((lock.expiresAt - Date.now()) / 1000)) : (lock.remainingSec || 60);
      const ownerName = lock.ownerName || lock.lockedByName || lock.ownerId || lock.lockedBy;

      if (remainingSec <= 0) {
        // Lock lease naturally expired!
        activeLocks = activeLocks.filter(l => l.docId !== currentEditingDocId);
        renderLocks();
        updateSingleDocumentLockUI(currentEditingDocId);
        updateEditModalLockState();
        return;
      }

      if (editDocLockBadge) {
        editDocLockBadge.className = 'lock-badge-locked';
        editDocLockBadge.innerHTML = `<i class="fa-solid fa-lock"></i> LOCKED`;
        editDocLockBadge.style.background = '#ef4444';
        editDocLockBadge.style.color = '#fff';
        editDocLockBadge.style.padding = '0.2rem 0.5rem';
        editDocLockBadge.style.borderRadius = '4px';
        editDocLockBadge.style.fontSize = '0.75rem';
        editDocLockBadge.style.fontWeight = 'bold';
      }
      if (editDocLockOwner) {
        editDocLockOwner.textContent = `Locked by: ${ownerName} ${isMine ? '(You)' : ''} | Lease: ~${remainingSec}s remaining`;
        editDocLockOwner.style.color = isMine ? '#15803d' : '#b91c1c';
        editDocLockOwner.style.fontWeight = '600';
      }
      if (editDocAcquireLockBtn) {
        if (isMine) {
          editDocAcquireLockBtn.disabled = true;
          editDocAcquireLockBtn.innerHTML = `<i class="fa-solid fa-lock"></i> Lock Held (You)`;
          editDocAcquireLockBtn.title = 'You currently own this exclusive lock';
        } else {
          editDocAcquireLockBtn.disabled = false;
          editDocAcquireLockBtn.innerHTML = `<i class="fa-solid fa-key"></i> Acquire Lock`;
          editDocAcquireLockBtn.title = `Currently held by ${ownerName}. Click to test contention`;
        }
      }
      if (editDocReleaseLockBtn) {
        editDocReleaseLockBtn.disabled = !isMine;
        editDocReleaseLockBtn.style.opacity = isMine ? '1' : '0.5';
      }
    } else {
      if (editDocLockBadge) {
        editDocLockBadge.className = 'lock-badge-unlocked';
        editDocLockBadge.innerHTML = `<i class="fa-solid fa-lock-open"></i> UNLOCKED`;
        editDocLockBadge.style.background = '#22c55e';
        editDocLockBadge.style.color = '#fff';
        editDocLockBadge.style.padding = '0.2rem 0.5rem';
        editDocLockBadge.style.borderRadius = '4px';
        editDocLockBadge.style.fontSize = '0.75rem';
        editDocLockBadge.style.fontWeight = 'bold';
      }
      if (editDocLockOwner) {
        editDocLockOwner.textContent = 'Free to acquire';
        editDocLockOwner.style.color = '#475569';
        editDocLockOwner.style.fontWeight = 'normal';
      }
      if (editDocAcquireLockBtn) {
        editDocAcquireLockBtn.disabled = false;
        editDocAcquireLockBtn.innerHTML = `<i class="fa-solid fa-key"></i> Acquire Lock`;
        editDocAcquireLockBtn.title = 'Acquire exclusive distributed lock';
      }
      if (editDocReleaseLockBtn) {
        editDocReleaseLockBtn.disabled = true;
        editDocReleaseLockBtn.style.opacity = '0.5';
      }
    }
  }

  function closeEditDocModal() {
    currentEditingDocId = null;
    if (editDocLockTimer) {
      clearInterval(editDocLockTimer);
      editDocLockTimer = null;
    }
    if (editDocModal) {
      editDocModal.classList.remove('active');
      editDocModal.style.display = 'none';
    }
  }

  if (editDocModalCloseBtn) editDocModalCloseBtn.addEventListener('click', closeEditDocModal);
  if (cancelEditDocBtn) cancelEditDocBtn.addEventListener('click', closeEditDocModal);

  if (editDocAcquireLockBtn) {
    editDocAcquireLockBtn.addEventListener('click', async () => {
      if (!currentEditingDocId || editDocAcquireLockBtn.disabled) return;
      editDocAcquireLockBtn.disabled = true;
      editDocAcquireLockBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Acquiring...';
      try {
        await acquireLock(currentEditingDocId);
      } finally {
        updateEditModalLockState();
      }
    });
  }

  if (editDocReleaseLockBtn) {
    editDocReleaseLockBtn.addEventListener('click', async () => {
      if (!currentEditingDocId || editDocReleaseLockBtn.disabled) return;
      editDocReleaseLockBtn.disabled = true;
      editDocReleaseLockBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Releasing...';
      try {
        await releaseLock(currentEditingDocId);
      } finally {
        updateEditModalLockState();
      }
    });
  }

  if (saveEditDocBtn) {
    saveEditDocBtn.addEventListener('click', async () => {
      if (!currentEditingDocId || !currentUser || saveEditDocBtn.disabled) return;
      const content = editDocTextarea ? editDocTextarea.value : '';

      saveEditDocBtn.disabled = true;
      const origHtml = saveEditDocBtn.innerHTML;
      saveEditDocBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Saving...';

      try {
        appendLog(`[UPDATE] Submitting new version of '${currentEditingDocId}' as ${currentUser.name}...`, 'log-info');
        const res = await fetch('/api/documents/update', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          credentials: 'include',
          body: JSON.stringify({
            docId: currentEditingDocId,
            content,
            updatedBy: currentUser.id
          })
        });

        const data = await res.json();

        if (res.ok) {
          appendLog(`[UPDATE] SUCCESS: Created version ${data.document.version} of '${data.document.name}' (Vector: ${JSON.stringify(data.document.vectorClock)})`, 'log-success');
          const idx = documents.findIndex(d => d.docId === data.document.docId);
          if (idx >= 0) documents[idx] = data.document;
          else documents.push(data.document);
          closeEditDocModal();
          renderDocuments();
        } else if (res.status === 423) {
          alert(`⛔ MUTUAL EXCLUSION LOCK REQUIRED (423 Locked)\n\n${data.error}\n\nPlease click "Acquire Lock" first before editing.`);
          appendLog(`[UPDATE] FAILED: ${data.error}`, 'log-error');
        } else {
          alert(`Update failed: ${data.error}`);
          appendLog(`[UPDATE] FAILED: ${data.error}`, 'log-error');
        }
      } catch (err) {
        alert(`Update error: ${err.message}`);
      } finally {
        saveEditDocBtn.disabled = false;
        saveEditDocBtn.innerHTML = origHtml;
      }
    });
  }

  if (editDocSimulateConflictBtn) {
    editDocSimulateConflictBtn.addEventListener('click', async () => {
      if (!currentEditingDocId) return;
      try {
        appendLog(`[VECTOR] Simulating concurrent conflicting updates on '${currentEditingDocId}' by User A and User B...`, 'log-info');
        const res = await fetch('/api/documents/simulate-conflict', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          credentials: 'include',
          body: JSON.stringify({ docId: currentEditingDocId })
        });
        const data = await res.json();
        closeEditDocModal();
        if (res.ok) {
          appendLog(`[VECTOR] CONFLICT GENERATED: ${data.message}`, 'log-error');
          openConflictModal(currentEditingDocId);
        } else {
          alert(`Conflict simulation failed: ${data.error}`);
        }
      } catch (e) {
        alert(`Error: ${e.message}`);
      }
    });
  }

  // ── FA-2: Version History Modal ───────────────────────────────────────────

  async function openVersionHistoryModal(docId) {
    const doc = documents.find(d => d.docId === docId);
    if (!doc) return;

    if (historyDocTitle) historyDocTitle.textContent = `Version History: ${doc.name}`;
    if (historyDocSubtitle) historyDocSubtitle.textContent = `Document ID: ${docId} • Total Recorded Versions: ${doc.versions ? doc.versions.length : 1}`;
    if (versionHistoryTableBody) {
      versionHistoryTableBody.innerHTML = `<tr><td colspan="6" style="text-align:center; padding: 1rem;"><i class="fa-solid fa-spinner fa-spin"></i> Loading versions...</td></tr>`;
    }

    if (versionHistoryModal) {
      versionHistoryModal.classList.add('active');
      versionHistoryModal.style.display = 'flex';
    }

    try {
      const res = await fetch(`/api/documents/${docId}/versions`, { credentials: 'include' });
      const data = await res.json();
      const versions = data.versions || (doc.versions || []);

      if (versions.length === 0) {
        versionHistoryTableBody.innerHTML = `<tr><td colspan="6" style="text-align:center; color:var(--text-muted); padding:1rem;">No history records found.</td></tr>`;
        return;
      }

      versionHistoryTableBody.innerHTML = versions.slice().reverse().map(v => {
        const checksumShort = v.checksum ? v.checksum.slice(0, 16) + '...' : '—';
        return `
          <tr>
            <td><span class="badge" style="background: rgba(139, 92, 246, 0.15); color: var(--accent-purple); font-weight:700;">v${v.version}</span></td>
            <td style="font-weight: 600;">${escapeHtml(v.updatedBy || v.author || 'User A')}</td>
            <td style="color: var(--accent-cyan); font-weight: 700; font-family: monospace;">L=${v.lamportTimestamp || '—'}</td>
            <td><span class="vector-clock-tag">${escapeHtml(JSON.stringify(v.vectorClock || {}))}</span></td>
            <td style="font-family: monospace; font-size: 0.75rem;" title="${v.checksum || ''}">${checksumShort}</td>
            <td style="font-size: 0.75rem; color: var(--text-muted);">${formatTime(v.updatedAt || v.timestamp)}</td>
          </tr>
        `;
      }).join('');
    } catch (e) {
      versionHistoryTableBody.innerHTML = `<tr><td colspan="6" style="text-align:center; color:#ef4444; padding:1rem;">Failed to load version history: ${e.message}</td></tr>`;
    }
  }

  function closeVersionHistoryModal() {
    if (versionHistoryModal) {
      versionHistoryModal.classList.remove('active');
      versionHistoryModal.style.display = 'none';
    }
  }

  if (versionHistoryCloseBtn) versionHistoryCloseBtn.addEventListener('click', closeVersionHistoryModal);
  if (closeVersionHistoryBtn) closeVersionHistoryBtn.addEventListener('click', closeVersionHistoryModal);

  // ── FA-2: Conflict Resolution Modal ───────────────────────────────────────

  function openConflictModal(docId) {
    const doc = documents.find(d => d.docId === docId);
    if (!doc || !doc.conflictData) {
      if (activeConflictDoc && activeConflictDoc.docId === docId) {
        renderConflictModalData(activeConflictDoc.versionA, activeConflictDoc.versionB);
      } else {
        alert('No active conflict data found for this document.');
        return;
      }
    } else {
      activeConflictDoc = { docId: doc.docId, versionA: doc.conflictData.versionA, versionB: doc.conflictData.versionB };
      renderConflictModalData(doc.conflictData.versionA, doc.conflictData.versionB);
    }
    if (conflictModal) {
      conflictModal.classList.add('active');
      conflictModal.style.display = 'flex';
    }
  }

  function renderConflictModalData(verA, verB) {
    if (conflictVerALabel) conflictVerALabel.textContent = `v${verA.version || '1A'}`;
    if (conflictVerAMeta) conflictVerAMeta.textContent = `Author: ${verA.updatedBy || 'User A'} • Lamport: L=${verA.lamportTimestamp || 0}`;
    if (conflictVerAVector) conflictVerAVector.textContent = JSON.stringify(verA.vectorClock || {});
    if (conflictVerAPreview) conflictVerAPreview.textContent = verA.contentPreview || 'Version A branch content';

    if (conflictVerBLabel) conflictVerBLabel.textContent = `v${verB.version || '1B'}`;
    if (conflictVerBMeta) conflictVerBMeta.textContent = `Author: ${verB.updatedBy || 'User B'} • Lamport: L=${verB.lamportTimestamp || 0}`;
    if (conflictVerBVector) conflictVerBVector.textContent = JSON.stringify(verB.vectorClock || {});
    if (conflictVerBPreview) conflictVerBPreview.textContent = verB.contentPreview || 'Version B branch content';

    if (conflictMergeTextarea) {
      conflictMergeTextarea.value = `${verA.contentPreview || 'Version A content'}\n--- MERGED ---\n${verB.contentPreview || 'Version B content'}`;
    }
  }

  function closeConflictModal() {
    if (conflictModal) {
      conflictModal.classList.remove('active');
      conflictModal.style.display = 'none';
    }
  }

  if (conflictModalCloseBtn) conflictModalCloseBtn.addEventListener('click', closeConflictModal);
  if (conflictCancelBtn) conflictCancelBtn.addEventListener('click', closeConflictModal);

  async function resolveConflictChoice(choice, mergedContent = '') {
    if (!activeConflictDoc) return;
    const docId = activeConflictDoc.docId;
    try {
      appendLog(`[VECTOR] Resolving conflict on '${docId}' with choice: ${choice}...`, 'log-info');
      const res = await fetch('/api/documents/resolve-conflict', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({
          docId,
          resolutionChoice: choice,
          mergedContent,
          resolvedBy: currentUser ? currentUser.id : 'User A'
        })
      });

      const data = await res.json();
      if (res.ok) {
        const resolvedVer = data.document?.version || data.version?.version || '';
        appendLog(`[VECTOR] Conflict on '${docId}' successfully resolved! Established version ${resolvedVer ? 'v' + resolvedVer : ''}`, 'log-success');
        activeConflictDoc = null;
        renderConflictBanner();
        closeConflictModal();
        fetchDocuments();
      } else {
        alert(`Resolution failed: ${data.error}`);
      }
    } catch (e) {
      alert(`Error resolving conflict: ${e.message}`);
    }
  }

  if (chooseVerABtn) {
    chooseVerABtn.addEventListener('click', () => resolveConflictChoice('A'));
  }

  if (chooseVerBBtn) {
    chooseVerBBtn.addEventListener('click', () => resolveConflictChoice('B'));
  }

  if (confirmMergeBtn) {
    confirmMergeBtn.addEventListener('click', () => {
      const merged = conflictMergeTextarea ? conflictMergeTextarea.value : '';
      resolveConflictChoice('MERGE', merged);
    });
  }

  // ── Delete Document ───────────────────────────────────────────────────────

  async function handleDeleteDocument(docId, docName) {
    if (!currentUser) return;
    const confirmed = confirm(`🗑️ Delete "${docName}"?\n\nThis will permanently remove the document and all its chunks from all storage nodes. This cannot be undone.`);
    if (!confirmed) return;

    try {
      appendLog(`[DELETE] Requesting deletion of '${docName}' (${docId})...`, 'log-info');
      const res = await fetch(`/api/documents/${docId}`, {
        method: 'DELETE',
        credentials: 'include'
      });

      const data = await res.json();

      if (res.ok) {
        documents = documents.filter(d => d.docId !== docId);
        renderDocuments();
        appendLog(`[DELETE] SUCCESS: '${docName}' permanently deleted from all replicas`, 'log-error');
      } else if (res.status === 403) {
        alert(`⛔ Permission Denied\n\nOnly the document owner can delete it.`);
        appendLog(`[DELETE] DENIED: ${data.error}`, 'log-error');
      } else {
        alert(`Delete failed: ${data.error || 'Unknown error'}`);
        appendLog(`[DELETE] FAILED: ${data.error}`, 'log-error');
      }
    } catch (err) {
      appendLog(`[DELETE] ERROR: ${err.message}`, 'log-error');
      alert(`Delete Error: ${err.message}`);
    }
  }

  // ── Stream Download with Access Control & Failover ───────────────────────

  async function handleStreamDownload(docId) {
    if (!currentUser) {
      alert('You must be logged in to download documents.');
      return;
    }
    try {
      progressWrapper.style.display = 'block';
      progressBar.style.width = '0%';
      progressText.textContent = `Authenticating session... Locating document ${docId}...`;

      appendLog(`[DOWNLOAD] Requesting document ${docId} as ${currentUser.name}`, 'log-info');

      const result = await streamClient.downloadDocument(docId, (progress) => {
        progressBar.style.width = `${progress.percent}%`;
        progressText.textContent = `Streaming chunk ${progress.chunkIndex + 1}/${progress.totalChunks} from ${progress.usedNodeId || 'replica'} (${progress.percent}%)`;
      });

      progressText.textContent = `✅ Download complete! File reconstructed from healthy replicas.`;
      appendLog(`[DOWNLOAD] SUCCESS: '${result.fileName}' downloaded (${(result.blob.size / 1024).toFixed(1)} KB)`, 'log-success');

      const url = URL.createObjectURL(result.blob);
      const a   = document.createElement('a');
      a.href     = url;
      a.download = result.fileName;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);

      setTimeout(() => { progressWrapper.style.display = 'none'; }, 4000);
    } catch (err) {
      progressWrapper.style.display = 'none';
      if (err.message && (err.message.includes('403') || err.message.toLowerCase().includes('forbidden'))) {
        appendLog(`[ACCESS DENIED] 403 Forbidden: ${err.message}`, 'log-error');
        alert(`⛔ ACCESS DENIED (403 Forbidden)\n\nUser '${currentUser.id}' does not have permissions for this document.\nAsk the owner to share it with you.`);
      } else {
        appendLog(`[DOWNLOAD FAULT] ${err.message}`, 'log-error');
        alert(`Download Failed: ${err.message}`);
      }
    }
  }

  // ── Document Viewer & Preview Controller ─────────────────────────────────

  function closeViewModal() {
    if (viewModal) viewModal.classList.remove('active');
    if (currentPreviewUrl) {
      URL.revokeObjectURL(currentPreviewUrl);
      currentPreviewUrl = null;
    }
    currentPreviewText = '';
    if (viewCopyBtn) viewCopyBtn.style.display = 'none';
    if (copyToast) copyToast.style.display = 'none';
  }

  if (viewModalCloseBtn) viewModalCloseBtn.addEventListener('click', closeViewModal);
  if (viewCloseBtn) viewCloseBtn.addEventListener('click', closeViewModal);

  if (viewCopyBtn) {
    viewCopyBtn.addEventListener('click', () => {
      if (!currentPreviewText) return;
      const showToast = () => {
        if (copyToast) {
          copyToast.style.display = 'inline-block';
          setTimeout(() => { copyToast.style.display = 'none'; }, 2000);
        }
      };
      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(currentPreviewText).then(showToast).catch(() => {
          fallbackCopyText(currentPreviewText, showToast);
        });
      } else {
        fallbackCopyText(currentPreviewText, showToast);
      }
    });
  }

  function fallbackCopyText(text, cb) {
    const ta = document.createElement('textarea');
    ta.value = text;
    ta.style.position = 'fixed';
    ta.style.opacity = '0';
    document.body.appendChild(ta);
    ta.select();
    try {
      document.execCommand('copy');
      if (cb) cb();
    } catch (e) {}
    document.body.removeChild(ta);
  }

  async function handleViewDocument(docId) {
    if (!currentUser) {
      alert('You must be logged in to view documents.');
      return;
    }

    const doc = documents.find(d => d.docId === docId);
    if (!doc) {
      alert(`Document '${docId}' not found.`);
      return;
    }

    closeViewModal();

    if (viewDocTitle) viewDocTitle.textContent = doc.name;

    if (viewMetaSize) viewMetaSize.textContent = `${(doc.size / 1024).toFixed(1)} KB (${doc.totalChunks} chunks)`;
    const ownerObj = allUsers.find(u => u.id === doc.ownerId);
    if (viewMetaOwner) viewMetaOwner.textContent = ownerObj ? `${ownerObj.name} (${doc.ownerId})` : doc.ownerId;
    const isOwner = doc.ownerId === currentUser.id;
    const perm = isOwner ? 'Owner (Full Control)' : (doc.shares ? doc.shares[currentUser.id] || 'Shared' : 'Shared');
    if (viewMetaAccess) viewMetaAccess.textContent = perm;

    const healthyReplicas = (doc.replicaNodeIds || [])
      .map(id => nodes.find(n => n.id === id))
      .filter(n => n && n.status === 'HEALTHY');
    if (viewMetaReplicas) viewMetaReplicas.textContent = `${healthyReplicas.length} / ${(doc.replicaNodeIds || []).length} healthy`;

    if (viewModalBody) {
      viewModalBody.innerHTML = `
        <div style="margin: auto; text-align: center; color: var(--text-muted);">
          <i class="fa-solid fa-spinner fa-spin" style="font-size: 2.2rem; color: var(--accent-cyan); margin-bottom: 0.75rem;"></i>
          <div style="font-weight: 500;">Reconstructing document chunks from distributed storage nodes...</div>
          <div style="font-size: 0.78rem; margin-top: 0.3rem; color: var(--accent-purple);">Connecting via Coordinator P2P proxy...</div>
        </div>
      `;
    }

    if (viewDownloadBtn) {
      viewDownloadBtn.onclick = () => handleStreamDownload(docId);
    }

    if (viewModal) viewModal.classList.add('active');

    try {
      appendLog(`[VIEWER] Fetching & reconstructing '${doc.name}' (${docId})...`, 'log-info');
      const result = await streamClient.downloadDocument(docId);
      const blob = result.blob;

      const fileName = doc.name.toLowerCase();
      const ext = fileName.includes('.') ? fileName.split('.').pop() : '';
      const mime = (doc.mimeType || blob.type || '').toLowerCase();

      const textExts = ['txt', 'json', 'js', 'css', 'html', 'htm', 'md', 'xml', 'csv', 'py', 'c', 'cpp', 'java', 'log', 'sh', 'sql', 'yml', 'yaml', 'env', 'ts', 'jsx', 'tsx'];
      const imageExts = ['png', 'jpg', 'jpeg', 'gif', 'svg', 'webp', 'bmp', 'ico'];
      const audioExts = ['mp3', 'wav', 'ogg', 'aac', 'm4a', 'flac'];
      const videoExts = ['mp4', 'webm', 'mov', 'mkv', 'avi'];

      const isText  = textExts.includes(ext) || mime.startsWith('text/') || mime.includes('json') || mime.includes('javascript') || mime.includes('xml');
      const isImage = imageExts.includes(ext) || mime.startsWith('image/');
      const isPdf   = ext === 'pdf' || mime.includes('pdf');
      const isAudio = audioExts.includes(ext) || mime.startsWith('audio/');
      const isVideo = videoExts.includes(ext) || mime.startsWith('video/');

      if (isText) {
        const rawText = await blob.text();
        let nonPrintable = 0;
        const checkLen = Math.min(rawText.length, 500);
        for (let i = 0; i < checkLen; i++) {
          const code = rawText.charCodeAt(i);
          if (code < 9 || (code > 13 && code < 32)) nonPrintable++;
        }

        if (nonPrintable > 20) {
          renderBinaryView(doc, result);
        } else {
          currentPreviewText = rawText;
          if (viewCopyBtn) viewCopyBtn.style.display = 'inline-flex';
          const lineCount = rawText.split('\n').length;
          const charCount = rawText.length;
          if (viewModalBody) {
            viewModalBody.innerHTML = `
              <div style="display: flex; justify-content: space-between; font-size: 0.75rem; color: var(--text-muted); margin-bottom: 0.5rem; border-bottom: 1px solid rgba(255,255,255,0.06); padding-bottom: 0.4rem;">
                <span><i class="fa-solid fa-code"></i> Text Preview (${lineCount} lines, ${charCount} chars)</span>
                <span>UTF-8 Plaintext</span>
              </div>
              <pre class="file-text-preview">${escapeHtml(rawText)}</pre>
            `;
          }
        }
      } else if (isImage) {
        currentPreviewUrl = URL.createObjectURL(blob);
        if (viewModalBody) {
          viewModalBody.innerHTML = `
            <div style="margin: auto; text-align: center; width: 100%;">
              <img src="${currentPreviewUrl}" class="file-image-preview" alt="${escapeHtml(doc.name)}">
            </div>
          `;
        }
      } else if (isPdf) {
        currentPreviewUrl = URL.createObjectURL(blob);
        if (viewModalBody) {
          viewModalBody.innerHTML = `
            <iframe src="${currentPreviewUrl}" class="file-pdf-preview"></iframe>
          `;
        }
      } else if (isAudio) {
        currentPreviewUrl = URL.createObjectURL(blob);
        if (viewModalBody) {
          viewModalBody.innerHTML = `
            <div class="file-binary-card">
              <i class="fa-solid fa-music" style="font-size: 3rem; color: var(--accent-cyan); margin-bottom: 1rem;"></i>
              <div style="font-weight: 600; margin-bottom: 1rem; color: #fff;">${escapeHtml(doc.name)}</div>
              <audio controls src="${currentPreviewUrl}" class="file-media-preview"></audio>
            </div>
          `;
        }
      } else if (isVideo) {
        currentPreviewUrl = URL.createObjectURL(blob);
        if (viewModalBody) {
          viewModalBody.innerHTML = `
            <div style="margin: auto; width: 100%;">
              <video controls src="${currentPreviewUrl}" class="file-media-preview"></video>
            </div>
          `;
        }
      } else {
        renderBinaryView(doc, result);
      }

      appendLog(`[VIEWER] Successfully loaded '${doc.name}' inside preview modal`, 'log-success');

    } catch (err) {
      appendLog(`[VIEWER ERROR] Failed to load '${doc.name}': ${err.message}`, 'log-error');
      if (viewModalBody) {
        viewModalBody.innerHTML = `
          <div class="file-binary-card">
            <i class="fa-solid fa-circle-exclamation" style="font-size: 2.5rem; color: var(--accent-red); margin-bottom: 0.75rem;"></i>
            <div style="color: var(--accent-red); font-weight: 600; font-size: 1rem;">Unable to Open File</div>
            <div style="font-size: 0.85rem; margin-top: 0.4rem; color: var(--text-muted);">${escapeHtml(err.message)}</div>
          </div>
        `;
      }
    }
  }

  function renderBinaryView(doc, result) {
    if (!viewModalBody) return;
    const formattedSize = (doc.size / 1024).toFixed(1) + ' KB';
    viewModalBody.innerHTML = `
      <div class="file-binary-card">
        <i class="fa-solid fa-file-binary" style="font-size: 3.5rem; color: var(--accent-purple); margin-bottom: 1rem;"></i>
        <div style="font-size: 1.1rem; font-weight: 700; color: #fff; margin-bottom: 0.4rem;">${escapeHtml(doc.name)}</div>
        <div style="font-size: 0.85rem; color: var(--text-muted); margin-bottom: 1.2rem;">
          Binary Document (${formattedSize} • ${doc.totalChunks} chunk(s) stored across distributed replicas)
        </div>
        <div style="font-size: 0.8rem; color: var(--accent-green); background: rgba(16, 185, 129, 0.1); border: 1px solid rgba(16, 185, 129, 0.3); padding: 0.6rem; border-radius: 8px; max-width: 400px; margin: 0 auto 1.2rem auto;">
          ✓ SHA-256 integrity verified across storage replicas
        </div>
        <button class="btn btn-green" onclick="document.getElementById('viewDownloadBtn').click();" style="padding: 0.6rem 1.2rem;">
          <i class="fa-solid fa-cloud-arrow-down"></i> Download File to View Externally
        </button>
      </div>
    `;
  }

  // ── Share Modal ───────────────────────────────────────────────────────────

  function openShareModal(docId, docName) {
    if (!shareModal || !currentUser) return;
    shareDocIdInput.value   = docId;
    shareDocNameInput.value = docName;

    shareTargetUserSelect.innerHTML = allUsers
      .filter(u => u.id !== currentUser.id)
      .map(u => `<option value="${u.id}">${u.name} (${u.id})</option>`)
      .join('');

    shareModal.classList.add('active');
  }

  function closeShareModal() {
    if (shareModal) shareModal.classList.remove('active');
  }

  if (modalCloseBtn) modalCloseBtn.addEventListener('click', closeShareModal);
  if (cancelShareBtn) cancelShareBtn.addEventListener('click', closeShareModal);

  if (confirmShareBtn) {
    confirmShareBtn.addEventListener('click', async () => {
      const docId        = shareDocIdInput.value;
      const targetUserId = shareTargetUserSelect.value;
      const permission   = sharePermissionSelect.value;

      if (!docId || !targetUserId) return;

      try {
        if (permission === 'REVOKE' || permission === 'REMOVE') {
          const res = await fetch('/api/documents/unshare', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'include',
            body: JSON.stringify({ docId, targetUserId })
          });
          const data = await res.json();
          if (res.ok) {
            closeShareModal();
            appendLog(`[ACCESS CONTROL] Access revoked for user '${targetUserId}' on document '${docId}'`, 'log-info');
            alert(`✅ Access Revoked: User '${targetUserId}' can no longer access this document.`);
            fetchDocuments();
          } else {
            alert(`Revoke failed: ${data.error}`);
          }
          return;
        }

        await rpc.call('rpc.share_document', { docId, targetUserId, permission });
        closeShareModal();
        appendLog(`[ACCESS CONTROL] Document '${docId}' shared with ${targetUserId} (${permission}) by ${currentUser.name}`, 'log-success');
        alert(`✅ Success! Document shared with ${targetUserId} (${permission} Access).`);
        fetchDocuments();
      } catch (err) {
        if (err.message && err.message.includes('-32001')) {
          alert(`⛔ Permission Denied (403)\n${err.message}`);
        } else {
          alert(`Sharing Error: ${err.message}`);
        }
      }
    });
  }

  // ── File Upload ──────────────────────────────────────────────────────────

  if (dropZone) {
    dropZone.addEventListener('click', () => fileInput && fileInput.click());
    dropZone.addEventListener('dragover', (e) => { e.preventDefault(); dropZone.classList.add('drag-over'); });
    dropZone.addEventListener('dragleave', () => dropZone.classList.remove('drag-over'));
    dropZone.addEventListener('drop', (e) => {
      e.preventDefault();
      dropZone.classList.remove('drag-over');
      if (e.dataTransfer.files.length > 0) uploadFile(e.dataTransfer.files[0]);
    });
  }

  if (fileInput) {
    fileInput.addEventListener('change', () => {
      if (fileInput.files.length > 0) uploadFile(fileInput.files[0]);
    });
  }

  async function uploadFile(file) {
    if (!currentUser) {
      alert('You must be logged in to upload documents.');
      return;
    }

    const formData = new FormData();
    formData.append('file', file);

    progressWrapper.style.display = 'block';
    progressBar.style.width = '30%';
    progressText.textContent = `Uploading '${file.name}' as ${currentUser.name} — chunking with SHA-256...`;

    appendLog(`[UPLOAD] '${file.name}' (${(file.size / 1024).toFixed(1)} KB) by ${currentUser.name} — initiating P2P replication...`, 'log-info');

    try {
      const res = await fetch('/api/documents/upload', {
        method: 'POST',
        credentials: 'include',
        body: formData
      });

      const data = await res.json();

      if (res.ok) {
        progressBar.style.width = '100%';
        const nodes = (data.document.replicaNodeIds || []).join(', ');
        progressText.textContent = `✅ Upload + Replication complete! Stored on [${nodes}] • SHA-256 verified`;
        appendLog(`[UPLOAD] SUCCESS: '${file.name}' replicated to [${nodes}] with ${data.document.totalChunks} chunks (Version ${data.document.version || 1})`, 'log-success');
        setTimeout(() => { progressWrapper.style.display = 'none'; }, 3500);
        fetchDocuments();
      } else if (res.status === 401) {
        progressWrapper.style.display = 'none';
        alert('Session expired. Please login again.');
        showLoginScreen();
      } else {
        progressWrapper.style.display = 'none';
        appendLog(`[UPLOAD] FAILED: ${data.error}`, 'log-error');
        alert(`Upload Failed: ${data.error}`);
      }
    } catch (err) {
      progressWrapper.style.display = 'none';
      appendLog(`[UPLOAD] ERROR: ${err.message}`, 'log-error');
      alert(`Upload Error: ${err.message}`);
    } finally {
      if (fileInput) fileInput.value = '';
    }
  }

  // ── Quick Text Document Creator ───────────────────────────────────────────

  if (createQuickDocBtn) {
    createQuickDocBtn.addEventListener('click', () => {
      const title   = (quickDocTitle ? quickDocTitle.value.trim() : '') || 'Project_Notes.txt';
      const content = (quickDocContent ? quickDocContent.value : '') || 'Sample distributed document content.';

      const blob = new Blob([content], { type: 'text/plain' });
      const file = new File([blob], title.endsWith('.txt') ? title : `${title}.txt`, { type: 'text/plain' });
      uploadFile(file);

      if (quickDocTitle)   quickDocTitle.value   = '';
      if (quickDocContent) quickDocContent.value = '';
    });
  }

  // ── Horizontal Collapse for Topology Simulation Panel ────────────────────────

  const toggleTopologyBtn  = document.getElementById('toggleTopologyBtn');
  const topologyPanel      = document.getElementById('topologyPanel');
  const dashboardGrid      = document.getElementById('dashboardGrid');
  const topologyToggleIcon = document.getElementById('topologyToggleIcon');

  if (toggleTopologyBtn && topologyPanel && dashboardGrid) {
    toggleTopologyBtn.addEventListener('click', () => {
      const isCollapsed = topologyPanel.classList.toggle('panel-collapsed');
      dashboardGrid.classList.toggle('topology-collapsed', isCollapsed);

      if (topologyToggleIcon) {
        topologyToggleIcon.className = isCollapsed ? 'fa-solid fa-chevron-right' : 'fa-solid fa-chevron-left';
      }

      toggleTopologyBtn.title = isCollapsed ? 'Expand Topology Simulation' : 'Hide/Collapse to Left Side';

      if (!isCollapsed) {
        setTimeout(renderTopology, 100);
      }
    });
  }

  // ── Fault Injection Controls ──────────────────────────────────────────────

  document.querySelectorAll('.btn-fault').forEach(btn => {
    btn.addEventListener('click', async (e) => {
      const targetBtn = e.target.closest('.btn-fault');
      const nodeId    = targetBtn ? targetBtn.getAttribute('data-nodeid') : null;
      const action    = targetBtn ? targetBtn.getAttribute('data-action')  : null;

      if (!nodeId || !action || targetBtn.disabled) return;

      const origHtml = targetBtn.innerHTML;
      targetBtn.disabled = true;
      targetBtn.innerHTML = `<i class="fa-solid fa-spinner fa-spin"></i> ${action === 'kill' ? 'Killing...' : 'Reviving...'}`;

      try {
        await rpc.call('rpc.simulate_fault', { nodeId, action });
        const verb = action === 'kill' ? 'KILLED' : 'REVIVED';
        appendLog(`[FAULT INJECT] Node ${nodeId} ${verb} by operator`, action === 'kill' ? 'log-error' : 'log-success');
      } catch (err) {
        alert(`Fault Simulation Error: ${err.message}`);
      } finally {
        targetBtn.disabled = false;
        targetBtn.innerHTML = origHtml;
      }
    });
  });

  // ── Categorized Event Monitor & Log Terminal ──────────────────────────────

  if (logFilterTabs) {
    logFilterTabs.querySelectorAll('.log-filter-btn').forEach(btn => {
      btn.addEventListener('click', (e) => {
        logFilterTabs.querySelectorAll('.log-filter-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        currentLogFilter = btn.getAttribute('data-filter');
        renderLogs();
      });
    });
  }

  function renderLogs() {
    if (!logTerminal) return;
    const filterKey = currentLogFilter.replace(/[\[\]]/g, '').toUpperCase();
    const filtered = logs.filter(l => {
      if (currentLogFilter === 'all') return true;
      if (typeof l === 'object' && l !== null) {
        if (filterKey === 'RPC') return true;
        const text = `${l.method || ''} ${JSON.stringify(l.request || '')}`.toUpperCase();
        return text.includes(filterKey);
      }
      return String(l).toUpperCase().includes(filterKey);
    });

    logTerminal.innerHTML = filtered.map(l => {
      if (typeof l === 'string') {
        const cls = getLogStyleClass(l);
        return `<div class="log-line ${cls}">${escapeHtml(l)}</div>`;
      }
      return `
        <div class="log-line">
          <span class="log-time">[${l.timestamp}]</span>
          <span class="log-method">${escapeHtml(l.method)}</span>
          <span class="log-success"> ${escapeHtml(JSON.stringify(l.request || {}).slice(0, 120))}</span>
        </div>
      `;
    }).join('');
    logTerminal.scrollTop = logTerminal.scrollHeight;
  }

  function getLogStyleClass(msg) {
    if (msg.includes('ERROR') || msg.includes('FAILED') || msg.includes('DENIED') || msg.includes('DEADLOCK') || msg.includes('CONFLICT')) return 'log-error';
    if (msg.includes('SUCCESS') || msg.includes('HEALTHY') || msg.includes('replicated') || msg.includes('reclaimed')) return 'log-success';
    return 'log-info';
  }

  function appendLog(msg, cssClass = 'log-info') {
    const formatted = `[${new Date().toLocaleTimeString()}] ${msg}`;
    logs.push(formatted);
    if (logs.length > 200) logs.shift();

    if (!logTerminal) return;
    const filterKey = currentLogFilter.replace(/[\[\]]/g, '').toUpperCase();
    if (currentLogFilter !== 'all' && !msg.toUpperCase().includes(filterKey)) return;

    const div = document.createElement('div');
    div.className = `log-line ${cssClass}`;
    div.textContent = formatted;
    logTerminal.appendChild(div);
    logTerminal.scrollTop = logTerminal.scrollHeight;
  }

  function appendRpcLogEntry(l) {
    if (!logTerminal) return;
    const filterKey = currentLogFilter.replace(/[\[\]]/g, '').toUpperCase();
    if (currentLogFilter !== 'all' && filterKey !== 'RPC') {
      const text = `${l.method || ''} ${JSON.stringify(l.request || '')}`.toUpperCase();
      if (!text.includes(filterKey)) return;
    }
    const div = document.createElement('div');
    div.className = 'log-line';
    div.innerHTML = `
      <span class="log-time">[${escapeHtml(l.timestamp || new Date().toLocaleTimeString())}]</span>
      <span class="log-method">${escapeHtml(l.method || '')}</span>
      <span class="log-success"> ${escapeHtml(JSON.stringify(l.request || {}).slice(0, 120))}</span>
    `;
    logTerminal.appendChild(div);
    if (logTerminal.children.length > 200) {
      logTerminal.removeChild(logTerminal.firstChild);
    }
    logTerminal.scrollTop = logTerminal.scrollHeight;
  }

  // ── Real-Time User Chat Controller ────────────────────────────────────────

  function populateChatUserSelect() {
    if (!chatTargetUserSelect || !currentUser) return;
    const targets = allUsers.filter(u => u.id !== currentUser.id);
    if (targets.length === 0) {
      chatTargetUserSelect.innerHTML = '<option value="">No other users</option>';
      return;
    }
    const currentVal = chatTargetUserSelect.value;
    chatTargetUserSelect.innerHTML = targets.map(u => `<option value="${u.id}">${u.name} (${u.id})</option>`).join('');
    if (currentVal && targets.some(u => u.id === currentVal)) {
      chatTargetUserSelect.value = currentVal;
    }
  }

  function toggleChatDrawer(forceState) {
    isChatOpen = typeof forceState === 'boolean' ? forceState : !isChatOpen;
    if (chatDrawer) {
      chatDrawer.style.display = isChatOpen ? 'flex' : 'none';
    }
    if (isChatOpen) {
      unreadCount = 0;
      updateChatBadges();
      populateChatUserSelect();
      renderChatMessages();
      if (chatInput) chatInput.focus();
    }
  }

  function updateChatBadges() {
    const show = unreadCount > 0;
    const txt  = unreadCount > 99 ? '99+' : unreadCount;
    if (floatingChatBadge) {
      floatingChatBadge.textContent = txt;
      floatingChatBadge.style.display = show ? 'inline-block' : 'none';
    }
    if (chatNavBadge) {
      chatNavBadge.textContent = txt;
      chatNavBadge.style.display = show ? 'inline-block' : 'none';
    }
  }

  function getFileIconClass(fileName, mimeType = '') {
    const name = (fileName || '').toLowerCase();
    if (name.endsWith('.pdf') || mimeType.includes('pdf')) return 'fa-file-pdf';
    if (name.match(/\.(jpg|jpeg|png|gif|webp|svg)$/) || mimeType.includes('image')) return 'fa-file-image';
    if (name.match(/\.(mp4|webm|avi|mov)$/) || mimeType.includes('video')) return 'fa-file-video';
    if (name.match(/\.(mp3|wav|ogg)$/) || mimeType.includes('audio')) return 'fa-file-audio';
    if (name.match(/\.(js|json|html|css|py|java|cpp|c|cs|go|rs|md|txt)$/)) return 'fa-file-code';
    if (name.match(/\.(zip|tar|gz|rar|7z)$/)) return 'fa-file-zipper';
    return 'fa-file-lines';
  }

  function formatBytes(bytes) {
    if (!bytes || isNaN(bytes)) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
  }

  function renderChatMessages() {
    if (!chatMessageList || !currentUser) return;
    const targetUserId = chatTargetUserSelect ? chatTargetUserSelect.value : null;

    if (!targetUserId) {
      chatMessageList.innerHTML = '<div class="chat-empty-state"><div>Select a user to start chatting</div></div>';
      return;
    }

    const conversation = chatMessages.filter(m =>
      (m.senderId === currentUser.id && m.targetUserId === targetUserId) ||
      (m.senderId === targetUserId && m.targetUserId === currentUser.id)
    );

    if (conversation.length === 0) {
      chatMessageList.innerHTML = `
        <div class="chat-empty-state">
          <i class="fa-regular fa-comment-dots" style="font-size: 2.5rem; color: var(--text-muted); margin-bottom: 0.5rem;"></i>
          <div>No messages with <strong>${targetUserId}</strong> yet.</div>
          <div style="font-size: 0.75rem; margin-top: 0.25rem;">Type a message or attach a file below!</div>
        </div>
      `;
      return;
    }

    chatMessageList.innerHTML = conversation.map(m => {
      const isSent = m.senderId === currentUser.id;
      const timeStr = m.timestamp ? new Date(m.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) : '';
      let attachmentHtml = '';

      if (m.fileAttachment) {
        const fa = m.fileAttachment;
        const iconClass = getFileIconClass(fa.fileName, fa.mimeType);
        const sizeStr = formatBytes(fa.fileSize);
        attachmentHtml = `
          <div class="chat-file-card">
            <div class="chat-file-header">
              <i class="fa-solid ${iconClass} chat-file-icon"></i>
              <div class="chat-file-info">
                <div class="chat-file-name" title="${escapeHtml(fa.fileName)}">${escapeHtml(fa.fileName)}</div>
                <div class="chat-file-size">${sizeStr} • ${fa.totalChunks || 1} chunk(s)</div>
              </div>
            </div>
            <div class="chat-file-actions">
              <button class="btn-chat-card-action btn-chat-card-view chat-view-file-btn" data-docid="${fa.docId}" title="View File">
                <i class="fa-solid fa-eye"></i>
              </button>
              <button class="btn-chat-card-action btn-chat-card-dl chat-dl-file-btn" data-docid="${fa.docId}" title="Download File">
                <i class="fa-solid fa-cloud-arrow-down"></i>
              </button>
              <button class="btn-chat-card-action btn-chat-card-forward chat-forward-msg-btn" data-msgid="${m.id}" title="Forward File">
                <i class="fa-solid fa-share"></i>
              </button>
              ${isSent ? `
                <button class="btn-chat-card-action btn-chat-card-unsend chat-unsend-msg-btn" data-msgid="${m.id}" title="Unsend File">
                  <i class="fa-solid fa-trash"></i>
                </button>
              ` : ''}
            </div>
          </div>
        `;
      }

      return `
        <div class="chat-bubble ${isSent ? 'sent' : 'received'}" data-msgid="${m.id}">
          ${m.text ? `<div class="chat-text">${escapeHtml(m.text)}</div>` : ''}
          ${attachmentHtml}
          ${!m.fileAttachment ? `
            <div class="chat-actions-bar">
              <button class="btn-chat-inline-action chat-forward-msg-btn" data-msgid="${m.id}" title="Forward Message">
                <i class="fa-solid fa-share"></i>
              </button>
              ${isSent ? `
                <button class="btn-chat-inline-action chat-unsend-msg-btn" data-msgid="${m.id}" title="Unsend Message">
                  <i class="fa-solid fa-trash"></i>
                </button>
              ` : ''}
            </div>
          ` : ''}
          <div class="chat-meta">
            <span>${isSent ? 'You' : escapeHtml(m.senderName || m.senderId)}</span>
            <span>${timeStr}</span>
          </div>
        </div>
      `;
    }).join('');

    chatMessageList.scrollTop = chatMessageList.scrollHeight;
  }

  function escapeHtml(text) {
    if (!text) return '';
    return String(text).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#039;");
  }

  async function handleSendChatMessage(e) {
    if (e) e.preventDefault();
    if (!chatInput || !currentUser) return;

    const text = chatInput.value.trim();
    const targetUserId = chatTargetUserSelect ? chatTargetUserSelect.value : null;

    if (!text || !targetUserId) return;

    chatInput.value = '';
    await sendChatMessageWithAttachment(targetUserId, text, null);
  }

  async function sendChatMessageWithAttachment(targetUserId, text, fileAttachment) {
    if (!currentUser || !targetUserId) return;

    try {
      const res = await fetch('/api/chat/send', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({ targetUserId, text, fileAttachment })
      });

      if (!res.ok) {
        if (ws && ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({
            type: 'SEND_CHAT_MESSAGE',
            payload: { senderId: currentUser.id, targetUserId, text, fileAttachment }
          }));
        }
      }
    } catch (err) {
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({
          type: 'SEND_CHAT_MESSAGE',
          payload: { senderId: currentUser.id, targetUserId, text, fileAttachment }
        }));
      }
    }
  }

  // ── Chat File Attachments & Sharing ──────────────────────────────────────

  if (chatAttachBtn && chatFileInput) {
    chatAttachBtn.addEventListener('click', () => {
      const targetUserId = chatTargetUserSelect ? chatTargetUserSelect.value : null;
      if (!targetUserId) {
        alert('Please select a user to chat with first.');
        return;
      }
      chatFileInput.click();
    });

    chatFileInput.addEventListener('change', async () => {
      if (!chatFileInput.files || chatFileInput.files.length === 0) return;
      const file = chatFileInput.files[0];
      const targetUserId = chatTargetUserSelect ? chatTargetUserSelect.value : null;
      if (!targetUserId) return;

      appendLog(`[CHAT FILE] Uploading '${file.name}' to share with ${targetUserId}...`, 'log-info');
      try {
        const formData = new FormData();
        formData.append('file', file);

        const uploadRes = await fetch('/api/documents/upload', {
          method: 'POST',
          credentials: 'include',
          body: formData
        });

        if (!uploadRes.ok) {
          const errData = await uploadRes.json();
          throw new Error(errData.error || 'Upload failed');
        }

        const uploadData = await uploadRes.json();
        const docId = uploadData.document.docId;

        await sendChatMessageWithAttachment(targetUserId, `Shared file: ${file.name}`, { docId });
        appendLog(`[CHAT FILE] '${file.name}' shared with ${targetUserId} successfully!`, 'log-success');
      } catch (err) {
        alert(`File share failed: ${err.message}`);
        appendLog(`[CHAT FILE] Error sharing file: ${err.message}`, 'log-error');
      } finally {
        chatFileInput.value = '';
      }
    });
  }

  function openChatSelectDocModal() {
    const targetUserId = chatTargetUserSelect ? chatTargetUserSelect.value : null;
    if (!targetUserId) {
      alert('Please select a chat partner first.');
      return;
    }

    if (!currentUser) return;
    const userDocs = documents.filter(d => d.ownerId === currentUser.id || (d.shares && d.shares[currentUser.id]));
    if (userDocs.length === 0) {
      alert('No documents available in your library to share.');
      return;
    }

    chatSelectDocDropdown.innerHTML = userDocs.map(d => `<option value="${d.docId}">${escapeHtml(d.name)} (${(d.size / 1024).toFixed(1)} KB)</option>`).join('');
    if (chatSelectDocModal) {
      chatSelectDocModal.classList.add('active');
      chatSelectDocModal.style.display = 'flex';
    }
  }

  function closeChatSelectDocModal() {
    if (chatSelectDocModal) {
      chatSelectDocModal.classList.remove('active');
      chatSelectDocModal.style.display = 'none';
    }
  }

  if (chatShareDocBtn) chatShareDocBtn.addEventListener('click', openChatSelectDocModal);
  if (chatSelectDocCloseBtn) chatSelectDocCloseBtn.addEventListener('click', closeChatSelectDocModal);
  if (cancelChatSelectDocBtn) cancelChatSelectDocBtn.addEventListener('click', closeChatSelectDocModal);

  if (confirmChatSelectDocBtn) {
    confirmChatSelectDocBtn.addEventListener('click', async () => {
      const docId = chatSelectDocDropdown.value;
      const targetUserId = chatTargetUserSelect ? chatTargetUserSelect.value : null;
      if (!docId || !targetUserId) return;

      const doc = documents.find(d => d.docId === docId);
      closeChatSelectDocModal();

      await sendChatMessageWithAttachment(targetUserId, `Shared file: ${doc ? doc.name : 'document'}`, { docId });
    });
  }

  async function handleShareDocInChat(docId, docName) {
    toggleChatDrawer(true);
    let targetUserId = chatTargetUserSelect ? chatTargetUserSelect.value : null;
    if (!targetUserId) {
      alert(`Chat window opened! Please select a user from the dropdown to share '${docName}'.`);
      return;
    }
    await sendChatMessageWithAttachment(targetUserId, `Shared file: ${docName}`, { docId });
  }

  async function handleRemoveSharedDoc(docId, docName) {
    if (!confirm(`Are you sure you want to remove '${docName}' from your Shared With Me library?`)) return;
    try {
      const res = await fetch('/api/documents/unshare', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({ docId })
      });
      if (res.ok) {
        appendLog(`[ACCESS CONTROL] Removed shared file '${docName}' (${docId})`, 'log-info');
        fetchDocuments();
      } else {
        const err = await res.json();
        alert(`Failed to remove shared file: ${err.error}`);
      }
    } catch (e) {
      alert(`Error removing shared file: ${e.message}`);
    }
  }

  // ── Unsend and Forward Handlers ──────────────────────────────────────────

  const chatForwardModal      = document.getElementById('chatForwardModal');
  const chatForwardCloseBtn   = document.getElementById('chatForwardCloseBtn');
  const cancelChatForwardBtn  = document.getElementById('cancelChatForwardBtn');
  const confirmChatForwardBtn = document.getElementById('confirmChatForwardBtn');
  const chatForwardUserSelect = document.getElementById('chatForwardUserSelect');
  const chatForwardPreview    = document.getElementById('chatForwardPreview');
  let selectedForwardMsgId    = null;

  function openForwardModal(msgId) {
    const msg = chatMessages.find(m => m.id === msgId);
    if (!msg || !currentUser) return;

    selectedForwardMsgId = msgId;
    const targets = allUsers.filter(u => u.id !== currentUser.id);

    if (targets.length === 0) {
      alert('No other users available to forward to.');
      return;
    }

    chatForwardUserSelect.innerHTML = targets.map(u => `<option value="${u.id}">${u.name} (${u.id})</option>`).join('');

    let previewContent = escapeHtml(msg.text || '');
    if (msg.fileAttachment) {
      previewContent += ` <div style="margin-top:0.25rem; font-weight:600;"><i class="fa-solid fa-paperclip"></i> Attachment: ${escapeHtml(msg.fileAttachment.fileName)}</div>`;
    }
    chatForwardPreview.innerHTML = previewContent;

    if (chatForwardModal) {
      chatForwardModal.classList.add('active');
      chatForwardModal.style.display = 'flex';
    }
  }

  function closeForwardModal() {
    selectedForwardMsgId = null;
    if (chatForwardModal) {
      chatForwardModal.classList.remove('active');
      chatForwardModal.style.display = 'none';
    }
  }

  if (chatForwardCloseBtn) chatForwardCloseBtn.addEventListener('click', closeForwardModal);
  if (cancelChatForwardBtn) cancelChatForwardBtn.addEventListener('click', closeForwardModal);

  if (confirmChatForwardBtn) {
    confirmChatForwardBtn.addEventListener('click', async () => {
      if (!selectedForwardMsgId || !currentUser) return;
      const targetUserId = chatForwardUserSelect ? chatForwardUserSelect.value : null;
      if (!targetUserId) return;

      const msg = chatMessages.find(m => m.id === selectedForwardMsgId);
      closeForwardModal();
      if (!msg) return;

      const textToSend = msg.text ? `[Forwarded] ${msg.text}` : (msg.fileAttachment ? `Shared file: ${msg.fileAttachment.fileName}` : '');
      const fileAtt = msg.fileAttachment ? { docId: msg.fileAttachment.docId } : null;

      await sendChatMessageWithAttachment(targetUserId, textToSend, fileAtt);

      if (chatTargetUserSelect) {
        chatTargetUserSelect.value = targetUserId;
        renderChatMessages();
      }

      appendLog(`[CHAT] Forwarded message/file to ${targetUserId}`, 'log-info');
    });
  }

  async function handleUnsendMessage(msgId) {
    if (!confirm('Are you sure you want to unsend this message?')) return;
    try {
      const res = await fetch('/api/chat/delete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({ messageId: msgId })
      });

      if (res.ok) {
        chatMessages = chatMessages.filter(m => m.id !== msgId);
        renderChatMessages();
        appendLog(`[CHAT] Un-sent message '${msgId}'`, 'log-info');
      } else {
        if (ws && ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({
            type: 'DELETE_CHAT_MESSAGE',
            payload: { messageId: msgId, senderId: currentUser.id }
          }));
          chatMessages = chatMessages.filter(m => m.id !== msgId);
          renderChatMessages();
        }
      }
    } catch (err) {
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({
          type: 'DELETE_CHAT_MESSAGE',
          payload: { messageId: msgId, senderId: currentUser.id }
        }));
        chatMessages = chatMessages.filter(m => m.id !== msgId);
        renderChatMessages();
      }
    }
  }

  // ── Bind Chat Event Listeners ─────────────────────────────────────────────

  if (floatingChatBtn) floatingChatBtn.addEventListener('click', () => toggleChatDrawer());
  if (openChatNavBtn) openChatNavBtn.addEventListener('click', () => toggleChatDrawer(true));
  if (closeChatBtn) closeChatBtn.addEventListener('click', () => toggleChatDrawer(false));
  if (chatTargetUserSelect) chatTargetUserSelect.addEventListener('change', () => renderChatMessages());
  if (chatForm) chatForm.addEventListener('submit', handleSendChatMessage);

  // Delegated click listener for Chat Message List (view, download, unsend, forward)
  if (chatMessageList) {
    chatMessageList.addEventListener('click', (e) => {
      const viewBtn = e.target.closest('.chat-view-file-btn');
      if (viewBtn) {
        const docId = viewBtn.getAttribute('data-docid');
        if (docId) handleViewDocument(docId);
        return;
      }
      const dlBtn = e.target.closest('.chat-dl-file-btn');
      if (dlBtn) {
        const docId = dlBtn.getAttribute('data-docid');
        if (docId) handleStreamDownload(docId);
        return;
      }
      const unsendBtn = e.target.closest('.chat-unsend-msg-btn');
      if (unsendBtn) {
        const msgId = unsendBtn.getAttribute('data-msgid');
        if (msgId) handleUnsendMessage(msgId);
        return;
      }
      const forwardBtn = e.target.closest('.chat-forward-msg-btn');
      if (forwardBtn) {
        const msgId = forwardBtn.getAttribute('data-msgid');
        if (msgId) openForwardModal(msgId);
        return;
      }
    });
  }

  // ── Initialization ────────────────────────────────────────────────────────

  checkSession();
});
