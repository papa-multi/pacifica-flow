const WebSocket = global.WebSocket || require("ws");

function normalizeWallet(value) {
  const text = String(value || "").trim();
  return text || "";
}

function deriveRpcWsUrl(rpcUrl = "") {
  const raw = String(rpcUrl || "").trim();
  if (!raw) return "";
  if (raw.startsWith("wss://") || raw.startsWith("ws://")) return raw;
  if (raw.startsWith("https://")) return `wss://${raw.slice("https://".length)}`;
  if (raw.startsWith("http://")) return `ws://${raw.slice("http://".length)}`;
  return raw;
}

function accountEntryFromKey(key, index, source = "message") {
  if (typeof key === "string") {
    return {
      pubkey: normalizeWallet(key),
      signer: index === 0,
      writable: false,
      source,
      index,
    };
  }
  if (!key || typeof key !== "object") {
    return {
      pubkey: "",
      signer: false,
      writable: false,
      source,
      index,
    };
  }
  return {
    pubkey: normalizeWallet(key.pubkey || key.address || key.key),
    signer: Boolean(key.signer),
    writable: Boolean(key.writable),
    source,
    index,
  };
}

function collectAccountEntries(tx = {}) {
  const message =
    tx && tx.transaction && tx.transaction.message && typeof tx.transaction.message === "object"
      ? tx.transaction.message
      : {};
  const staticKeys = Array.isArray(message.accountKeys) ? message.accountKeys : [];
  const entries = staticKeys.map((key, index) => accountEntryFromKey(key, index, "message"));
  const loadedAddresses = tx && tx.meta && tx.meta.loadedAddresses ? tx.meta.loadedAddresses : {};
  const writable = Array.isArray(loadedAddresses.writable) ? loadedAddresses.writable : [];
  const readonly = Array.isArray(loadedAddresses.readonly) ? loadedAddresses.readonly : [];
  writable.forEach((key) => {
    entries.push(accountEntryFromKey(key, entries.length, "loaded_writable"));
  });
  readonly.forEach((key) => {
    entries.push(accountEntryFromKey(key, entries.length, "loaded_readonly"));
  });
  return entries;
}

function parseInstructionAccountIndexes(instruction = {}) {
  if (Array.isArray(instruction.accounts)) return instruction.accounts;
  if (Array.isArray(instruction.accountKeyIndexes)) return instruction.accountKeyIndexes;
  return [];
}

function instructionProgramId(instruction = {}, accountEntries = []) {
  if (instruction && typeof instruction.programId === "string") return normalizeWallet(instruction.programId);
  const index = Number(instruction && instruction.programIdIndex);
  if (!Number.isFinite(index) || index < 0) return "";
  const entry = accountEntries[index];
  return normalizeWallet(entry && entry.pubkey);
}

function appendCandidate(map, wallet, candidate = {}) {
  const normalized = normalizeWallet(wallet);
  if (!normalized) return;
  const score = Number(candidate.score || 0);
  const next = {
    wallet: normalized,
    method: String(candidate.method || "unknown").trim() || "unknown",
    confidence: String(candidate.confidence || "low").trim() || "low",
    score,
    programId: normalizeWallet(candidate.programId || "") || null,
  };
  const prev = map.get(normalized);
  if (!prev || score > Number(prev.score || 0)) {
    map.set(normalized, next);
  }
}

function extractWalletCandidates(tx = {}, options = {}) {
  const maxCandidates = Math.max(1, Number(options.maxCandidates || 6));
  const programIds = new Set(
    (Array.isArray(options.programIds) ? options.programIds : [])
      .map((value) => normalizeWallet(value))
      .filter(Boolean)
  );
  const accountEntries = collectAccountEntries(tx);
  const candidates = new Map();
  const message =
    tx && tx.transaction && tx.transaction.message && typeof tx.transaction.message === "object"
      ? tx.transaction.message
      : {};
  const staticKeys = Array.isArray(message.accountKeys) ? message.accountKeys : [];
  let instructionMatchCount = 0;

  staticKeys.forEach((key, index) => {
    const entry = accountEntryFromKey(key, index, "message");
    if (!entry.pubkey || !entry.signer) return;
    appendCandidate(candidates, entry.pubkey, {
      method: index === 0 ? "fee_payer" : "signer",
      confidence: index === 0 ? "very_high" : "high",
      score: index === 0 ? 100 : 90,
      programId: null,
    });
  });

  const ingestInstruction = (instruction) => {
    const programId = instructionProgramId(instruction, accountEntries);
    if (programIds.size > 0 && (!programId || !programIds.has(programId))) return;
    instructionMatchCount += 1;
    const indexes = parseInstructionAccountIndexes(instruction);
    indexes.forEach((indexValue, accountIdx) => {
      const index = Number(indexValue);
      if (!Number.isFinite(index) || index < 0) return;
      const entry = accountEntries[index];
      if (!entry || !entry.pubkey) return;
      if (programIds.has(entry.pubkey)) return;
      if (entry.signer) {
        appendCandidate(candidates, entry.pubkey, {
          method: accountIdx === 0 ? "instruction_signer_primary" : "instruction_signer",
          confidence: accountIdx === 0 ? "very_high" : "high",
          score: accountIdx === 0 ? 98 : 88,
          programId,
        });
        return;
      }
      if (entry.writable || entry.source === "loaded_writable") {
        appendCandidate(candidates, entry.pubkey, {
          method: "instruction_writable_account",
          confidence: "medium",
          score: 70,
          programId,
        });
      }
    });
  };

  (Array.isArray(message.instructions) ? message.instructions : []).forEach(ingestInstruction);
  const innerInstructions = Array.isArray(tx && tx.meta && tx.meta.innerInstructions)
    ? tx.meta.innerInstructions
    : [];
  innerInstructions.forEach((group) => {
    const instructions = Array.isArray(group && group.instructions) ? group.instructions : [];
    instructions.forEach(ingestInstruction);
  });

  return {
    candidates: Array.from(candidates.values())
      .sort((a, b) => Number(b.score || 0) - Number(a.score || 0))
      .slice(0, maxCandidates),
    instructionMatchCount,
  };
}

class SolanaLogsTriggerMonitor {
  constructor(options = {}) {
    this.enabled = Boolean(options.enabled);
    this.logger = options.logger || console;
    this.rpcClient = options.rpcClient || null;
    this.triggerStore = options.triggerStore || null;
    this.wsUrl = deriveRpcWsUrl(options.wsUrl || options.rpcUrl || "");
    this.programIds = Array.from(
      new Set(
        (Array.isArray(options.programIds) ? options.programIds : [])
          .map((item) => normalizeWallet(item))
          .filter(Boolean)
      )
    );
    this.maxConcurrentTx = Math.max(1, Number(options.maxConcurrentTx || 4));
    this.maxWalletCandidates = Math.max(1, Number(options.maxWalletCandidates || 6));
    this.txTimeoutMs = Math.max(1000, Number(options.txTimeoutMs || 10000));
    this.signatureTtlMs = Math.max(60000, Number(options.signatureTtlMs || 10 * 60 * 1000));
    this.includeErroredTx = Boolean(options.includeErroredTx);

    this.ws = null;
    this.shouldRun = false;
    this.reconnectTimer = null;
    this.inFlight = 0;
    this.queue = [];
    this.queuedSet = new Set();
    this.seenSignatures = new Map();
    this.rpcRequestId = 0;
    this.pendingSubscribeIds = new Map();
    this.subscriptionIds = new Map();

    this.status = {
      enabled: this.enabled && Boolean(this.wsUrl) && this.programIds.length > 0,
      wsUrl: this.wsUrl || null,
      subscriptions: 0,
      connection: "idle",
      notifications: 0,
      queuedSignatures: 0,
      txFetches: 0,
      txResolved: 0,
      txSkippedErrored: 0,
      triggeredWallets: 0,
      candidateWallets: 0,
      decodedInstructionMatches: 0,
      lastNotificationAt: null,
      lastTriggerAt: null,
      lastResolvedSignature: null,
      lastErrorAt: null,
      lastError: null,
    };
  }

  start() {
    if (!this.status.enabled || this.shouldRun) return;
    this.shouldRun = true;
    this.connect();
  }

  stop() {
    this.shouldRun = false;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.ws) {
      try {
        this.ws.close();
      } catch (_error) {
        // ignore
      }
    }
    this.ws = null;
    this.status.connection = "stopped";
  }

  connect() {
    if (!this.shouldRun || !this.status.enabled) return;
    this.status.connection = "connecting";
    const ws = new WebSocket(this.wsUrl);
    this.ws = ws;

    ws.on("open", () => {
      this.status.connection = "open";
      this.pendingSubscribeIds.clear();
      this.subscriptionIds.clear();
      this.programIds.forEach((programId) => {
        const id = ++this.rpcRequestId;
        this.pendingSubscribeIds.set(id, programId);
        ws.send(
          JSON.stringify({
            jsonrpc: "2.0",
            id,
            method: "logsSubscribe",
            params: [{ mentions: [programId] }, { commitment: "confirmed" }],
          })
        );
      });
    });

    ws.on("message", (buf) => {
      this.handleMessage(buf.toString());
    });

    ws.on("error", (error) => {
      this.status.lastErrorAt = Date.now();
      this.status.lastError = String(error && error.message ? error.message : error || "ws_error");
      this.status.connection = "error";
    });

    ws.on("close", () => {
      this.ws = null;
      this.status.connection = this.shouldRun ? "closed" : "stopped";
      this.scheduleReconnect();
    });
  }

  scheduleReconnect() {
    if (!this.shouldRun || this.reconnectTimer) return;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, 2000);
  }

  handleMessage(text) {
    let payload = null;
    try {
      payload = JSON.parse(text);
    } catch (_error) {
      return;
    }
    if (!payload || typeof payload !== "object") return;
    if (Number.isFinite(Number(payload.id)) && payload.result !== undefined) {
      const reqId = Number(payload.id);
      const programId = this.pendingSubscribeIds.get(reqId);
      if (programId) {
        this.pendingSubscribeIds.delete(reqId);
        this.subscriptionIds.set(Number(payload.result), programId);
        this.status.subscriptions = this.subscriptionIds.size;
      }
      return;
    }
    if (payload.method !== "logsNotification") return;
    const result = payload.params && payload.params.result ? payload.params.result : {};
    const value = result && result.value ? result.value : {};
    const signature = normalizeWallet(value.signature);
    if (!signature) return;
    this.status.notifications += 1;
    this.status.lastNotificationAt = Date.now();
    this.enqueueSignature(signature, {
      slot: Number(result.context && result.context.slot ? result.context.slot : 0),
      err: value.err || null,
      logs: Array.isArray(value.logs) ? value.logs : [],
      subscriptionId: Number(payload.params && payload.params.subscription ? payload.params.subscription : 0) || null,
    });
  }

  cleanupSeenSignatures(now = Date.now()) {
    const cutoff = now - this.signatureTtlMs;
    for (const [signature, seenAt] of this.seenSignatures.entries()) {
      if (Number(seenAt || 0) < cutoff) this.seenSignatures.delete(signature);
    }
  }

  enqueueSignature(signature, meta = {}) {
    const normalized = normalizeWallet(signature);
    if (!normalized || !this.rpcClient || typeof this.rpcClient.getTransaction !== "function") return;
    const now = Date.now();
    this.cleanupSeenSignatures(now);
    if (this.seenSignatures.has(normalized) || this.queuedSet.has(normalized)) return;
    this.queuedSet.add(normalized);
    this.queue.push({
      signature: normalized,
      at: now,
      meta,
    });
    this.status.queuedSignatures = this.queue.length;
    this.drainQueue();
  }

  drainQueue() {
    while (this.inFlight < this.maxConcurrentTx && this.queue.length > 0) {
      const item = this.queue.shift();
      if (!item) continue;
      this.queuedSet.delete(item.signature);
      this.inFlight += 1;
      this.status.queuedSignatures = this.queue.length;
      this.resolveSignature(item)
        .catch((error) => {
          this.status.lastErrorAt = Date.now();
          this.status.lastError = String(error && error.message ? error.message : error || "tx_trigger_failed");
        })
        .finally(() => {
          this.inFlight = Math.max(0, this.inFlight - 1);
          this.drainQueue();
        });
    }
  }

  async resolveSignature(item) {
    this.status.txFetches += 1;
    const tx = await this.rpcClient.getTransaction(item.signature, {
      timeoutMs: this.txTimeoutMs,
      cost: 1,
      disableCache: false,
    });
    if (!tx) {
      this.seenSignatures.set(item.signature, Date.now());
      return;
    }
    const txErrored = Boolean(tx && tx.meta && tx.meta.err);
    if (txErrored && !this.includeErroredTx) {
      this.status.txSkippedErrored += 1;
      this.seenSignatures.set(item.signature, Date.now());
      return;
    }
    const programId =
      item.meta && item.meta.subscriptionId && this.subscriptionIds.has(item.meta.subscriptionId)
        ? this.subscriptionIds.get(item.meta.subscriptionId)
        : null;
    const { candidates, instructionMatchCount } = extractWalletCandidates(tx, {
      maxCandidates: this.maxWalletCandidates,
      programIds: programId ? [programId] : this.programIds,
    });
    const at = Number(
      (tx.blockTime ? tx.blockTime * 1000 : 0) ||
        (tx.meta && tx.meta.blockTime ? tx.meta.blockTime * 1000 : 0) ||
        Date.now()
    );
    this.status.decodedInstructionMatches += instructionMatchCount;
    this.status.candidateWallets += candidates.length;
    candidates.forEach((candidate) => {
      if (!this.triggerStore || typeof this.triggerStore.append !== "function") return;
      const appended = this.triggerStore.append({
        wallet: candidate.wallet,
        at,
        slot: Number(item.meta && item.meta.slot ? item.meta.slot : 0) || null,
        source: "solana_logs_ws",
        reason: candidate.method || "solana_logs_ws",
        method: candidate.method || "solana_logs_ws",
        confidence: candidate.confidence || "medium",
        signature: item.signature,
        programId: candidate.programId || programId || null,
      });
      if (appended) {
        this.status.triggeredWallets += 1;
        this.status.lastTriggerAt = at;
      }
    });
    this.status.txResolved += 1;
    this.status.lastResolvedSignature = item.signature;
    this.seenSignatures.set(item.signature, Date.now());
  }

  getStatus() {
    return {
      ...this.status,
      queuedSignatures: this.queue.length,
      inFlight: this.inFlight,
    };
  }
}

module.exports = {
  SolanaLogsTriggerMonitor,
  deriveRpcWsUrl,
};
