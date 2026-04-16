const fs = require("fs");
const path = require("path");
const readline = require("readline");
const {
  appendLine,
  ensureDir,
  readJson,
  writeJsonAtomic,
} = require("../pipeline/utils");
const { assignShardByKey, normalizeShardItems } = require("./sharding");
const {
  extractCandidateWalletsFromTransaction,
  extractDepositWalletsFromTransaction,
  normalizeAddress,
} = require("./wallet_extractor");

const STATE_VERSION = 5;
const RAW_TX_RECOVERY_VERSION = 1;
const DEFAULT_START_TIME_MS = Date.UTC(2025, 4, 31, 23, 2, 32, 0); // 2025-05-31T23:02:32Z
const DEFAULT_PACIFICA_VAULT = "72R843XwZxqWhsJceARQQTTbYtWy6Zw9et2YV4FpRHTa";

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function normalizeProgramIds(list = []) {
  return Array.from(
    new Set(
      (Array.isArray(list) ? list : [])
        .map((value) => normalizeAddress(value))
        .filter(Boolean)
    )
  );
}

function normalizeScanTargets({
  programIds = [],
  depositVaults = [],
  scanTargets = [],
  discoveryType = "deposit_only",
} = {}) {
  const explicitTargets = normalizeShardItems(
    (Array.isArray(scanTargets) ? scanTargets : []).map((target, idx) => ({
      id: String((target && target.id) || `target_${idx + 1}`),
      address: normalizeAddress(target && target.address),
      type: String((target && target.type) || "program").trim().toLowerCase(),
      label: String((target && target.label) || "").trim(),
    }))
  ).filter((target) => target.address);

  if (explicitTargets.length) {
    return explicitTargets.map((target) => ({
      ...target,
      label: target.label || `${target.type}:${target.address.slice(0, 8)}`,
    }));
  }

  const discovered = [];
  normalizeProgramIds(depositVaults).forEach((address, idx) => {
    discovered.push({
      id: `vault_${idx + 1}`,
      address,
      type: "deposit_vault",
      label: `deposit_vault:${address.slice(0, 8)}`,
    });
  });
  if (String(discoveryType || "deposit_only").toLowerCase() !== "deposit_only" || !discovered.length) {
    normalizeProgramIds(programIds).forEach((address, idx) => {
      if (discovered.some((row) => row.address === address)) return;
      discovered.push({
        id: `program_${idx + 1}`,
        address,
        type: "program",
        label: `program:${address.slice(0, 8)}`,
      });
    });
  }
  return discovered;
}

function emptyProgramState(programId) {
  return {
    programId,
    scanTargetId: null,
    scanTargetType: "program",
    scanTargetLabel: null,
    preferredRpcShardId: null,
    preferredRpcShardIndex: null,
    before: null,
    done: false,
    scannedPages: 0,
    scannedSignatures: 0,
    scannedTransactions: 0,
    discoveredWallets: 0,
    firstSlot: null,
    lastSlot: null,
    firstBlockTimeMs: null,
    lastBlockTimeMs: null,
    lastSignature: null,
    lastScanAt: null,
    lastError: null,
  };
}

function emptyState(programIds, options = {}) {
  const programs = {};
  normalizeProgramIds(programIds).forEach((id) => {
    programs[id] = emptyProgramState(id);
  });

  return {
    version: STATE_VERSION,
    mode: options.mode === "live" ? "live" : "backfill",
    startTimeMs: Number.isFinite(Number(options.startTimeMs))
      ? Number(options.startTimeMs)
      : DEFAULT_START_TIME_MS,
    endTimeMs: Number.isFinite(Number(options.endTimeMs))
      ? Number(options.endTimeMs)
      : null,
    programCursor: 0,
    programs,
    recentSignatures: [],
    pendingTransactions: {},
    walletCount: 0,
    firstDepositAtMs: null,
    backfillCompletedAt: null,
    liveModeSince: null,
    lastModeTransitionAt: null,
    rawRecoveryVersion: 0,
    rawRecoveryAt: null,
    lastScanAt: null,
    lastValidationAt: null,
    lastCheckpointAt: null,
    lastError: null,
  };
}

function nowMs() {
  return Date.now();
}

function blockTimeToMs(blockTime) {
  const value = Number(blockTime);
  if (!Number.isFinite(value) || value <= 0) return null;
  return value * 1000;
}

function formatDurationMs(value) {
  const ms = Number(value);
  if (!Number.isFinite(ms) || ms < 0) return "n/a";
  if (ms === 0) return "0s";

  const totalSec = Math.floor(ms / 1000);
  const days = Math.floor(totalSec / 86400);
  const hours = Math.floor((totalSec % 86400) / 3600);
  const mins = Math.floor((totalSec % 3600) / 60);
  const secs = totalSec % 60;
  if (days > 0) return `${days}d ${hours}h ${mins}m`;
  if (hours > 0) return `${hours}h ${mins}m ${secs}s`;
  if (mins > 0) return `${mins}m ${secs}s`;
  return `${secs}s`;
}

function formatFixedKv(prefix, orderedEntries = []) {
  const parts = orderedEntries.map(([key, value]) => `${key}=${value}`);
  return `${prefix} ${parts.join(" ")}`;
}

function buildDepositEvidenceKey(evidence = {}) {
  const signature = String(evidence.signature || "");
  const scope = String(evidence.scope || "");
  const destination = String(evidence.destination || "");
  const wallet = String(evidence.wallet || "");
  const selectedBy = String(evidence.selectedBy || "");
  return [signature, scope, destination, wallet, selectedBy].join("|");
}

class OnChainWalletDiscovery {
  constructor(options = {}) {
    this.rpcClient = options.rpcClient;
    this.restClient = options.restClient || null;
    this.logger = options.logger || console;

    this.dataDir = options.dataDir || path.join(process.cwd(), "data", "indexer");
    this.statePath = options.statePath || path.join(this.dataDir, "onchain_state.json");
    this.walletsPath = options.walletsPath || path.join(this.dataDir, "wallet_discovery.json");
    this.summaryPath =
      options.summaryPath || path.join(this.dataDir, "wallet_discovery_summary.json");
    this.depositWalletsPath =
      options.depositWalletsPath || path.join(this.dataDir, "deposit_wallets.json");
    this.rawSignaturesPath =
      options.rawSignaturesPath || path.join(this.dataDir, "raw_signatures.ndjson");
    this.rawTransactionsPath =
      options.rawTransactionsPath || path.join(this.dataDir, "raw_transactions.ndjson");

    this.protocolProgramIds = normalizeProgramIds(options.programIds || []);
    this.programIds = this.protocolProgramIds.slice();
    this.excludeAddresses = normalizeProgramIds(options.excludeAddresses || []);
    this.depositVaults = normalizeProgramIds(
      options.depositVaults && options.depositVaults.length
        ? options.depositVaults
        : [DEFAULT_PACIFICA_VAULT]
    );
    this.discoveryType =
      String(options.discoveryType || "deposit_only").toLowerCase() === "all_interactions"
        ? "all_interactions"
        : "deposit_only";
    this.scanTargets = normalizeScanTargets({
      programIds: this.protocolProgramIds,
      depositVaults: this.depositVaults,
      scanTargets: options.scanTargets,
      discoveryType: this.discoveryType,
    });
    if (this.scanTargets.length) {
      this.programIds = this.scanTargets.map((target) => target.address);
    }
    this.scanAddressIds = this.programIds.slice();

    this.signaturePageLimit = Math.max(
      1,
      Math.min(1000, Number(options.signaturePageLimit || 20))
    );
    this.scanPagesPerCycle = Math.max(1, Number(options.scanPagesPerCycle || 1));

    this.txConcurrencyMin = 1;
    this.txConcurrencyMax = Math.max(1, Math.min(12, Number(options.txConcurrency || 1)));
    this.txConcurrency = this.txConcurrencyMax;

    this.validationBatchSize = Math.max(0, Number(options.validationBatchSize || 20));
    this.maxTransactionsPerCycle = Math.max(1, Number(options.maxTransactionsPerCycle || 12));

    this.signatureScanCost = Math.max(0.1, Number(options.signatureScanCost || 1));
    this.transactionScanCost = Math.max(0.1, Number(options.transactionScanCost || 1));

    this.scanIntervalMs = Math.max(5000, Number(options.scanIntervalMs || 45000));
    this.checkpointIntervalMs = Math.max(500, Number(options.checkpointIntervalMs || 3000));
    this.logIntervalMs = Math.max(3000, Number(options.logIntervalMs || 30000));
    this.logFormat =
      String(options.logFormat || "kv").toLowerCase() === "json" ? "json" : "kv";

    this.maxRecentSignatures = Math.max(1000, Number(options.maxRecentSignatures || 200000));
    this.pendingRetryBaseMs = Math.max(500, Number(options.pendingRetryBaseMs || 2500));
    this.pendingRetryMaxMs = Math.max(
      this.pendingRetryBaseMs,
      Number(options.pendingRetryMaxMs || 180000)
    );
    this.pendingMaxAttempts = Math.max(1, Number(options.pendingMaxAttempts || 12));
    this.txRampQuietMs = Math.max(10000, Number(options.txRampQuietMs || 180000));
    this.txRampStepMs = Math.max(5000, Number(options.txRampStepMs || 60000));

    this.state = emptyState(this.programIds, {
      startTimeMs: options.startTimeMs,
      endTimeMs: options.endTimeMs,
      mode: options.mode,
    });
    this.configuredStartTimeMs = Number.isFinite(Number(options.startTimeMs))
      ? Number(options.startTimeMs)
      : null;

    this.wallets = {};

    this.runtime = {
      running: false,
      inScan: false,
      timer: null,
      nextRunAt: null,
      nextDelayMs: null,
      lastStepDurationMs: null,
      recentSignatureSet: new Set(),
      lastCheckpointAt: 0,
      lastLogAt: 0,
      lastTx429At: 0,
      lastTxRampAt: 0,
      throughput: {
        lastAt: 0,
        lastRemainingMs: null,
        smoothedRate: null, // historical ms reduced per wall-clock ms
      },
      rawRecoveryRunning: false,
    };
  }

  load() {
    ensureDir(this.dataDir);

    const loadedState = readJson(this.statePath, null);
    if (
      loadedState &&
      (
        loadedState.version === STATE_VERSION ||
        loadedState.version === 4 ||
        loadedState.version === 3 ||
        loadedState.version === 2 ||
        loadedState.version === 1
      )
    ) {
      this.state = {
        ...emptyState(this.programIds, {
          startTimeMs: loadedState.startTimeMs,
          endTimeMs: loadedState.endTimeMs,
          mode: loadedState.mode,
        }),
        ...loadedState,
        version: STATE_VERSION,
      };
    } else {
      this.state = emptyState(this.programIds, {
        startTimeMs: this.state.startTimeMs,
        endTimeMs: this.state.endTimeMs,
        mode: this.state.mode,
      });
    }

    if (!Number.isFinite(Number(this.state.startTimeMs)) || Number(this.state.startTimeMs) <= 0) {
      this.state.startTimeMs = DEFAULT_START_TIME_MS;
    } else {
      this.state.startTimeMs = Number(this.state.startTimeMs);
    }
    if (
      Number.isFinite(this.configuredStartTimeMs) &&
      this.configuredStartTimeMs > 0 &&
      this.configuredStartTimeMs < this.state.startTimeMs
    ) {
      this.state.startTimeMs = this.configuredStartTimeMs;
    }

    if (!Number.isFinite(Number(this.state.endTimeMs)) || Number(this.state.endTimeMs) <= 0) {
      this.state.endTimeMs = null;
    } else {
      this.state.endTimeMs = Number(this.state.endTimeMs);
    }

    if (this.state.mode === "live") {
      const fallbackAt =
        Number(this.state.liveModeSince || this.state.backfillCompletedAt || this.state.lastScanAt || 0) ||
        nowMs();
      this.state.backfillCompletedAt = Number(this.state.backfillCompletedAt || 0) || fallbackAt;
      this.state.liveModeSince = Number(this.state.liveModeSince || 0) || fallbackAt;
      this.state.lastModeTransitionAt =
        Number(this.state.lastModeTransitionAt || 0) || this.state.liveModeSince;
    }

    if (!this.state.pendingTransactions || typeof this.state.pendingTransactions !== "object") {
      this.state.pendingTransactions = {};
    }

    const loadedWallets = readJson(this.walletsPath, { wallets: {} });
    this.wallets =
      loadedWallets && loadedWallets.wallets && typeof loadedWallets.wallets === "object"
        ? loadedWallets.wallets
        : {};

    this.configurePrograms(this.programIds);
    this.reopenBackfillIfWindowNotReached();

    this.state.recentSignatures = Array.isArray(this.state.recentSignatures)
      ? this.state.recentSignatures.filter((sig) => typeof sig === "string")
      : [];

    this.runtime.recentSignatureSet = new Set(this.state.recentSignatures);
    this.state.walletCount = Object.keys(this.wallets).length;

    this.save();

    return {
      state: this.state,
      walletCount: this.state.walletCount,
    };
  }

  reopenBackfillIfWindowNotReached() {
    if (this.state.mode !== "backfill") return false;

    const programRows = Object.values(this.state.programs || {});
    if (!programRows.length) return false;

    const allDone = programRows.every((row) => Boolean(row && row.done));
    if (!allDone) return false;

    const startMs = Number(this.state.startTimeMs || 0);
    if (!Number.isFinite(startMs) || startMs <= 0) return false;

    const oldestReached = programRows
      .map((row) => Number(row && row.firstBlockTimeMs ? row.firstBlockTimeMs : 0))
      .filter((v) => Number.isFinite(v) && v > 0)
      .sort((a, b) => a - b)[0];

    // Backfill is truly complete when oldest reached <= requested start.
    if (Number.isFinite(oldestReached) && oldestReached <= startMs) return false;

    const candidate = programRows
      .slice()
      .sort((a, b) => {
        const aBt = Number(a && a.firstBlockTimeMs ? a.firstBlockTimeMs : Number.MAX_SAFE_INTEGER);
        const bBt = Number(b && b.firstBlockTimeMs ? b.firstBlockTimeMs : Number.MAX_SAFE_INTEGER);
        if (aBt !== bBt) return aBt - bBt;
        return String(a.programId || "").localeCompare(String(b.programId || ""));
      })[0];

    if (!candidate || !candidate.programId || !this.state.programs[candidate.programId]) return false;

    const row = this.state.programs[candidate.programId];
    row.done = false;
    row.lastError = null;

    this.logger.warn(
      `[onchain] resume backfill: checkpoint marked done before start window; reopened program=${candidate.programId} before=${
        row.before ? String(row.before).slice(0, 16) : "n/a"
      } start=${new Date(startMs).toISOString()} oldest_seen=${
        Number.isFinite(oldestReached) ? new Date(oldestReached).toISOString() : "n/a"
      }`
    );

    return true;
  }

  save() {
    const summary = this.buildDiscoverySummary();
    writeJsonAtomic(this.statePath, this.state);
    writeJsonAtomic(this.walletsPath, {
      updatedAt: nowMs(),
      wallets: this.wallets,
    });
    writeJsonAtomic(this.summaryPath, summary);
    writeJsonAtomic(this.depositWalletsPath, {
      updatedAt: nowMs(),
      total: Object.keys(this.wallets || {}).length,
      wallets: this.listDepositWalletAddresses(),
    });
  }

  buildDiscoverySummary() {
    const now = nowMs();
    const walletEntries = Object.entries(this.wallets || {});
    const oneHourAgo = now - 60 * 60 * 1000;
    const sixHoursAgo = now - 6 * 60 * 60 * 1000;
    const twentyFourHoursAgo = now - 24 * 60 * 60 * 1000;
    let confirmedWallets = 0;
    let new1h = 0;
    let new6h = 0;
    let new24h = 0;
    let latestNewWalletAt = 0;
    const latestWallets = [];

    for (const [wallet, row] of walletEntries) {
      const validationStatus = String(
        (row && row.validation && row.validation.status) || ""
      ).trim().toLowerCase();
      if (validationStatus === "confirmed") {
        confirmedWallets += 1;
      }

      const firstSeenAt = Number((row && row.firstSeenAt) || 0) || 0;
      if (!Number.isFinite(firstSeenAt) || firstSeenAt <= 0) continue;
      if (firstSeenAt >= oneHourAgo) new1h += 1;
      if (firstSeenAt >= sixHoursAgo) new6h += 1;
      if (firstSeenAt >= twentyFourHoursAgo) new24h += 1;
      if (firstSeenAt > latestNewWalletAt) latestNewWalletAt = firstSeenAt;
      latestWallets.push({
        wallet,
        firstSeenAt,
        validationStatus: validationStatus || "unknown",
      });
    }

    latestWallets.sort((a, b) => Number(b.firstSeenAt || 0) - Number(a.firstSeenAt || 0));
    const progress = this.computeProgress();
    const lastScanAt = Number(this.state.lastScanAt || 0) || null;
    const staleAfterMs = Math.max(this.scanIntervalMs * 3, 5 * 60 * 1000);
    let status = "idle";
    if (lastScanAt) {
      status = now - lastScanAt <= staleAfterMs ? (this.state.mode === "live" ? "live" : "catch_up") : "stale";
    }

    return {
      updatedAt: now,
      status,
      mode: String(this.state.mode || "backfill"),
      scanIntervalMs: this.scanIntervalMs,
      staleAfterMs,
      lastScanAt,
      lastValidationAt: Number(this.state.lastValidationAt || 0) || null,
      lastCheckpointAt: Number(this.state.lastCheckpointAt || 0) || null,
      backfillCompletedAt: Number(this.state.backfillCompletedAt || 0) || null,
      liveModeSince: Number(this.state.liveModeSince || 0) || null,
      latestNewWalletAt: latestNewWalletAt || null,
      walletCount: walletEntries.length,
      confirmedWallets,
      historyExhausted: Boolean(progress && Number(progress.percentComplete || 0) >= 99.999),
      percentComplete: Number(progress && progress.percentComplete) || 0,
      pendingTransactions: Object.keys(this.state.pendingTransactions || {}).length,
      newWallets: {
        last1h: new1h,
        last6h: new6h,
        last24h: new24h,
      },
      latestWallets: latestWallets.slice(0, 5),
    };
  }

  maybeCheckpoint(force = false) {
    const now = nowMs();
    if (!force && now - Number(this.runtime.lastCheckpointAt || 0) < this.checkpointIntervalMs) {
      return false;
    }

    this.state.lastCheckpointAt = now;
    this.runtime.lastCheckpointAt = now;
    this.save();
    return true;
  }

  noteDepositTime(blockTimeMs) {
    const value = Number(blockTimeMs || 0);
    if (!Number.isFinite(value) || value <= 0) return;
    if (!this.state.firstDepositAtMs || value < Number(this.state.firstDepositAtMs)) {
      this.state.firstDepositAtMs = value;
    }
  }

  async recoverWalletsFromRawTransactions(options = {}) {
    if (this.runtime.rawRecoveryRunning) {
      return {
        ok: true,
        skipped: true,
        reason: "recovery_already_running",
        newWallets: [],
      };
    }

    const force = Boolean(options.force);
    if (
      !force &&
      Number(this.state.rawRecoveryVersion || 0) >= RAW_TX_RECOVERY_VERSION
    ) {
      return {
        ok: true,
        skipped: true,
        reason: "recovery_up_to_date",
        newWallets: [],
      };
    }

    if (!this.rawTransactionsPath || !fs.existsSync(this.rawTransactionsPath)) {
      this.state.rawRecoveryVersion = RAW_TX_RECOVERY_VERSION;
      this.state.rawRecoveryAt = nowMs();
      this.maybeCheckpoint(true);
      return {
        ok: true,
        skipped: true,
        reason: "raw_transactions_missing",
        newWallets: [],
      };
    }

    this.runtime.rawRecoveryRunning = true;
    const newWalletSet = new Set();
    const out = {
      ok: true,
      skipped: false,
      lines: 0,
      parsed: 0,
      depositMatches: 0,
      walletsDiscovered: 0,
      walletsUpdated: 0,
      newWallets: [],
    };

    try {
      const rl = readline.createInterface({
        input: fs.createReadStream(this.rawTransactionsPath, { encoding: "utf8" }),
        crlfDelay: Infinity,
      });

      for await (const line of rl) {
        const text = String(line || "").trim();
        if (!text) continue;
        out.lines += 1;

        let row;
        try {
          row = JSON.parse(text);
        } catch (_error) {
          continue;
        }

        const tx = row && row.tx ? row.tx : null;
        if (!tx) continue;
        out.parsed += 1;

        const signature = row && row.signature ? String(row.signature) : null;
        const programId = row && row.programId ? String(row.programId) : null;
        const slot = Number.isFinite(Number(row && row.slot)) ? Number(row.slot) : null;
        const blockTimeMs =
          Number.isFinite(Number(row && row.blockTimeMs)) ? Number(row.blockTimeMs) : null;

        const extracted =
          this.discoveryType === "deposit_only"
            ? extractDepositWalletsFromTransaction(tx, {
                programIds:
                  this.protocolProgramIds && this.protocolProgramIds.length
                    ? this.protocolProgramIds
                    : this.programIds,
                vaults: this.depositVaults,
                exclude: this.excludeAddresses,
              })
            : extractCandidateWalletsFromTransaction(tx, {
                programIds:
                  this.protocolProgramIds && this.protocolProgramIds.length
                    ? this.protocolProgramIds
                    : this.programIds,
                exclude: this.excludeAddresses,
              });

        const candidates = Array.isArray(extracted.candidates) ? extracted.candidates : [];
        const deposits = Array.isArray(extracted.deposits) ? extracted.deposits : [];
        out.depositMatches += deposits.length;

        deposits.forEach((deposit) => {
          if (!deposit || !deposit.wallet) return;
          this.upsertWalletDepositEvidence({
            wallet: deposit.wallet,
            signature,
            slot,
            blockTimeMs,
            programId,
            deposit,
          });
          this.noteDepositTime(blockTimeMs);
        });

        candidates.forEach((candidate) => {
          const isNew = this.upsertWalletEvidence({
            wallet: candidate.wallet,
            programId,
            signature,
            slot,
            blockTimeMs,
            candidate,
          });
          if (isNew) {
            out.walletsDiscovered += 1;
            newWalletSet.add(candidate.wallet);
          } else {
            out.walletsUpdated += 1;
          }
        });
      }

      out.newWallets = Array.from(newWalletSet.values());
      this.state.rawRecoveryVersion = RAW_TX_RECOVERY_VERSION;
      this.state.rawRecoveryAt = nowMs();
      this.state.walletCount = Object.keys(this.wallets || {}).length;
      this.maybeCheckpoint(true);
      return out;
    } finally {
      this.runtime.rawRecoveryRunning = false;
    }
  }

  getScanTarget(programId) {
    const normalized = normalizeAddress(programId);
    if (!normalized) return null;
    return (
      this.scanTargets.find((target) => target && target.address === normalized) || {
        id: normalized,
        address: normalized,
        type: "program",
        label: `program:${normalized.slice(0, 8)}`,
      }
    );
  }

  computeRpcShardPreference(programId) {
    const rpcStats =
      this.rpcClient && typeof this.rpcClient.getStats === "function"
        ? this.rpcClient.getStats()
        : null;
    const shards =
      rpcStats && Array.isArray(rpcStats.shards)
        ? rpcStats.shards
            .map((row, idx) => ({
              id: String((row && row.id) || `rpc_${idx + 1}`),
              index: idx,
            }))
            .filter((row) => row.id)
        : [];
    if (!shards.length) {
      return {
        preferredRpcShardId: null,
        preferredRpcShardIndex: null,
      };
    }
    const assigned = assignShardByKey(`scan:${programId}`, shards);
    return {
      preferredRpcShardId: assigned && assigned.item ? assigned.item.id : shards[0].id,
      preferredRpcShardIndex: assigned ? assigned.index : 0,
    };
  }

  configurePrograms(programIds = []) {
    const nextIds = normalizeProgramIds(programIds);
    if (nextIds.length) {
      this.programIds = nextIds;
    }

    const knownStates =
      this.state.programs && typeof this.state.programs === "object" ? this.state.programs : {};

    const merged = {};
    this.programIds.forEach((programId) => {
      const prev = knownStates[programId] || emptyProgramState(programId);
      const target = this.getScanTarget(programId);
      const rpcShardPreference = this.computeRpcShardPreference(programId);
      merged[programId] = {
        ...emptyProgramState(programId),
        ...prev,
        programId,
        scanTargetId: target && target.id ? target.id : programId,
        scanTargetType: target && target.type ? target.type : "program",
        scanTargetLabel: target && target.label ? target.label : null,
        preferredRpcShardId: rpcShardPreference.preferredRpcShardId,
        preferredRpcShardIndex: rpcShardPreference.preferredRpcShardIndex,
      };
    });

    this.state.programs = merged;
  }

  setMode(mode) {
    const nextMode = mode === "live" ? "live" : "backfill";
    const changed = this.state.mode !== nextMode;
    this.state.mode = nextMode;
    if (changed) {
      const at = nowMs();
      this.state.lastModeTransitionAt = at;
      if (nextMode === "live") {
        this.state.backfillCompletedAt = this.state.backfillCompletedAt || at;
        this.state.liveModeSince = at;
      }
    }
    this.maybeCheckpoint(true);
  }

  shouldAutoPromoteToLive(progress = null) {
    const safeProgress = progress && typeof progress === "object" ? progress : this.computeProgress();
    return (
      this.state.mode === "backfill" &&
      Number(safeProgress && safeProgress.pct) >= 100 &&
      Number(safeProgress && safeProgress.pendingTransactions) <= 0 &&
      Boolean(safeProgress && safeProgress.historyExhausted) &&
      Boolean(safeProgress && safeProgress.windowReached)
    );
  }

  maybePromoteToLive(progress = null) {
    const safeProgress = progress && typeof progress === "object" ? progress : this.computeProgress();
    if (!this.shouldAutoPromoteToLive(safeProgress)) return false;
    this.setMode("live");
    return true;
  }

  setWindow({ startTimeMs, endTimeMs }) {
    if (Number.isFinite(Number(startTimeMs))) {
      this.state.startTimeMs = Number(startTimeMs);
    }
    if (Number.isFinite(Number(endTimeMs))) {
      this.state.endTimeMs = Number(endTimeMs);
    } else if (endTimeMs === null || endTimeMs === undefined || endTimeMs === "") {
      this.state.endTimeMs = null;
    }
    this.maybeCheckpoint(true);
  }

  resetBackfill({ resetWallets = false } = {}) {
    this.state.programCursor = 0;
    Object.keys(this.state.programs || {}).forEach((programId) => {
      this.state.programs[programId] = emptyProgramState(programId);
    });

    this.state.recentSignatures = [];
    this.runtime.recentSignatureSet = new Set();
    this.state.pendingTransactions = {};

    if (resetWallets) {
      this.wallets = {};
      this.state.walletCount = 0;
    }

    this.maybeCheckpoint(true);
  }

  rememberSignature(signature) {
    if (!signature || this.runtime.recentSignatureSet.has(signature)) return;

    this.runtime.recentSignatureSet.add(signature);
    this.state.recentSignatures.push(signature);

    if (this.state.recentSignatures.length > this.maxRecentSignatures) {
      const overflow = this.state.recentSignatures.length - this.maxRecentSignatures;
      const removed = this.state.recentSignatures.splice(0, overflow);
      removed.forEach((sig) => this.runtime.recentSignatureSet.delete(sig));
    }
  }

  isSignatureSeen(signature) {
    return this.runtime.recentSignatureSet.has(signature);
  }

  appendRawSignature(record) {
    appendLine(this.rawSignaturesPath, JSON.stringify(record));
  }

  appendRawTransaction(record) {
    appendLine(this.rawTransactionsPath, JSON.stringify(record));
  }

  pickProgram() {
    const programIds = this.programIds;
    if (!programIds.length) return null;

    const start = this.state.programCursor % programIds.length;

    if (this.state.mode === "backfill") {
      for (let offset = 0; offset < programIds.length; offset += 1) {
        const idx = (start + offset) % programIds.length;
        const id = programIds[idx];
        const state = this.state.programs[id];
        if (state && !state.done) {
          this.state.programCursor = (idx + 1) % programIds.length;
          return id;
        }
      }
      return null;
    }

    const id = programIds[start];
    this.state.programCursor = (start + 1) % programIds.length;
    return id;
  }

  queuePendingTransaction(row = {}) {
    const signature = row && row.signature ? String(row.signature) : null;
    if (!signature) return false;

    if (this.state.pendingTransactions[signature]) {
      return false;
    }

    this.state.pendingTransactions[signature] = {
      signature,
      programId: row.programId || null,
      slot: Number(row.slot || 0),
      blockTimeMs: Number(row.blockTimeMs || 0) || null,
      createdAt: nowMs(),
      attempts: 0,
      lastAttemptAt: null,
      lastError: null,
      nextRetryAt: nowMs(),
    };

    return true;
  }

  removePendingTransaction(signature) {
    if (!signature) return;
    delete this.state.pendingTransactions[signature];
  }

  getDuePendingTransactions(options = {}) {
    const limit = Math.max(1, Number(options.limit || this.maxTransactionsPerCycle));
    const programId = options.programId || null;
    const now = nowMs();

    return Object.values(this.state.pendingTransactions || {})
      .filter((row) => row && row.signature)
      .filter((row) => !programId || row.programId === programId)
      .filter((row) => Number(row.nextRetryAt || 0) <= now)
      .sort((a, b) => {
        const retryDiff = Number(a.nextRetryAt || 0) - Number(b.nextRetryAt || 0);
        if (retryDiff !== 0) return retryDiff;
        return Number(a.slot || 0) - Number(b.slot || 0);
      })
      .slice(0, limit);
  }

  upsertWalletEvidence({ wallet, programId, signature, slot, blockTimeMs, candidate }) {
    const normalizedWallet = normalizeAddress(wallet);
    if (!normalizedWallet) return false;

    const record = this.wallets[normalizedWallet] || {
      wallet: normalizedWallet,
      firstSeenAt: nowMs(),
      lastSeenAt: null,
      firstSeenSignature: null,
      lastSeenSignature: null,
      firstSeenBlockTimeMs: null,
      lastSeenBlockTimeMs: null,
      confidenceMax: 0,
      confidenceScore: 0,
      evidenceCount: 0,
      signatureCount: 0,
      sourceCounts: {},
      programCounts: {},
      recentEvidenceSignatures: [],
      depositEvidenceCount: 0,
      recentDepositEvidence: [],
      validation: {
        status: "pending",
        checkedAt: null,
        reason: null,
      },
      updatedAt: null,
    };

    const isNew = !this.wallets[normalizedWallet];

    record.lastSeenAt = nowMs();
    record.updatedAt = nowMs();

    if (!record.firstSeenSignature) record.firstSeenSignature = signature;
    if (!record.firstSeenBlockTimeMs && blockTimeMs) record.firstSeenBlockTimeMs = blockTimeMs;

    record.lastSeenSignature = signature;
    if (blockTimeMs) {
      record.lastSeenBlockTimeMs = blockTimeMs;
      if (!record.firstSeenBlockTimeMs || blockTimeMs < record.firstSeenBlockTimeMs) {
        record.firstSeenBlockTimeMs = blockTimeMs;
      }
    }

    const confidence = Number(candidate && candidate.confidence ? candidate.confidence : 0);
    record.confidenceMax = Math.max(record.confidenceMax || 0, confidence);

    const recent = Array.isArray(record.recentEvidenceSignatures)
      ? record.recentEvidenceSignatures
      : [];

    if (!recent.includes(signature)) {
      record.signatureCount = Number(record.signatureCount || 0) + 1;
      record.evidenceCount = Number(record.evidenceCount || 0) + 1;
      record.confidenceScore = Number((Number(record.confidenceScore || 0) + confidence).toFixed(6));

      recent.push(signature);
      if (recent.length > 64) recent.splice(0, recent.length - 64);
      record.recentEvidenceSignatures = recent;

      const sourceCounts =
        record.sourceCounts && typeof record.sourceCounts === "object" ? record.sourceCounts : {};

      if (candidate && candidate.sourceCounts && typeof candidate.sourceCounts === "object") {
        Object.entries(candidate.sourceCounts).forEach(([source, count]) => {
          sourceCounts[source] = Number(sourceCounts[source] || 0) + Number(count || 0);
        });
      }
      record.sourceCounts = sourceCounts;

      const programCounts =
        record.programCounts && typeof record.programCounts === "object" ? record.programCounts : {};
      programCounts[programId] = Number(programCounts[programId] || 0) + 1;
      record.programCounts = programCounts;
    }

    this.wallets[normalizedWallet] = record;
    this.state.walletCount = Object.keys(this.wallets).length;

    return isNew;
  }

  upsertWalletDepositEvidence({ wallet, signature, slot, blockTimeMs, programId, deposit }) {
    const normalizedWallet = normalizeAddress(wallet);
    if (!normalizedWallet) return false;

    const record = this.wallets[normalizedWallet] || {
      wallet: normalizedWallet,
      firstSeenAt: nowMs(),
      lastSeenAt: null,
      firstSeenSignature: null,
      lastSeenSignature: null,
      firstSeenBlockTimeMs: null,
      lastSeenBlockTimeMs: null,
      confidenceMax: 0,
      confidenceScore: 0,
      evidenceCount: 0,
      signatureCount: 0,
      sourceCounts: {},
      programCounts: {},
      recentEvidenceSignatures: [],
      depositEvidenceCount: 0,
      recentDepositEvidence: [],
      validation: {
        status: "pending",
        checkedAt: null,
        reason: null,
      },
      updatedAt: null,
    };

    const evidence = {
      wallet: normalizedWallet,
      signature: signature || null,
      slot: Number.isFinite(Number(slot)) ? Number(slot) : null,
      blockTimeMs: Number.isFinite(Number(blockTimeMs)) ? Number(blockTimeMs) : null,
      programId: programId || null,
      vault: deposit && deposit.destination ? String(deposit.destination) : null,
      destination: deposit && deposit.destination ? String(deposit.destination) : null,
      source: deposit && deposit.source ? String(deposit.source) : null,
      mint: deposit && deposit.mint ? String(deposit.mint) : null,
      authority: deposit && deposit.authority ? String(deposit.authority) : null,
      sourceOwner: deposit && deposit.sourceOwner ? String(deposit.sourceOwner) : null,
      selectedBy: deposit && deposit.selectedBy ? String(deposit.selectedBy) : null,
      scope: deposit && deposit.scope ? String(deposit.scope) : null,
      instructionType: deposit && deposit.instructionType ? String(deposit.instructionType) : null,
      instructionKind: deposit && deposit.instructionKind ? String(deposit.instructionKind) : null,
      instructionIndex:
        deposit && Number.isFinite(Number(deposit.instructionIndex))
          ? Number(deposit.instructionIndex)
          : null,
      parentInstructionIndex:
        deposit && Number.isFinite(Number(deposit.parentInstructionIndex))
          ? Number(deposit.parentInstructionIndex)
          : null,
      innerInstructionIndex:
        deposit && Number.isFinite(Number(deposit.innerInstructionIndex))
          ? Number(deposit.innerInstructionIndex)
          : null,
      observedAt: nowMs(),
    };

    const key = buildDepositEvidenceKey(evidence);
    const current = Array.isArray(record.recentDepositEvidence) ? record.recentDepositEvidence : [];
    const exists = current.some((row) => buildDepositEvidenceKey(row) === key);
    if (exists) {
      this.wallets[normalizedWallet] = record;
      return false;
    }

    current.push(evidence);
    if (current.length > 256) {
      current.splice(0, current.length - 256);
    }
    record.recentDepositEvidence = current;
    record.depositEvidenceCount = Number(record.depositEvidenceCount || 0) + 1;
    record.updatedAt = nowMs();
    this.wallets[normalizedWallet] = record;
    this.state.walletCount = Object.keys(this.wallets).length;
    this.noteDepositTime(blockTimeMs);
    return true;
  }

  async validateWallet(wallet) {
    if (!this.restClient) {
      return {
        wallet,
        status: "pending",
        reason: "rest_client_not_configured",
      };
    }

    const record = this.wallets[wallet];
    if (!record) {
      return {
        wallet,
        status: "missing",
        reason: "wallet_not_found",
      };
    }

    try {
      const res = await this.restClient.get("/account", {
        query: { account: wallet },
        cost: 1,
        timeoutMs: 12000,
      });

      const payload = res && res.payload ? res.payload : {};
      const success = Boolean(payload.success);
      const hasData = payload.data && typeof payload.data === "object";

      if (success && hasData) {
        record.validation = {
          status: "confirmed",
          checkedAt: nowMs(),
          reason: null,
        };
        record.updatedAt = nowMs();

        return {
          wallet,
          status: "confirmed",
          reason: null,
        };
      }

      record.validation = {
        status: "rejected",
        checkedAt: nowMs(),
        reason: payload.error || "account_not_found",
      };
      record.updatedAt = nowMs();

      return {
        wallet,
        status: "rejected",
        reason: record.validation.reason,
      };
    } catch (error) {
      const status = Number(error.status || 0);
      const retryable = [429, 500, 502, 503, 504].includes(status);

      record.validation = {
        status: retryable ? "pending" : "rejected",
        checkedAt: nowMs(),
        reason: error.message,
      };
      record.updatedAt = nowMs();

      return {
        wallet,
        status: record.validation.status,
        reason: error.message,
      };
    }
  }

  async validatePendingWallets(limit = this.validationBatchSize) {
    const cap = Math.max(0, Number(limit || 0));
    if (!cap) {
      return {
        checked: 0,
        confirmed: 0,
        rejected: 0,
        pending: 0,
        wallets: [],
      };
    }

    const candidates = Object.values(this.wallets)
      .filter((row) => row && row.wallet)
      .filter((row) => {
        const status = row.validation && row.validation.status ? row.validation.status : "pending";
        return status !== "confirmed";
      })
      .sort((a, b) => Number(b.updatedAt || 0) - Number(a.updatedAt || 0))
      .slice(0, cap);

    const out = [];
    for (const row of candidates) {
      out.push(await this.validateWallet(row.wallet));
    }

    this.state.lastValidationAt = nowMs();
    this.maybeCheckpoint();

    return {
      checked: out.length,
      confirmed: out.filter((r) => r.status === "confirmed").length,
      rejected: out.filter((r) => r.status === "rejected").length,
      pending: out.filter((r) => r.status === "pending").length,
      wallets: out,
    };
  }

  async fetchTransactionBatch(pendingRows = [], options = {}) {
    const list = Array.isArray(pendingRows) ? pendingRows : [];
    if (!list.length) return [];

    const results = [];
    let index = 0;

    const worker = async () => {
      while (true) {
        const current = index;
        index += 1;
        if (current >= list.length) break;

        const pending = list[current];
        const signature = pending.signature;
        try {
          const tx = await this.rpcClient.getTransaction(signature, {
            commitment: options.commitment || "confirmed",
            cost: this.transactionScanCost,
            timeoutMs: 20000,
            partitionKey: `transaction:${signature}`,
          });

          results[current] = {
            signature,
            pending,
            ok: true,
            tx,
          };
        } catch (error) {
          results[current] = {
            signature,
            pending,
            ok: false,
            error: error.message,
            tx: null,
          };
        }
      }
    };

    const workers = [];
    for (let i = 0; i < this.txConcurrency; i += 1) {
      workers.push(worker());
    }

    await Promise.all(workers);

    return results.filter(Boolean);
  }

  schedulePendingRetry(pending, errorMessage) {
    const attempts = Number(pending.attempts || 0) + 1;
    const contains429 = /(^|\D)429(\D|$)/.test(String(errorMessage || ""));

    if (attempts > this.pendingMaxAttempts) {
      this.removePendingTransaction(pending.signature);
      return {
        waitMs: 0,
        dropped: true,
      };
    }

    if (contains429) {
      this.runtime.lastTx429At = nowMs();
      const prev = this.txConcurrency;
      this.txConcurrency = Math.max(this.txConcurrencyMin, Math.floor(this.txConcurrency / 2) || 1);
      if (prev !== this.txConcurrency) {
        this.logger.warn(
          `[onchain] adaptive tx concurrency ${prev}->${this.txConcurrency} due to 429`
        );
      }
    }

    const exp = Math.min(10, attempts);
    const base = this.pendingRetryBaseMs * 2 ** (exp - 1);
    const jitter = Math.floor(Math.random() * Math.max(250, base * 0.35));
    const waitMs = Math.min(this.pendingRetryMaxMs, base + jitter);

    pending.attempts = attempts;
    pending.lastAttemptAt = nowMs();
    pending.lastError = errorMessage || "unknown";
    pending.nextRetryAt = nowMs() + waitMs;

    this.state.pendingTransactions[pending.signature] = pending;

    return {
      waitMs,
      dropped: false,
    };
  }

  adaptTxConcurrencyFromRpc() {
    if (!this.rpcClient || typeof this.rpcClient.getStats !== "function") return;

    const rpc = this.rpcClient.getStats();
    const txMethod = rpc && rpc.methods ? rpc.methods.getTransaction : null;
    if (!txMethod) return;

    const now = nowMs();

    if (Number(txMethod.rate429PerMin || 0) > 0 || Number(txMethod.backoffRemainingMs || 0) > 0) {
      this.runtime.lastTx429At = now;
      const next = Math.max(this.txConcurrencyMin, Math.floor(this.txConcurrency / 2) || 1);
      this.txConcurrency = next;
      return;
    }

    if (
      this.txConcurrency < this.txConcurrencyMax &&
      now - Number(this.runtime.lastTx429At || 0) > this.txRampQuietMs &&
      now - Number(this.runtime.lastTxRampAt || 0) > this.txRampStepMs
    ) {
      this.txConcurrency += 1;
      this.txConcurrency = clamp(this.txConcurrency, this.txConcurrencyMin, this.txConcurrencyMax);
      this.runtime.lastTxRampAt = now;
      this.logger.info(`[onchain] adaptive tx concurrency increased to ${this.txConcurrency}`);
    }
  }

  async processPendingTransactions(options = {}) {
    const due = this.getDuePendingTransactions({
      limit: Number(options.limit || this.maxTransactionsPerCycle),
      programId: options.programId || null,
    });

    if (!due.length) {
      return {
        attempted: 0,
        fetched: 0,
        deferred: 0,
        missing: 0,
        retriesScheduled: 0,
        walletsDiscovered: 0,
        walletsUpdated: 0,
        errors: [],
        newWallets: [],
      };
    }

    const results = await this.fetchTransactionBatch(due, {
      commitment: options.commitment || "confirmed",
    });

    const newWalletSet = new Set();
    const out = {
      attempted: due.length,
      fetched: 0,
      deferred: 0,
      missing: 0,
      retriesScheduled: 0,
      retriesDropped: 0,
      depositMatches: 0,
      depositAuthorityMatches: 0,
      depositSourceOwnerMatches: 0,
      depositFeePayerFallbackMatches: 0,
      walletsDiscovered: 0,
      walletsUpdated: 0,
      errors: [],
      newWallets: [],
    };

    for (const row of results) {
      const pending = row.pending;
      const signature = row.signature;
      const programId = pending.programId;
      const programState = this.state.programs[programId] || emptyProgramState(programId);
      this.state.programs[programId] = programState;

      if (!row.ok) {
        const retry = this.schedulePendingRetry(pending, row.error);
        out.deferred += 1;
        if (retry.dropped) {
          out.retriesDropped += 1;
          out.errors.push(
            `tx:${signature}:${row.error} dropped=max_attempts(${this.pendingMaxAttempts})`
          );
        } else {
          out.retriesScheduled += 1;
          out.errors.push(`tx:${signature}:${row.error} retry_ms=${retry.waitMs}`);
        }
        programState.lastError = row.error;
        continue;
      }

      pending.lastAttemptAt = nowMs();

      if (!row.tx) {
        const retry = this.schedulePendingRetry(pending, "transaction_null");
        out.missing += 1;
        if (retry.dropped) {
          out.retriesDropped += 1;
          out.errors.push(
            `tx:${signature}:null dropped=max_attempts(${this.pendingMaxAttempts})`
          );
        } else {
          out.retriesScheduled += 1;
          out.errors.push(`tx:${signature}:null retry_ms=${retry.waitMs}`);
        }
        continue;
      }

      this.removePendingTransaction(signature);

      out.fetched += 1;
      programState.scannedTransactions += 1;
      programState.lastError = null;
      if (Number.isFinite(Number(pending.slot)) && Number(pending.slot) > 0) {
        const slot = Number(pending.slot);
        if (!programState.firstSlot || slot < programState.firstSlot) {
          programState.firstSlot = slot;
        }
        if (!programState.lastSlot || slot > programState.lastSlot) {
          programState.lastSlot = slot;
        }
      }

      this.appendRawTransaction({
        observedAt: nowMs(),
        source: "solana_rpc",
        programId,
        signature,
        slot: pending.slot || null,
        blockTimeMs: pending.blockTimeMs || null,
        tx: row.tx,
      });

      const extracted =
        this.discoveryType === "deposit_only"
          ? extractDepositWalletsFromTransaction(row.tx, {
              programIds:
                this.protocolProgramIds && this.protocolProgramIds.length
                  ? this.protocolProgramIds
                  : this.programIds,
              vaults: this.depositVaults,
              exclude: this.excludeAddresses,
            })
          : extractCandidateWalletsFromTransaction(row.tx, {
              programIds:
                this.protocolProgramIds && this.protocolProgramIds.length
                  ? this.protocolProgramIds
                  : this.programIds,
              exclude: this.excludeAddresses,
            });

      const candidates = Array.isArray(extracted.candidates) ? extracted.candidates : [];
      const deposits = Array.isArray(extracted.deposits) ? extracted.deposits : [];
      const depositMatches = deposits.length;
      out.depositMatches += depositMatches;
      deposits.forEach((deposit) => {
        const selectedBy = String((deposit && deposit.selectedBy) || "");
        if (selectedBy === "authority") out.depositAuthorityMatches += 1;
        else if (selectedBy === "source_owner") out.depositSourceOwnerMatches += 1;
        else if (selectedBy === "fee_payer_fallback") out.depositFeePayerFallbackMatches += 1;

        if (deposit && deposit.wallet) {
          this.upsertWalletDepositEvidence({
            wallet: deposit.wallet,
            signature,
            slot: pending.slot || null,
            blockTimeMs: pending.blockTimeMs || null,
            programId,
            deposit,
          });
          this.noteDepositTime(pending.blockTimeMs || null);
        }
      });
      for (const candidate of candidates) {
        const isNew = this.upsertWalletEvidence({
          wallet: candidate.wallet,
          programId,
          signature,
          slot: pending.slot || null,
          blockTimeMs: pending.blockTimeMs || null,
          candidate,
        });

        if (isNew) {
          out.walletsDiscovered += 1;
          programState.discoveredWallets += 1;
          newWalletSet.add(candidate.wallet);
        } else {
          out.walletsUpdated += 1;
        }
      }

      this.maybeCheckpoint();
    }

    out.newWallets = Array.from(newWalletSet.values());
    return out;
  }

  async scanProgram(programId) {
    const programState = this.state.programs[programId] || emptyProgramState(programId);
    this.state.programs[programId] = programState;
    const scanTarget = this.getScanTarget(programId);

    const out = {
      programId,
      scanTargetId: programState.scanTargetId || (scanTarget && scanTarget.id) || programId,
      scanTargetType: programState.scanTargetType || (scanTarget && scanTarget.type) || "program",
      scanTargetLabel:
        programState.scanTargetLabel || (scanTarget && scanTarget.label) || null,
      preferredRpcShardId: programState.preferredRpcShardId || null,
      preferredRpcShardIndex: Number(programState.preferredRpcShardIndex || 0) || 0,
      pages: 0,
      signaturesFetched: 0,
      signaturesAccepted: 0,
      pendingQueued: 0,
      transactionsFetched: 0,
      transactionsDeferred: 0,
      transactionsMissing: 0,
      transactionsRetried: 0,
      depositMatches: 0,
      walletsDiscovered: 0,
      walletsUpdated: 0,
      pendingRemaining: 0,
      currentSlot: null,
      currentBlockTimeMs: null,
      done: false,
      reachedWindowStart: false,
      errors: [],
      newWallets: [],
    };

    const newWalletSet = new Set();

    for (let page = 0; page < this.scanPagesPerCycle; page += 1) {
      const query = {
        commitment: "confirmed",
        limit: this.signaturePageLimit,
      };

      if (this.state.mode === "backfill" && programState.before) {
        query.before = programState.before;
      }

      let signatures;
      try {
        signatures = await this.rpcClient.getSignaturesForAddress(programId, {
          ...query,
          cost: this.signatureScanCost,
          partitionKey: `scan:${programId}`,
        });
      } catch (error) {
        programState.lastError = error.message;
        out.errors.push(error.message);
        break;
      }

      out.pages += 1;
      programState.scannedPages += 1;
      programState.lastScanAt = nowMs();

      if (!signatures.length) {
        if (this.state.mode === "backfill") {
          programState.done = true;
          out.done = true;
        }
        break;
      }

      out.signaturesFetched += signatures.length;

      let reachedStart = false;

      for (const row of signatures) {
        const signature = row && row.signature ? String(row.signature) : null;
        if (!signature) continue;

        const blockTimeMs = blockTimeToMs(row.blockTime);

        if (this.state.endTimeMs && blockTimeMs && blockTimeMs > this.state.endTimeMs) {
          continue;
        }

        if (this.state.startTimeMs && blockTimeMs && blockTimeMs < this.state.startTimeMs) {
          reachedStart = true;
          out.reachedWindowStart = true;
          break;
        }

        if (this.isSignatureSeen(signature)) {
          continue;
        }

        this.rememberSignature(signature);

        out.signaturesAccepted += 1;
        programState.scannedSignatures += 1;
        const slot = Number(row.slot || 0);
        if (Number.isFinite(slot) && slot > 0) {
          if (!programState.firstSlot || slot < programState.firstSlot) {
            programState.firstSlot = slot;
          }
          if (!programState.lastSlot || slot > programState.lastSlot) {
            programState.lastSlot = slot;
          }
        }

        if (blockTimeMs) {
          if (!programState.firstBlockTimeMs || blockTimeMs < programState.firstBlockTimeMs) {
            programState.firstBlockTimeMs = blockTimeMs;
          }
          if (!programState.lastBlockTimeMs || blockTimeMs > programState.lastBlockTimeMs) {
            programState.lastBlockTimeMs = blockTimeMs;
          }
        }

        const queued = this.queuePendingTransaction({
          signature,
          programId,
          slot: Number(row.slot || 0),
          blockTimeMs,
        });
        if (queued) out.pendingQueued += 1;

        this.appendRawSignature({
          observedAt: nowMs(),
          source: "solana_rpc",
          programId,
          scanTargetId: out.scanTargetId,
          scanTargetType: out.scanTargetType,
          ...row,
          blockTimeMs,
        });
      }

      if (this.state.mode === "backfill") {
        const tail = signatures[signatures.length - 1];
        if (tail && tail.signature) {
          programState.before = String(tail.signature);
          programState.lastSignature = String(tail.signature);
        }
      }

      if (reachedStart) {
        programState.done = true;
        out.done = true;
      }

      const pendingResult = await this.processPendingTransactions({
        programId,
        limit: this.maxTransactionsPerCycle,
      });

      out.transactionsFetched += pendingResult.fetched;
      out.transactionsDeferred += pendingResult.deferred;
      out.transactionsMissing += pendingResult.missing;
      out.transactionsRetried += pendingResult.retriesScheduled;
      out.depositMatches += Number(pendingResult.depositMatches || 0);
      out.walletsDiscovered += pendingResult.walletsDiscovered;
      out.walletsUpdated += pendingResult.walletsUpdated;
      pendingResult.errors.forEach((error) => out.errors.push(error));
      pendingResult.newWallets.forEach((wallet) => newWalletSet.add(wallet));

      this.state.lastScanAt = nowMs();
      this.maybeCheckpoint();

      if (reachedStart) break;
      if (this.state.mode === "live") break;
    }

    out.pendingRemaining = Object.values(this.state.pendingTransactions || {}).filter(
      (row) => row && row.programId === programId
    ).length;
    out.currentSlot = programState.firstSlot || programState.lastSlot || null;
    out.currentBlockTimeMs = programState.firstBlockTimeMs || programState.lastBlockTimeMs || null;

    programState.lastError = out.errors.length ? out.errors[out.errors.length - 1] : null;
    out.newWallets = Array.from(newWalletSet.values());

    return out;
  }

  computeProgress() {
    const programRows = Object.values(this.state.programs || {});
    const pendingTxCount = Object.keys(this.state.pendingTransactions || {}).length;
    if (!programRows.length) {
      return {
        mode: this.state.mode,
        donePrograms: 0,
        totalPrograms: 0,
        pendingTransactions: pendingTxCount,
        pct: null,
      };
    }

    const donePrograms = programRows.filter((row) => row.done).length;
    const historyExhausted = donePrograms === programRows.length;

    if (this.state.mode !== "backfill") {
      return {
        mode: this.state.mode,
        donePrograms,
        totalPrograms: programRows.length,
        pendingTransactions: pendingTxCount,
        historyExhausted: donePrograms === programRows.length,
        windowReached: null,
        pct: historyExhausted && pendingTxCount === 0 ? 100 : null,
      };
    }

    const latestReached = programRows
      .map((row) => Number(row.lastBlockTimeMs || 0))
      .filter((v) => Number.isFinite(v) && v > 0)
      .sort((a, b) => b - a)[0];

    const oldestReached = programRows
      .map((row) => Number(row.firstBlockTimeMs || 0))
      .filter((v) => Number.isFinite(v) && v > 0)
      .sort((a, b) => a - b)[0];

    const newestSlot = programRows
      .map((row) => Number(row.lastSlot || 0))
      .filter((v) => Number.isFinite(v) && v > 0)
      .sort((a, b) => b - a)[0];

    const oldestSlot = programRows
      .map((row) => Number(row.firstSlot || 0))
      .filter((v) => Number.isFinite(v) && v > 0)
      .sort((a, b) => a - b)[0];

    const windowReached =
      Number.isFinite(oldestReached) &&
      Number.isFinite(Number(this.state.startTimeMs || 0)) &&
      oldestReached <= Number(this.state.startTimeMs || 0);
    const doneFully = historyExhausted && pendingTxCount === 0 && windowReached;

    let pct = null;
    if (
      Number.isFinite(latestReached) &&
      Number.isFinite(oldestReached) &&
      latestReached > this.state.startTimeMs
    ) {
      const covered = latestReached - oldestReached;
      const target = latestReached - this.state.startTimeMs;
      pct = clamp((covered / target) * 100, 0, 100);
    }

    const remainingMs =
      Number.isFinite(oldestReached) && Number.isFinite(this.state.startTimeMs)
        ? Math.max(0, oldestReached - this.state.startTimeMs)
        : null;

    if (doneFully) {
      pct = 100;
    }

    let estimatedRemainingSlots = null;
    if (
      Number.isFinite(newestSlot) &&
      Number.isFinite(oldestSlot) &&
      Number.isFinite(latestReached) &&
      Number.isFinite(oldestReached) &&
      latestReached > oldestReached &&
      newestSlot > oldestSlot &&
      Number.isFinite(remainingMs)
    ) {
      const slotsPerMs = (newestSlot - oldestSlot) / (latestReached - oldestReached);
      if (Number.isFinite(slotsPerMs) && slotsPerMs > 0) {
        estimatedRemainingSlots = Math.max(0, Math.round(remainingMs * slotsPerMs));
      }
    }

    const pendingRows = Object.values(this.state.pendingTransactions || {});
    const pendingOldest = pendingRows
      .slice()
      .sort((a, b) => {
        const aBt = Number(a && a.blockTimeMs ? a.blockTimeMs : Number.MAX_SAFE_INTEGER);
        const bBt = Number(b && b.blockTimeMs ? b.blockTimeMs : Number.MAX_SAFE_INTEGER);
        if (aBt !== bBt) return aBt - bBt;
        return Number(a && a.slot ? a.slot : Number.MAX_SAFE_INTEGER) - Number(b && b.slot ? b.slot : Number.MAX_SAFE_INTEGER);
      })[0];

    const activeProgram = programRows
      .slice()
      .sort((a, b) => Number(a.firstBlockTimeMs || Number.MAX_SAFE_INTEGER) - Number(b.firstBlockTimeMs || Number.MAX_SAFE_INTEGER))[0] || null;

    return {
      mode: this.state.mode,
      donePrograms,
      totalPrograms: programRows.length,
      pendingTransactions: pendingTxCount,
      latestReached,
      oldestReached,
      newestSlot: Number.isFinite(newestSlot) ? newestSlot : null,
      oldestSlot: Number.isFinite(oldestSlot) ? oldestSlot : null,
      remainingMs: Number.isFinite(remainingMs) ? remainingMs : null,
      estimatedRemainingSlots,
      startTimeMs: this.state.startTimeMs,
      pct: pct === null ? null : Number(pct.toFixed(2)),
      historyExhausted,
      windowReached,
      cursor: {
        signature: activeProgram ? activeProgram.before || activeProgram.lastSignature || null : null,
        slot:
          (activeProgram && (activeProgram.firstSlot || activeProgram.lastSlot)) ||
          (pendingOldest ? Number(pendingOldest.slot || 0) || null : null),
        blockTimeMs:
          (activeProgram && (activeProgram.firstBlockTimeMs || activeProgram.lastBlockTimeMs)) ||
          (pendingOldest ? Number(pendingOldest.blockTimeMs || 0) || null : null),
      },
    };
  }

  logCycleMetrics({ scanResults, validation }) {
    const now = nowMs();
    if (now - Number(this.runtime.lastLogAt || 0) < this.logIntervalMs) return;

    this.runtime.lastLogAt = now;

    const totalSignatures = (scanResults || []).reduce(
      (sum, row) => sum + Number(row.signaturesAccepted || 0),
      0
    );
    const totalPages = (scanResults || []).reduce((sum, row) => sum + Number(row.pages || 0), 0);
    const totalTx = (scanResults || []).reduce(
      (sum, row) => sum + Number(row.transactionsFetched || 0),
      0
    );
    const totalTxAttempted = (scanResults || []).reduce(
      (sum, row) => sum + Number(row.transactionsFetched || 0) + Number(row.transactionsDeferred || 0) + Number(row.transactionsMissing || 0),
      0
    );
    const totalTxDeferred = (scanResults || []).reduce(
      (sum, row) => sum + Number(row.transactionsDeferred || 0),
      0
    );
    const totalTxMissing = (scanResults || []).reduce(
      (sum, row) => sum + Number(row.transactionsMissing || 0),
      0
    );
    const totalTxRetried = (scanResults || []).reduce(
      (sum, row) => sum + Number(row.transactionsRetried || 0),
      0
    );
    const totalTxDropped = (scanResults || []).reduce(
      (sum, row) => sum + Number(row.retriesDropped || 0),
      0
    );
    const totalDepositMatches = (scanResults || []).reduce(
      (sum, row) => sum + Number(row.depositMatches || 0),
      0
    );
    const totalDepositByAuthority = (scanResults || []).reduce(
      (sum, row) => sum + Number(row.depositAuthorityMatches || 0),
      0
    );
    const totalDepositBySourceOwner = (scanResults || []).reduce(
      (sum, row) => sum + Number(row.depositSourceOwnerMatches || 0),
      0
    );
    const totalDepositByFeePayerFallback = (scanResults || []).reduce(
      (sum, row) => sum + Number(row.depositFeePayerFallbackMatches || 0),
      0
    );

    const rpc = this.rpcClient && typeof this.rpcClient.getStats === "function"
      ? this.rpcClient.getStats()
      : null;

    const txMethod = rpc && rpc.methods ? rpc.methods.getTransaction : null;
    const progress = this.computeProgress();
    const currentSlot = progress.cursor && progress.cursor.slot ? progress.cursor.slot : progress.oldestSlot || progress.newestSlot || "n/a";
    const currentBlockTime = progress.cursor && progress.cursor.blockTimeMs ? progress.cursor.blockTimeMs : progress.oldestReached || progress.latestReached || null;
    const currentIso = currentBlockTime ? new Date(currentBlockTime).toISOString() : "n/a";
    const remainingSlotsText =
      progress.estimatedRemainingSlots === null || progress.estimatedRemainingSlots === undefined
        ? "n/a"
        : progress.estimatedRemainingSlots;
    const remainingTimeText =
      progress.remainingMs === null || progress.remainingMs === undefined
        ? "n/a"
        : formatDurationMs(progress.remainingMs);

    let etaMs = null;
    if (Number.isFinite(progress.remainingMs)) {
      const t = this.runtime.throughput;
      if (Number.isFinite(t.lastRemainingMs) && t.lastRemainingMs > progress.remainingMs) {
        const elapsedMs = Math.max(1, now - Number(t.lastAt || now));
        const reducedMs = t.lastRemainingMs - progress.remainingMs;
        const instRate = reducedMs / elapsedMs;
        if (Number.isFinite(instRate) && instRate > 0) {
          t.smoothedRate = Number.isFinite(t.smoothedRate)
            ? t.smoothedRate * 0.7 + instRate * 0.3
            : instRate;
        }
      }
      t.lastAt = now;
      t.lastRemainingMs = progress.remainingMs;
      if (Number.isFinite(t.smoothedRate) && t.smoothedRate > 0) {
        etaMs = Math.round(progress.remainingMs / t.smoothedRate);
      }
    }
    const etaText = Number.isFinite(etaMs) && etaMs >= 0 ? formatDurationMs(etaMs) : "n/a";
    const etaAtText = Number.isFinite(etaMs) && etaMs >= 0 ? new Date(now + etaMs).toISOString() : "n/a";
    const cursorSigText =
      progress.cursor && progress.cursor.signature ? String(progress.cursor.signature).slice(0, 16) : "n/a";

    const progressPayload = {
      type: "onchain_progress",
      mode: this.discoveryType,
      pages: totalPages,
      signatures: totalSignatures,
      transactions_fetched: totalTx,
      transactions_attempted: totalTxAttempted,
      tx_deferred: totalTxDeferred,
      tx_missing: totalTxMissing,
      tx_retried: totalTxRetried,
      tx_dropped: totalTxDropped,
      deposit_matches: totalDepositMatches,
      deposit_by_authority: totalDepositByAuthority,
      deposit_by_source_owner: totalDepositBySourceOwner,
      deposit_by_fee_payer_fallback: totalDepositByFeePayerFallback,
      unique_deposit_wallets: this.state.walletCount,
      pending_tx: Object.keys(this.state.pendingTransactions || {}).length,
      tx_concurrency: this.txConcurrency,
      rpc_rps: rpc ? rpc.rps1m : "n/a",
      tx_rps: txMethod ? txMethod.rps1m : "n/a",
      rpc_429: rpc ? rpc.rate429PerMin : "n/a",
      tx_backoff_ms: txMethod ? txMethod.backoffRemainingMs : "n/a",
      current_slot: currentSlot,
      current_block_time: currentIso,
      cursor_sig: cursorSigText,
      history_exhausted: progress.historyExhausted ? "true" : "false",
      window_reached:
        progress.windowReached === null ? "n/a" : progress.windowReached ? "true" : "false",
      remaining_slots: remainingSlotsText,
      remaining_time: remainingTimeText,
      eta: etaText,
      eta_at: etaAtText,
      progress_pct: progress.pct === null ? "n/a" : progress.pct,
    };

    if (this.logFormat === "json") {
      this.logger.info(JSON.stringify(progressPayload));
    } else {
      this.logger.info(
        formatFixedKv("[onchain]", [
          ["mode", progressPayload.mode],
          ["pages", progressPayload.pages],
          ["sig", progressPayload.signatures],
          ["tx", `${progressPayload.transactions_fetched}/${progressPayload.transactions_attempted}`],
          ["tx_deferred", progressPayload.tx_deferred],
          ["tx_missing", progressPayload.tx_missing],
          ["tx_retried", progressPayload.tx_retried],
          ["tx_dropped", progressPayload.tx_dropped],
          ["deposit_matches", progressPayload.deposit_matches],
          ["deposit_by_authority", progressPayload.deposit_by_authority],
          ["deposit_by_source_owner", progressPayload.deposit_by_source_owner],
          ["deposit_by_fee_payer_fallback", progressPayload.deposit_by_fee_payer_fallback],
          ["unique_deposit_wallets", progressPayload.unique_deposit_wallets],
          ["pending_tx", progressPayload.pending_tx],
          ["tx_concurrency", progressPayload.tx_concurrency],
          ["rpc_rps", progressPayload.rpc_rps],
          ["tx_rps", progressPayload.tx_rps],
          ["rpc_429", progressPayload.rpc_429],
          ["tx_backoff_ms", progressPayload.tx_backoff_ms],
          ["current_slot", progressPayload.current_slot],
          ["current_block_time", progressPayload.current_block_time],
          ["cursor_sig", progressPayload.cursor_sig],
          ["history_exhausted", progressPayload.history_exhausted],
          ["window_reached", progressPayload.window_reached],
          ["remaining_slots", progressPayload.remaining_slots],
          ["remaining_time", progressPayload.remaining_time],
          ["eta", progressPayload.eta],
          ["eta_at", progressPayload.eta_at],
          ["progress_pct", progressPayload.progress_pct],
        ])
      );
    }

    if (validation && validation.checked) {
      const validationPayload = {
        type: "onchain_validation",
        checked: Number(validation.checked || 0),
        confirmed: Number(validation.confirmed || 0),
        pending: Number(validation.pending || 0),
        rejected: Number(validation.rejected || 0),
      };

      if (this.logFormat === "json") {
        this.logger.info(JSON.stringify(validationPayload));
      } else {
        this.logger.info(
          formatFixedKv("[onchain]", [
            ["validation_checked", validationPayload.checked],
            ["validation_confirmed", validationPayload.confirmed],
            ["validation_pending", validationPayload.pending],
            ["validation_rejected", validationPayload.rejected],
          ])
        );
      }
    }
  }

  async discoverStep(options = {}) {
    if (this.runtime.inScan) {
      return {
        ok: false,
        running: true,
        error: "discovery already running",
      };
    }

    if (!this.programIds.length) {
      return {
        ok: false,
        running: false,
        error: "no_program_ids_configured",
      };
    }

    this.runtime.inScan = true;

    try {
      const scanResults = [];
      const newWalletSet = new Set();
      const recovery = await this.recoverWalletsFromRawTransactions();
      if (
        recovery &&
        !recovery.skipped &&
        (
          Number(recovery.depositMatches || 0) > 0 ||
          Number(recovery.walletsDiscovered || 0) > 0 ||
          Number(recovery.walletsUpdated || 0) > 0
        )
      ) {
        scanResults.push({
          programId: "__raw_recovery__",
          pages: 0,
          signaturesFetched: 0,
          signaturesAccepted: 0,
          pendingQueued: 0,
          transactionsFetched: 0,
          transactionsDeferred: 0,
          transactionsMissing: 0,
          transactionsRetried: 0,
          retriesDropped: 0,
          depositMatches: Number(recovery.depositMatches || 0),
          walletsDiscovered: Number(recovery.walletsDiscovered || 0),
          walletsUpdated: Number(recovery.walletsUpdated || 0),
          pendingRemaining: Object.keys(this.state.pendingTransactions || {}).length,
          currentSlot: null,
          currentBlockTimeMs: null,
          done: false,
          reachedWindowStart: false,
          errors: [],
          newWallets: recovery.newWallets || [],
        });
        (recovery.newWallets || []).forEach((wallet) => newWalletSet.add(wallet));
      }

      const pages = Math.max(1, Number(options.pages || 1));

      for (let i = 0; i < pages; i += 1) {
        const programId = this.pickProgram();
        if (!programId) break;

        const result = await this.scanProgram(programId);
        scanResults.push(result);
        (result.newWallets || []).forEach((wallet) => newWalletSet.add(wallet));

        this.maybeCheckpoint();
      }

      // Always try draining pending transactions, even when all programs are marked done.
      // This avoids a stalled loop with pending_tx > 0 and no further program scans.
      const pendingDrain = await this.processPendingTransactions({
        limit: this.maxTransactionsPerCycle,
      });
      if (
        pendingDrain.attempted ||
        pendingDrain.fetched ||
        pendingDrain.deferred ||
        pendingDrain.missing ||
        pendingDrain.retriesScheduled ||
        pendingDrain.walletsDiscovered ||
        pendingDrain.walletsUpdated
      ) {
        scanResults.push({
          programId: "__pending__",
          pages: 0,
          signaturesFetched: 0,
          signaturesAccepted: 0,
          pendingQueued: 0,
          transactionsFetched: pendingDrain.fetched,
          transactionsDeferred: pendingDrain.deferred,
          transactionsMissing: pendingDrain.missing,
          transactionsRetried: pendingDrain.retriesScheduled,
          retriesDropped: pendingDrain.retriesDropped,
          depositMatches: Number(pendingDrain.depositMatches || 0),
          walletsDiscovered: pendingDrain.walletsDiscovered,
          walletsUpdated: pendingDrain.walletsUpdated,
          pendingRemaining: Object.keys(this.state.pendingTransactions || {}).length,
          currentSlot: null,
          currentBlockTimeMs: null,
          done: false,
          reachedWindowStart: false,
          errors: pendingDrain.errors || [],
          newWallets: pendingDrain.newWallets || [],
        });
      }
      (pendingDrain.newWallets || []).forEach((wallet) => newWalletSet.add(wallet));

      const validateLimit = Number.isFinite(Number(options.validateLimit))
        ? Number(options.validateLimit)
        : this.validationBatchSize;

      const validation = await this.validatePendingWallets(validateLimit);

      this.adaptTxConcurrencyFromRpc();

      this.state.lastError = null;
      this.maybeCheckpoint(true);

      const rpc = this.rpcClient && typeof this.rpcClient.getStats === "function"
        ? this.rpcClient.getStats()
        : null;

      let progress = this.computeProgress();
      if (this.maybePromoteToLive(progress)) {
        progress = this.computeProgress();
      }

      this.logCycleMetrics({
        scanResults,
        validation,
      });

      return {
        ok: true,
        discoveryType: this.discoveryType,
        scannedPrograms: scanResults.length,
        results: scanResults,
        newWallets: Array.from(newWalletSet.values()),
        validation,
        progress,
        pendingTransactions: Object.keys(this.state.pendingTransactions || {}).length,
        txConcurrency: this.txConcurrency,
        rpc,
      };
    } catch (error) {
      this.state.lastError = error.message;
      this.maybeCheckpoint(true);

      return {
        ok: false,
        error: error.message,
      };
    } finally {
      this.runtime.inScan = false;
    }
  }

  listWallets(options = {}) {
    const rows = Object.values(this.wallets || {});

    const confidenceMin = Number.isFinite(Number(options.confidenceMin))
      ? Number(options.confidenceMin)
      : 0;

    const onlyConfirmed = Boolean(options.onlyConfirmed);

    return rows
      .filter((row) => row && row.wallet)
      .filter((row) => Number(row.confidenceMax || 0) >= confidenceMin)
      .filter((row) => {
        if (!onlyConfirmed) return true;
        return row.validation && row.validation.status === "confirmed";
      })
      .sort((a, b) => Number(b.confidenceMax || 0) - Number(a.confidenceMax || 0));
  }

  listDepositWalletAddresses() {
    return Object.keys(this.wallets || {})
      .filter((wallet) => Boolean(wallet))
      .sort((a, b) => a.localeCompare(b));
  }

  getWalletRecord(wallet) {
    const normalized = normalizeAddress(wallet);
    if (!normalized) return null;
    const record = this.wallets[normalized];
    if (!record || typeof record !== "object") return null;
    return record;
  }

  getWalletDepositEvidence(wallet, options = {}) {
    const record = this.getWalletRecord(wallet);
    if (!record) {
      return {
        wallet: normalizeAddress(wallet),
        found: false,
        total: 0,
        page: 1,
        pageSize: 0,
        rows: [],
      };
    }

    const page = Math.max(1, Number(options.page || 1));
    const pageSize = Math.max(1, Math.min(500, Number(options.pageSize || 100)));
    const all = Array.isArray(record.recentDepositEvidence)
      ? record.recentDepositEvidence.slice()
      : [];

    all.sort((a, b) => {
      const aSlot = Number(a && a.slot ? a.slot : 0);
      const bSlot = Number(b && b.slot ? b.slot : 0);
      if (aSlot !== bSlot) return bSlot - aSlot;
      const aObs = Number(a && a.observedAt ? a.observedAt : 0);
      const bObs = Number(b && b.observedAt ? b.observedAt : 0);
      return bObs - aObs;
    });

    const total = all.length;
    const start = (page - 1) * pageSize;
    const rows = all.slice(start, start + pageSize);

    return {
      wallet: record.wallet,
      found: true,
      total,
      page,
      pageSize,
      rows,
      summary: {
        depositEvidenceCount: Number(record.depositEvidenceCount || 0),
        confidenceMax: Number(record.confidenceMax || 0),
        validation: record.validation || { status: "pending" },
        firstSeenSignature: record.firstSeenSignature || null,
        lastSeenSignature: record.lastSeenSignature || null,
        firstSeenBlockTimeMs: record.firstSeenBlockTimeMs || null,
        lastSeenBlockTimeMs: record.lastSeenBlockTimeMs || null,
      },
    };
  }

  buildSchedulingSnapshot() {
    return {
      baseIntervalMs: this.scanIntervalMs,
      nextRunAt: this.runtime.nextRunAt,
      nextDelayMs: this.runtime.nextDelayMs,
      lastDurationMs: this.runtime.lastStepDurationMs,
    };
  }

  computeEtaMs(progress = null) {
    const safeProgress = progress && typeof progress === "object" ? progress : this.computeProgress();
    if (!Number.isFinite(Number(safeProgress.remainingMs))) return null;
    const throughput = this.runtime.throughput;
    if (
      Number.isFinite(Number(throughput.smoothedRate)) &&
      Number(throughput.smoothedRate) > 0
    ) {
      return Math.round(Number(safeProgress.remainingMs) / Number(throughput.smoothedRate));
    }
    return null;
  }

  computeStepDelayMs(lastResult = null) {
    const progress = this.computeProgress();
    const rpc =
      this.rpcClient && typeof this.rpcClient.getStats === "function"
        ? this.rpcClient.getStats()
        : null;
    const txMethod = rpc && rpc.methods ? rpc.methods.getTransaction : null;
    const sigMethod = rpc && rpc.methods ? rpc.methods.getSignaturesForAddress : null;
    const ratePressure =
      Number((txMethod && txMethod.backoffRemainingMs) || 0) > 0 ||
      Number((sigMethod && sigMethod.backoffRemainingMs) || 0) > 0 ||
      Number((rpc && rpc.rate429PerMin) || 0) > 0;
    let delayMs = this.scanIntervalMs;

    if (progress.mode === "backfill" && Number(progress.pct || 0) < 100) {
      delayMs = Math.min(delayMs, 5000);
    }
    if (Number(progress.pendingTransactions || 0) > 0) {
      delayMs = Math.min(delayMs, 2500);
    }
    if (lastResult && Array.isArray(lastResult.newWallets) && lastResult.newWallets.length > 0) {
      delayMs = Math.min(delayMs, 3000);
    }
    if (ratePressure) {
      delayMs = Math.max(
        delayMs,
        Number((txMethod && txMethod.backoffRemainingMs) || 0),
        Number((sigMethod && sigMethod.backoffRemainingMs) || 0),
        Math.round(this.scanIntervalMs * 1.5)
      );
    }
    if (
      progress.mode === "backfill" &&
      Number(progress.pct || 0) >= 100 &&
      Number(progress.pendingTransactions || 0) <= 0
    ) {
      delayMs = Math.max(delayMs, Math.round(this.scanIntervalMs * 2));
    }

    return Math.max(1500, Math.round(delayMs));
  }

  scheduleLoop(delayMs = null) {
    if (!this.runtime.running) return;
    if (this.runtime.timer) {
      clearTimeout(this.runtime.timer);
      this.runtime.timer = null;
    }
    const nextDelay = Math.max(0, Number(delayMs !== null ? delayMs : this.computeStepDelayMs()) || 0);
    this.runtime.nextDelayMs = nextDelay;
    this.runtime.nextRunAt = Date.now() + nextDelay;
    this.runtime.timer = setTimeout(async () => {
      this.runtime.timer = null;
      const startedAt = Date.now();
      let result = null;
      try {
        result = await this.discoverStep({
          pages: this.scanPagesPerCycle,
          validateLimit: this.validationBatchSize,
        });
        this.maybePromoteToLive(result && result.progress ? result.progress : null);
        this.state.lastError = null;
      } catch (error) {
        this.state.lastError = error.message;
        this.maybeCheckpoint(true);
      } finally {
        this.runtime.lastStepDurationMs = Date.now() - startedAt;
        this.scheduleLoop(this.computeStepDelayMs(result));
      }
    }, nextDelay);
  }

  start() {
    if (this.runtime.running) return;

    this.runtime.running = true;
    this.scheduleLoop(0);
  }

  stop() {
    this.runtime.running = false;
    if (this.runtime.timer) {
      clearTimeout(this.runtime.timer);
      this.runtime.timer = null;
    }
    this.runtime.nextRunAt = null;
    this.runtime.nextDelayMs = null;
    this.maybeCheckpoint(true);
  }

  getStatus() {
    const now = nowMs();
    const programStatuses = Object.values(this.state.programs || {}).map((row) => ({
      programId: row.programId,
      scanTargetId: row.scanTargetId || row.programId,
      scanTargetType: row.scanTargetType || "program",
      scanTargetLabel: row.scanTargetLabel || null,
      preferredRpcShardId: row.preferredRpcShardId || null,
      preferredRpcShardIndex:
        Number.isFinite(Number(row.preferredRpcShardIndex)) ? Number(row.preferredRpcShardIndex) : null,
      before: row.before,
      done: Boolean(row.done),
      scannedPages: Number(row.scannedPages || 0),
      scannedSignatures: Number(row.scannedSignatures || 0),
      scannedTransactions: Number(row.scannedTransactions || 0),
      discoveredWallets: Number(row.discoveredWallets || 0),
      firstSlot: row.firstSlot || null,
      lastSlot: row.lastSlot || null,
      firstBlockTimeMs: row.firstBlockTimeMs || null,
      lastBlockTimeMs: row.lastBlockTimeMs || null,
      lastSignature: row.lastSignature || null,
      lastScanAt: row.lastScanAt || null,
      lastError: row.lastError || null,
    }));

    const confirmed = Object.values(this.wallets || {}).filter(
      (row) => row && row.validation && row.validation.status === "confirmed"
    ).length;

    const rpc = this.rpcClient && typeof this.rpcClient.getStats === "function"
      ? this.rpcClient.getStats()
      : null;
    const progress = this.computeProgress();
    const etaMs = this.computeEtaMs(progress);

    return {
      running: this.runtime.running,
      inScan: this.runtime.inScan,
      discoveryType: this.discoveryType,
      protocolProgramIds: this.protocolProgramIds,
      depositVaults: this.depositVaults,
      scanTargets: this.scanTargets,
      mode: this.state.mode,
      backfillCompletedAt: Number(this.state.backfillCompletedAt || 0) || null,
      liveModeSince: Number(this.state.liveModeSince || 0) || null,
      lastModeTransitionAt: Number(this.state.lastModeTransitionAt || 0) || null,
      startTimeMs: this.state.startTimeMs,
      endTimeMs: this.state.endTimeMs,
      programCount: this.programIds.length,
      programs: programStatuses,
      walletCount: this.state.walletCount,
      confirmedWallets: confirmed,
      firstDepositAtMs: this.state.firstDepositAtMs || null,
      rawRecoveryVersion: Number(this.state.rawRecoveryVersion || 0),
      rawRecoveryAt: this.state.rawRecoveryAt || null,
      pendingTransactions: Object.keys(this.state.pendingTransactions || {}).length,
      txConcurrency: {
        current: this.txConcurrency,
        min: this.txConcurrencyMin,
        max: this.txConcurrencyMax,
      },
      progress,
      discoverySummary: this.buildDiscoverySummary(),
      etaMs,
      autoLiveTransitionReady: this.shouldAutoPromoteToLive(progress),
      lastScanAt: this.state.lastScanAt,
      lastValidationAt: this.state.lastValidationAt,
      lastCheckpointAt: this.state.lastCheckpointAt,
      recentSignatureBuffer: this.state.recentSignatures.length,
      lastError: this.state.lastError,
      secondsSinceLastTx429:
        this.runtime.lastTx429At > 0 ? Math.floor((now - this.runtime.lastTx429At) / 1000) : null,
      scheduling: this.buildSchedulingSnapshot(),
      rpc,
    };
  }
}

module.exports = {
  DEFAULT_START_TIME_MS,
  OnChainWalletDiscovery,
  normalizeProgramIds,
};
