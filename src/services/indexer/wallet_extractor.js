const BASE58_RE = /^[1-9A-HJ-NP-Za-km-z]{32,44}$/;

const DEFAULT_EXCLUDED_PROGRAMS = new Set([
  "11111111111111111111111111111111",
  "ComputeBudget111111111111111111111111111111",
  "AddressLookupTab1e1111111111111111111111111",
  "Vote111111111111111111111111111111111111111",
  "Stake11111111111111111111111111111111111111",
  "BPFLoader1111111111111111111111111111111111",
  "BPFLoaderUpgradeab1e11111111111111111111111",
  "SysvarC1ock11111111111111111111111111111111",
  "SysvarRent111111111111111111111111111111111",
  "SysvarS1otHashes111111111111111111111111111",
  "SysvarEpochSchedu1e111111111111111111111111",
  "SysvarRecentB1ockHashes11111111111111111111",
  "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
  "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",
  "So11111111111111111111111111111111111111112",
  "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr",
  // Known Pacifica infrastructure accounts from SDK examples (not user wallets)
  "9Gdmhq4Gv1LnNMp7aiS1HSVd7pNnXNMsbuXALCQRmGjY", // central state
  "72R843XwZxqWhsJceARQQTTbYtWy6Zw9et2YV4FpRHTa", // pacifica vault
  "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", // USDC mint
]);

const DIRECT_USER_KEYS = new Set([
  "user",
  "owner",
  "authority",
  "trader",
  "maker",
  "taker",
  "signer",
  "payer",
  "wallet",
  "account",
  "from",
  "to",
  "source",
  "destination",
]);

function isBase58Address(value) {
  if (!value || typeof value !== "string") return false;
  return BASE58_RE.test(value.trim());
}

function normalizeAddress(value) {
  if (!isBase58Address(value)) return null;
  return String(value).trim();
}

function createEvidenceAccumulator() {
  return new Map();
}

function addEvidence(map, wallet, evidenceType, weight, meta = null) {
  const key = normalizeAddress(wallet);
  if (!key) return;

  const prev = map.get(key) || {
    wallet: key,
    confidence: 0,
    evidence: [],
    sourceCounts: {},
  };

  prev.confidence = Math.min(0.99, prev.confidence + weight);
  prev.evidence.push({
    type: evidenceType,
    ...(meta ? { meta } : {}),
  });
  prev.sourceCounts[evidenceType] = (prev.sourceCounts[evidenceType] || 0) + 1;

  map.set(key, prev);
}

function toPubkeyText(entry) {
  if (!entry) return null;
  if (typeof entry === "string") return normalizeAddress(entry);
  if (typeof entry === "object") {
    if (typeof entry.pubkey === "string") return normalizeAddress(entry.pubkey);
    if (entry.pubkey && typeof entry.pubkey.toString === "function") {
      return normalizeAddress(entry.pubkey.toString());
    }
  }
  return null;
}

function extractAccountKeys(tx = {}) {
  const keys = [];
  const message = tx.transaction && tx.transaction.message ? tx.transaction.message : {};
  const accountKeys = Array.isArray(message.accountKeys) ? message.accountKeys : [];

  accountKeys.forEach((entry, index) => {
    if (typeof entry === "string") {
      const pubkey = normalizeAddress(entry);
      if (pubkey) {
        keys.push({
          index,
          pubkey,
          signer: index === 0,
          writable: false,
          source: null,
        });
      }
      return;
    }

    const pubkey = toPubkeyText(entry);
    if (!pubkey) return;

    keys.push({
      index,
      pubkey,
      signer: Boolean(entry.signer),
      writable: Boolean(entry.writable),
      source: entry.source || null,
    });
  });

  return keys;
}

function resolveProgramIdFromInstruction(ix, accountKeys = []) {
  if (!ix || typeof ix !== "object") return null;

  if (typeof ix.programId === "string") return normalizeAddress(ix.programId);

  if (Number.isInteger(ix.programIdIndex)) {
    const key = accountKeys[ix.programIdIndex];
    return key && key.pubkey ? key.pubkey : null;
  }

  return null;
}

function collectProgramIds(tx = {}) {
  const out = new Set();
  const message = tx.transaction && tx.transaction.message ? tx.transaction.message : {};
  const accountKeys = extractAccountKeys(tx);

  const instructions = Array.isArray(message.instructions) ? message.instructions : [];
  instructions.forEach((ix) => {
    const id = resolveProgramIdFromInstruction(ix, accountKeys);
    if (id) out.add(id);
  });

  const inner = tx.meta && Array.isArray(tx.meta.innerInstructions) ? tx.meta.innerInstructions : [];
  inner.forEach((group) => {
    const list = Array.isArray(group.instructions) ? group.instructions : [];
    list.forEach((ix) => {
      const id = resolveProgramIdFromInstruction(ix, accountKeys);
      if (id) out.add(id);
    });
  });

  return out;
}

function collectParsedInstructions(tx = {}) {
  const message = tx.transaction && tx.transaction.message ? tx.transaction.message : {};
  const topInstructions = Array.isArray(message.instructions) ? message.instructions : [];
  const innerGroups = tx.meta && Array.isArray(tx.meta.innerInstructions) ? tx.meta.innerInstructions : [];

  const out = [];
  topInstructions.forEach((ix, idx) => {
    out.push({
      ix,
      scope: `ix:${idx}`,
      location: {
        kind: "top",
        instructionIndex: idx,
        parentInstructionIndex: null,
        innerInstructionIndex: null,
      },
    });
  });

  innerGroups.forEach((group, gIdx) => {
    const inner = Array.isArray(group.instructions) ? group.instructions : [];
    inner.forEach((ix, idx) => {
      out.push({
        ix,
        scope: `inner:${gIdx}:${idx}`,
        location: {
          kind: "inner",
          instructionIndex: Number(group && Number.isFinite(Number(group.index)) ? group.index : gIdx),
          parentInstructionIndex: Number.isFinite(Number(group && group.index))
            ? Number(group.index)
            : null,
          innerInstructionIndex: idx,
        },
      });
    });
  });

  return out;
}

function shouldInspectFieldName(key = "") {
  const normalized = String(key || "").toLowerCase();
  if (DIRECT_USER_KEYS.has(normalized)) return true;
  if (normalized.endsWith("owner")) return true;
  if (normalized.endsWith("authority")) return true;
  if (normalized.endsWith("signer")) return true;
  if (normalized.endsWith("wallet")) return true;
  if (normalized.endsWith("account")) return true;
  return false;
}

function walkInfoObject(value, onAddress, maxDepth = 5, depth = 0, path = "") {
  if (depth > maxDepth || value === null || value === undefined) return;

  if (typeof value === "string") {
    const addr = normalizeAddress(value);
    if (addr) onAddress(addr, path || "value");
    return;
  }

  if (Array.isArray(value)) {
    value.forEach((item, index) => {
      walkInfoObject(item, onAddress, maxDepth, depth + 1, `${path}[${index}]`);
    });
    return;
  }

  if (typeof value !== "object") return;

  Object.entries(value).forEach(([key, item]) => {
    const nextPath = path ? `${path}.${key}` : key;
    if (typeof item === "string" && shouldInspectFieldName(key)) {
      const addr = normalizeAddress(item);
      if (addr) onAddress(addr, nextPath);
      return;
    }
    walkInfoObject(item, onAddress, maxDepth, depth + 1, nextPath);
  });
}

function extractInstructionCandidates(tx = {}, evidence) {
  function inspectInstruction(ix, scopeLabel) {
    if (!ix || typeof ix !== "object") return;

    if (ix.parsed && ix.parsed.info) {
      walkInfoObject(ix.parsed.info, (addr, fieldPath) => {
        addEvidence(evidence, addr, "parsed_info", 0.55, {
          scope: scopeLabel,
          field: fieldPath,
          program: ix.program || null,
        });
      });
    }
  }

  collectParsedInstructions(tx).forEach(({ ix, scope, location }) => {
    inspectInstruction(ix, scope);
  });
}

function extractTokenOwners(tx = {}, evidence) {
  const meta = tx.meta || {};
  const pre = Array.isArray(meta.preTokenBalances) ? meta.preTokenBalances : [];
  const post = Array.isArray(meta.postTokenBalances) ? meta.postTokenBalances : [];

  [...pre, ...post].forEach((row) => {
    if (row && row.owner) {
      addEvidence(evidence, row.owner, "token_owner", 0.45, {
        mint: row.mint || null,
      });
    }
  });
}

function extractCandidateWalletsFromTransaction(tx, options = {}) {
  const accountKeys = extractAccountKeys(tx);
  const evidence = createEvidenceAccumulator();

  if (!accountKeys.length) {
    return {
      candidates: [],
      context: {
        feePayer: null,
        signerCount: 0,
        programIds: [],
      },
    };
  }

  const feePayer = accountKeys[0] ? accountKeys[0].pubkey : null;
  if (feePayer) {
    addEvidence(evidence, feePayer, "fee_payer", 0.95);
  }

  accountKeys.forEach((key) => {
    if (key.signer) {
      addEvidence(evidence, key.pubkey, "signer", 0.75, {
        writable: key.writable,
      });
    }
  });

  extractInstructionCandidates(tx, evidence);
  extractTokenOwners(tx, evidence);

  const runtimeProgramIds = collectProgramIds(tx);
  const excluded = new Set(DEFAULT_EXCLUDED_PROGRAMS);

  (options.programIds || []).forEach((id) => {
    const normalized = normalizeAddress(id);
    if (normalized) excluded.add(normalized);
  });

  (options.exclude || []).forEach((id) => {
    const normalized = normalizeAddress(id);
    if (normalized) excluded.add(normalized);
  });

  runtimeProgramIds.forEach((id) => excluded.add(id));

  const candidates = Array.from(evidence.values())
    .filter((row) => !excluded.has(row.wallet))
    .filter((row) => {
      const src = row.sourceCounts || {};
      const hasStrong = src.fee_payer || src.signer || src.parsed_info;
      if (hasStrong) return true;
      return row.confidence >= 0.7;
    })
    .map((row) => ({
      wallet: row.wallet,
      confidence: Number(row.confidence.toFixed(4)),
      evidence: row.evidence,
      sourceCounts: row.sourceCounts,
    }))
    .sort((a, b) => b.confidence - a.confidence);

  return {
    candidates,
    context: {
      feePayer,
      signerCount: accountKeys.filter((k) => k.signer).length,
      programIds: Array.from(runtimeProgramIds.values()),
    },
  };
}

function buildTokenOwnerIndex(tx = {}) {
  const accountKeys = extractAccountKeys(tx);
  const pubkeyByIndex = new Map(accountKeys.map((row) => [Number(row.index), row.pubkey]));

  const ownersByTokenAccount = new Map();
  const balances = [
    ...(Array.isArray(tx.meta && tx.meta.preTokenBalances) ? tx.meta.preTokenBalances : []),
    ...(Array.isArray(tx.meta && tx.meta.postTokenBalances) ? tx.meta.postTokenBalances : []),
  ];

  balances.forEach((row) => {
    if (!row || !row.owner) return;
    const owner = normalizeAddress(row.owner);
    if (!owner) return;

    if (Number.isFinite(Number(row.accountIndex))) {
      const idx = Number(row.accountIndex);
      const tokenAccount = pubkeyByIndex.get(idx);
      if (tokenAccount) ownersByTokenAccount.set(tokenAccount, owner);
    }
  });

  return ownersByTokenAccount;
}

function extractDepositWalletsFromTransaction(tx, options = {}) {
  const accountKeys = extractAccountKeys(tx);
  const feePayer = accountKeys[0] ? accountKeys[0].pubkey : null;
  const signerWallets = accountKeys.filter((k) => Boolean(k.signer)).map((k) => k.pubkey);
  const runtimeProgramIds = collectProgramIds(tx);
  const runtimeAccountAddresses = new Set(
    accountKeys.map((row) => (row && row.pubkey ? row.pubkey : null)).filter(Boolean)
  );

  const configuredPrograms = new Set(
    (Array.isArray(options.programIds) ? options.programIds : [])
      .map((id) => normalizeAddress(id))
      .filter(Boolean)
  );
  const vaults = new Set(
    (Array.isArray(options.vaults) ? options.vaults : [])
      .map((id) => normalizeAddress(id))
      .filter(Boolean)
  );

  if (!vaults.size) {
    return {
      candidates: [],
      deposits: [],
      context: {
        feePayer,
        signerCount: accountKeys.filter((k) => k.signer).length,
        programIds: Array.from(runtimeProgramIds.values()),
      },
    };
  }

  if (configuredPrograms.size) {
    let hasProgram = false;
    configuredPrograms.forEach((id) => {
      if (runtimeProgramIds.has(id) || runtimeAccountAddresses.has(id)) hasProgram = true;
    });
    if (!hasProgram) {
      return {
        candidates: [],
        deposits: [],
        context: {
          feePayer,
          signerCount: accountKeys.filter((k) => k.signer).length,
          programIds: Array.from(runtimeProgramIds.values()),
        },
      };
    }
  }

  const tokenOwnerBySourceAccount = buildTokenOwnerIndex(tx);
  const excluded = new Set(DEFAULT_EXCLUDED_PROGRAMS);

  (options.exclude || []).forEach((id) => {
    const normalized = normalizeAddress(id);
    if (normalized) excluded.add(normalized);
  });
  configuredPrograms.forEach((id) => excluded.add(id));
  vaults.forEach((id) => excluded.add(id));

  const candidateMap = new Map();
  const deposits = [];

  function addCandidate(wallet, confidence, sourceType, meta = {}) {
    const key = normalizeAddress(wallet);
    if (!key || excluded.has(key)) return;

    const prev = candidateMap.get(key) || {
      wallet: key,
      confidence: 0,
      evidence: [],
      sourceCounts: {},
    };

    prev.confidence = Math.min(0.99, Math.max(prev.confidence, confidence));
    prev.evidence.push({ type: sourceType, meta });
    prev.sourceCounts[sourceType] = (prev.sourceCounts[sourceType] || 0) + 1;
    candidateMap.set(key, prev);
  }

  collectParsedInstructions(tx).forEach(({ ix, scope, location }) => {
    if (!ix || !ix.parsed || !ix.parsed.info) return;

    const parsedType = String(ix.parsed.type || "").toLowerCase();
    if (parsedType !== "transfer" && parsedType !== "transferchecked") return;

    const info = ix.parsed.info || {};
    const destination = normalizeAddress(info.destination);
    if (!destination || !vaults.has(destination)) return;

    const source = normalizeAddress(info.source);
    const authority = normalizeAddress(
      info.authority || info.owner || info.sourceOwner || info.multisigAuthority
    );
    const sourceOwner = source ? tokenOwnerBySourceAccount.get(source) : null;

    const normalizedSourceOwner = normalizeAddress(sourceOwner);
    const normalizedFeePayer = normalizeAddress(feePayer);
    const feePayerFallbackAllowed =
      !authority &&
      !normalizedSourceOwner &&
      normalizedFeePayer &&
      signerWallets.length === 1 &&
      signerWallets[0] === normalizedFeePayer;

    const wallet =
      authority ||
      normalizedSourceOwner ||
      (feePayerFallbackAllowed ? normalizedFeePayer : null);

    if (!wallet) return;

    const selectedBy = authority
      ? "authority"
      : normalizedSourceOwner
      ? "source_owner"
      : "fee_payer_fallback";

    addCandidate(
      wallet,
      authority ? 0.99 : normalizedSourceOwner ? 0.92 : 0.8,
      authority
        ? "deposit_authority"
        : normalizedSourceOwner
        ? "deposit_source_owner"
        : "deposit_fee_payer",
      {
        scope,
        destination,
        source,
        selectedBy,
      }
    );

    deposits.push({
      wallet,
      destination,
      source: source || null,
      mint: normalizeAddress(info.mint) || null,
      authority: authority || null,
      sourceOwner: normalizedSourceOwner || null,
      scope,
      instructionType: parsedType,
      selectedBy,
      instructionKind: location && location.kind ? location.kind : null,
      instructionIndex:
        location && Number.isFinite(Number(location.instructionIndex))
          ? Number(location.instructionIndex)
          : null,
      parentInstructionIndex:
        location && Number.isFinite(Number(location.parentInstructionIndex))
          ? Number(location.parentInstructionIndex)
          : null,
      innerInstructionIndex:
        location && Number.isFinite(Number(location.innerInstructionIndex))
          ? Number(location.innerInstructionIndex)
          : null,
    });
  });

  const candidates = Array.from(candidateMap.values())
    .map((row) => ({
      wallet: row.wallet,
      confidence: Number(row.confidence.toFixed(4)),
      evidence: row.evidence,
      sourceCounts: row.sourceCounts,
    }))
    .sort((a, b) => b.confidence - a.confidence);

  return {
    candidates,
    deposits,
    context: {
      feePayer,
      signerCount: accountKeys.filter((k) => k.signer).length,
      programIds: Array.from(runtimeProgramIds.values()),
    },
  };
}

module.exports = {
  DEFAULT_EXCLUDED_PROGRAMS: Array.from(DEFAULT_EXCLUDED_PROGRAMS.values()),
  extractCandidateWalletsFromTransaction,
  extractDepositWalletsFromTransaction,
  isBase58Address,
  normalizeAddress,
};
