const fs = require("fs");
const path = require("path");
const { normalizeAddress } = require("../pipeline/utils");

function walletFromCandidate(value) {
  if (value === null || value === undefined) return null;

  if (typeof value === "object") {
    return normalizeAddress(
      value.wallet || value.address || value.account || value.id || null
    );
  }

  return normalizeAddress(value);
}

function uniqWallets(list = []) {
  return Array.from(
    new Set(
      (Array.isArray(list) ? list : [])
        .map((value) => walletFromCandidate(value))
        .filter(Boolean)
    )
  );
}

function parseWalletsPayload(payload) {
  if (!payload) return [];

  if (Array.isArray(payload)) return uniqWallets(payload);

  if (Array.isArray(payload.wallets)) return uniqWallets(payload.wallets);

  if (Array.isArray(payload.data)) return uniqWallets(payload.data);

  if (payload.data && Array.isArray(payload.data.wallets)) {
    return uniqWallets(payload.data.wallets);
  }

  return [];
}

function readWalletsFromFile(filePath) {
  if (!filePath) return [];
  const resolved = path.resolve(filePath);
  if (!fs.existsSync(resolved)) return [];

  const text = fs.readFileSync(resolved, "utf8");
  const lines = text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line && !line.startsWith("#"));

  return uniqWallets(lines);
}

function withTimeout(ms, fn) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("wallet source timeout")), ms);
    fn()
      .then((value) => {
        clearTimeout(timer);
        resolve(value);
      })
      .catch((error) => {
        clearTimeout(timer);
        reject(error);
      });
  });
}

async function readWalletsFromUrl(url, timeoutMs = 10000) {
  if (!url) return [];
  if (typeof fetch !== "function") return [];

  const result = await withTimeout(timeoutMs, async () => {
    const response = await fetch(url, {
      method: "GET",
      headers: {
        Accept: "application/json",
      },
    });

    if (!response.ok) {
      throw new Error(`wallet source URL failed with ${response.status}`);
    }

    const payload = await response.json();
    return parseWalletsPayload(payload);
  });

  return uniqWallets(result);
}

function createWalletSource({ filePath, url, inlineWallets = [], logger = console }) {
  const state = {
    lastFetchedAt: null,
    lastError: null,
    lastCount: 0,
  };

  async function fetchWallets() {
    const out = [];

    try {
      out.push(...readWalletsFromFile(filePath));
    } catch (error) {
      state.lastError = `file: ${error.message}`;
      logger.warn(`[wallet-source] file load failed: ${error.message}`);
    }

    out.push(...uniqWallets(inlineWallets));

    if (url) {
      try {
        const fromUrl = await readWalletsFromUrl(url, 10000);
        out.push(...fromUrl);
      } catch (error) {
        state.lastError = `url: ${error.message}`;
        logger.warn(`[wallet-source] url load failed: ${error.message}`);
      }
    }

    const unique = uniqWallets(out);
    state.lastFetchedAt = Date.now();
    state.lastCount = unique.length;
    return unique;
  }

  function getState() {
    return {
      filePath: filePath || null,
      url: url || null,
      lastFetchedAt: state.lastFetchedAt,
      lastError: state.lastError,
      lastCount: state.lastCount,
    };
  }

  return {
    fetchWallets,
    getState,
  };
}

module.exports = {
  createWalletSource,
  parseWalletsPayload,
  readWalletsFromFile,
  readWalletsFromUrl,
  uniqWallets,
};
