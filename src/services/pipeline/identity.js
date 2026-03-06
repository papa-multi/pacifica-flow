const path = require("path");
const { ensureDir, normalizeAddress, readJson, writeJsonAtomic } = require("./utils");

function emptyState() {
  return {
    account: null,
    updatedAt: null,
  };
}

class IdentityService {
  constructor(options = {}) {
    this.dataDir = options.dataDir;
    this.filePath = options.filePath || path.join(this.dataDir || ".", "identity.json");
    this.state = emptyState();
  }

  load() {
    ensureDir(path.dirname(this.filePath));
    this.state = readJson(this.filePath, emptyState()) || emptyState();
    return this.state;
  }

  setAccount(addressRaw) {
    const next = normalizeAddress(addressRaw);
    this.state.account = next;
    this.state.updatedAt = Date.now();
    return this.state;
  }

  getAccount() {
    return this.state.account;
  }

  save() {
    writeJsonAtomic(this.filePath, this.state);
  }
}

module.exports = {
  IdentityService,
};
