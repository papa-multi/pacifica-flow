const crypto = require("crypto");
const path = require("path");
const {
  ensureDir,
  normalizeAddress,
  readJson,
  writeJsonAtomic,
} = require("../pipeline/utils");

function emptyDb() {
  return {
    collections: [],
    updatedAt: null,
  };
}

function validateCollectionInput(input = {}) {
  const errors = [];
  const name = String(input.name || "").trim();
  const symbol = String(input.symbol || "").trim().toUpperCase();
  const description = String(input.description || "").trim();
  const network = String(input.network || "").trim().toLowerCase();
  const supplyType = String(input.supplyType || "fixed").trim().toLowerCase();
  const royaltyBps = Number(input.royaltyBps || 0);

  if (!name || name.length < 3) errors.push("name must be at least 3 chars");
  if (!symbol || symbol.length < 2 || symbol.length > 10)
    errors.push("symbol must be 2-10 chars");
  if (!description) errors.push("description is required");
  if (!["testnet", "mainnet"].includes(network)) errors.push("network must be testnet or mainnet");
  if (!["fixed", "unlimited"].includes(supplyType))
    errors.push("supplyType must be fixed or unlimited");
  if (!Number.isFinite(royaltyBps) || royaltyBps < 0 || royaltyBps > 10000)
    errors.push("royaltyBps must be between 0 and 10000");

  const mintPrice = String(input.mintPrice || "0").trim();
  if (!mintPrice) errors.push("mintPrice is required");

  const mintStart = Number(input.mintStart || 0);
  const mintEnd = Number(input.mintEnd || 0);
  if (!Number.isFinite(mintStart) || mintStart <= 0) errors.push("mintStart must be a unix ms timestamp");
  if (!Number.isFinite(mintEnd) || mintEnd <= mintStart)
    errors.push("mintEnd must be greater than mintStart");

  let supplyCap = null;
  if (supplyType === "fixed") {
    supplyCap = Number(input.supplyCap || 0);
    if (!Number.isFinite(supplyCap) || supplyCap <= 0)
      errors.push("supplyCap must be > 0 for fixed supply");
  }

  return {
    ok: errors.length === 0,
    errors,
    normalized: {
      name,
      symbol,
      description,
      logoUrl: String(input.logoUrl || "").trim() || null,
      bannerUrl: String(input.bannerUrl || "").trim() || null,
      network,
      supplyType,
      supplyCap,
      mintPrice,
      mintStart,
      mintEnd,
      royaltyBps,
    },
  };
}

class CollectionStore {
  constructor(options = {}) {
    this.dataDir = options.dataDir;
    this.filePath = options.filePath || path.join(this.dataDir || ".", "creator", "collections.json");
    this.db = emptyDb();
  }

  load() {
    ensureDir(path.dirname(this.filePath));
    this.db = readJson(this.filePath, emptyDb()) || emptyDb();
    return this.db;
  }

  save() {
    this.db.updatedAt = Date.now();
    writeJsonAtomic(this.filePath, this.db);
  }

  list(owner = null) {
    const ownerAddress = normalizeAddress(owner);
    const all = Array.isArray(this.db.collections) ? this.db.collections : [];
    if (!ownerAddress) return all;
    return all.filter((item) => String(item.owner || "") === ownerAddress);
  }

  getById(collectionId) {
    return this.list().find((item) => item.id === collectionId) || null;
  }

  createDraft(owner, input) {
    const ownerAddress = normalizeAddress(owner);
    if (!ownerAddress) {
      return {
        ok: false,
        errors: ["owner wallet is required"],
      };
    }

    const validation = validateCollectionInput(input);
    if (!validation.ok) return validation;

    const now = Date.now();
    const draft = {
      id: `col_${crypto.randomUUID()}`,
      owner: ownerAddress,
      status: "draft",
      createdAt: now,
      updatedAt: now,
      ...validation.normalized,
      mintConfig: null,
      launchPlan: null,
    };

    this.db.collections.unshift(draft);
    this.save();

    return {
      ok: true,
      collection: draft,
    };
  }

  updateMintConfig(collectionId, mintConfig) {
    const collection = this.getById(collectionId);
    if (!collection) return null;
    collection.mintConfig = mintConfig;
    collection.updatedAt = Date.now();
    this.save();
    return collection;
  }

  setLaunchPlan(collectionId, launchPlan) {
    const collection = this.getById(collectionId);
    if (!collection) return null;
    collection.launchPlan = launchPlan;
    collection.updatedAt = Date.now();
    this.save();
    return collection;
  }
}

module.exports = {
  CollectionStore,
  validateCollectionInput,
};
