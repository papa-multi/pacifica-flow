const fs = require("fs");
const path = require("path");

function ensureDir(dirPath) {
  if (!dirPath) return;
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }
}

function readJson(filePath, fallback = null) {
  try {
    if (!fs.existsSync(filePath)) return fallback;
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch (_error) {
    return fallback;
  }
}

function writeJsonAtomic(filePath, data) {
  ensureDir(path.dirname(filePath));
  const tmpPath = `${filePath}.${process.pid}.${Date.now()}.${Math.random().toString(16).slice(2)}.tmp`;
  fs.writeFileSync(tmpPath, JSON.stringify(data, null, 2));
  fs.renameSync(tmpPath, filePath);
}

function appendLine(filePath, line) {
  ensureDir(path.dirname(filePath));
  fs.appendFileSync(filePath, `${line}\n`);
}

function splitLines(content) {
  if (!content) return [];
  return String(content)
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
}

function nowMs() {
  return Date.now();
}

function normalizeAddress(value) {
  if (value === null || value === undefined) return null;
  const text = String(value).trim();
  return text || null;
}

function parseNumber(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function toFixedSafe(value, digits = 2) {
  const num = parseNumber(value, NaN);
  if (!Number.isFinite(num)) return "0";
  return num.toFixed(digits);
}

module.exports = {
  appendLine,
  ensureDir,
  nowMs,
  normalizeAddress,
  parseNumber,
  readJson,
  splitLines,
  toFixedSafe,
  writeJsonAtomic,
};
