"use strict";

const fs = require("fs");
const os = require("os");
const path = require("path");
const { spawnSync } = require("child_process");
const crypto = require("crypto");

const REPO_ROOT = path.resolve(__dirname, "..");
const CONFIG_PATH =
  process.env.PACIFICA_REPO_STATE_SYNC_CONFIG ||
  path.join(REPO_ROOT, "config", "repo-state-sync.json");

function readJson(filePath, fallback) {
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch (_error) {
    return fallback;
  }
}

function writeJson(filePath, value) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, JSON.stringify(value, null, 2) + "\n", "utf8");
}

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    cwd: options.cwd || REPO_ROOT,
    encoding: "utf8",
    stdio: options.stdio || ["ignore", "pipe", "pipe"],
    env: options.env || process.env,
  });
  if (options.allowFailure) return result;
  if (result.status !== 0) {
    const stderr = String(result.stderr || "").trim();
    const stdout = String(result.stdout || "").trim();
    throw new Error(
      `${command} ${args.join(" ")} failed (${result.status}): ${stderr || stdout || "unknown error"}`
    );
  }
  return result;
}

function loadConfig() {
  const raw = readJson(CONFIG_PATH, null);
  if (!raw || typeof raw !== "object") {
    throw new Error(`Missing or invalid config: ${CONFIG_PATH}`);
  }
  return {
    remote: String(raw.remote || "origin").trim(),
    branch: String(raw.branch || "state-sync").trim(),
    worktree: String(raw.worktree || "/root/pacifica-flow-state-sync").trim(),
    stateRoot: String(raw.stateRoot || "runtime-state").trim(),
    mirrorPaths: Array.isArray(raw.mirrorPaths) ? raw.mirrorPaths.slice() : [],
    chunkedLogs: Array.isArray(raw.chunkedLogs) ? raw.chunkedLogs.slice() : [],
  };
}

function repoHead() {
  return run("git", ["rev-parse", "HEAD"]).stdout.trim();
}

function branchExists(ref) {
  return run("git", ["show-ref", "--verify", "--quiet", ref], { allowFailure: true }).status === 0;
}

function remoteBranchExists(remote, branch) {
  const ref = `refs/remotes/${remote}/${branch}`;
  return branchExists(ref);
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function removePath(targetPath) {
  fs.rmSync(targetPath, { recursive: true, force: true });
}

function listNonGitEntries(dirPath) {
  if (!fs.existsSync(dirPath)) return [];
  return fs.readdirSync(dirPath).filter((name) => name !== ".git");
}

function ensureStateBranchWorktree(config) {
  if (fs.existsSync(path.join(config.worktree, ".git"))) {
    return { created: false };
  }
  ensureDir(path.dirname(config.worktree));
  if (branchExists(`refs/heads/${config.branch}`)) {
    run("git", ["worktree", "add", "--force", config.worktree, config.branch]);
    return { created: false };
  }
  if (remoteBranchExists(config.remote, config.branch)) {
    run("git", [
      "worktree",
      "add",
      "--force",
      "-B",
      config.branch,
      config.worktree,
      `${config.remote}/${config.branch}`,
    ]);
    return { created: false };
  }
  run("git", ["worktree", "add", "--force", "-b", config.branch, config.worktree, "HEAD"]);
  return { created: true };
}

function ensureWorktreeGitIdentity(worktree) {
  const name =
    String(process.env.PACIFICA_STATE_SYNC_GIT_NAME || "Pacifica State Sync").trim() ||
    "Pacifica State Sync";
  const email =
    String(process.env.PACIFICA_STATE_SYNC_GIT_EMAIL || "pacifica-state-sync@local").trim() ||
    "pacifica-state-sync@local";
  run("git", ["config", "user.name", name], { cwd: worktree });
  run("git", ["config", "user.email", email], { cwd: worktree });
}

function resetWorktreeToStateOnly(config) {
  const entries = listNonGitEntries(config.worktree);
  for (const entry of entries) {
    removePath(path.join(config.worktree, entry));
  }
  const readmePath = path.join(config.worktree, "README.md");
  fs.writeFileSync(
    readmePath,
    [
      "# Pacifica Flow Runtime State",
      "",
      "This branch is managed automatically.",
      "It stores resumable runtime state and chunked append-only logs for recovery on another server.",
      "",
    ].join("\n"),
    "utf8"
  );
}

function copyFileExact(srcPath, destPath) {
  ensureDir(path.dirname(destPath));
  fs.copyFileSync(srcPath, destPath);
  const stat = fs.statSync(srcPath);
  fs.utimesSync(destPath, stat.atime, stat.mtime);
}

function syncPathIntoWorktree(repoRelativePath, currentRoot) {
  const srcPath = path.join(REPO_ROOT, repoRelativePath);
  const destPath = path.join(currentRoot, repoRelativePath);
  if (!fs.existsSync(srcPath)) {
    removePath(destPath);
    return { path: repoRelativePath, exists: false, type: null, bytes: 0 };
  }
  const stat = fs.statSync(srcPath);
  if (stat.isDirectory()) {
    removePath(destPath);
    ensureDir(destPath);
    run("rsync", ["-a", "--delete", `${srcPath}/`, `${destPath}/`]);
    const du = run("du", ["-sb", destPath]);
    const bytes = Number(String(du.stdout || "").trim().split(/\s+/)[0] || 0) || 0;
    return { path: repoRelativePath, exists: true, type: "dir", bytes };
  }
  copyFileExact(srcPath, destPath);
  return { path: repoRelativePath, exists: true, type: "file", bytes: stat.size };
}

async function copyChunk(srcPath, destPath, start, endExclusive) {
  await new Promise((resolve, reject) => {
    ensureDir(path.dirname(destPath));
    const input = fs.createReadStream(srcPath, { start, end: endExclusive - 1 });
    const output = fs.createWriteStream(destPath);
    input.on("error", reject);
    output.on("error", reject);
    output.on("close", resolve);
    input.pipe(output);
  });
}

async function syncChunkedLog(spec, config, stateRootPath) {
  const sourceRelative = String(spec.source || "").trim();
  const destRelative = String(spec.dest || "").trim();
  const chunkBytes = Math.max(1024 * 1024, Number(spec.chunkBytes || 64 * 1024 * 1024) || 64 * 1024 * 1024);
  const srcPath = path.join(REPO_ROOT, sourceRelative);
  const destDir = path.join(stateRootPath, destRelative);
  if (!fs.existsSync(srcPath)) {
    removePath(destDir);
    return {
      source: sourceRelative,
      dest: destRelative,
      exists: false,
      bytes: 0,
      chunkBytes,
      chunkCount: 0,
    };
  }
  ensureDir(destDir);
  const size = fs.statSync(srcPath).size;
  const chunkCount = Math.ceil(size / chunkBytes);
  for (let index = 0; index < chunkCount; index += 1) {
    const start = index * chunkBytes;
    const end = Math.min(size, start + chunkBytes);
    const chunkName = `${String(start).padStart(16, "0")}.ndjson`;
    const chunkPath = path.join(destDir, chunkName);
    const expectedSize = end - start;
    let needsWrite = true;
    if (fs.existsSync(chunkPath)) {
      const existingSize = fs.statSync(chunkPath).size;
      needsWrite = existingSize !== expectedSize;
    }
    if (needsWrite) {
      const tempPath = `${chunkPath}.tmp-${process.pid}`;
      await copyChunk(srcPath, tempPath, start, end);
      fs.renameSync(tempPath, chunkPath);
    }
  }
  const expectedNames = new Set();
  for (let index = 0; index < chunkCount; index += 1) {
    expectedNames.add(`${String(index * chunkBytes).padStart(16, "0")}.ndjson`);
  }
  for (const name of fs.readdirSync(destDir)) {
    if (name === "manifest.json") continue;
    if (!expectedNames.has(name)) {
      removePath(path.join(destDir, name));
    }
  }
  const manifest = {
    source: sourceRelative,
    bytes: size,
    chunkBytes,
    chunkCount,
    updatedAt: Date.now(),
  };
  writeJson(path.join(destDir, "manifest.json"), manifest);
  return {
    source: sourceRelative,
    dest: destRelative,
    exists: true,
    bytes: size,
    chunkBytes,
    chunkCount,
  };
}

function gitStatusHasChanges(worktree) {
  const result = run("git", ["status", "--porcelain"], { cwd: worktree });
  return String(result.stdout || "").trim().length > 0;
}

function gitCommit(worktree, message) {
  run("git", ["add", "-A"], { cwd: worktree });
  if (!gitStatusHasChanges(worktree)) {
    return false;
  }
  run("git", ["commit", "-m", message], { cwd: worktree });
  return true;
}

function gitPush(worktree, remote, branch) {
  return run("git", ["push", remote, `${branch}:${branch}`], {
    cwd: worktree,
    allowFailure: true,
  });
}

function sha256File(filePath) {
  const hash = crypto.createHash("sha256");
  const fd = fs.openSync(filePath, "r");
  try {
    const buffer = Buffer.allocUnsafe(1024 * 1024);
    while (true) {
      const bytesRead = fs.readSync(fd, buffer, 0, buffer.length, null);
      if (!bytesRead) break;
      hash.update(buffer.subarray(0, bytesRead));
    }
  } finally {
    fs.closeSync(fd);
  }
  return hash.digest("hex");
}

async function snapshot() {
  const config = loadConfig();
  const worktreeInfo = ensureStateBranchWorktree(config);
  ensureWorktreeGitIdentity(config.worktree);
  if (worktreeInfo.created) {
    resetWorktreeToStateOnly(config);
    gitCommit(config.worktree, "state-sync: initialize branch");
  }

  const stateRootPath = path.join(config.worktree, config.stateRoot);
  const currentRoot = path.join(stateRootPath, "current");
  const manifestsDir = path.join(stateRootPath, "manifests");
  ensureDir(currentRoot);
  ensureDir(manifestsDir);

  const mirrorSummary = [];
  for (const repoRelativePath of config.mirrorPaths) {
    mirrorSummary.push(syncPathIntoWorktree(repoRelativePath, currentRoot));
  }

  const chunkSummary = [];
  for (const spec of config.chunkedLogs) {
    chunkSummary.push(await syncChunkedLog(spec, config, stateRootPath));
  }

  const now = new Date();
  const timestamp = now.toISOString().replace(/\.\d{3}Z$/, "Z");
  const manifest = {
    generatedAt: Date.now(),
    generatedAtIso: timestamp,
    hostname: os.hostname(),
    repoHead: repoHead(),
    remote: config.remote,
    branch: config.branch,
    stateRoot: config.stateRoot,
    mirror: mirrorSummary,
    chunkedLogs: chunkSummary,
    restoreCommand: "node scripts/repo_state_sync.js restore --fetch --start",
  };
  writeJson(path.join(manifestsDir, "latest.json"), manifest);
  writeJson(path.join(manifestsDir, `${timestamp.replace(/[:]/g, "-")}.json`), manifest);

  const committed = gitCommit(config.worktree, `state-sync: ${timestamp}`);
  const pushResult = gitPush(config.worktree, config.remote, config.branch);
  const pushed = pushResult.status === 0;
  const summary = {
    committed,
    pushed,
    pushStatus: pushResult.status,
    pushStdout: String(pushResult.stdout || "").trim() || null,
    pushStderr: String(pushResult.stderr || "").trim() || null,
    manifestSha256: sha256File(path.join(manifestsDir, "latest.json")),
    worktree: config.worktree,
    branch: config.branch,
  };
  console.log(JSON.stringify(summary, null, 2));
  if (!pushed) {
    throw new Error(summary.pushStderr || "git push failed");
  }
}

function restoreChunkedLog(logRoot, destPath) {
  const manifest = readJson(path.join(logRoot, "manifest.json"), null);
  if (!manifest || !fs.existsSync(logRoot)) {
    return;
  }
  ensureDir(path.dirname(destPath));
  const tempPath = `${destPath}.tmp-${process.pid}`;
  const outputFd = fs.openSync(tempPath, "w");
  const files = fs
    .readdirSync(logRoot)
    .filter((name) => /^\d+\.ndjson$/.test(name))
    .sort();
  try {
    for (const file of files) {
      const sourcePath = path.join(logRoot, file);
      const inputFd = fs.openSync(sourcePath, "r");
      try {
        const buffer = Buffer.allocUnsafe(1024 * 1024);
        while (true) {
          const bytesRead = fs.readSync(inputFd, buffer, 0, buffer.length, null);
          if (!bytesRead) break;
          fs.writeSync(outputFd, buffer, 0, bytesRead);
        }
      } finally {
        fs.closeSync(inputFd);
      }
    }
  } finally {
    fs.closeSync(outputFd);
  }
  fs.renameSync(tempPath, destPath);
}

function fetchRemoteBranch(config) {
  return run("git", ["fetch", config.remote, config.branch], {
    cwd: REPO_ROOT,
    allowFailure: true,
  });
}

function syncFromCurrentMirror(currentRoot) {
  if (!fs.existsSync(currentRoot)) {
    throw new Error(`Missing current mirror: ${currentRoot}`);
  }
  run("rsync", ["-a", `${currentRoot}/`, `${REPO_ROOT}/`]);
}

async function restore({ fetch = false, start = false } = {}) {
  const config = loadConfig();
  if (fetch) {
    const fetchResult = fetchRemoteBranch(config);
    if (fetchResult.status !== 0) {
      console.error(String(fetchResult.stderr || fetchResult.stdout || "").trim());
    }
  }
  ensureStateBranchWorktree(config);
  const stateRootPath = path.join(config.worktree, config.stateRoot);
  const currentRoot = path.join(stateRootPath, "current");
  syncFromCurrentMirror(currentRoot);
  for (const spec of config.chunkedLogs) {
    const sourceRelative = String(spec.source || "").trim();
    const destRelative = String(spec.dest || "").trim();
    const logRoot = path.join(stateRootPath, destRelative);
    if (!fs.existsSync(logRoot)) continue;
    const destPath = path.join(REPO_ROOT, sourceRelative);
    restoreChunkedLog(logRoot, destPath);
  }
  if (start) {
    run("bash", [path.join(REPO_ROOT, "scripts", "systemd", "install.sh"), "--now"]);
  }
  const manifest = readJson(path.join(stateRootPath, "manifests", "latest.json"), null);
  console.log(
    JSON.stringify(
      {
        restored: true,
        fetched: fetch,
        started: start,
        worktree: config.worktree,
        branch: config.branch,
        manifestGeneratedAt: manifest ? manifest.generatedAtIso || manifest.generatedAt : null,
      },
      null,
      2
    )
  );
}

function status() {
  const config = loadConfig();
  const manifest = readJson(
    path.join(config.worktree, config.stateRoot, "manifests", "latest.json"),
    null
  );
  console.log(
    JSON.stringify(
      {
        config,
        latest: manifest,
      },
      null,
      2
    )
  );
}

async function main() {
  const command = String(process.argv[2] || "status").trim();
  if (command === "snapshot") {
    await snapshot();
    return;
  }
  if (command === "restore") {
    const fetch = process.argv.includes("--fetch");
    const start = process.argv.includes("--start");
    await restore({ fetch, start });
    return;
  }
  if (command === "status") {
    status();
    return;
  }
  throw new Error(`Unknown command: ${command}`);
}

main().catch((error) => {
  console.error(error.message || String(error));
  process.exit(1);
});
