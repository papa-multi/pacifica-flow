function stableShardHash(value) {
  const text = String(value || "");
  let hash = 5381;
  for (let i = 0; i < text.length; i += 1) {
    hash = ((hash << 5) + hash + text.charCodeAt(i)) >>> 0;
  }
  return hash >>> 0;
}

function normalizeShardItems(items = []) {
  return (Array.isArray(items) ? items : [])
    .map((item, idx) => {
      if (!item || typeof item !== "object") return null;
      const id = String(item.id || `shard_${idx}`).trim();
      if (!id) return null;
      return {
        ...item,
        id,
      };
    })
    .filter(Boolean);
}

function shardIndexForKey(key, shardCount) {
  const count = Math.max(1, Math.floor(Number(shardCount || 1)));
  return stableShardHash(key) % count;
}

function assignShardByKey(key, items = []) {
  const normalized = normalizeShardItems(items);
  if (!normalized.length) return null;
  const index = shardIndexForKey(key, normalized.length);
  return {
    index,
    item: normalized[index],
  };
}

module.exports = {
  assignShardByKey,
  normalizeShardItems,
  shardIndexForKey,
  stableShardHash,
};
