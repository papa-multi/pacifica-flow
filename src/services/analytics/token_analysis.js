"use strict";

const {
  loadState: loadNextgenState,
} = require("../nextgen/core/state-store");

const DAY_MS = 24 * 60 * 60 * 1000;

function toNum(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function toTimestampMs(value) {
  const num = Number(value);
  if (!Number.isFinite(num) || num <= 0) return null;
  return num < 1e12 ? Math.round(num * 1000) : Math.round(num);
}

function normalizeWalletKey(value) {
  return String(value || "").trim();
}

function normalizeTokenSymbol(value) {
  return String(value || "").trim().toUpperCase();
}

function normalizeTokenAnalyticsTimeframe(value) {
  const normalized = String(value || "24h").trim().toLowerCase();
  if (normalized === "24h" || normalized === "7d" || normalized === "30d" || normalized === "all") {
    return normalized;
  }
  return "24h";
}

function dedupeBy(rows = [], keyFn) {
  const seen = new Set();
  const out = [];
  for (const row of Array.isArray(rows) ? rows : []) {
    const key = keyFn(row);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(row);
  }
  return out;
}

function sortByTimestampAsc(rows = [], key = "timestamp") {
  return (Array.isArray(rows) ? rows : [])
    .slice()
    .sort((a, b) => toNum(a && a[key], 0) - toNum(b && b[key], 0));
}

function round(value, digits = 2) {
  const num = Number(value);
  if (!Number.isFinite(num)) return null;
  const scale = 10 ** digits;
  return Math.round(num * scale) / scale;
}

function titleCase(value) {
  return String(value || "")
    .replace(/[_-]+/g, " ")
    .split(/\s+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase())
    .join(" ");
}

function mean(values = []) {
  const rows = (Array.isArray(values) ? values : []).filter((value) => Number.isFinite(Number(value)));
  if (!rows.length) return null;
  return rows.reduce((sum, value) => sum + Number(value), 0) / rows.length;
}

function sum(values = []) {
  return (Array.isArray(values) ? values : []).reduce((acc, value) => acc + toNum(value, 0), 0);
}

function calculatePositionUnrealizedPnlUsd({
  entry = null,
  mark = null,
  size = null,
  positionUsd = null,
  side = null,
  fallback = 0,
} = {}) {
  const entryNum = toNum(entry, NaN);
  const markNum = toNum(mark, NaN);
  let sizeNum = Math.abs(toNum(size, 0));
  if (!(sizeNum > 0) && Number.isFinite(entryNum) && entryNum > 0) {
    const notionalUsd = Math.abs(toNum(positionUsd, 0));
    if (notionalUsd > 0) {
      sizeNum = notionalUsd / entryNum;
    }
  }
  if (Number.isFinite(entryNum) && Number.isFinite(markNum) && sizeNum > 0) {
    const sideText = String(side || "").trim().toLowerCase();
    const direction = sideText.includes("short") || sideText === "ask" || sideText === "sell" ? -1 : 1;
    return round((markNum - entryNum) * sizeNum * direction, 2);
  }
  return round(toNum(fallback, 0), 2);
}

function normalizeTokenWalletRow(row = {}) {
  return row && typeof row === "object" ? row : {};
}

function getWalletSymbolVolumeUsd(walletRow = {}, symbol = "", nextgenState = null) {
  const normalizedSymbol = normalizeTokenSymbol(symbol || "");
  if (!normalizedSymbol) return 0;
  const safeRow = normalizeTokenWalletRow(walletRow);
  const directVolumes =
    safeRow.symbolVolumes && typeof safeRow.symbolVolumes === "object"
      ? safeRow.symbolVolumes
      : safeRow.symbolBreakdown && typeof safeRow.symbolBreakdown === "object"
      ? safeRow.symbolBreakdown
      : safeRow.symbol_breakdown && typeof safeRow.symbol_breakdown === "object"
      ? safeRow.symbol_breakdown
      : {};
  const directValue = toNum(
    directVolumes[normalizedSymbol] !== undefined
      ? directVolumes[normalizedSymbol]
      : directVolumes[normalizedSymbol.toLowerCase()],
    NaN
  );
  if (Number.isFinite(directValue) && directValue > 0) return directValue;
  const breakdownRows = Array.isArray(safeRow.symbolBreakdown)
    ? safeRow.symbolBreakdown
    : Array.isArray(safeRow.symbolsBreakdown)
    ? safeRow.symbolsBreakdown
    : Array.isArray(safeRow.symbol_breakdown)
    ? safeRow.symbol_breakdown
    : [];
  const breakdownValue = breakdownRows.reduce((acc, entry) => {
    if (!entry || typeof entry !== "object") return acc;
    const entrySymbol = normalizeTokenSymbol(entry.symbol || entry.market || "");
    if (entrySymbol !== normalizedSymbol) return acc;
    return (
      acc +
      Math.max(
        0,
        toNum(
          entry.volumeUsd !== undefined
            ? entry.volumeUsd
            : entry.volume_usd !== undefined
            ? entry.volume_usd
            : entry.volume_24h_usd !== undefined
            ? entry.volume_24h_usd
            : entry.notionalUsd !== undefined
            ? entry.notionalUsd
            : 0,
          0
        )
      )
    );
  }, 0);
  if (breakdownValue > 0) return breakdownValue;
  try {
    const stateSnapshot =
      nextgenState && typeof nextgenState === "object"
        ? nextgenState
        : loadNextgenState(false) || {};
    const stateWallets =
      stateSnapshot.wallets && typeof stateSnapshot.wallets === "object"
        ? stateSnapshot.wallets
        : stateSnapshot && typeof stateSnapshot === "object"
        ? stateSnapshot
        : {};
    const stateRow = stateWallets && stateWallets[String(safeRow.wallet || "").trim()];
    if (stateRow && typeof stateRow === "object") {
      const stateBreakdown = Array.isArray(stateRow.symbol_breakdown)
        ? stateRow.symbol_breakdown
        : Array.isArray(stateRow.symbolBreakdown)
        ? stateRow.symbolBreakdown
        : [];
      const stateValue = stateBreakdown.reduce((acc, entry) => {
        if (!entry || typeof entry !== "object") return acc;
        const entrySymbol = normalizeTokenSymbol(entry.symbol || entry.market || "");
        if (entrySymbol !== normalizedSymbol) return acc;
        return acc + Math.max(0, toNum(entry.volumeUsd !== undefined ? entry.volumeUsd : entry.volume_usd, 0));
      }, 0);
      if (stateValue > 0) return stateValue;
      const stateDirect = toNum(stateRow.volumeUsdSymbol?.[normalizedSymbol], NaN);
      if (Number.isFinite(stateDirect) && stateDirect > 0) return stateDirect;
    }
  } catch (_error) {
    // Best effort only.
  }
  return 0;
}

function buildTokenWalletPositionTable({
  symbol,
  walletRows = [],
  explorerDataset = null,
  lifecyclePositions = [],
  page = 1,
  pageSize = 20,
  sortKey = "openedAt",
  sortDir = "asc",
  currentPrice = null,
} = {}) {
  const normalizedSymbol = normalizeTokenSymbol(symbol || "");
  const currentPriceNum = toNum(currentPrice, NaN);
  const now = Date.now();
  const monthAgo = now - 30 * DAY_MS;
  const nextgenState = loadNextgenState(false) || {};
  const positions = (Array.isArray(explorerDataset && explorerDataset.positions)
    ? explorerDataset.positions
    : []
  ).filter((row) => normalizeTokenSymbol(row && row.symbol) === normalizedSymbol);
  const mergedWalletRows = [];
  const seenWallets = new Set();
  const walletRowSources = [
    Array.isArray(walletRows) ? walletRows : [],
    Array.isArray(explorerDataset && explorerDataset.preparedRows) ? explorerDataset.preparedRows : [],
    Array.isArray(explorerDataset && explorerDataset.walletRows) ? explorerDataset.walletRows : [],
  ];
  walletRowSources.forEach((rows) => {
    rows.forEach((row) => {
      const wallet = normalizeWalletKey(row && row.wallet);
      if (!wallet || seenWallets.has(wallet)) return;
      seenWallets.add(wallet);
      mergedWalletRows.push(normalizeTokenWalletRow(row));
    });
  });
  const walletRowsByWallet = new Map(
    mergedWalletRows.map((row) => [normalizeWalletKey(row && row.wallet), row]).filter(([wallet]) => Boolean(wallet))
  );
  const positionRowsByWallet = new Map();
  positions.forEach((row) => {
    const wallet = normalizeWalletKey(row && row.wallet);
    if (!wallet) return;
    const bucket = positionRowsByWallet.get(wallet) || [];
    bucket.push(row);
    positionRowsByWallet.set(wallet, bucket);
  });
  const lifecycleRows = Array.isArray(lifecyclePositions) ? lifecyclePositions : [];
  const lifecycleRowsByWallet = new Map();
  const lifecycleRowsByWalletAll = new Map();
  lifecycleRows.forEach((row) => {
    const wallet = normalizeWalletKey(row && row.wallet);
    if (!wallet) return;
    const allBucket = lifecycleRowsByWalletAll.get(wallet) || [];
    allBucket.push(row);
    lifecycleRowsByWalletAll.set(wallet, allBucket);
    const rowSymbol = normalizeTokenSymbol(row && row.symbol);
    if (rowSymbol !== normalizedSymbol) return;
    const bucket = lifecycleRowsByWallet.get(wallet) || [];
    bucket.push(row);
    lifecycleRowsByWallet.set(wallet, bucket);
  });

  const rows = positions
    .map((position) => {
      const wallet = normalizeWalletKey(position && position.wallet);
      if (!wallet) return null;
      const walletRow = walletRowsByWallet.get(wallet) || {};
      const walletPositions = positionRowsByWallet.get(wallet) || [];
      const walletLifecycleRows = lifecycleRowsByWallet.get(wallet) || [];
      const walletLifecycleAllRows = lifecycleRowsByWalletAll.get(wallet) || [];
      const lifecycleOpenRow =
        walletLifecycleRows.find((row) => row && row.currentOpen) ||
        walletLifecycleAllRows.find((row) => row && row.currentOpen) ||
        null;
      const entry = toNum(
        position && (position.entry !== undefined ? position.entry : position.entryPrice !== undefined ? position.entryPrice : position.entry_price),
        NaN
      );
      const positionPnlUsd = calculatePositionUnrealizedPnlUsd({
        entry,
        mark:
          position &&
          (Number.isFinite(currentPriceNum)
            ? currentPriceNum
            : position.mark !== undefined
            ? position.mark
            : position.markPrice !== undefined
            ? position.markPrice
            : position.currentPrice !== undefined
            ? position.currentPrice
            : NaN),
        size: position && position.size !== undefined ? position.size : position && position.amount !== undefined ? position.amount : position && position.qty !== undefined ? position.qty : null,
        positionUsd:
          position && (position.positionUsd !== undefined ? position.positionUsd : position.notionalUsd !== undefined ? position.notionalUsd : position.positionSizeUsd !== undefined ? position.positionSizeUsd : null),
        side: position && position.side,
        fallback:
          position && (position.unrealizedPnlUsd !== undefined ? position.unrealizedPnlUsd : position.pnl),
      });
      const positionUsd = round(toNum(position && position.positionUsd, 0), 2);
      const openedAt =
        toTimestampMs(
          position &&
            (position.openedAt ||
              position.timestamp ||
              position.observedAt ||
              position.lastObservedAt ||
              position.updatedAt)
        ) ||
        toTimestampMs(
          lifecycleOpenRow &&
            (lifecycleOpenRow.currentOpenedAt ||
              lifecycleOpenRow.currentFirstSeenAt ||
              lifecycleOpenRow.firstSeenAt ||
              lifecycleOpenRow.lastOpenedAt)
        );
      const openDate = openedAt ? new Date(openedAt).toISOString().slice(0, 10) : null;
      const direction = (() => {
        const sideText = String(position && (position.side || position.rawSide || "")).trim().toLowerCase();
        if (sideText.includes("short") || sideText === "ask" || sideText === "sell") return "Short";
        return "Long";
      })();
      const explicitOpenPositions = toNum(
        walletRow.openPositions !== undefined
          ? walletRow.openPositions
          : walletRow.open_positions !== undefined
          ? walletRow.open_positions
          : walletRow.openPositionsCount !== undefined
          ? walletRow.openPositionsCount
          : walletRow.open_positions_count !== undefined
          ? walletRow.open_positions_count
          : NaN,
        NaN
      );
      const fallbackTotalOpenPositions = walletPositions.length;
      const totalOpenPositions = Math.max(
        Number.isFinite(explicitOpenPositions) ? explicitOpenPositions : 0,
        fallbackTotalOpenPositions
      );
      const realizedPnlUsd = toNum(
        walletRow.totalPnlUsd !== undefined
          ? walletRow.totalPnlUsd
          : walletRow.pnlUsd !== undefined
          ? walletRow.pnlUsd
          : walletRow.realizedPnlUsd !== undefined
          ? walletRow.realizedPnlUsd
          : walletRow.realized_pnl_usd !== undefined
          ? walletRow.realized_pnl_usd
          : walletRow.all && walletRow.all.pnlUsd !== undefined
          ? walletRow.all.pnlUsd
          : 0,
        0
      );
      const unrealizedPnlUsd = toNum(
        walletRow.unrealizedPnlUsd !== undefined
          ? walletRow.unrealizedPnlUsd
          : walletRow.unrealized_pnl_usd !== undefined
          ? walletRow.unrealized_pnl_usd
          : walletRow.unrealized_pnl !== undefined
          ? walletRow.unrealized_pnl
          : 0,
        0
      );
      const totalPnlUsd = round(
        realizedPnlUsd + unrealizedPnlUsd,
        2
      );
      const openedIn30d = walletLifecycleAllRows.reduce((count, row) => {
        const rowOpenedAt = toTimestampMs(
          row && (row.currentOpenedAt || row.currentFirstSeenAt || row.firstSeenAt || row.lastOpenedAt)
        );
        if (!rowOpenedAt || rowOpenedAt < monthAgo) return count;
        return count + 1;
      }, 0);
      const avgOpenPositions30d = round(openedIn30d / 30, 3);
      const totalVolumeOnCoin = round(getWalletSymbolVolumeUsd(walletRow, normalizedSymbol, nextgenState), 2);
      return {
        wallet,
        entryPrice: Number.isFinite(entry) ? round(entry, 6) : null,
        positionSizeUsd: positionUsd,
        positionPnlUsd,
        unrealizedPnlUsd: positionPnlUsd,
        direction,
        openDate,
        openedAt,
        totalOpenPositions,
        totalPnlUsd,
        avgOpenPositions30d,
        totalVolumeOnCoin,
        pnlDirection: Number.isFinite(positionPnlUsd) && positionPnlUsd >= 0 ? "positive" : "negative",
      };
    })
    .filter(Boolean);

  const sortKeyValue = String(sortKey || "openedAt");
  const sortDirValue = String(sortDir || "asc").toLowerCase() === "desc" ? "desc" : "asc";
  const dir = sortDirValue === "asc" ? 1 : -1;
  const getSortValue = (row, key) => {
    switch (key) {
      case "wallet":
        return String(row && row.wallet ? row.wallet : "");
      case "direction":
        return String(row && row.direction ? row.direction : "");
      case "entryPrice":
        return toNum(row && row.entryPrice, NaN);
      case "positionSizeUsd":
        return toNum(row && row.positionSizeUsd, NaN);
      case "positionPnlUsd":
        return toNum(row && row.positionPnlUsd, NaN);
      case "openedAt":
        return Number(row && row.openedAt ? row.openedAt : 0);
      case "totalOpenPositions":
        return toNum(row && row.totalOpenPositions, NaN);
      case "totalPnlUsd":
        return toNum(row && row.totalPnlUsd, NaN);
      case "avgOpenPositions30d":
        return toNum(row && row.avgOpenPositions30d, NaN);
      case "totalVolumeOnCoin":
        return toNum(row && row.totalVolumeOnCoin, NaN);
      default:
        return Number(row && row.openedAt ? row.openedAt : 0);
    }
  };
  rows.sort((left, right) => {
    const leftValue = getSortValue(left, sortKeyValue);
    const rightValue = getSortValue(right, sortKeyValue);
    const leftFinite = Number.isFinite(leftValue);
    const rightFinite = Number.isFinite(rightValue);
    if (leftFinite && rightFinite) {
      if (leftValue !== rightValue) {
        return (leftValue - rightValue) * dir;
      }
    } else if (leftFinite && !rightFinite) {
      return -1 * dir;
    } else if (!leftFinite && rightFinite) {
      return 1 * dir;
    } else {
      const leftText = String(leftValue || "");
      const rightText = String(rightValue || "");
      if (leftText !== rightText) {
        return leftText.localeCompare(rightText) * dir;
      }
    }
    return String(left && left.wallet ? left.wallet : "").localeCompare(String(right && right.wallet ? right.wallet : "")) * dir;
  });

  const totalRows = rows.length;
  const pageSizeValue = Math.max(1, Math.min(100, Math.floor(toNum(pageSize, 20))));
  const totalPages = Math.max(1, Math.ceil(totalRows / pageSizeValue));
  const currentPage = Math.min(Math.max(1, Math.floor(toNum(page, 1))), totalPages);
  const startIndex = (currentPage - 1) * pageSizeValue;
  const pageRows = rows.slice(startIndex, startIndex + pageSizeValue);
  const walletCount = new Set(rows.map((row) => row.wallet)).size;

  const collectRange = (selector) => {
    const values = [];
    for (const row of rows) {
      const value = selector(row);
      if (Number.isFinite(value)) values.push(value);
    }
    if (!values.length) return null;
    return { min: Math.min(...values), max: Math.max(...values) };
  };
  const totalPositionUsd = rows.reduce((acc, row) => acc + Math.max(0, toNum(row.positionSizeUsd, 0)), 0);
  const totalWalletPnlUsd = rows.reduce((acc, row) => acc + toNum(row.totalPnlUsd, 0), 0);
  const totalCoinVolumeUsd = rows.reduce((acc, row) => acc + Math.max(0, toNum(row.totalVolumeOnCoin, 0)), 0);
  const avgOpenPositions30d = rows.length
    ? rows.reduce((acc, row) => acc + toNum(row.avgOpenPositions30d, 0), 0) / rows.length
    : 0;
  const longPositions = rows.filter((row) => String(row.direction || "").toLowerCase() === "long").length;
  const shortPositions = rows.filter((row) => String(row.direction || "").toLowerCase() === "short").length;
  const longPositionUsd = rows.reduce((acc, row) => {
    if (String(row.direction || "").toLowerCase() !== "long") return acc;
    return acc + Math.max(0, toNum(row.positionSizeUsd, 0));
  }, 0);
  const shortPositionUsd = rows.reduce((acc, row) => {
    if (String(row.direction || "").toLowerCase() !== "short") return acc;
    return acc + Math.max(0, toNum(row.positionSizeUsd, 0));
  }, 0);
  const profitableRows = rows.filter((row) => toNum(row.totalPnlUsd, 0) > 0);
  const profitableLongPositions = profitableRows.filter(
    (row) => String(row.direction || "").toLowerCase() === "long"
  ).length;
  const profitableShortPositions = profitableRows.filter(
    (row) => String(row.direction || "").toLowerCase() === "short"
  ).length;
  const profitableLongPositionUsd = profitableRows.reduce((acc, row) => {
    if (String(row.direction || "").toLowerCase() !== "long") return acc;
    return acc + Math.max(0, toNum(row.positionSizeUsd, 0));
  }, 0);
  const profitableShortPositionUsd = profitableRows.reduce((acc, row) => {
    if (String(row.direction || "").toLowerCase() !== "short") return acc;
    return acc + Math.max(0, toNum(row.positionSizeUsd, 0));
  }, 0);
  const profitableWalletCount = new Set(
    profitableRows.map((row) => normalizeWalletKey(row && row.wallet)).filter(Boolean)
  ).size;
  const sourceValidation =
    explorerDataset &&
    explorerDataset.positionValidation &&
    typeof explorerDataset.positionValidation === "object"
      ? explorerDataset.positionValidation
      : null;

  return {
    symbol: normalizedSymbol,
    walletCount,
    positionCount: totalRows,
    page: currentPage,
    pageSize: pageSizeValue,
    totalRows,
    totalPages,
    sortKey: sortKeyValue,
    sortDir: sortDirValue,
    totalPositionUsd: round(totalPositionUsd, 2),
    totalWalletPnlUsd: round(totalWalletPnlUsd, 2),
    totalCoinVolumeUsd: round(totalCoinVolumeUsd, 2),
    avgOpenPositions30d: round(avgOpenPositions30d, 3),
    longPositions,
    shortPositions,
    validation: {
      source: sourceValidation && sourceValidation.source ? sourceValidation.source : "current_open_positions",
      refreshedAt:
        Number((sourceValidation && sourceValidation.refreshedAt) || 0) || null,
      sourceLastSuccessAt:
        Number((sourceValidation && sourceValidation.sourceLastSuccessAt) || 0) || null,
      walletsWithOpenPositions:
        Number((sourceValidation && sourceValidation.walletsWithOpenPositions) || 0) || null,
      openPositionsTotal:
        Number((sourceValidation && sourceValidation.openPositionsTotal) || 0) || null,
      currentOpenSet:
        sourceValidation && sourceValidation.currentOpenSet !== undefined
          ? Boolean(sourceValidation.currentOpenSet)
          : true,
      autoUpdating:
        sourceValidation && sourceValidation.autoUpdating !== undefined
          ? Boolean(sourceValidation.autoUpdating)
          : true,
      symbolWalletCount: walletCount,
      symbolPositionCount: totalRows,
      countedWalletsMethod: "unique_wallets_with_symbol_in_current_open_set",
      countedPositionsMethod: "open_position_rows_with_symbol_in_current_open_set",
    },
    stats: {
      entryPrice: collectRange((row) => toNum(row.entryPrice, NaN)),
      positionSizeUsd: collectRange((row) => toNum(row.positionSizeUsd, NaN)),
      positionPnlUsd: collectRange((row) => toNum(row.positionPnlUsd, NaN)),
      openedAt: collectRange((row) => Number(row.openedAt || 0)),
      totalOpenPositions: collectRange((row) => toNum(row.totalOpenPositions, NaN)),
      totalPnlUsd: collectRange((row) => toNum(row.totalPnlUsd, NaN)),
      avgOpenPositions30d: collectRange((row) => toNum(row.avgOpenPositions30d, NaN)),
      totalVolumeOnCoin: collectRange((row) => toNum(row.totalVolumeOnCoin, NaN)),
      direction: {
        long: longPositions,
        short: shortPositions,
        longUsd: round(longPositionUsd, 2),
        shortUsd: round(shortPositionUsd, 2),
      },
      profitableDirection: {
        long: profitableLongPositions,
        short: profitableShortPositions,
        longUsd: round(profitableLongPositionUsd, 2),
        shortUsd: round(profitableShortPositionUsd, 2),
        positions: profitableRows.length,
        wallets: profitableWalletCount,
      },
    },
    rows: pageRows,
  };
}

function buildCoinOpenPositionGroups({ explorerDataset = null, walletRows = [] } = {}) {
  const positions = Array.isArray(explorerDataset && explorerDataset.positions)
    ? explorerDataset.positions
    : [];
  const walletRowSources = [
    Array.isArray(walletRows) ? walletRows : [],
    Array.isArray(explorerDataset && explorerDataset.preparedRows)
      ? explorerDataset.preparedRows
      : [],
    Array.isArray(explorerDataset && explorerDataset.walletRows)
      ? explorerDataset.walletRows
      : [],
  ];
  const walletRowsByWallet = new Map();
  walletRowSources.forEach((rows) => {
    rows.forEach((row) => {
      const wallet = normalizeWalletKey(row && row.wallet);
      if (!wallet || walletRowsByWallet.has(wallet)) return;
      walletRowsByWallet.set(wallet, normalizeTokenWalletRow(row));
    });
  });

  const bySymbol = new Map();
  const allWallets = new Set();
  positions.forEach((position) => {
    const symbol = normalizeTokenSymbol(position && position.symbol);
    const wallet = normalizeWalletKey(position && position.wallet);
    if (!symbol || !wallet) return;
    allWallets.add(wallet);
    if (!bySymbol.has(symbol)) {
      bySymbol.set(symbol, {
        symbol,
        walletSet: new Set(),
        walletPnlWalletSet: new Set(),
        positionCount: 0,
        longCount: 0,
        shortCount: 0,
        totalPositionUsd: 0,
        totalUnrealizedPnlUsd: 0,
        totalWalletPnlUsd: 0,
        latestOpenedAt: 0,
      });
    }
    const bucket = bySymbol.get(symbol);
    bucket.walletSet.add(wallet);
    bucket.positionCount += 1;
    const sideText = String(position && (position.side || position.rawSide || ""))
      .trim()
      .toLowerCase();
    if (sideText.includes("short") || sideText === "ask" || sideText === "sell") {
      bucket.shortCount += 1;
    } else {
      bucket.longCount += 1;
    }
    const positionUsd = Math.max(
      0,
      toNum(
        position &&
          (position.positionUsd !== undefined
            ? position.positionUsd
            : position.notionalUsd !== undefined
            ? position.notionalUsd
            : position.positionSizeUsd !== undefined
            ? position.positionSizeUsd
            : position.value !== undefined
            ? position.value
            : 0),
        0
      )
    );
    bucket.totalPositionUsd += positionUsd;
    const entry = toNum(
      position &&
        (position.entry !== undefined
          ? position.entry
          : position.entryPrice !== undefined
          ? position.entryPrice
          : position.entry_price),
      NaN
    );
    const unrealizedPnlUsd = calculatePositionUnrealizedPnlUsd({
      entry,
      mark:
        position &&
        (position.mark !== undefined
          ? position.mark
          : position.markPrice !== undefined
          ? position.markPrice
          : position.currentPrice !== undefined
          ? position.currentPrice
          : NaN),
      size:
        position && position.size !== undefined
          ? position.size
          : position && position.amount !== undefined
          ? position.amount
          : position && position.qty !== undefined
          ? position.qty
          : null,
      positionUsd,
      side: position && position.side,
      fallback:
        position &&
        (position.unrealizedPnlUsd !== undefined
          ? position.unrealizedPnlUsd
          : position.pnl),
    });
    bucket.totalUnrealizedPnlUsd += toNum(unrealizedPnlUsd, 0);
    if (!bucket.walletPnlWalletSet.has(wallet)) {
      bucket.walletPnlWalletSet.add(wallet);
      const walletRow = walletRowsByWallet.get(wallet) || {};
      const realizedPnlUsd = toNum(
        walletRow.totalPnlUsd !== undefined
          ? walletRow.totalPnlUsd
          : walletRow.pnlUsd !== undefined
          ? walletRow.pnlUsd
          : walletRow.realizedPnlUsd !== undefined
          ? walletRow.realizedPnlUsd
          : walletRow.realized_pnl_usd !== undefined
          ? walletRow.realized_pnl_usd
          : walletRow.all && walletRow.all.pnlUsd !== undefined
          ? walletRow.all.pnlUsd
          : 0,
        0
      );
      const unrealizedWalletPnlUsd = toNum(
        walletRow.unrealizedPnlUsd !== undefined
          ? walletRow.unrealizedPnlUsd
          : walletRow.unrealized_pnl_usd !== undefined
          ? walletRow.unrealized_pnl_usd
          : walletRow.unrealized_pnl !== undefined
          ? walletRow.unrealized_pnl
          : 0,
        0
      );
      bucket.totalWalletPnlUsd += realizedPnlUsd + unrealizedWalletPnlUsd;
    }
    const openedAt =
      toTimestampMs(
        position &&
          (position.openedAt ||
            position.timestamp ||
            position.observedAt ||
            position.lastObservedAt ||
            position.updatedAt)
      ) || 0;
    if (openedAt > bucket.latestOpenedAt) bucket.latestOpenedAt = openedAt;
  });

  const rows = Array.from(bySymbol.values())
    .map((bucket) => ({
      symbol: bucket.symbol,
      walletCount: bucket.walletSet.size,
      positionCount: bucket.positionCount,
      longCount: bucket.longCount,
      shortCount: bucket.shortCount,
      totalPositionUsd: round(bucket.totalPositionUsd, 2),
      totalUnrealizedPnlUsd: round(bucket.totalUnrealizedPnlUsd, 2),
      totalWalletPnlUsd: round(bucket.totalWalletPnlUsd, 2),
      latestOpenedAt: bucket.latestOpenedAt || null,
    }))
    .sort((left, right) => {
      const leftSize = toNum(left && left.totalPositionUsd, 0);
      const rightSize = toNum(right && right.totalPositionUsd, 0);
      if (leftSize !== rightSize) return rightSize - leftSize;
      const leftCount = toNum(left && left.positionCount, 0);
      const rightCount = toNum(right && right.positionCount, 0);
      if (leftCount !== rightCount) return rightCount - leftCount;
      return String(left && left.symbol ? left.symbol : "").localeCompare(
        String(right && right.symbol ? right.symbol : "")
      );
    });

  return {
    totalSymbols: rows.length,
    totalPositions: rows.reduce((acc, row) => acc + Math.max(0, toNum(row.positionCount, 0)), 0),
    totalWallets: allWallets.size,
    rows,
  };
}

function startOfUtcDayMs(value = Date.now()) {
  const ts = Number(value);
  if (!Number.isFinite(ts)) return Math.floor(Date.now() / DAY_MS) * DAY_MS;
  return Math.floor(ts / DAY_MS) * DAY_MS;
}

function percentile(values = [], pct = 0.5) {
  const rows = (Array.isArray(values) ? values : [])
    .map((value) => Number(value))
    .filter((value) => Number.isFinite(value))
    .sort((a, b) => a - b);
  if (!rows.length) return null;
  const normalizedPct = clamp(Number(pct), 0, 1);
  const index = (rows.length - 1) * normalizedPct;
  const lower = Math.floor(index);
  const upper = Math.ceil(index);
  if (lower === upper) return rows[lower];
  const ratio = index - lower;
  return rows[lower] + (rows[upper] - rows[lower]) * ratio;
}

function metricValue(value, digits = 2) {
  const num = Number(value);
  if (!Number.isFinite(num)) return null;
  return round(num, digits);
}

function buildMetric({
  key,
  label,
  value = null,
  unit = "number",
  status = "unavailable",
  method = "derived",
  source = null,
  note = null,
  coverageStart = null,
  coverageEnd = null,
  completeThrough = null,
  extra = null,
} = {}) {
  return {
    key: key || null,
    label: label || key || null,
    value: value !== undefined && value !== null && Number.isFinite(Number(value)) ? Number(value) : null,
    unit,
    status,
    method,
    source,
    note,
    coverage: {
      start: coverageStart,
      end: coverageEnd,
      completeThrough: completeThrough,
    },
    extra: extra && typeof extra === "object" ? { ...extra } : null,
  };
}

function coverageState({ firstTs = null, lastTs = null, targetWindowMs = 0, allowEstimated = false } = {}) {
  const start = toTimestampMs(firstTs);
  const end = toTimestampMs(lastTs);
  if (!start || !end || end <= start) {
    return {
      status: "unavailable",
      coverageStart: start,
      coverageEnd: end,
    };
  }
  if (!targetWindowMs || targetWindowMs <= 0) {
    return {
      status: allowEstimated ? "estimated" : "exact",
      coverageStart: start,
      coverageEnd: end,
    };
  }
  const coveredMs = end - start;
  if (coveredMs >= targetWindowMs * 0.9) {
    return {
      status: allowEstimated ? "estimated" : "exact",
      coverageStart: start,
      coverageEnd: end,
    };
  }
  if (coveredMs >= targetWindowMs * 0.35) {
    return {
      status: "partial",
      coverageStart: start,
      coverageEnd: end,
    };
  }
  return {
    status: "unavailable",
    coverageStart: start,
    coverageEnd: end,
  };
}

function buildPriceHistory({ candlesRows = [], tradesRows = [], priceRow = null } = {}) {
  const candlePoints = (Array.isArray(candlesRows) ? candlesRows : [])
    .map((row) => {
      const timestamp = toTimestampMs(row && row.t);
      const close = Number(row && row.c);
      if (!timestamp || !Number.isFinite(close) || close <= 0) return null;
      const volumeBase = toNum(row && row.v, 0);
      return {
        timestamp,
        value: close,
        high: Number.isFinite(Number(row && row.h)) ? Number(row.h) : close,
        low: Number.isFinite(Number(row && row.l)) ? Number(row.l) : close,
        volumeUsd: round(close * volumeBase, 4) || 0,
        source: "candles",
      };
    })
    .filter(Boolean);
  if (candlePoints.length) return sortByTimestampAsc(candlePoints);

  const tradePoints = bucketTradesByTime(tradesRows, 5 * 60 * 1000).map((bucket) => ({
    timestamp: bucket.timestamp,
    value: bucket.close || bucket.value,
    high: bucket.high,
    low: bucket.low,
    volumeUsd: bucket.volumeUsd,
    source: "trades",
  }));
  if (tradePoints.length) return sortByTimestampAsc(tradePoints);

  const fallbackPrice = Number(
    priceRow && (priceRow.mark !== undefined ? priceRow.mark : priceRow.mid !== undefined ? priceRow.mid : priceRow.oracle)
  );
  if (Number.isFinite(fallbackPrice) && fallbackPrice > 0) {
    return [
      {
        timestamp: Date.now(),
        value: fallbackPrice,
        high: fallbackPrice,
        low: fallbackPrice,
        volumeUsd: 0,
        source: "snapshot",
      },
    ];
  }
  return [];
}

function bucketTradesByTime(tradesRows = [], bucketMs = 15 * 60 * 1000) {
  const buckets = new Map();
  const rows = sortByTimestampAsc(tradesRows);
  for (const row of rows) {
    const timestamp = toTimestampMs(row && row.timestamp);
    if (!timestamp) continue;
    const bucketStart = Math.floor(timestamp / bucketMs) * bucketMs;
    let bucket = buckets.get(bucketStart);
    if (!bucket) {
      bucket = {
        timestamp: bucketStart,
        value: 0,
        open: null,
        close: null,
        high: null,
        low: null,
        buyVolumeUsd: 0,
        sellVolumeUsd: 0,
        volumeUsd: 0,
        netUsd: 0,
        tradeCount: 0,
        largeTradeCount: 0,
        longLiquidationsUsd: 0,
        shortLiquidationsUsd: 0,
      };
      buckets.set(bucketStart, bucket);
    }
    const price = toNum(row && row.price, NaN);
    const notionalUsd = Math.abs(toNum(row && row.notionalUsd, 0));
    const side = String((row && row.side) || "").trim().toLowerCase();
    const isBuy =
      side === "buy" ||
      side === "b" ||
      side === "open_long" ||
      side === "close_short";
    const isSell =
      side === "sell" ||
      side === "s" ||
      side === "open_short" ||
      side === "close_long";
    const cause = String((row && row.cause) || "").trim().toLowerCase();
    const isLiquidation = cause.includes("liq") || cause.includes("liquid");

    bucket.tradeCount += 1;
    bucket.volumeUsd += notionalUsd;
    if (isBuy) {
      bucket.buyVolumeUsd += notionalUsd;
      bucket.netUsd += notionalUsd;
    } else if (isSell) {
      bucket.sellVolumeUsd += notionalUsd;
      bucket.netUsd -= notionalUsd;
    }
    if (!bucket.open && Number.isFinite(price)) bucket.open = price;
    if (Number.isFinite(price)) {
      bucket.close = price;
      bucket.high = bucket.high === null ? price : Math.max(bucket.high, price);
      bucket.low = bucket.low === null ? price : Math.min(bucket.low, price);
    }
    if (isLiquidation) {
      if (isSell) bucket.longLiquidationsUsd += notionalUsd;
      else if (isBuy) bucket.shortLiquidationsUsd += notionalUsd;
    }
  }

  const thresholds = (Array.isArray(tradesRows) ? tradesRows : [])
    .map((row) => Math.abs(toNum(row && row.notionalUsd, 0)))
    .filter((value) => value > 0);
  const largeThreshold = Math.max(100000, percentile(thresholds, 0.95) || 0);
  for (const row of Array.isArray(tradesRows) ? tradesRows : []) {
    const timestamp = toTimestampMs(row && row.timestamp);
    if (!timestamp) continue;
    const bucketStart = Math.floor(timestamp / bucketMs) * bucketMs;
    const bucket = buckets.get(bucketStart);
    if (!bucket) continue;
    const notionalUsd = Math.abs(toNum(row && row.notionalUsd, 0));
    if (notionalUsd >= largeThreshold) bucket.largeTradeCount += 1;
  }

  return Array.from(buckets.values()).sort((a, b) => a.timestamp - b.timestamp);
}

function estimateOiSeries(priceHistory = [], priceRow = null) {
  const mark = toNum(
    priceRow && (priceRow.mark !== undefined ? priceRow.mark : priceRow.mid !== undefined ? priceRow.mid : priceRow.oracle),
    NaN
  );
  const openInterest = toNum(
    priceRow &&
      (priceRow.open_interest !== undefined
        ? priceRow.open_interest
        : priceRow.openInterest !== undefined
        ? priceRow.openInterest
        : priceRow.openInterestBase),
    NaN
  );
  if (!Number.isFinite(mark) || mark <= 0 || !Number.isFinite(openInterest) || openInterest <= 0) {
    return [];
  }
  const oiUsdNow = mark * openInterest;
  const lastPrice = toNum(priceHistory[priceHistory.length - 1] && priceHistory[priceHistory.length - 1].value, mark);
  return priceHistory.map((row) => {
    const price = toNum(row && row.value, lastPrice);
    const scale = lastPrice > 0 ? price / lastPrice : 1;
    return {
      timestamp: row.timestamp,
      value: Math.max(0, oiUsdNow * scale),
    };
  });
}

function buildFundingSeries(fundingRows = [], priceRow = null) {
  const rows = sortByTimestampAsc(
    (Array.isArray(fundingRows) ? fundingRows : [])
      .map((row) => ({
        timestamp: toTimestampMs(row && row.createdAt),
        value: toNum(row && row.fundingRatePct, NaN),
      }))
      .filter((row) => row.timestamp && Number.isFinite(row.value))
  );
  if (rows.length) return rows;
  const funding = toNum(
    priceRow &&
      (priceRow.funding !== undefined ? priceRow.funding : priceRow.fundingRate !== undefined ? priceRow.fundingRate : NaN),
    NaN
  );
  if (Number.isFinite(funding)) {
    return [
      {
        timestamp: Date.now(),
        value: funding * 100,
      },
    ];
  }
  return [];
}

function computeWindowChange(points = [], windowMs, nowPrice = null) {
  const rows = sortByTimestampAsc(points);
  if (!rows.length) {
    return {
      value: null,
      status: "unavailable",
      coverageStart: null,
      coverageEnd: null,
    };
  }
  const latest = rows[rows.length - 1];
  const latestValue = Number.isFinite(Number(nowPrice)) ? Number(nowPrice) : toNum(latest && latest.value, NaN);
  if (!Number.isFinite(latestValue)) {
    return {
      value: null,
      status: "unavailable",
      coverageStart: rows[0].timestamp,
      coverageEnd: latest.timestamp,
    };
  }
  const targetStart = latest.timestamp - Number(windowMs || 0);
  let baseline = null;
  for (let idx = rows.length - 1; idx >= 0; idx -= 1) {
    const row = rows[idx];
    if (toNum(row && row.timestamp, 0) <= targetStart) {
      baseline = row;
      break;
    }
  }
  if (!baseline) baseline = rows[0];
  const baseValue = toNum(baseline && baseline.value, NaN);
  if (!Number.isFinite(baseValue) || baseValue === 0) {
    return {
      value: null,
      status: "unavailable",
      coverageStart: rows[0].timestamp,
      coverageEnd: latest.timestamp,
    };
  }
  const pct = ((latestValue - baseValue) / Math.abs(baseValue)) * 100;
  const coverage = coverageState({
    firstTs: baseline.timestamp,
    lastTs: latest.timestamp,
    targetWindowMs: windowMs,
  });
  return {
    value: round(pct, 4),
    status: coverage.status,
    coverageStart: coverage.coverageStart,
    coverageEnd: coverage.coverageEnd,
  };
}

function computeDailyRange(priceHistory = []) {
  const rows = sortByTimestampAsc(priceHistory).filter((row) => Date.now() - toNum(row && row.timestamp, 0) <= DAY_MS);
  if (!rows.length) {
    return {
      high: null,
      low: null,
      status: "unavailable",
    };
  }
  const highs = rows.map((row) => toNum(row && row.high !== undefined ? row.high : row.value, NaN)).filter(Number.isFinite);
  const lows = rows.map((row) => toNum(row && row.low !== undefined ? row.low : row.value, NaN)).filter(Number.isFinite);
  return {
    high: highs.length ? Math.max(...highs) : null,
    low: lows.length ? Math.min(...lows) : null,
    status: "exact",
  };
}

function computeVolatility(priceHistory = [], windowMs = DAY_MS) {
  const rows = sortByTimestampAsc(priceHistory).filter((row) => Date.now() - toNum(row && row.timestamp, 0) <= windowMs);
  if (rows.length < 6) {
    return {
      value: null,
      status: "unavailable",
    };
  }
  const returns = [];
  for (let idx = 1; idx < rows.length; idx += 1) {
    const prev = toNum(rows[idx - 1] && rows[idx - 1].value, NaN);
    const next = toNum(rows[idx] && rows[idx].value, NaN);
    if (!Number.isFinite(prev) || prev <= 0 || !Number.isFinite(next) || next <= 0) continue;
    returns.push(Math.log(next / prev));
  }
  if (returns.length < 4) {
    return {
      value: null,
      status: "unavailable",
    };
  }
  const avg = mean(returns);
  const variance = returns.reduce((acc, value) => acc + (value - avg) ** 2, 0) / Math.max(1, returns.length - 1);
  return {
    value: Math.sqrt(variance) * Math.sqrt(returns.length) * 100,
    status: "derived",
  };
}

function computeTrend(priceHistory = []) {
  const rows = sortByTimestampAsc(priceHistory);
  if (rows.length < 12) {
    return {
      structure: "insufficient_data",
      support: null,
      resistance: null,
      status: "partial",
    };
  }
  const prices = rows.map((row) => toNum(row && row.value, NaN)).filter(Number.isFinite);
  const last = prices[prices.length - 1];
  const smaShort = mean(prices.slice(-20));
  const smaMedium = mean(prices.slice(-60));
  const recentSlice = prices.slice(-48);
  const support = percentile(recentSlice, 0.2);
  const resistance = percentile(recentSlice, 0.8);

  let structure = "range";
  if (Number.isFinite(last) && Number.isFinite(smaShort) && Number.isFinite(smaMedium)) {
    if (last > smaShort && smaShort > smaMedium) structure = "bullish_structure";
    else if (last < smaShort && smaShort < smaMedium) structure = "bearish_structure";
    else if (last > smaShort && smaShort <= smaMedium) structure = "reclaiming";
    else if (last < smaShort && smaShort >= smaMedium) structure = "breaking_down";
  }
  return {
    structure,
    support: Number.isFinite(support) ? support : null,
    resistance: Number.isFinite(resistance) ? resistance : null,
    status: "derived",
  };
}

function buildWalletTokenSnapshot({ symbol, explorerDataset = null, lifecyclePositions = [] } = {}) {
  const dataset = explorerDataset && typeof explorerDataset === "object" ? explorerDataset : {};
  const preparedRows = Array.isArray(dataset.preparedRows) ? dataset.preparedRows : [];
  const walletRows = Array.isArray(dataset.walletRows) && dataset.walletRows.length ? dataset.walletRows : preparedRows;
  const livePositions = Array.isArray(dataset.positions) ? dataset.positions : [];
  const allLifecycleRows = Array.isArray(lifecyclePositions) ? lifecyclePositions : [];
  const normalizedSymbol = String(symbol || "").trim().toUpperCase();
  const nowMs = Date.now();
  const todayStartMs = startOfUtcDayMs(nowMs);

  function rowHasSymbolMembership(row = {}, targetSymbol = "") {
    const normalizedTarget = String(targetSymbol || "").trim().toUpperCase();
    if (!normalizedTarget) return false;
    const directSymbols = Array.isArray(row && row.symbols)
      ? row.symbols.map((value) => String(value || "").trim().toUpperCase()).filter(Boolean)
      : [];
    if (directSymbols.includes(normalizedTarget)) return true;
    const breakdownRows = Array.isArray(row && row.symbolBreakdown)
      ? row.symbolBreakdown
      : Array.isArray(row && row.symbolsBreakdown)
      ? row.symbolsBreakdown
      : [];
    return breakdownRows.some((entry) => {
      if (!entry || typeof entry !== "object") return false;
      const entrySymbol = String(entry.symbol || entry.market || "").trim().toUpperCase();
      if (entrySymbol !== normalizedTarget) return false;
      const volumeUsd = toNum(
        entry.volumeUsd !== undefined
          ? entry.volumeUsd
          : entry.volume_usd !== undefined
          ? entry.volume_usd
          : entry.volume_24h_usd !== undefined
          ? entry.volume_24h_usd
          : entry.notionalUsd !== undefined
          ? entry.notionalUsd
          : 0,
        0
      );
      return volumeUsd > 0;
    });
  }

  const activeRows = walletRows.filter((row) => rowHasSymbolMembership(row, normalizedSymbol));
  const currentPositions = livePositions.filter(
    (row) => String((row && row.symbol) || "").trim().toUpperCase() === normalizedSymbol
  );
  const lifecycleRows = allLifecycleRows.filter(
    (row) => String((row && row.symbol) || "").trim().toUpperCase() === normalizedSymbol
  );
  const preparedByWallet = new Map();
  walletRows.forEach((row) => {
    const wallet = String((row && row.wallet) || "").trim();
    if (wallet) preparedByWallet.set(wallet, row);
  });

  const holdersByWallet = new Map();
  let longExposureUsd = 0;
  let shortExposureUsd = 0;
  let longEntryWeightedUsd = 0;
  let shortEntryWeightedUsd = 0;
  let longPositionCount = 0;
  let shortPositionCount = 0;
  currentPositions.forEach((row) => {
    const wallet = String((row && row.wallet) || "").trim();
    if (!wallet) return;
    const sideValue = String((row && row.side) || (row && row.rawSide) || "").trim().toLowerCase();
    const entry = toNum(
      row && (row.entry !== undefined ? row.entry : row.entryPrice !== undefined ? row.entryPrice : row.entry_price),
      NaN
    );
    const positionUsd = Math.max(0, toNum(row && row.positionUsd, 0));
    const isShort = sideValue.includes("short") || sideValue === "ask" || sideValue === "sell";
    const existing = holdersByWallet.get(wallet) || {
      wallet,
      side: String((row && row.side) || "").trim().toLowerCase(),
      positionUsd: 0,
      unrealizedPnlUsd: 0,
      positions: 0,
    };
    existing.positionUsd += positionUsd;
    existing.unrealizedPnlUsd += toNum(
      row && (row.unrealizedPnlUsd !== undefined ? row.unrealizedPnlUsd : row.pnl),
      0
    );
    existing.positions += 1;
    holdersByWallet.set(wallet, existing);

    if (isShort) {
      shortExposureUsd += positionUsd;
      shortPositionCount += 1;
      if (Number.isFinite(entry) && entry > 0 && positionUsd > 0) shortEntryWeightedUsd += entry * positionUsd;
    } else {
      longExposureUsd += positionUsd;
      longPositionCount += 1;
      if (Number.isFinite(entry) && entry > 0 && positionUsd > 0) longEntryWeightedUsd += entry * positionUsd;
    }
  });

  const holderRows = Array.from(holdersByWallet.values()).sort((a, b) => b.positionUsd - a.positionUsd);
  const totalTrackedExposureUsd = sum(holderRows.map((row) => row.positionUsd));
  const top1SharePct = totalTrackedExposureUsd > 0 ? (toNum(holderRows[0] && holderRows[0].positionUsd, 0) / totalTrackedExposureUsd) * 100 : 0;
  const top5SharePct =
    totalTrackedExposureUsd > 0
      ? (sum(holderRows.slice(0, 5).map((row) => row.positionUsd)) / totalTrackedExposureUsd) * 100
      : 0;

  const longWallets = new Set(
    holderRows
      .filter((row) => String(row.side || "").includes("long") || String(row.side || "") === "buy")
      .map((row) => row.wallet)
  );
  const shortWallets = new Set(
    holderRows
      .filter((row) => String(row.side || "").includes("short") || String(row.side || "") === "sell")
      .map((row) => row.wallet)
  );

  const entryRows = lifecycleRows
    .filter((row) => Boolean(row && row.currentOpen) && toTimestampMs(row && row.currentOpenedAt))
    .sort((a, b) => toNum(b && b.currentOpenedAt, 0) - toNum(a && a.currentOpenedAt, 0))
    .slice(0, 12)
    .map((row) => ({
      wallet: String(row.wallet || "").trim(),
      side: String(row.side || "").trim().toLowerCase(),
      timestamp: toTimestampMs(row.currentOpenedAt),
      sizeUsd: round(toNum(row.lastKnownPositionUsd, 0), 2),
      action: "entered",
    }));

  const exitRows = lifecycleRows
    .filter((row) => toTimestampMs(row && row.lastClosedAt))
    .sort((a, b) => toNum(b && b.lastClosedAt, 0) - toNum(a && a.lastClosedAt, 0))
    .slice(0, 12)
    .map((row) => ({
      wallet: String(row.wallet || "").trim(),
      side: String(row.side || "").trim().toLowerCase(),
      timestamp: toTimestampMs(row.lastClosedAt),
      sizeUsd: round(toNum(row.lastKnownPositionUsd, 0), 2),
      action: "exited",
    }));

  const profitableRows = holderRows
    .slice()
    .sort((a, b) => toNum(b.unrealizedPnlUsd, 0) - toNum(a.unrealizedPnlUsd, 0))
    .slice(0, 10)
    .map((row) => ({
      wallet: row.wallet,
      side: row.side,
      positionUsd: round(row.positionUsd, 2),
      unrealizedPnlUsd: round(row.unrealizedPnlUsd, 2),
    }));

  const activeWallets = new Set(activeRows.map((row) => String(row.wallet || "").trim()).filter(Boolean));
  holderRows.forEach((row) => activeWallets.add(row.wallet));
  lifecycleRows.forEach((row) => {
    if (row && row.wallet) activeWallets.add(String(row.wallet).trim());
  });

  const volumeThreshold = percentile(
    activeRows.map((row) =>
      toNum(
        row && (row.volumeUsd !== undefined ? row.volumeUsd : row.volumeUsdRaw !== undefined ? row.volumeUsdRaw : 0),
        0
      )
    ),
    0.65
  );
  const exposureThreshold = percentile(holderRows.map((row) => toNum(row.positionUsd, 0)), 0.6);
  const walletRowsByWallet = new Map(
    walletRows
      .map((row) => {
        const wallet = String((row && row.wallet) || "").trim();
        return wallet ? [wallet, row] : null;
      })
      .filter(Boolean)
  );
  const smartCandidateWallets = new Set();
  walletRows.forEach((row) => {
    const wallet = String((row && row.wallet) || "").trim();
    if (!wallet) return;
    if (rowHasSymbolMembership(row, normalizedSymbol)) {
      smartCandidateWallets.add(wallet);
    }
  });
  holderRows.forEach((row) => {
    if (row && row.wallet) smartCandidateWallets.add(String(row.wallet).trim());
  });
  lifecycleRows.forEach((row) => {
    if (row && row.wallet) smartCandidateWallets.add(String(row.wallet).trim());
  });
  const smartWalletRows = Array.from(smartCandidateWallets)
    .map((wallet) => {
      const holder = holdersByWallet.get(wallet) || {
        wallet,
        side: "",
        positionUsd: 0,
        unrealizedPnlUsd: 0,
        positions: 0,
      };
      const profile = preparedByWallet.get(wallet) || walletRowsByWallet.get(wallet) || {};
      const lifecycleRowsForWallet = allLifecycleRows.filter(
        (row) => String((row && row.wallet) || "").trim() === wallet
      );
      const winRate = toNum(
        profile && (profile.winRate !== undefined ? profile.winRate : profile.winRatePct !== undefined ? profile.winRatePct : 0),
        0
      );
      const realizedPnlUsd = toNum(
        profile &&
          (profile.pnlUsd !== undefined
            ? profile.pnlUsd
            : profile.totalPnlUsd !== undefined
            ? profile.totalPnlUsd
            : 0),
        0
      );
      const tradedVolumeUsd = toNum(
        profile &&
          (profile.volumeUsd !== undefined
            ? profile.volumeUsd
            : profile.volumeUsdRaw !== undefined
            ? profile.volumeUsdRaw
            : 0),
        0
      );
      const lastTradeAt = toTimestampMs(
        profile &&
          (profile.lastTrade !== undefined
            ? profile.lastTrade
            : profile.lastActivity !== undefined
            ? profile.lastActivity
            : 0)
      );
      const completedPositionCount = lifecycleRowsForWallet.filter((row) => !Boolean(row && row.currentOpen)).length;
      const totalObservedPositions = lifecycleRowsForWallet.length;

      let score = 0;
      if (winRate >= 65) score += 30;
      else if (winRate >= 58) score += 24;
      else if (winRate >= 52) score += 14;
      if (realizedPnlUsd >= 25000) score += 22;
      else if (realizedPnlUsd >= 5000) score += 16;
      else if (realizedPnlUsd > 0) score += 8;
      if (tradedVolumeUsd >= Math.max(1000000, toNum(volumeThreshold, 0))) score += 18;
      if (holder.positionUsd >= Math.max(25000, toNum(exposureThreshold, 0))) score += 16;
      if (holder.unrealizedPnlUsd > 0) score += 10;
      if (lastTradeAt && Date.now() - lastTradeAt <= 14 * DAY_MS) score += 8;
      if (completedPositionCount >= 8) score += 6;
      else if (completedPositionCount >= 4) score += 4;
      else if (completedPositionCount >= 2) score += 2;
      if (totalObservedPositions >= 10) score += 4;
      else if (totalObservedPositions >= 5) score += 2;

      return {
        wallet: holder.wallet,
        side: holder.side,
        positionUsd: round(holder.positionUsd, 2),
        unrealizedPnlUsd: round(holder.unrealizedPnlUsd, 2),
        winRate: round(winRate, 2),
        realizedPnlUsd: round(realizedPnlUsd, 2),
        tradedVolumeUsd: round(tradedVolumeUsd, 2),
        completedPositionCount,
        totalObservedPositions,
        score,
      };
    })
    .filter((row) => row.score >= 55)
    .sort((a, b) => b.score - a.score || b.positionUsd - a.positionUsd);

  const smartWalletSet = new Set(smartWalletRows.map((row) => row.wallet));
  const smartCurrentPositions = currentPositions.filter((row) => smartWalletSet.has(String(row.wallet || "").trim()));
  let smartCurrentLongExposureUsd = 0;
  let smartCurrentShortExposureUsd = 0;
  let smartCurrentLongEntryWeightedUsd = 0;
  let smartCurrentShortEntryWeightedUsd = 0;
  smartCurrentPositions.forEach((row) => {
    const sideValue = String((row && row.side) || (row && row.rawSide) || "").trim().toLowerCase();
    const entry = toNum(
      row && (row.entry !== undefined ? row.entry : row.entryPrice !== undefined ? row.entryPrice : row.entry_price),
      NaN
    );
    const positionUsd = Math.max(0, toNum(row && row.positionUsd, 0));
    const isShort = sideValue.includes("short") || sideValue === "ask" || sideValue === "sell";
    if (isShort) {
      smartCurrentShortExposureUsd += positionUsd;
      if (Number.isFinite(entry) && entry > 0 && positionUsd > 0) {
        smartCurrentShortEntryWeightedUsd += entry * positionUsd;
      }
    } else {
      smartCurrentLongExposureUsd += positionUsd;
      if (Number.isFinite(entry) && entry > 0 && positionUsd > 0) {
        smartCurrentLongEntryWeightedUsd += entry * positionUsd;
      }
    }
  });

  const smartTodayRows = dedupeBy(
    allLifecycleRows
      .filter((row) => {
        const wallet = String((row && row.wallet) || "").trim();
        if (!wallet || !smartWalletSet.has(wallet)) return false;
        const openedAt = toTimestampMs(
          row && (row.currentOpenedAt || row.currentFirstSeenAt || row.firstSeenAt || row.lastOpenedAt)
        );
        return Boolean(openedAt && openedAt >= todayStartMs);
      })
      .map((row) => ({
        id: String(row && row.id ? row.id : `${row.wallet}:${row.positionKey || row.symbol || ""}:${row.currentOpenedAt || row.firstSeenAt || 0}`),
        wallet: String(row && row.wallet ? row.wallet : "").trim(),
        positionKey: String(row && row.positionKey ? row.positionKey : "").trim(),
        symbol: String(row && row.symbol ? row.symbol : "").trim().toUpperCase(),
        side: String(row && row.side ? row.side : "").trim().toLowerCase(),
        currentOpen: Boolean(row && row.currentOpen),
        currentOpenedAt: toTimestampMs(row && (row.currentOpenedAt || row.currentFirstSeenAt || row.firstSeenAt || row.lastOpenedAt)),
        lastKnownPositionUsd: Number(toNum(row && row.lastKnownPositionUsd, 0).toFixed(2)),
      })),
    (row) => row.id
  )
    .filter((row) => row.currentOpenedAt && row.currentOpenedAt >= todayStartMs)
    .sort((a, b) => toNum(b.currentOpenedAt, 0) - toNum(a.currentOpenedAt, 0));
  const smartTodayWalletCounts = new Map();
  const smartTodayWalletSymbols = new Map();
  const smartTodaySymbolCounts = new Map();
  smartTodayRows.forEach((row) => {
    smartTodayWalletCounts.set(row.wallet, (smartTodayWalletCounts.get(row.wallet) || 0) + 1);
    const walletSymbols = smartTodayWalletSymbols.get(row.wallet) || new Set();
    walletSymbols.add(row.symbol);
    smartTodayWalletSymbols.set(row.wallet, walletSymbols);
    smartTodaySymbolCounts.set(row.symbol, (smartTodaySymbolCounts.get(row.symbol) || 0) + 1);
  });
  const smartOpenedSymbolsToday = Array.from(smartTodaySymbolCounts.entries())
    .sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]))
    .slice(0, 12)
    .map(([symbolName, count]) => ({
      symbol: symbolName,
      count,
    }));
  const smartAverageLongEntryPrice =
    smartCurrentLongExposureUsd > 0 ? round(smartCurrentLongEntryWeightedUsd / smartCurrentLongExposureUsd, 6) : null;
  const smartAverageShortEntryPrice =
    smartCurrentShortExposureUsd > 0
      ? round(smartCurrentShortEntryWeightedUsd / smartCurrentShortExposureUsd, 6)
      : null;
  const smartPositionsOpenedToday = smartTodayRows.length;
  const smartWalletsOpenedToday = smartTodayWalletCounts.size;
  const smartSymbolsOpenedToday = smartTodaySymbolCounts.size;
  const smartCurrentPositionCount = smartCurrentPositions.length;

  const smartExposureUsd = sum(smartWalletRows.map((row) => row.positionUsd));
  const smartLongExposureUsd = sum(
    smartWalletRows
      .filter((row) => String(row.side || "").includes("long") || String(row.side || "") === "buy")
      .map((row) => row.positionUsd)
  );
  const smartShortExposureUsd = sum(
    smartWalletRows
      .filter((row) => String(row.side || "").includes("short") || String(row.side || "") === "sell")
      .map((row) => row.positionUsd)
  );
  const smartMoneySharePct =
    totalTrackedExposureUsd > 0 ? (smartExposureUsd / totalTrackedExposureUsd) * 100 : 0;
  const smartBiasPct =
    smartExposureUsd > 0 ? ((smartLongExposureUsd - smartShortExposureUsd) / smartExposureUsd) * 100 : 0;

  return {
    activeWallets: activeWallets.size,
    currentHolders: holderRows.length,
    exitedWallets: new Set(exitRows.map((row) => row.wallet)).size,
    longWallets: longWallets.size,
    shortWallets: shortWallets.size,
    netWalletBias: longWallets.size - shortWallets.size,
    totalTrackedExposureUsd: round(totalTrackedExposureUsd, 2),
    top1SharePct: round(top1SharePct, 2),
    top5SharePct: round(top5SharePct, 2),
    topHolders: holderRows.slice(0, 10).map((row) => ({
      wallet: row.wallet,
      side: row.side,
      positionUsd: round(row.positionUsd, 2),
      unrealizedPnlUsd: round(row.unrealizedPnlUsd, 2),
      sharePct: totalTrackedExposureUsd > 0 ? round((row.positionUsd / totalTrackedExposureUsd) * 100, 2) : 0,
    })),
    topProfitableWallets: profitableRows,
    recentEntries: entryRows,
    recentExits: exitRows,
    holdingWalletCount: holderRows.length,
    universeRows: activeRows.length,
    longExposureUsd: round(longExposureUsd, 2),
    shortExposureUsd: round(shortExposureUsd, 2),
    averageLongEntryPrice:
      longExposureUsd > 0 ? round(longEntryWeightedUsd / longExposureUsd, 6) : null,
    averageShortEntryPrice:
      shortExposureUsd > 0 ? round(shortEntryWeightedUsd / shortExposureUsd, 6) : null,
    longPositionCount,
    shortPositionCount,
    smartMoney: {
      walletCount: smartWalletRows.length,
      exposureUsd: round(smartExposureUsd, 2),
      sharePct: round(smartMoneySharePct, 2),
      biasPct: round(smartBiasPct, 2),
      longExposureUsd: round(smartLongExposureUsd, 2),
      shortExposureUsd: round(smartShortExposureUsd, 2),
      positionCount: smartCurrentPositionCount,
      averageLongEntryPrice: smartAverageLongEntryPrice,
      averageShortEntryPrice: smartAverageShortEntryPrice,
      positionsOpenedToday: smartPositionsOpenedToday,
      walletsOpenedToday: smartWalletsOpenedToday,
      symbolsOpenedToday: smartSymbolsOpenedToday,
      openedSymbolsToday: smartOpenedSymbolsToday,
      topWallets: smartWalletRows.slice(0, 10).map((row) => {
        const openedTodayCount = smartTodayWalletCounts.get(row.wallet) || 0;
        const openedTodaySymbols = smartTodayWalletSymbols.get(row.wallet);
        return {
          ...row,
          openedTodayCount,
          openedTodaySymbolCount: openedTodaySymbols ? openedTodaySymbols.size : 0,
        };
      }),
    },
  };
}

function normalizeBookLevels(levels = []) {
  return (Array.isArray(levels) ? levels : [])
    .map((row) => {
      const price = toNum(Array.isArray(row) ? row[0] : row && (row.price ?? row.p), NaN);
      const size = toNum(Array.isArray(row) ? row[1] : row && (row.size ?? row.amount ?? row.a), NaN);
      if (!Number.isFinite(price) || price <= 0 || !Number.isFinite(size) || size <= 0) return null;
      return {
        price,
        size,
        notionalUsd: price * size,
      };
    })
    .filter(Boolean);
}

function buildOrderbookAnalytics({ orderbookRow = null, priceNow = null } = {}) {
  const book = orderbookRow && typeof orderbookRow === "object" ? orderbookRow : {};
  const levels = Array.isArray(book.levels) ? book.levels : [[], []];
  const bids = normalizeBookLevels(levels[0]);
  const asks = normalizeBookLevels(levels[1]);
  const mid = Number.isFinite(Number(priceNow))
    ? Number(priceNow)
    : bids[0] && asks[0]
    ? (bids[0].price + asks[0].price) / 2
    : null;

  if (!mid || (!bids.length && !asks.length)) {
    return {
      available: false,
      bids,
      asks,
      zones: [],
      visibleBidDepthUsd: null,
      visibleAskDepthUsd: null,
      nearBidDepthUsd: null,
      nearAskDepthUsd: null,
      imbalancePct: null,
      maxLiquidityZone: null,
      minLiquidityZone: null,
      largestOrder: null,
      smallestOrder: null,
      orderCount: 0,
      timestamp: toTimestampMs(book.timestamp),
    };
  }

  const visibleBidDepthUsd = sum(bids.map((row) => row.notionalUsd));
  const visibleAskDepthUsd = sum(asks.map((row) => row.notionalUsd));
  const nearBand = 0.01;
  const nearBidDepthUsd = sum(
    bids.filter((row) => (mid - row.price) / mid <= nearBand).map((row) => row.notionalUsd)
  );
  const nearAskDepthUsd = sum(
    asks.filter((row) => (row.price - mid) / mid <= nearBand).map((row) => row.notionalUsd)
  );
  const totalNearDepth = nearBidDepthUsd + nearAskDepthUsd;
  const imbalancePct =
    totalNearDepth > 0 ? ((nearBidDepthUsd - nearAskDepthUsd) / totalNearDepth) * 100 : 0;

  const levelsWithSide = [
    ...bids.map((row) => ({ ...row, side: "bid", distancePct: ((mid - row.price) / mid) * 100 })),
    ...asks.map((row) => ({ ...row, side: "ask", distancePct: ((row.price - mid) / mid) * 100 })),
  ];
  const zoneStepPct = 0.25;
  const zonesMap = new Map();
  levelsWithSide.forEach((row) => {
    const bucketIndex = Math.floor(Math.max(0, row.distancePct) / zoneStepPct);
    const key = `${row.side}:${bucketIndex}`;
    const startPct = bucketIndex * zoneStepPct;
    const endPct = startPct + zoneStepPct;
    const current = zonesMap.get(key) || {
      side: row.side,
      startPct,
      endPct,
      notionalUsd: 0,
      levelCount: 0,
      centerPrice:
        row.side === "bid"
          ? mid * (1 - (startPct + endPct) / 200)
          : mid * (1 + (startPct + endPct) / 200),
    };
    current.notionalUsd += row.notionalUsd;
    current.levelCount += 1;
    zonesMap.set(key, current);
  });
  const zones = Array.from(zonesMap.values()).sort((a, b) => b.notionalUsd - a.notionalUsd);
  const largestOrder = levelsWithSide.slice().sort((a, b) => b.notionalUsd - a.notionalUsd)[0] || null;
  const meaningfulFloor = Math.max(
    2500,
    (percentile(levelsWithSide.map((row) => row.notionalUsd), 0.1) || 0)
  );
  const smallestOrder =
    levelsWithSide
      .filter((row) => row.notionalUsd >= meaningfulFloor)
      .sort((a, b) => a.notionalUsd - b.notionalUsd)[0] || null;

  return {
    available: true,
    bids,
    asks,
    zones,
    visibleBidDepthUsd: round(visibleBidDepthUsd, 2),
    visibleAskDepthUsd: round(visibleAskDepthUsd, 2),
    nearBidDepthUsd: round(nearBidDepthUsd, 2),
    nearAskDepthUsd: round(nearAskDepthUsd, 2),
    imbalancePct: round(imbalancePct, 2),
    maxLiquidityZone: zones[0]
      ? {
          side: zones[0].side,
          price: round(zones[0].centerPrice, 6),
          startPct: round(zones[0].startPct, 2),
          endPct: round(zones[0].endPct, 2),
          notionalUsd: round(zones[0].notionalUsd, 2),
          levelCount: zones[0].levelCount,
        }
      : null,
    minLiquidityZone:
      zones
        .slice()
        .filter((row) => row.notionalUsd >= meaningfulFloor)
        .sort((a, b) => a.notionalUsd - b.notionalUsd)[0]
        ? {
            side:
              zones
                .slice()
                .filter((row) => row.notionalUsd >= meaningfulFloor)
                .sort((a, b) => a.notionalUsd - b.notionalUsd)[0].side,
            price: round(
              zones
                .slice()
                .filter((row) => row.notionalUsd >= meaningfulFloor)
                .sort((a, b) => a.notionalUsd - b.notionalUsd)[0].centerPrice,
              6
            ),
            notionalUsd: round(
              zones
                .slice()
                .filter((row) => row.notionalUsd >= meaningfulFloor)
                .sort((a, b) => a.notionalUsd - b.notionalUsd)[0].notionalUsd,
              2
            ),
          }
        : null,
    largestOrder:
      largestOrder
        ? {
            side: largestOrder.side,
            price: round(largestOrder.price, 6),
            notionalUsd: round(largestOrder.notionalUsd, 2),
            size: round(largestOrder.size, 6),
          }
        : null,
    smallestOrder:
      smallestOrder
        ? {
            side: smallestOrder.side,
            price: round(smallestOrder.price, 6),
            notionalUsd: round(smallestOrder.notionalUsd, 2),
            size: round(smallestOrder.size, 6),
          }
        : null,
    orderCount: levelsWithSide.length,
    timestamp: toTimestampMs(book.timestamp),
  };
}

function buildFlowWindows(tradesRows = [], nowMs = Date.now()) {
  const windows = {
    m5: 5 * 60 * 1000,
    m15: 15 * 60 * 1000,
    h1: 60 * 60 * 1000,
    h4: 4 * 60 * 60 * 1000,
    h24: DAY_MS,
  };
  return Object.entries(windows).reduce((acc, [key, windowMs]) => {
    const rows = (Array.isArray(tradesRows) ? tradesRows : []).filter((row) => {
      const ts = toTimestampMs(row && row.timestamp);
      return ts && nowMs - ts <= windowMs;
    });
    const buyUsd = sum(
      rows
        .filter((row) => {
          const side = String((row && row.side) || "").trim().toLowerCase();
          return side === "buy" || side === "b" || side === "open_long" || side === "close_short";
        })
        .map((row) => Math.abs(toNum(row && row.notionalUsd, 0)))
    );
    const sellUsd = sum(
      rows
        .filter((row) => {
          const side = String((row && row.side) || "").trim().toLowerCase();
          return side === "sell" || side === "s" || side === "open_short" || side === "close_long";
        })
        .map((row) => Math.abs(toNum(row && row.notionalUsd, 0)))
    );
    const totalUsd = buyUsd + sellUsd;
    acc[key] = {
      volumeUsd: round(totalUsd, 2),
      buyVolumeUsd: round(buyUsd, 2),
      sellVolumeUsd: round(sellUsd, 2),
      netUsd: round(buyUsd - sellUsd, 2),
      tradeCount: rows.length,
      buyVwap:
        buyUsd > 0
          ? round(
              sum(
                rows
                  .filter((row) => {
                    const side = String((row && row.side) || "").trim().toLowerCase();
                    return side === "buy" || side === "b" || side === "open_long" || side === "close_short";
                  })
                  .map((row) => Math.abs(toNum(row && row.notionalUsd, 0)) * toNum(row && row.price, 0))
              ) / buyUsd,
              6
            )
          : null,
      sellVwap:
        sellUsd > 0
          ? round(
              sum(
                rows
                  .filter((row) => {
                    const side = String((row && row.side) || "").trim().toLowerCase();
                    return side === "sell" || side === "s" || side === "open_short" || side === "close_long";
                  })
                  .map((row) => Math.abs(toNum(row && row.notionalUsd, 0)) * toNum(row && row.price, 0))
              ) / sellUsd,
              6
            )
          : null,
    };
    return acc;
  }, {});
}

function buildFearGreedProxy({
  priceChange24hPct = 0,
  oiChange24hPct = 0,
  fundingRatePct = 0,
  buySellImbalancePct = 0,
  volatility24hPct = 0,
  positioningRegime = "mixed",
} = {}) {
  let score = 50;
  score += clamp(toNum(priceChange24hPct, 0) * 1.4, -18, 18);
  score += clamp(toNum(buySellImbalancePct, 0) * 0.45, -16, 16);
  score += clamp(toNum(oiChange24hPct, 0) * 0.35, -10, 10);
  if (positioningRegime === "long_buildup") score += 8;
  if (positioningRegime === "short_buildup") score -= 8;
  if (positioningRegime === "short_covering") score += 2;
  if (positioningRegime === "long_liquidation") score -= 2;
  if (Math.abs(toNum(fundingRatePct, 0)) >= 0.02) score -= 6;
  if (Math.abs(toNum(volatility24hPct, 0)) >= 8) score -= 4;
  score = clamp(score, 0, 100);
  const label =
    score >= 75
      ? "Greed"
      : score >= 60
      ? "Risk-On"
      : score <= 25
      ? "Fear"
      : score <= 40
      ? "Risk-Off"
      : "Neutral";
  return {
    value: round(score, 1),
    label,
    note:
      "Derived market proxy from price momentum, buy/sell imbalance, open-interest shift, funding crowding, and realized volatility.",
  };
}

function classifyPositioningRegime(priceChangePct, oiChangePct) {
  const priceDelta = toNum(priceChangePct, 0);
  const oiDelta = toNum(oiChangePct, 0);
  if (priceDelta >= 0 && oiDelta >= 0) return "long_buildup";
  if (priceDelta < 0 && oiDelta >= 0) return "short_buildup";
  if (priceDelta >= 0 && oiDelta < 0) return "short_covering";
  if (priceDelta < 0 && oiDelta < 0) return "long_liquidation";
  return "mixed";
}

function buildRiskFlags({
  volume24hUsd = null,
  tradeCount24h = 0,
  walletSnapshot = null,
  fundingRatePct = null,
  basisPct = null,
  oiChange24hPct = null,
  priceChange24hPct = null,
  liquidationImbalancePct = null,
  burstScore = 0,
} = {}) {
  const flags = [];
  const wallet = walletSnapshot || {};
  if (toNum(wallet.top1SharePct, 0) >= 55 || toNum(wallet.top5SharePct, 0) >= 85) {
    flags.push({
      severity: "high",
      key: "concentration_risk",
      label: "Concentration risk",
      reason: "Tracked exposure is concentrated in a small number of wallets.",
    });
  }
  if (toNum(volume24hUsd, 0) < 1000000 || Number(tradeCount24h || 0) < 150) {
    flags.push({
      severity: "medium",
      key: "thin_participation",
      label: "Thin participation",
      reason: "Volume or trade count is light for a clean futures read.",
    });
  }
  if (Math.abs(toNum(fundingRatePct, 0)) >= 0.02 || Math.abs(toNum(basisPct, 0)) >= 0.75) {
    flags.push({
      severity: Math.abs(toNum(fundingRatePct, 0)) >= 0.04 ? "high" : "medium",
      key: "crowded_positioning",
      label: toNum(fundingRatePct, 0) >= 0 ? "Crowded long" : "Crowded short",
      reason: "Funding and basis imply one-sided positioning pressure.",
    });
  }
  if (Math.abs(toNum(priceChange24hPct, 0)) >= 10 && Math.abs(toNum(oiChange24hPct, 0)) >= 10) {
    flags.push({
      severity: "medium",
      key: "overheated_move",
      label: "Overheated move",
      reason: "Price and positioning both expanded quickly.",
    });
  }
  if (Math.abs(toNum(liquidationImbalancePct, 0)) >= 35) {
    flags.push({
      severity: "medium",
      key: "squeeze_risk",
      label: "Squeeze risk",
      reason: "Liquidation pressure is heavily one-sided.",
    });
  }
  if (burstScore >= 2.5) {
    flags.push({
      severity: "low",
      key: "unusual_flow",
      label: "Unusual volume spike",
      reason: "Recent trade activity is running well above its baseline.",
    });
  }
  return flags;
}

function scoreBand(value) {
  const score = clamp(toNum(value, 0), 0, 100);
  if (score >= 75) return "high";
  if (score >= 55) return "medium";
  return "low";
}

function biasLabelFromCode(code) {
  if (code === "long_bias") return "Long Bias";
  if (code === "short_bias") return "Short Bias";
  return "Neutral";
}

function crowdingLabelFromCode(code) {
  if (code === "long_crowded") return "Long Crowded";
  if (code === "short_crowded") return "Short Crowded";
  return "Balanced";
}

function actionLabelFromCode(code) {
  if (code === "favor_longs") return "Favor Longs";
  if (code === "favor_shorts") return "Favor Shorts";
  if (code === "avoid") return "Avoid";
  return "Wait";
}

function qualityLabelFromState(state) {
  const code = String(state || "").trim().toLowerCase();
  if (code === "strong_continuation") return "Strong Continuation";
  if (code === "constructive_continuation") return "Constructive Continuation";
  if (code === "exhaustion_risk") return "Exhaustion Risk";
  if (code === "reversal_risk") return "Reversal Risk";
  if (code === "squeeze_driven") return "Squeeze-Driven";
  if (code === "weak_setup") return "Weak Setup";
  return "Mixed Setup";
}

function toneFromBias(code) {
  if (code === "long_bias") return "bullish";
  if (code === "short_bias") return "bearish";
  return "neutral";
}

function toneFromAction(code) {
  if (code === "favor_longs") return "bullish";
  if (code === "favor_shorts") return "bearish";
  if (code === "avoid") return "risk";
  return "neutral";
}

function buildTokenDecisionModel({
  trend = null,
  priceChanges = {},
  positioningRegime = "mixed",
  fundingRatePct = null,
  predictedFundingPct = null,
  basisPct = null,
  oiChange24hPct = null,
  buyVolume24hUsd = 0,
  sellVolume24hUsd = 0,
  netFlow24hUsd = 0,
  volume24hUsd = 0,
  tradeCount24h = 0,
  burstScore = 0,
  liquidationImbalancePct = 0,
  walletSnapshot = null,
  riskFlags = [],
} = {}) {
  const price1h = toNum(priceChanges?.h1?.value, 0);
  const price4h = toNum(priceChanges?.h4?.value, 0);
  const price24h = toNum(priceChanges?.h24?.value, 0);
  const oi24h = toNum(oiChange24hPct, 0);
  const funding = toNum(fundingRatePct, 0);
  const predictedFunding = toNum(predictedFundingPct, 0);
  const basis = toNum(basisPct, 0);
  const flowRatio = volume24hUsd > 0 ? netFlow24hUsd / volume24hUsd : 0;
  const buySellImbalancePct =
    volume24hUsd > 0 ? ((buyVolume24hUsd - sellVolume24hUsd) / volume24hUsd) * 100 : 0;
  const wallet = walletSnapshot || {};
  const walletBias = toNum(wallet.netWalletBias, 0);
  const longWallets = toNum(wallet.longWallets, 0);
  const shortWallets = toNum(wallet.shortWallets, 0);
  const longShortRatio =
    shortWallets > 0 ? longWallets / shortWallets : longWallets > 0 ? null : 1;
  const top1SharePct = toNum(wallet.top1SharePct, 0);
  const top5SharePct = toNum(wallet.top5SharePct, 0);
  const smartSharePct = toNum(wallet.smartMoney?.sharePct, 0);
  const smartBiasPct = toNum(wallet.smartMoney?.biasPct, 0);
  const smartPositionsOpenedToday = toNum(wallet.smartMoney?.positionsOpenedToday, 0);
  const smartWalletsOpenedToday = toNum(wallet.smartMoney?.walletsOpenedToday, 0);
  const smartSymbolsOpenedToday = toNum(wallet.smartMoney?.symbolsOpenedToday, 0);

  let longScore = 50;
  let shortScore = 50;
  let longCrowding = 0;
  let shortCrowding = 0;
  let squeezeRisk = 18;
  let continuation = 40;
  let reversalRisk = 24;
  let setupQuality = 50;
  let alignment = 0;
  let contradiction = 0;

  const reasons = [];
  const pushReason = (weight, tone, title, detail) => {
    reasons.push({ weight, tone, title, detail });
  };

  const structure = String(trend && trend.structure ? trend.structure : "unknown");
  if (structure === "bullish_structure") {
    longScore += 12;
    continuation += 8;
    alignment += 1;
    pushReason(88, "bullish", "Trend structure is still bullish", "Price remains above its short and medium trend anchors, which keeps the path of least resistance higher.");
  } else if (structure === "bearish_structure") {
    shortScore += 12;
    continuation += 8;
    alignment += 1;
    pushReason(88, "bearish", "Trend structure is still bearish", "Price remains below its short and medium trend anchors, which keeps downside pressure intact.");
  } else if (structure === "reclaiming") {
    longScore += 7;
    continuation += 4;
    pushReason(62, "bullish", "Price is reclaiming structure", "The market is trying to recover trend support, which gives bulls a constructive short-term backdrop.");
  } else if (structure === "breaking_down") {
    shortScore += 7;
    continuation += 4;
    pushReason(62, "bearish", "Price is breaking down", "The market is losing trend support, which keeps short-side pressure relevant.");
  }

  if (price4h >= 2) {
    longScore += 10;
    continuation += 8;
    alignment += 1;
    pushReason(84, "bullish", "Short-term price momentum is strong", `Price is up ${round(price4h, 2)}% over 4h, which favors long-side continuation unless positioning gets too crowded.`);
  } else if (price4h >= 0.75) {
    longScore += 6;
    continuation += 4;
  } else if (price4h <= -2) {
    shortScore += 10;
    continuation += 8;
    alignment += 1;
    pushReason(84, "bearish", "Short-term downside momentum is strong", `Price is down ${Math.abs(round(price4h, 2))}% over 4h, which keeps short setups in control unless the move is already exhausted.`);
  } else if (price4h <= -0.75) {
    shortScore += 6;
    continuation += 4;
  }

  if (price1h >= 0.5) {
    longScore += 4;
  } else if (price1h <= -0.5) {
    shortScore += 4;
  }

  if (positioningRegime === "long_buildup") {
    longScore += 14;
    continuation += 14;
    setupQuality += 8;
    alignment += 1;
    longCrowding += Math.abs(oi24h) >= 8 && price24h >= 5 ? 12 : 4;
    pushReason(94, "bullish", "Price is rising with open interest", "That is classic long buildup: fresh longs are opening rather than the move being driven only by shorts covering.");
  } else if (positioningRegime === "short_buildup") {
    shortScore += 14;
    continuation += 14;
    setupQuality += 8;
    alignment += 1;
    shortCrowding += Math.abs(oi24h) >= 8 && price24h <= -5 ? 12 : 4;
    pushReason(94, "bearish", "Price is falling with open interest", "That is classic short buildup: fresh shorts are opening and leaning into the move.");
  } else if (positioningRegime === "short_covering") {
    longScore += 8;
    squeezeRisk += 18;
    reversalRisk += 10;
    setupQuality -= 3;
    contradiction += 1;
    pushReason(86, "risk", "The move looks squeeze-driven", "Price is rising while open interest falls, which often points to shorts getting forced out rather than durable new long positioning.");
  } else if (positioningRegime === "long_liquidation") {
    shortScore += 8;
    squeezeRisk += 18;
    reversalRisk += 10;
    setupQuality -= 3;
    contradiction += 1;
    pushReason(86, "risk", "The selloff looks liquidation-driven", "Price is falling while open interest drops, which often means longs are getting forced out rather than fresh shorts building cleanly.");
  }

  if (flowRatio >= 0.12) {
    longScore += 12;
    continuation += 8;
    alignment += 1;
    pushReason(82, "bullish", "Aggressive flow is still net buying", `Buy-side notional is leading by ${round(buySellImbalancePct, 1)}% of tracked volume, which supports a bullish tape.`);
  } else if (flowRatio >= 0.05) {
    longScore += 7;
    continuation += 4;
  } else if (flowRatio <= -0.12) {
    shortScore += 12;
    continuation += 8;
    alignment += 1;
    pushReason(82, "bearish", "Aggressive flow is still net selling", `Sell-side notional is leading by ${Math.abs(round(buySellImbalancePct, 1))}% of tracked volume, which supports bearish tape.`);
  } else if (flowRatio <= -0.05) {
    shortScore += 7;
    continuation += 4;
  } else {
    contradiction += 1;
  }

  if (walletBias >= 5) {
    longScore += 8;
    continuation += 5;
    pushReason(68, "bullish", "Tracked wallets lean net long", `${longWallets} tracked wallets are long versus ${shortWallets} short, which gives the move smart-wallet confirmation.`);
  } else if (walletBias >= 2) {
    longScore += 4;
  } else if (walletBias <= -5) {
    shortScore += 8;
    continuation += 5;
    pushReason(68, "bearish", "Tracked wallets lean net short", `${shortWallets} tracked wallets are short versus ${longWallets} long, which keeps pressure on the downside.`);
  } else if (walletBias <= -2) {
    shortScore += 4;
  } else {
    contradiction += 1;
  }

  if (smartSharePct >= 8 && smartBiasPct >= 20) {
    longScore += 8;
    continuation += 6;
    alignment += 1;
    pushReason(
      66,
      "bullish",
      "Smart wallets are building net long exposure",
      `Smart-scored wallets hold ${round(smartSharePct, 2)}% of tracked exposure and lean ${round(
        smartBiasPct,
        1
      )}% net long.`
    );
  } else if (smartSharePct >= 8 && smartBiasPct <= -20) {
    shortScore += 8;
    continuation += 6;
    alignment += 1;
    pushReason(
      66,
      "bearish",
      "Smart wallets are building net short exposure",
      `Smart-scored wallets hold ${round(smartSharePct, 2)}% of tracked exposure and lean ${Math.abs(
        round(smartBiasPct, 1)
      )}% net short.`
    );
  } else if (smartSharePct >= 12 && Math.abs(smartBiasPct) < 10) {
    setupQuality += 2;
    pushReason(
      48,
      "neutral",
      "Smart wallets are active but balanced",
      `Smart-scored wallets hold ${round(smartSharePct, 2)}% of tracked exposure, but their long/short split is still balanced.`
    );
  }

  if (smartPositionsOpenedToday >= 8) {
    continuation += 3;
    setupQuality += 2;
  } else if (smartPositionsOpenedToday >= 3) {
    continuation += 1;
  }

  if (liquidationImbalancePct >= 25) {
    longScore += 6;
    squeezeRisk += 10;
    reversalRisk += 4;
    pushReason(64, "bullish", "Short-side liquidations are dominating", "Shorts are getting squeezed harder than longs, which can keep upside moves sharp but less stable.");
  } else if (liquidationImbalancePct <= -25) {
    shortScore += 6;
    squeezeRisk += 10;
    reversalRisk += 4;
    pushReason(64, "bearish", "Long-side liquidations are dominating", "Longs are getting forced out harder than shorts, which can accelerate downside but also increase bounce risk later.");
  }

  if (funding >= 0.01) {
    longScore += 4;
    longCrowding += funding >= 0.04 ? 36 : funding >= 0.02 ? 26 : 12;
    if (funding >= 0.02) {
      reversalRisk += 8;
      setupQuality -= 4;
      pushReason(76, "risk", "Funding shows the long side is getting crowded", `Funding is running at ${round(funding, 4)}%, which says longs are paying up to stay in the trade.`);
    }
  } else if (funding <= -0.01) {
    shortScore += 4;
    shortCrowding += funding <= -0.04 ? 36 : funding <= -0.02 ? 26 : 12;
    if (funding <= -0.02) {
      reversalRisk += 8;
      setupQuality -= 4;
      pushReason(76, "risk", "Funding shows the short side is getting crowded", `Funding is running at ${round(funding, 4)}%, which says shorts are leaning heavily into the trade.`);
    }
  }

  if (predictedFunding >= 0.02) longCrowding += 10;
  if (predictedFunding <= -0.02) shortCrowding += 10;

  if (basis >= 0.2) {
    longScore += 3;
    longCrowding += basis >= 0.75 ? 20 : 10;
  } else if (basis <= -0.2) {
    shortScore += 3;
    shortCrowding += basis <= -0.75 ? 20 : 10;
  }

  if (volume24hUsd >= 5000000) setupQuality += 10;
  else if (volume24hUsd >= 1000000) setupQuality += 4;
  else setupQuality -= 8;

  if (tradeCount24h >= 400) setupQuality += 8;
  else if (tradeCount24h >= 150) setupQuality += 3;
  else setupQuality -= 10;

  if (burstScore >= 2.5) {
    continuation += 6;
    setupQuality += 4;
    pushReason(58, "neutral", "Participation is elevated versus baseline", `Recent volume is running ${round(burstScore, 2)}x the recent average, so the move has real attention.`);
  } else if (burstScore <= 0.8) {
    setupQuality -= 4;
  }

  const highRiskCount = (Array.isArray(riskFlags) ? riskFlags : []).filter((flag) => flag && flag.severity === "high").length;
  const mediumRiskCount = (Array.isArray(riskFlags) ? riskFlags : []).filter((flag) => flag && flag.severity === "medium").length;
  (Array.isArray(riskFlags) ? riskFlags : []).forEach((flag) => {
    if (!flag || !flag.key) return;
    if (flag.key === "thin_participation") setupQuality -= 10;
    if (flag.key === "concentration_risk") {
      setupQuality -= 8;
      reversalRisk += 6;
    }
    if (flag.key === "overheated_move") {
      continuation -= 4;
      reversalRisk += 16;
      pushReason(72, "risk", "The move is overheating", "Price and positioning expanded together fast enough to raise exhaustion risk for late entries.");
    }
    if (flag.key === "crowded_positioning") {
      reversalRisk += 8;
      setupQuality -= 4;
    }
    if (flag.key === "squeeze_risk") {
      squeezeRisk += 12;
      reversalRisk += 6;
    }
  });

  if (top1SharePct >= 55 || top5SharePct >= 85) {
    reversalRisk += 8;
    setupQuality -= 6;
  }

  longScore = clamp(longScore, 0, 100);
  shortScore = clamp(shortScore, 0, 100);
  longCrowding = clamp(longCrowding, 0, 100);
  shortCrowding = clamp(shortCrowding, 0, 100);
  squeezeRisk = clamp(squeezeRisk, 0, 100);
  continuation = clamp(continuation, 0, 100);
  reversalRisk = clamp(reversalRisk, 0, 100);
  setupQuality = clamp(setupQuality, 0, 100);

  const biasGap = Math.abs(longScore - shortScore);
  const biasCode =
    biasGap < 8 ? "neutral" : longScore > shortScore ? "long_bias" : "short_bias";
  const dominantCrowding =
    Math.max(longCrowding, shortCrowding) < 35
      ? "balanced"
      : longCrowding >= shortCrowding
      ? "long_crowded"
      : "short_crowded";
  const crowdingScore = clamp(Math.max(longCrowding, shortCrowding), 0, 100);

  let setupState = "mixed_setup";
  if (reversalRisk >= 75) setupState = "reversal_risk";
  else if (crowdingScore >= 70 && squeezeRisk >= 60) setupState = "exhaustion_risk";
  else if (squeezeRisk >= 72) setupState = "squeeze_driven";
  else if (setupQuality >= 72 && continuation >= 68) setupState = "strong_continuation";
  else if (setupQuality >= 56 && continuation >= 54) setupState = "constructive_continuation";
  else if (setupQuality <= 42) setupState = "weak_setup";

  const confidenceScore = clamp(
    38 + biasGap * 1.35 + alignment * 7 - contradiction * 4 - highRiskCount * 8 - mediumRiskCount * 3,
    5,
    95
  );
  const confidenceBand = scoreBand(confidenceScore);

  let action = "wait";
  if (setupQuality <= 34 || (highRiskCount >= 2 && crowdingScore >= 65)) {
    action = "avoid";
  } else if (biasCode === "long_bias" && confidenceScore >= 58 && reversalRisk < 70) {
    action = crowdingScore >= 78 ? "wait" : "favor_longs";
  } else if (biasCode === "short_bias" && confidenceScore >= 58 && reversalRisk < 70) {
    action = crowdingScore >= 78 ? "wait" : "favor_shorts";
  }

  const verdict =
    action === "favor_longs"
      ? crowdingScore >= 60
        ? "Favor long setups, but manage crowding risk."
        : "Favor long setups while positioning and flow stay aligned."
      : action === "favor_shorts"
      ? crowdingScore >= 60
        ? "Favor short setups, but manage squeeze risk carefully."
        : "Favor short setups while positioning and flow stay aligned."
      : action === "avoid"
      ? "Risk is too asymmetric here. Avoid forcing a trade."
      : "Mixed read. Wait for cleaner alignment before trading.";

  const orderedReasons = reasons
    .sort((a, b) => b.weight - a.weight)
    .slice(0, 5)
    .map((row, index) => ({
      ...row,
      rank: index + 1,
    }));

  return {
    verdict,
    summary: {
      bias: {
        key: "bias",
        code: biasCode,
        label: biasLabelFromCode(biasCode),
        tone: toneFromBias(biasCode),
        note:
          biasCode === "neutral"
            ? "Long and short signals are too mixed for a clean directional edge."
            : biasCode === "long_bias"
            ? "Trend, positioning, and flow lean more supportive for longs than shorts."
            : "Trend, positioning, and flow lean more supportive for shorts than longs.",
        score: round(Math.max(longScore, shortScore), 1),
      },
      crowding: {
        key: "crowding",
        code: dominantCrowding,
        label: crowdingLabelFromCode(dominantCrowding),
        tone: dominantCrowding === "balanced" ? "neutral" : "risk",
        note:
          dominantCrowding === "balanced"
            ? "Funding and basis do not show a dangerously one-sided market."
            : dominantCrowding === "long_crowded"
            ? "Longs are getting crowded, so breakout longs are less attractive."
            : "Shorts are getting crowded, so chase-shorting is less attractive.",
        score: round(crowdingScore, 1),
      },
      positioning: {
        key: "positioning",
        code: positioningRegime,
        label: titleCase(String(positioningRegime || "mixed").replace(/_/g, " ")),
        tone:
          positioningRegime === "long_buildup" || positioningRegime === "short_covering"
            ? "bullish"
            : positioningRegime === "short_buildup" || positioningRegime === "long_liquidation"
            ? "bearish"
            : "neutral",
        note:
          positioningRegime === "long_buildup"
            ? "Fresh longs appear to be opening with the move."
            : positioningRegime === "short_buildup"
            ? "Fresh shorts appear to be opening with the move."
            : positioningRegime === "short_covering"
            ? "The move is being helped by shorts closing out."
            : positioningRegime === "long_liquidation"
            ? "The move is being helped by longs getting forced out."
            : "Price and positioning are not cleanly aligned.",
      },
      quality: {
        key: "quality",
        code: setupState,
        label: qualityLabelFromState(setupState),
        tone:
          setupState === "strong_continuation" || setupState === "constructive_continuation"
            ? "bullish"
            : setupState === "weak_setup"
            ? "neutral"
            : "risk",
        note:
          setupState === "strong_continuation"
            ? "The move has broad enough confirmation to trust follow-through more."
            : setupState === "constructive_continuation"
            ? "There is enough confirmation to trade, but not enough to be aggressive."
            : setupState === "exhaustion_risk"
            ? "The move is extended and one-sided enough to raise trap risk."
            : setupState === "reversal_risk"
            ? "Crowding and exhaustion raise the chance of a snap-back."
            : setupState === "squeeze_driven"
            ? "The move looks more forced than organic, so it can reverse quickly."
            : "The setup does not have enough aligned evidence yet.",
        score: round(setupQuality, 1),
      },
      action: {
        key: "action",
        code: action,
        label: actionLabelFromCode(action),
        tone: toneFromAction(action),
        note: verdict,
      },
      confidence: {
        key: "confidence",
        code: confidenceBand,
        label: titleCase(confidenceBand),
        tone: confidenceBand === "high" ? "bullish" : confidenceBand === "medium" ? "neutral" : "risk",
        note: `${round(confidenceScore, 0)}/100 confidence based on signal alignment versus risk.`,
        score: round(confidenceScore, 0),
      },
    },
    scores: [
      {
        key: "long_bias",
        label: "Long Bias",
        value: round(longScore, 1),
        tone: "bullish",
        note: "Higher means the tape favors long setups.",
      },
      {
        key: "short_bias",
        label: "Short Bias",
        value: round(shortScore, 1),
        tone: "bearish",
        note: "Higher means the tape favors short setups.",
      },
      {
        key: "crowding",
        label: "Crowding",
        value: round(crowdingScore, 1),
        tone: dominantCrowding === "balanced" ? "neutral" : "risk",
        note: "Higher means more one-sided positioning pressure.",
      },
      {
        key: "squeeze_risk",
        label: "Squeeze Risk",
        value: round(squeezeRisk, 1),
        tone: "risk",
        note: "Higher means forced moves are more likely.",
      },
      {
        key: "continuation",
        label: "Continuation",
        value: round(continuation, 1),
        tone: biasCode === "short_bias" ? "bearish" : "bullish",
        note: "Higher means trend follow-through has better support.",
      },
      {
        key: "reversal_risk",
        label: "Reversal Risk",
        value: round(reversalRisk, 1),
        tone: "risk",
        note: "Higher means late entries face more trap risk.",
      },
    ],
    reasoning: orderedReasons,
  };
}

function buildChart(series = [], mode = "line", status = "unavailable", label = "", coverageNote = null) {
  const rows = sortByTimestampAsc(series).map((row) => ({
    timestamp: toTimestampMs(row && row.timestamp),
    value: Number.isFinite(Number(row && row.value)) ? Number(row.value) : 0,
    longValue: Number.isFinite(Number(row && row.longValue)) ? Number(row.longValue) : 0,
    shortValue: Number.isFinite(Number(row && row.shortValue)) ? Number(row.shortValue) : 0,
    buyValue: Number.isFinite(Number(row && row.buyVolumeUsd)) ? Number(row.buyVolumeUsd) : 0,
    sellValue: Number.isFinite(Number(row && row.sellVolumeUsd)) ? Number(row.sellVolumeUsd) : 0,
    tradeCount: Number.isFinite(Number(row && row.tradeCount)) ? Number(row.tradeCount) : 0,
  }));
  return {
    label,
    mode,
    status,
    coverageNote: coverageNote || null,
    series: rows,
  };
}

function shortWallet(wallet) {
  const value = String(wallet || "").trim();
  if (!value) return "unknown";
  if (value.length <= 12) return value;
  return `${value.slice(0, 6)}...${value.slice(-4)}`;
}

function buildRecentActivity({
  tradesRows = [],
  walletSnapshot = null,
} = {}) {
  const largeTradeThreshold = Math.max(
    100000,
    percentile(
      (Array.isArray(tradesRows) ? tradesRows : []).map((row) => Math.abs(toNum(row && row.notionalUsd, 0))),
      0.95
    ) || 0
  );
  const marketRows = (Array.isArray(tradesRows) ? tradesRows : [])
    .filter((row) => Math.abs(toNum(row && row.notionalUsd, 0)) >= largeTradeThreshold)
    .sort((a, b) => toNum(b && b.timestamp, 0) - toNum(a && a.timestamp, 0))
    .slice(0, 18)
    .map((row) => ({
      type: "market_trade",
      actorType: "market",
      actorLabel: "Market tape",
      wallet: null,
      side: String((row && row.side) || "").trim().toLowerCase() || null,
      action: isLiquidationCauseLocal(row && row.cause) ? "liquidation" : "trade",
      sizeUsd: round(Math.abs(toNum(row && row.notionalUsd, 0)), 2),
      price: round(toNum(row && row.price, NaN), 6),
      pnlUsd: null,
      timestamp: toTimestampMs(row && row.timestamp),
      note: String((row && row.cause) || "").trim().toLowerCase() || null,
    }));
  const walletRows = [];
  const snapshot = walletSnapshot || {};
  (Array.isArray(snapshot.recentEntries) ? snapshot.recentEntries : []).forEach((row) => {
    walletRows.push({
      type: "tracked_wallet",
      actorType: "wallet",
      actorLabel: shortWallet(row.wallet),
      wallet: row.wallet,
      side: row.side,
      action: row.action,
      sizeUsd: round(toNum(row.sizeUsd, 0), 2),
      price: null,
      pnlUsd: null,
      timestamp: toTimestampMs(row.timestamp),
      note: "tracked wallet lifecycle",
    });
  });
  (Array.isArray(snapshot.recentExits) ? snapshot.recentExits : []).forEach((row) => {
    walletRows.push({
      type: "tracked_wallet",
      actorType: "wallet",
      actorLabel: shortWallet(row.wallet),
      wallet: row.wallet,
      side: row.side,
      action: row.action,
      sizeUsd: round(toNum(row.sizeUsd, 0), 2),
      price: null,
      pnlUsd: null,
      timestamp: toTimestampMs(row.timestamp),
      note: "tracked wallet lifecycle",
    });
  });

  return dedupeBy([...walletRows, ...marketRows], (row) => `${row.type}:${row.actorLabel}:${row.action}:${row.timestamp}:${row.sizeUsd}`)
    .filter((row) => row.timestamp)
    .sort((a, b) => b.timestamp - a.timestamp)
    .slice(0, 32);
}

function isLiquidationCauseLocal(cause) {
  const normalized = String(cause || "").trim().toLowerCase();
  return normalized.includes("liq") || normalized.includes("liquid");
}

function buildTokenAnalysisPayload({
  symbol,
  timeframe,
  priceRow = null,
  tradesRows = [],
  fundingRows = [],
  candlesRows = [],
  infoRow = null,
  orderbookRow = null,
  sourceMeta = null,
  explorerDataset = null,
  walletRows = [],
  lifecyclePositions = [],
} = {},
options = {}) {
  const generatedAt = Date.now();
  const normalizedSymbol = normalizeTokenSymbol(symbol || "");
  const walletOnly = Boolean(options && options.walletOnly);
  if (walletOnly) {
    const priceNow = toNum(
      priceRow &&
        (priceRow.mark !== undefined
          ? priceRow.mark
          : priceRow.mid !== undefined
          ? priceRow.mid
          : priceRow.oracle !== undefined
          ? priceRow.oracle
          : NaN),
      NaN
    );
    const oraclePrice = toNum(priceRow && priceRow.oracle, NaN);
    const fundingNowPct = toNum(
      priceRow &&
        (priceRow.funding !== undefined
          ? priceRow.funding
          : priceRow.fundingRate !== undefined
          ? priceRow.fundingRate
          : NaN),
      NaN
    );
    const nextFundingPct = toNum(
      priceRow &&
        (priceRow.nextFunding !== undefined
          ? priceRow.nextFunding
          : priceRow.next_funding !== undefined
          ? priceRow.next_funding
          : NaN),
      NaN
    );
    const volume24hUsd = toNum(
      priceRow &&
        (priceRow.volume_24h !== undefined ? priceRow.volume_24h : priceRow.volume24h !== undefined ? priceRow.volume24h : NaN),
      NaN
    );
    const basisPct =
      Number.isFinite(priceNow) && Number.isFinite(oraclePrice) && oraclePrice !== 0
        ? ((priceNow - oraclePrice) / Math.abs(oraclePrice)) * 100
        : null;
    const walletSnapshot = buildWalletTokenSnapshot({
      symbol,
      explorerDataset: {
        ...(explorerDataset && typeof explorerDataset === "object" ? explorerDataset : {}),
        walletRows: Array.isArray(walletRows) ? walletRows : [],
      },
      lifecyclePositions,
    });
    const walletPositionTable = buildTokenWalletPositionTable({
      symbol,
      walletRows,
      explorerDataset,
      lifecyclePositions,
      currentPrice: priceNow,
      page: options.walletTablePage,
      pageSize: options.walletTablePageSize,
      sortKey: options.walletTableSortKey,
      sortDir: options.walletTableSortDir,
    });
    const coinGroups = buildCoinOpenPositionGroups({
      explorerDataset,
      walletRows,
    });
    const longWallets = toNum(walletSnapshot.longWallets, 0);
    const shortWallets = toNum(walletSnapshot.shortWallets, 0);
    const longShortRatio =
      shortWallets > 0 ? longWallets / shortWallets : longWallets > 0 ? null : 1;
    const smartBiasPct = toNum(walletSnapshot.smartMoney.biasPct, 0);
    const biasCode = smartBiasPct >= 15 ? "long_bias" : smartBiasPct <= -15 ? "short_bias" : "neutral";
    const biasLabel = biasCode === "long_bias" ? "Bullish" : biasCode === "short_bias" ? "Bearish" : "Neutral";
    const actionCode =
      walletSnapshot.smartMoney.walletCount > 0
        ? smartBiasPct >= 15
          ? "favor_longs"
          : smartBiasPct <= -15
          ? "favor_shorts"
          : "wait"
        : "wait";
    const actionLabel =
      actionCode === "favor_longs" ? "Long Setup" : actionCode === "favor_shorts" ? "Short Setup" : "No Trade";
    const crowdingScore = clamp(
      50 +
        Math.max(-25, Math.min(25, smartBiasPct)) +
        Math.max(0, Math.min(20, toNum(walletSnapshot.smartMoney.sharePct, 0))) * 0.5,
      0,
      100
    );
    const freshnessUpdatedAt = generatedAt;
    const walletOnlyReasons = [];
    if (walletSnapshot.smartMoney.walletCount > 0) {
      walletOnlyReasons.push({
        title: "Smart wallets tracked",
        detail: `${walletSnapshot.smartMoney.walletCount} high-scoring wallets are currently contributing to BTC positioning.`,
        tone: "neutral",
      });
      walletOnlyReasons.push({
        title: "Current open-position set",
        detail: `${walletPositionTable.walletCount} wallets currently hold BTC positions in the live current-open set used by Live Trade.`,
        tone: smartBiasPct >= 0 ? "bullish" : "bearish",
      });
      walletOnlyReasons.push({
        title: "Average entries updated",
        detail:
          walletSnapshot.smartMoney.averageLongEntryPrice || walletSnapshot.smartMoney.averageShortEntryPrice
            ? `Average long entry ${Number.isFinite(Number(walletSnapshot.smartMoney.averageLongEntryPrice)) ? Math.trunc(Number(walletSnapshot.smartMoney.averageLongEntryPrice)) : "-"} and average short entry ${Number.isFinite(Number(walletSnapshot.smartMoney.averageShortEntryPrice)) ? Math.trunc(Number(walletSnapshot.smartMoney.averageShortEntryPrice)) : "-"}.`
            : "Average entry data is not available yet.",
        tone: "neutral",
      });
    } else {
      walletOnlyReasons.push({
        title: "No smart-wallet cohort yet",
        detail: "The BTC smart-wallet score model has not identified any wallets above threshold yet.",
        tone: "neutral",
      });
    }
    return {
      generatedAt,
      symbol: normalizedSymbol,
      timeframe: String(timeframe || "24h"),
      header: {
        symbol: normalizedSymbol,
        name: normalizedSymbol,
        address: null,
        addressStatus: "unavailable",
        addressNote: "Pacifica exposes perp market symbols here, not underlying token contract addresses.",
        listedAt: null,
        marketType: "perpetual",
      },
      hero: {
        currentPrice: buildMetric({
          key: "current_price",
          label: "Current Price",
          value: metricValue(priceNow, 6),
          unit: "price",
          status: Number.isFinite(priceNow) ? "exact" : "unavailable",
          method: "snapshot",
          source: "shared_kpi_snapshot",
        }),
        openInterestUsd: buildMetric({
          key: "open_interest",
          label: "Open Interest",
          value: metricValue(
            Number.isFinite(priceNow) && Number.isFinite(toNum(priceRow && (priceRow.open_interest ?? priceRow.openInterest ?? priceRow.openInterestBase), NaN))
              ? priceNow *
                toNum(priceRow && (priceRow.open_interest !== undefined ? priceRow.open_interest : priceRow.openInterest !== undefined ? priceRow.openInterest : priceRow.openInterestBase), NaN)
              : NaN,
            2
          ),
          unit: "usd",
          status: Number.isFinite(priceNow) ? "exact" : "unavailable",
          method: "snapshot",
          source: "shared_kpi_snapshot",
        }),
        fundingRatePct: buildMetric({
          key: "funding_rate",
          label: "Funding Rate",
          value: metricValue(fundingNowPct * 100, 6),
          unit: "pct",
          status: Number.isFinite(fundingNowPct) ? "exact" : "unavailable",
          method: "snapshot",
          source: "shared_kpi_snapshot",
        }),
        predictedFundingPct: buildMetric({
          key: "predicted_funding",
          label: "Predicted Funding",
          value: metricValue(nextFundingPct * 100, 6),
          unit: "pct",
          status: Number.isFinite(nextFundingPct) ? "exact" : "unavailable",
          method: "snapshot",
          source: "shared_kpi_snapshot",
        }),
        volume24hUsd: buildMetric({
          key: "volume_24h",
          label: "24H Volume",
          value: metricValue(volume24hUsd, 2),
          unit: "usd",
          status: Number.isFinite(volume24hUsd) ? "exact" : "unavailable",
          method: "snapshot",
          source: "shared_kpi_snapshot",
        }),
        longShortRatio: buildMetric({
          key: "tracked_long_short_ratio",
          label: "Long / Short Ratio",
          value: metricValue(longShortRatio, 3),
          unit: "ratio",
          status: longWallets > 0 || shortWallets > 0 ? "exact" : "unavailable",
          method: "current_open_positions",
          source: "tracked_wallet_positions",
          extra: {
            longWallets,
            shortWallets,
          },
        }),
      },
      focus: {
        bias: biasCode,
        positioningRegime: "wallet_only",
        crowding: crowdingScore >= 70 ? "crowded" : crowdingScore >= 45 ? "balanced" : "light",
        action: actionCode,
        squeezeRisk: crowdingScore >= 70 ? "elevated" : "normal",
      },
      decision: {
        summary: {
          bias: { code: biasCode, label: biasLabel },
          action: { code: actionCode, label: actionLabel },
          crowding: { score: crowdingScore },
          quality: { code: "wallet_only", label: "Wallet-only fast path" },
        },
        reasoning: walletOnlyReasons,
        scores: [],
      },
      sections: {
        smartMoney: {
          smartSentiment: buildMetric({
            key: "smart_sentiment",
            label: "Smart Sentiment",
            value: metricValue(walletSnapshot.smartMoney.sharePct, 2),
            unit: "pct",
            status: walletSnapshot.smartMoney.walletCount > 0 ? "partial" : "unavailable",
            method: "scored_tracked_wallet_model",
            source: "tracked_wallet_positions + wallet_performance",
          }),
          smartMoneyUsd: buildMetric({
            key: "smart_money_capital",
            label: "Smart Money Capital",
            value: metricValue(walletSnapshot.smartMoney.exposureUsd, 2),
            unit: "usd",
            status: walletSnapshot.smartMoney.walletCount > 0 ? "partial" : "unavailable",
            method: "scored_tracked_wallet_model",
            source: "tracked_wallet_positions",
          }),
          smartBias: buildMetric({
            key: "smart_money_bias",
            label: "Smart Money Bias",
            value: metricValue(walletSnapshot.smartMoney.biasPct, 2),
            unit: "pct",
            status: walletSnapshot.smartMoney.walletCount > 0 ? "partial" : "unavailable",
            method: "net_long_vs_short_smart_exposure",
            source: "tracked_wallet_positions",
          }),
          smartWallets: buildMetric({
            key: "smart_wallet_count",
            label: "Smart Wallets",
            value: metricValue(walletSnapshot.smartMoney.walletCount, 0),
            unit: "count",
            status: walletSnapshot.smartMoney.walletCount > 0 ? "partial" : "unavailable",
            method: "scored_tracked_wallet_model",
            source: "wallet_explorer_dataset",
          }),
          smartAverageLongEntryPrice: buildMetric({
            key: "smart_average_long_entry_price",
            label: "Smart Avg Long Entry",
            value: metricValue(walletSnapshot.smartMoney.averageLongEntryPrice, 6),
            unit: "price",
            status: Number.isFinite(toNum(walletSnapshot.smartMoney.averageLongEntryPrice, NaN))
              ? "exact"
              : "unavailable",
            method: "position_usd_weighted_open_long_entry",
            source: "smart_wallet_current_positions",
          }),
          smartAverageShortEntryPrice: buildMetric({
            key: "smart_average_short_entry_price",
            label: "Smart Avg Short Entry",
            value: metricValue(walletSnapshot.smartMoney.averageShortEntryPrice, 6),
            unit: "price",
            status: Number.isFinite(toNum(walletSnapshot.smartMoney.averageShortEntryPrice, NaN))
              ? "exact"
              : "unavailable",
            method: "position_usd_weighted_open_short_entry",
            source: "smart_wallet_current_positions",
          }),
          smartPositionsOpenedToday: buildMetric({
            key: "smart_positions_opened_today",
            label: "Smart Positions Opened Today",
            value: metricValue(walletSnapshot.smartMoney.positionsOpenedToday, 0),
            unit: "count",
            status: walletSnapshot.smartMoney.walletsOpenedToday > 0 ? "partial" : "unavailable",
            method: "current_opened_at_utc_day",
            source: "smart_wallet_lifecycle_positions",
          }),
          smartWalletsOpenedToday: buildMetric({
            key: "smart_wallets_opened_today",
            label: "Smart Wallets Opened Today",
            value: metricValue(walletSnapshot.smartMoney.walletsOpenedToday, 0),
            unit: "count",
            status: walletSnapshot.smartMoney.walletsOpenedToday > 0 ? "partial" : "unavailable",
            method: "current_opened_at_utc_day",
            source: "smart_wallet_lifecycle_positions",
          }),
          smartSymbolsOpenedToday: buildMetric({
            key: "smart_symbols_opened_today",
            label: "Smart Coins Opened Today",
            value: metricValue(walletSnapshot.smartMoney.symbolsOpenedToday, 0),
            unit: "count",
            status: walletSnapshot.smartMoney.symbolsOpenedToday > 0 ? "partial" : "unavailable",
            method: "current_opened_at_utc_day",
            source: "smart_wallet_lifecycle_positions",
          }),
          topWallets: walletSnapshot.smartMoney.topWallets,
        },
        trades: {
          status: Array.isArray(tradesRows) && tradesRows.length ? "exact" : "unavailable",
          method: "wallet_trade_events",
          source: sourceMeta && sourceMeta.trades ? sourceMeta.trades : "wallet_trade_events.json:BTC",
          symbol: normalizedSymbol,
          count: Array.isArray(tradesRows) ? tradesRows.length : 0,
          totalRows: Array.isArray(tradesRows) ? tradesRows.length : 0,
          positionCount: Array.isArray(tradesRows) ? tradesRows.length : 0,
          rows: Array.isArray(tradesRows) ? tradesRows : [],
        },
        wallets: {
          positions: buildMetric({
            key: `${normalizedSymbol}_wallet_positions`,
            label: `${normalizedSymbol} Wallet Positions`,
            value: metricValue(walletPositionTable.positionCount, 0),
            unit: "count",
            status: walletPositionTable.walletCount > 0 ? "exact" : "unavailable",
            method: "live_current_open_positions",
            source: "live_positions_baseline_current_open_set + wallet_summary",
          }),
          totalPnL: buildMetric({
            key: `${normalizedSymbol}_wallet_total_pnl`,
            label: "Total PnL",
            value: metricValue(walletPositionTable.totalWalletPnlUsd, 2),
            unit: "usd",
            status: walletPositionTable.walletCount > 0 ? "exact" : "unavailable",
            method: "wallet_total_pnl_merged",
            source: "wallet_records",
          }),
          avgOpenPositions30d: buildMetric({
            key: `${normalizedSymbol}_wallet_avg_open_positions_30d`,
            label: "Avg. Open Positions / 30d",
            value: metricValue(walletPositionTable.avgOpenPositions30d, 3),
            unit: "number",
            status: walletPositionTable.walletCount > 0 ? "derived" : "unavailable",
            method: "opened_positions_last_30d_divided_by_30",
            source: "lifecycle_positions",
          }),
          totalVolumeOnCoin: buildMetric({
            key: `${normalizedSymbol}_wallet_total_coin_volume`,
            label: "Total Volume",
            value: metricValue(walletPositionTable.totalCoinVolumeUsd, 2),
            unit: "usd",
            status: walletPositionTable.walletCount > 0 ? "exact" : "unavailable",
            method: "wallet_symbol_volume_merged",
            source: "wallet_records + symbol_breakdown",
          }),
          positionTable: {
            status: walletPositionTable.walletCount > 0 ? "exact" : "unavailable",
            method: "live_current_open_positions + wallet_summary",
            source: "live_positions_baseline_current_open_set + wallet_records",
            symbol: normalizedSymbol,
            walletCount: walletPositionTable.walletCount,
            positionCount: walletPositionTable.positionCount,
            page: walletPositionTable.page,
            pageSize: walletPositionTable.pageSize,
            totalRows: walletPositionTable.totalRows,
            totalPages: walletPositionTable.totalPages,
            sortKey: walletPositionTable.sortKey,
            sortDir: walletPositionTable.sortDir,
            totalPositionUsd: walletPositionTable.totalPositionUsd,
            totalWalletPnlUsd: walletPositionTable.totalWalletPnlUsd,
            totalCoinVolumeUsd: walletPositionTable.totalCoinVolumeUsd,
            avgOpenPositions30d: walletPositionTable.avgOpenPositions30d,
            validation: walletPositionTable.validation,
            stats: walletPositionTable.stats,
            rows: walletPositionTable.rows,
          },
        },
        coins: {
          status: coinGroups.totalSymbols > 0 ? "exact" : "unavailable",
          method: "live_current_open_positions_grouped_by_symbol",
          source: "live_positions_baseline_current_open_set + wallet_records",
          totalSymbols: coinGroups.totalSymbols,
          totalPositions: coinGroups.totalPositions,
          totalWallets: coinGroups.totalWallets,
          rows: coinGroups.rows,
        },
      },
      risk: {
        flags: [],
      },
      freshness: {
        updatedAt:
          Number((walletPositionTable.validation && walletPositionTable.validation.refreshedAt) || 0) ||
          Number((walletPositionTable.validation && walletPositionTable.validation.sourceLastSuccessAt) || 0) ||
          generatedAt,
        sourceLastSuccessAt:
          Number((walletPositionTable.validation && walletPositionTable.validation.sourceLastSuccessAt) || 0) || null,
      },
      meta: {
        source: sourceMeta || {},
        validation: walletPositionTable.validation,
        rows: {
          trades: Array.isArray(tradesRows) ? tradesRows.length : 0,
        },
      },
      tradeRows: Array.isArray(tradesRows) ? tradesRows : [],
      methodology: {
        items: [
          {
            key: "wallet_positions",
            label: "Wallet positions",
            formula: "Current open BTC positions and their unrealized PnL are derived from the live current-open set used by Live Trade, filtered to BTC at read time.",
          },
          {
            key: "smart_money",
            label: "Smart money",
            formula: "Smart wallets are scored from win rate, realized PnL, trading volume, exposure, recency, and observed position history.",
          },
          {
            key: "table",
            label: "Wallet table",
            formula: "The BTC wallet table is paginated client-side and only the current page is rendered in the UI.",
          },
        ],
      },
    };
  }
  const priceHistory = buildPriceHistory({
    candlesRows,
    tradesRows,
    priceRow,
  });
  const priceNow = toNum(
    priceRow &&
      (priceRow.mark !== undefined
        ? priceRow.mark
        : priceRow.mid !== undefined
        ? priceRow.mid
        : priceRow.oracle !== undefined
        ? priceRow.oracle
        : priceHistory[priceHistory.length - 1] && priceHistory[priceHistory.length - 1].value),
    NaN
  );
  const oraclePrice = toNum(priceRow && priceRow.oracle, NaN);
  const midPrice = toNum(priceRow && priceRow.mid, NaN);
  const yesterdayPrice = toNum(
    priceRow && (priceRow.yesterday_price !== undefined ? priceRow.yesterday_price : priceRow.yesterdayPrice),
    NaN
  );
  const openInterestBase = toNum(
    priceRow &&
      (priceRow.open_interest !== undefined
        ? priceRow.open_interest
        : priceRow.openInterest !== undefined
        ? priceRow.openInterest
        : NaN),
    NaN
  );
  const oiUsdNow = Number.isFinite(priceNow) && Number.isFinite(openInterestBase) ? priceNow * openInterestBase : null;
  const nextFundingPct = toNum(
    priceRow &&
      (priceRow.nextFunding !== undefined
        ? priceRow.nextFunding
        : priceRow.next_funding !== undefined
        ? priceRow.next_funding
        : NaN),
    NaN
  );
  const fundingNowPct = toNum(
    priceRow &&
      (priceRow.funding !== undefined
        ? priceRow.funding
        : priceRow.fundingRate !== undefined
        ? priceRow.fundingRate
        : NaN),
    NaN
  );
  const volume24hUsd = toNum(
    priceRow &&
      (priceRow.volume_24h !== undefined ? priceRow.volume_24h : priceRow.volume24h !== undefined ? priceRow.volume24h : NaN),
    NaN
  );
  const basisPct =
    Number.isFinite(priceNow) && Number.isFinite(oraclePrice) && oraclePrice !== 0
      ? ((priceNow - oraclePrice) / Math.abs(oraclePrice)) * 100
      : null;

  const priceChanges = {
    m5: computeWindowChange(priceHistory, 5 * 60 * 1000, priceNow),
    m15: computeWindowChange(priceHistory, 15 * 60 * 1000, priceNow),
    h1: computeWindowChange(priceHistory, 60 * 60 * 1000, priceNow),
    h4: computeWindowChange(priceHistory, 4 * 60 * 60 * 1000, priceNow),
    h24: Number.isFinite(yesterdayPrice) && yesterdayPrice !== 0
      ? {
          value: round(((priceNow - yesterdayPrice) / Math.abs(yesterdayPrice)) * 100, 4),
          status: "exact",
          coverageStart: Date.now() - DAY_MS,
          coverageEnd: Date.now(),
        }
      : computeWindowChange(priceHistory, DAY_MS, priceNow),
    d7: computeWindowChange(priceHistory, 7 * DAY_MS, priceNow),
  };

  const dailyRange = computeDailyRange(priceHistory);
  const volatility = computeVolatility(priceHistory, DAY_MS);
  const trend = computeTrend(priceHistory);

  const oiSeries = estimateOiSeries(priceHistory, priceRow);
  const fundingSeries = buildFundingSeries(fundingRows, priceRow);
  const tradeBuckets5m = bucketTradesByTime(tradesRows, 5 * 60 * 1000);
  const tradeBuckets1h = bucketTradesByTime(tradesRows, 60 * 60 * 1000);
  const liquidations24h = tradeBuckets5m
    .filter((row) => generatedAt - row.timestamp <= DAY_MS)
    .reduce(
      (acc, row) => {
        acc.longUsd += toNum(row.longLiquidationsUsd, 0);
        acc.shortUsd += toNum(row.shortLiquidationsUsd, 0);
        return acc;
      },
      { longUsd: 0, shortUsd: 0 }
    );
  const totalLiquidations24hUsd = liquidations24h.longUsd + liquidations24h.shortUsd;
  const liquidationImbalancePct =
    totalLiquidations24hUsd > 0
      ? ((liquidations24h.shortUsd - liquidations24h.longUsd) / totalLiquidations24hUsd) * 100
      : 0;

  const oiChange1h = computeWindowChange(oiSeries, 60 * 60 * 1000, oiUsdNow);
  const oiChange4h = computeWindowChange(oiSeries, 4 * 60 * 60 * 1000, oiUsdNow);
  const oiChange24h = computeWindowChange(oiSeries, DAY_MS, oiUsdNow);
  const fundingTrendSeries = fundingSeries.slice(-8).map((row) => row.value);
  const fundingTrendAvg = mean(fundingTrendSeries);
  const fundingTrendDelta =
    fundingSeries.length >= 2
      ? toNum(fundingSeries[fundingSeries.length - 1].value, 0) - toNum(fundingSeries[0].value, 0)
      : 0;

  const tradeRows24h = (Array.isArray(tradesRows) ? tradesRows : []).filter(
    (row) => generatedAt - toNum(row && row.timestamp, 0) <= DAY_MS
  );
  const buyVolume24hUsd = sum(
    tradeRows24h
      .filter((row) => {
        const side = String((row && row.side) || "").trim().toLowerCase();
        return side === "buy" || side === "b" || side === "open_long" || side === "close_short";
      })
      .map((row) => Math.abs(toNum(row && row.notionalUsd, 0)))
  );
  const sellVolume24hUsd = sum(
    tradeRows24h
      .filter((row) => {
        const side = String((row && row.side) || "").trim().toLowerCase();
        return side === "sell" || side === "s" || side === "open_short" || side === "close_long";
      })
      .map((row) => Math.abs(toNum(row && row.notionalUsd, 0)))
  );
  const netFlow24hUsd = buyVolume24hUsd - sellVolume24hUsd;
  const buySellImbalancePct =
    buyVolume24hUsd + sellVolume24hUsd > 0
      ? ((buyVolume24hUsd - sellVolume24hUsd) / (buyVolume24hUsd + sellVolume24hUsd)) * 100
      : 0;
  const tradeCount24h = tradeRows24h.length;
  const avgTradeSize24hUsd = tradeCount24h > 0 ? (buyVolume24hUsd + sellVolume24hUsd) / tradeCount24h : null;
  const tradeSizes24h = tradeRows24h.map((row) => Math.abs(toNum(row && row.notionalUsd, 0))).filter((value) => value > 0);
  const largeTradeThreshold = Math.max(100000, percentile(tradeSizes24h, 0.95) || 0);
  const largeTradeCount24h = tradeSizes24h.filter((value) => value >= largeTradeThreshold).length;
  const avgHourlyVolume = mean(tradeBuckets1h.map((row) => row.volumeUsd)) || 0;
  const latestHourlyVolume = toNum(tradeBuckets1h[tradeBuckets1h.length - 1] && tradeBuckets1h[tradeBuckets1h.length - 1].volumeUsd, 0);
  const burstScore = avgHourlyVolume > 0 ? latestHourlyVolume / avgHourlyVolume : 0;
  const flowWindows = buildFlowWindows(tradesRows, generatedAt);

  const walletSnapshot = buildWalletTokenSnapshot({
    symbol,
    explorerDataset: {
      ...(explorerDataset && typeof explorerDataset === "object" ? explorerDataset : {}),
      walletRows: Array.isArray(walletRows) ? walletRows : [],
    },
    lifecyclePositions,
  });
  const longWallets = toNum(walletSnapshot.longWallets, 0);
  const shortWallets = toNum(walletSnapshot.shortWallets, 0);
  const longShortRatio =
    shortWallets > 0 ? longWallets / shortWallets : longWallets > 0 ? null : 1;
  const orderbookAnalytics = buildOrderbookAnalytics({
    orderbookRow,
    priceNow,
  });
      const walletPositionTable = buildTokenWalletPositionTable({
        symbol,
        walletRows,
        explorerDataset,
        lifecyclePositions,
        currentPrice: priceNow,
      });
  const recentWalletActivitySeries = bucketTradesByTime(
    [
      ...walletSnapshot.recentEntries.map((row) => ({
        timestamp: row.timestamp,
        notionalUsd: row.sizeUsd,
        side: row.side,
      })),
      ...walletSnapshot.recentExits.map((row) => ({
        timestamp: row.timestamp,
        notionalUsd: row.sizeUsd,
        side: row.side,
      })),
    ],
    60 * 60 * 1000
  ).map((row) => ({
    timestamp: row.timestamp,
    value: row.tradeCount,
  }));

  const priceCoverage = coverageState({
    firstTs: priceHistory[0] && priceHistory[0].timestamp,
    lastTs: priceHistory[priceHistory.length - 1] && priceHistory[priceHistory.length - 1].timestamp,
    targetWindowMs: 7 * DAY_MS,
  });
  const tradeCoverage = coverageState({
    firstTs: tradesRows[0] && tradesRows[0].timestamp,
    lastTs: tradesRows[tradesRows.length - 1] && tradesRows[tradesRows.length - 1].timestamp,
    targetWindowMs: DAY_MS,
  });
  const fundingCoverage = coverageState({
    firstTs: fundingSeries[0] && fundingSeries[0].timestamp,
    lastTs: fundingSeries[fundingSeries.length - 1] && fundingSeries[fundingSeries.length - 1].timestamp,
    targetWindowMs: DAY_MS,
  });

  const positioningRegime = classifyPositioningRegime(priceChanges.h24.value, oiChange24h.value);
  const fearGreedProxy = buildFearGreedProxy({
    priceChange24hPct: priceChanges.h24.value,
    oiChange24hPct: oiChange24h.value,
    fundingRatePct: fundingNowPct * 100,
    buySellImbalancePct,
    volatility24hPct: volatility.value,
    positioningRegime,
  });
  const riskFlags = buildRiskFlags({
    volume24hUsd,
    tradeCount24h,
    walletSnapshot,
    fundingRatePct: fundingNowPct * 100,
    basisPct,
    oiChange24hPct: oiChange24h.value,
    priceChange24hPct: priceChanges.h24.value,
    liquidationImbalancePct,
    burstScore,
  });
  const decision = buildTokenDecisionModel({
    trend,
    priceChanges,
    positioningRegime,
    fundingRatePct: fundingNowPct * 100,
    predictedFundingPct: nextFundingPct * 100,
    basisPct,
    oiChange24hPct: oiChange24h.value,
    buyVolume24hUsd,
    sellVolume24hUsd,
    netFlow24hUsd,
    volume24hUsd,
    tradeCount24h,
    burstScore,
    liquidationImbalancePct,
    walletSnapshot,
    riskFlags,
  });

  const freshnessUpdatedAt = [
    generatedAt,
    priceHistory[priceHistory.length - 1] && priceHistory[priceHistory.length - 1].timestamp,
    fundingSeries[fundingSeries.length - 1] && fundingSeries[fundingSeries.length - 1].timestamp,
    tradesRows[tradesRows.length - 1] && tradesRows[tradesRows.length - 1].timestamp,
  ]
    .map((value) => toTimestampMs(value))
    .filter(Boolean)
    .reduce((max, value) => Math.max(max, value), 0);

  const recentActivity = buildRecentActivity({
    tradesRows,
    walletSnapshot,
  });

  const legacyEvents = (Array.isArray(tradesRows) ? tradesRows : [])
    .slice(-80)
    .reverse()
    .map((row) => ({
      timestamp: toTimestampMs(row && row.timestamp),
      side: String((row && row.side) || "").trim().toLowerCase(),
      cause: String((row && row.cause) || "").trim().toLowerCase() || "normal",
      notionalUsd: round(Math.abs(toNum(row && row.notionalUsd, 0)), 2),
    }))
    .filter((row) => row.timestamp);

  return {
    generatedAt,
    symbol,
    timeframe: String(timeframe || "all"),
    header: {
      symbol,
      name: symbol,
      address: null,
      addressStatus: "unavailable",
      addressNote: "Pacifica exposes perp market symbols here, not underlying token contract addresses.",
      listedAt: toTimestampMs(infoRow && infoRow.createdAt),
      marketType: "perpetual",
    },
    hero: {
      currentPrice: buildMetric({
        key: "current_price",
        label: "Current Price",
        value: metricValue(priceNow, 6),
        unit: "price",
        status: Number.isFinite(priceNow) ? "exact" : "unavailable",
        method: "snapshot",
        source: sourceMeta && sourceMeta.price ? sourceMeta.price : "market_price",
        coverageEnd: priceCoverage.coverageEnd,
      }),
      priceChange24h: buildMetric({
        key: "price_change_24h",
        label: "24H Change",
        value: metricValue(priceChanges.h24.value, 4),
        unit: "pct",
        status: priceChanges.h24.status || "unavailable",
        method: Number.isFinite(yesterdayPrice) ? "snapshot_comparison" : "derived",
        source: "mark_price_history",
        coverageStart: priceChanges.h24.coverageStart,
        coverageEnd: priceChanges.h24.coverageEnd,
      }),
      openInterestUsd: buildMetric({
        key: "open_interest",
        label: "Open Interest",
        value: metricValue(oiUsdNow, 2),
        unit: "usd",
        status: Number.isFinite(oiUsdNow) ? "exact" : "unavailable",
        method: "snapshot",
        source: "market_price_snapshot",
      }),
      fundingRatePct: buildMetric({
        key: "funding_rate",
        label: "Funding Rate",
        value: metricValue(fundingNowPct * 100, 6),
        unit: "pct",
        status: Number.isFinite(fundingNowPct) ? "exact" : "unavailable",
        method: "snapshot",
        source: "market_price_snapshot",
      }),
      predictedFundingPct: buildMetric({
        key: "predicted_funding",
        label: "Predicted Funding",
        value: metricValue(nextFundingPct * 100, 6),
        unit: "pct",
        status: Number.isFinite(nextFundingPct) ? "exact" : "unavailable",
        method: "snapshot",
        source: "market_price_snapshot",
      }),
      volume24hUsd: buildMetric({
        key: "volume_24h",
        label: "24H Volume",
        value: metricValue(volume24hUsd, 2),
        unit: "usd",
        status: Number.isFinite(volume24hUsd) ? "exact" : tradeCoverage.status,
        method: Number.isFinite(volume24hUsd) ? "snapshot" : "derived",
        source: Number.isFinite(volume24hUsd) ? "market_price_snapshot" : "public_trades",
        coverageStart: tradeCoverage.coverageStart,
        coverageEnd: tradeCoverage.coverageEnd,
      }),
      liquidations24hUsd: buildMetric({
        key: "liquidations_24h",
        label: "24H Liquidations",
        value: metricValue(totalLiquidations24hUsd, 2),
        unit: "usd",
        status: tradeCoverage.status === "unavailable" ? "unavailable" : tradeCoverage.status,
        method: "derived_from_trade_causes",
        source: "public_trades",
        coverageStart: tradeCoverage.coverageStart,
        coverageEnd: tradeCoverage.coverageEnd,
      }),
      fearGreedProxy: buildMetric({
        key: "fear_greed_proxy",
        label: "Fear & Greed",
        value: metricValue(fearGreedProxy.value, 1),
        unit: "number",
        status: "derived",
        method: "derived_market_proxy",
        source: "price + trades + oi + funding",
        note: `${fearGreedProxy.label} · ${fearGreedProxy.note}`,
      }),
      longShortRatio: buildMetric({
        key: "tracked_long_short_ratio",
        label: "Long / Short Ratio",
        value: metricValue(longShortRatio, 3),
        unit: "ratio",
        status: longWallets > 0 || shortWallets > 0 ? "exact" : "unavailable",
        method: "current_open_positions",
        source: "tracked_wallet_positions",
        note:
          shortWallets > 0
            ? `${longWallets} tracked long wallets versus ${shortWallets} short wallets.`
            : longWallets > 0
            ? `${longWallets} tracked long wallets and no tracked short wallets.`
            : "No tracked directional wallet split available.",
        extra: {
          longWallets,
          shortWallets,
        },
      }),
      trackedWalletsActive: buildMetric({
        key: "tracked_wallets_active",
        label: "Tracked Wallets Active",
        value: metricValue(walletSnapshot.activeWallets, 0),
        unit: "count",
        status: "exact",
        method: "tracked_wallet_universe",
        source: "wallet_explorer_dataset",
        note: "Scope is the tracked wallet universe, not the full exchange.",
      }),
    },
    focus: {
      bias: decision.summary.bias.code,
      positioningRegime: decision.summary.positioning.code,
      crowding: decision.summary.crowding.code,
      action: decision.summary.action.code,
      squeezeRisk: toNum(decision.scores.find((row) => row.key === "squeeze_risk")?.value, 0) >= 60 ? "elevated" : "normal",
    },
    decision,
    sections: {
      market: {
        windows: {
          m5: buildMetric({
            key: "price_change_5m",
            label: "5M Change",
            value: metricValue(priceChanges.m5.value, 4),
            unit: "pct",
            status: priceChanges.m5.status,
            method: "derived",
            source: "mark_price_history",
            coverageStart: priceChanges.m5.coverageStart,
            coverageEnd: priceChanges.m5.coverageEnd,
          }),
          m15: buildMetric({
            key: "price_change_15m",
            label: "15M Change",
            value: metricValue(priceChanges.m15.value, 4),
            unit: "pct",
            status: priceChanges.m15.status,
            method: "derived",
            source: "mark_price_history",
            coverageStart: priceChanges.m15.coverageStart,
            coverageEnd: priceChanges.m15.coverageEnd,
          }),
          h1: buildMetric({
            key: "price_change_1h",
            label: "1H Change",
            value: metricValue(priceChanges.h1.value, 4),
            unit: "pct",
            status: priceChanges.h1.status,
            method: "derived",
            source: "mark_price_history",
            coverageStart: priceChanges.h1.coverageStart,
            coverageEnd: priceChanges.h1.coverageEnd,
          }),
          h4: buildMetric({
            key: "price_change_4h",
            label: "4H Change",
            value: metricValue(priceChanges.h4.value, 4),
            unit: "pct",
            status: priceChanges.h4.status,
            method: "derived",
            source: "mark_price_history",
            coverageStart: priceChanges.h4.coverageStart,
            coverageEnd: priceChanges.h4.coverageEnd,
          }),
          h24: buildMetric({
            key: "price_change_24h",
            label: "24H Change",
            value: metricValue(priceChanges.h24.value, 4),
            unit: "pct",
            status: priceChanges.h24.status,
            method: Number.isFinite(yesterdayPrice) ? "snapshot_comparison" : "derived",
            source: "mark_price_snapshot",
            coverageStart: priceChanges.h24.coverageStart,
            coverageEnd: priceChanges.h24.coverageEnd,
          }),
          d7: buildMetric({
            key: "price_change_7d",
            label: "7D Change",
            value: metricValue(priceChanges.d7.value, 4),
            unit: "pct",
            status: priceChanges.d7.status,
            method: "derived",
            source: "mark_price_history",
            coverageStart: priceChanges.d7.coverageStart,
            coverageEnd: priceChanges.d7.coverageEnd,
          }),
        },
        dailyHigh: buildMetric({
          key: "daily_high",
          label: "Daily High",
          value: metricValue(dailyRange.high, 6),
          unit: "price",
          status: dailyRange.status,
          method: "derived",
          source: "mark_price_history",
        }),
        dailyLow: buildMetric({
          key: "daily_low",
          label: "Daily Low",
          value: metricValue(dailyRange.low, 6),
          unit: "price",
          status: dailyRange.status,
          method: "derived",
          source: "mark_price_history",
        }),
        volatility: buildMetric({
          key: "volatility_24h",
          label: "24H Volatility",
          value: metricValue(volatility.value, 4),
          unit: "pct",
          status: volatility.status,
          method: "derived",
          source: "mark_price_history",
          coverageStart: Date.now() - DAY_MS,
          coverageEnd: Date.now(),
        }),
        trend: {
          structure: trend.structure,
          support: metricValue(trend.support, 6),
          resistance: metricValue(trend.resistance, 6),
          status: trend.status,
          method: "derived_from_mark_structure",
          source: "mark_price_history",
        },
      },
      derivatives: {
        openInterest: buildMetric({
          key: "open_interest_usd",
          label: "Open Interest",
          value: metricValue(oiUsdNow, 2),
          unit: "usd",
          status: Number.isFinite(oiUsdNow) ? "exact" : "unavailable",
          method: "snapshot",
          source: "market_price_snapshot",
        }),
        oiChange1h: buildMetric({
          key: "oi_change_1h",
          label: "OI Change 1H",
          value: metricValue(oiChange1h.value, 4),
          unit: "pct",
          status: oiChange1h.status === "exact" ? "estimated" : oiChange1h.status,
          method: "estimated_from_current_oi_and_mark_curve",
          source: "mark_price_history + current_oi_snapshot",
          coverageStart: oiChange1h.coverageStart,
          coverageEnd: oiChange1h.coverageEnd,
        }),
        oiChange4h: buildMetric({
          key: "oi_change_4h",
          label: "OI Change 4H",
          value: metricValue(oiChange4h.value, 4),
          unit: "pct",
          status: oiChange4h.status === "exact" ? "estimated" : oiChange4h.status,
          method: "estimated_from_current_oi_and_mark_curve",
          source: "mark_price_history + current_oi_snapshot",
          coverageStart: oiChange4h.coverageStart,
          coverageEnd: oiChange4h.coverageEnd,
        }),
        oiChange24h: buildMetric({
          key: "oi_change_24h",
          label: "OI Change 24H",
          value: metricValue(oiChange24h.value, 4),
          unit: "pct",
          status: oiChange24h.status === "exact" ? "estimated" : oiChange24h.status,
          method: "estimated_from_current_oi_and_mark_curve",
          source: "mark_price_history + current_oi_snapshot",
          coverageStart: oiChange24h.coverageStart,
          coverageEnd: oiChange24h.coverageEnd,
        }),
        fundingRate: buildMetric({
          key: "funding_rate_current",
          label: "Funding Rate",
          value: metricValue(fundingNowPct * 100, 6),
          unit: "pct",
          status: Number.isFinite(fundingNowPct) ? "exact" : "unavailable",
          method: "snapshot",
          source: "market_price_snapshot",
        }),
        fundingTrend: buildMetric({
          key: "funding_trend",
          label: "Funding Trend",
          value: metricValue(fundingTrendDelta, 6),
          unit: "pct",
          status: fundingCoverage.status,
          method: "derived_from_funding_history",
          source: sourceMeta && sourceMeta.funding ? sourceMeta.funding : "funding_history",
          coverageStart: fundingCoverage.coverageStart,
          coverageEnd: fundingCoverage.coverageEnd,
          note:
            Number.isFinite(fundingTrendAvg) && fundingTrendAvg !== null
              ? `Average recent funding ${round(fundingTrendAvg, 6)}%`
              : null,
        }),
        predictedFunding: buildMetric({
          key: "predicted_funding",
          label: "Predicted Funding",
          value: metricValue(nextFundingPct * 100, 6),
          unit: "pct",
          status: Number.isFinite(nextFundingPct) ? "exact" : "unavailable",
          method: "snapshot",
          source: "market_price_snapshot",
        }),
        liquidations24h: buildMetric({
          key: "liquidations_24h",
          label: "Liquidations 24H",
          value: metricValue(totalLiquidations24hUsd, 2),
          unit: "usd",
          status: tradeCoverage.status,
          method: "derived_from_trade_causes",
          source: sourceMeta && sourceMeta.trades ? sourceMeta.trades : "public_trades",
          coverageStart: tradeCoverage.coverageStart,
          coverageEnd: tradeCoverage.coverageEnd,
        }),
        longLiquidations24h: buildMetric({
          key: "long_liquidations_24h",
          label: "Long Liquidations 24H",
          value: metricValue(liquidations24h.longUsd, 2),
          unit: "usd",
          status: tradeCoverage.status,
          method: "derived_from_trade_causes",
          source: "public_trades",
        }),
        shortLiquidations24h: buildMetric({
          key: "short_liquidations_24h",
          label: "Short Liquidations 24H",
          value: metricValue(liquidations24h.shortUsd, 2),
          unit: "usd",
          status: tradeCoverage.status,
          method: "derived_from_trade_causes",
          source: "public_trades",
        }),
        liquidationImbalance: buildMetric({
          key: "liquidation_imbalance",
          label: "Liquidation Imbalance",
          value: metricValue(liquidationImbalancePct, 4),
          unit: "pct",
          status: tradeCoverage.status,
          method: "derived_from_trade_causes",
          source: "public_trades",
        }),
        basis: buildMetric({
          key: "basis_pct",
          label: "Mark vs Oracle Basis",
          value: metricValue(basisPct, 6),
          unit: "pct",
          status: Number.isFinite(basisPct) ? "exact" : "unavailable",
          method: "snapshot_comparison",
          source: "mark + oracle snapshot",
        }),
        liquidationHeatmap: buildMetric({
          key: "liquidation_heatmap",
          label: "Liquidation Heatmap",
          value: null,
          unit: "state",
          status: "unavailable",
          method: "not_available_in_current_dataset",
          source: null,
          note: "A proper liquidation heatmap needs depth/liquidation ladder data that the current Pacifica inputs do not expose.",
        }),
        positioningRegime,
      },
      flow: {
        volume5m: buildMetric({
          key: "volume_5m",
          label: "Volume 5M",
          value: metricValue(flowWindows.m5.volumeUsd, 2),
          unit: "usd",
          status: tradeCoverage.status,
          method: "derived_from_public_trades",
          source: "public_trades",
        }),
        volume15m: buildMetric({
          key: "volume_15m",
          label: "Volume 15M",
          value: metricValue(flowWindows.m15.volumeUsd, 2),
          unit: "usd",
          status: tradeCoverage.status,
          method: "derived_from_public_trades",
          source: "public_trades",
        }),
        volume1h: buildMetric({
          key: "volume_1h",
          label: "Volume 1H",
          value: metricValue(flowWindows.h1.volumeUsd, 2),
          unit: "usd",
          status: tradeCoverage.status,
          method: "derived_from_public_trades",
          source: "public_trades",
        }),
        volume4h: buildMetric({
          key: "volume_4h",
          label: "Volume 4H",
          value: metricValue(flowWindows.h4.volumeUsd, 2),
          unit: "usd",
          status: tradeCoverage.status,
          method: "derived_from_public_trades",
          source: "public_trades",
        }),
        buyVolume24h: buildMetric({
          key: "buy_volume_24h",
          label: "Buy Volume 24H",
          value: metricValue(buyVolume24hUsd, 2),
          unit: "usd",
          status: tradeCoverage.status,
          method: "derived_from_public_trades",
          source: "public_trades",
          coverageStart: tradeCoverage.coverageStart,
          coverageEnd: tradeCoverage.coverageEnd,
        }),
        sellVolume24h: buildMetric({
          key: "sell_volume_24h",
          label: "Sell Volume 24H",
          value: metricValue(sellVolume24hUsd, 2),
          unit: "usd",
          status: tradeCoverage.status,
          method: "derived_from_public_trades",
          source: "public_trades",
        }),
        netFlow24h: buildMetric({
          key: "net_flow_24h",
          label: "Net Flow 24H",
          value: metricValue(netFlow24hUsd, 2),
          unit: "usd",
          status: tradeCoverage.status,
          method: "derived_from_public_trades",
          source: "public_trades",
        }),
        tradeCount24h: buildMetric({
          key: "trade_count_24h",
          label: "Trade Count 24H",
          value: metricValue(tradeCount24h, 0),
          unit: "count",
          status: tradeCoverage.status,
          method: "derived_from_public_trades",
          source: "public_trades",
        }),
        averageTradeSize24h: buildMetric({
          key: "avg_trade_size_24h",
          label: "Average Trade Size 24H",
          value: metricValue(avgTradeSize24hUsd, 2),
          unit: "usd",
          status: tradeCoverage.status,
          method: "derived_from_public_trades",
          source: "public_trades",
        }),
        largeTradeCount24h: buildMetric({
          key: "large_trade_count_24h",
          label: "Large Trade Count 24H",
          value: metricValue(largeTradeCount24h, 0),
          unit: "count",
          status: tradeCoverage.status,
          method: "derived_from_public_trades",
          source: "public_trades",
          note: `Large trade threshold ${round(largeTradeThreshold, 2) || 0} USD`,
        }),
        burstDetection: buildMetric({
          key: "burst_score",
          label: "Burst Score",
          value: metricValue(burstScore, 4),
          unit: "ratio",
          status: tradeCoverage.status,
          method: "derived_from_hourly_trade_buckets",
          source: "public_trades",
          note: "Latest hourly volume divided by average hourly volume in the retained trade window.",
        }),
        takerBuyVolume24h: buildMetric({
          key: "taker_buy_volume_24h",
          label: "Taker Buy Volume",
          value: metricValue(buyVolume24hUsd, 2),
          unit: "usd",
          status: tradeCoverage.status,
          method: "aggressor_side_from_public_trades",
          source: "public_trades",
        }),
        takerSellVolume24h: buildMetric({
          key: "taker_sell_volume_24h",
          label: "Taker Sell Volume",
          value: metricValue(sellVolume24hUsd, 2),
          unit: "usd",
          status: tradeCoverage.status,
          method: "aggressor_side_from_public_trades",
          source: "public_trades",
        }),
        makerActivityMirror24h: buildMetric({
          key: "maker_activity_mirror_24h",
          label: "Maker Activity",
          value: metricValue(buyVolume24hUsd + sellVolume24hUsd, 2),
          unit: "usd",
          status: tradeCoverage.status === "unavailable" ? "unavailable" : "estimated",
          method: "mirrored_passive_counterparty_notional",
          source: "public_trades",
          note:
            "Public trades expose the aggressor side. Passive maker notional is mirrored from the matched counterparty, not attributed to specific maker accounts.",
        }),
        makerTakerRatio: buildMetric({
          key: "maker_taker_ratio",
          label: "Maker / Taker Ratio",
          value: metricValue(1, 2),
          unit: "ratio",
          status: tradeCoverage.status === "unavailable" ? "unavailable" : "estimated",
          method: "mirrored_counterparty_estimate",
          source: "public_trades",
          note:
            "Every matched public trade has one taker and one maker. Ratio remains a parity estimate until maker-side attribution exists in the feed.",
        }),
      },
      depth: {
        visibleBidDepthUsd: buildMetric({
          key: "visible_bid_depth",
          label: "Visible Bid Depth",
          value: metricValue(orderbookAnalytics.visibleBidDepthUsd, 2),
          unit: "usd",
          status: orderbookAnalytics.available ? "exact" : "unavailable",
          method: "snapshot_orderbook_sum",
          source: "orderbook_snapshot",
        }),
        visibleAskDepthUsd: buildMetric({
          key: "visible_ask_depth",
          label: "Visible Ask Depth",
          value: metricValue(orderbookAnalytics.visibleAskDepthUsd, 2),
          unit: "usd",
          status: orderbookAnalytics.available ? "exact" : "unavailable",
          method: "snapshot_orderbook_sum",
          source: "orderbook_snapshot",
        }),
        nearBidDepthUsd: buildMetric({
          key: "near_bid_depth",
          label: "Bid Depth ±1%",
          value: metricValue(orderbookAnalytics.nearBidDepthUsd, 2),
          unit: "usd",
          status: orderbookAnalytics.available ? "exact" : "unavailable",
          method: "snapshot_orderbook_band_sum",
          source: "orderbook_snapshot",
        }),
        nearAskDepthUsd: buildMetric({
          key: "near_ask_depth",
          label: "Ask Depth ±1%",
          value: metricValue(orderbookAnalytics.nearAskDepthUsd, 2),
          unit: "usd",
          status: orderbookAnalytics.available ? "exact" : "unavailable",
          method: "snapshot_orderbook_band_sum",
          source: "orderbook_snapshot",
        }),
        imbalancePct: buildMetric({
          key: "depth_imbalance_pct",
          label: "Depth Imbalance",
          value: metricValue(orderbookAnalytics.imbalancePct, 2),
          unit: "pct",
          status: orderbookAnalytics.available ? "exact" : "unavailable",
          method: "bid_vs_ask_depth_ratio",
          source: "orderbook_snapshot",
          note: "Computed from visible depth within 1% of the current price.",
        }),
        maxLiquidity: buildMetric({
          key: "max_liquidity_zone",
          label: "Maximum Liquidity",
          value: metricValue(orderbookAnalytics.maxLiquidityZone && orderbookAnalytics.maxLiquidityZone.notionalUsd, 2),
          unit: "usd",
          status: orderbookAnalytics.maxLiquidityZone ? "exact" : "unavailable",
          method: "clustered_book_zone",
          source: "orderbook_snapshot",
          note:
            orderbookAnalytics.maxLiquidityZone
              ? `${titleCase(orderbookAnalytics.maxLiquidityZone.side)} around ${round(orderbookAnalytics.maxLiquidityZone.price, 4)}`
              : null,
        }),
        minLiquidity: buildMetric({
          key: "min_liquidity_zone",
          label: "Minimum Liquidity",
          value: metricValue(orderbookAnalytics.minLiquidityZone && orderbookAnalytics.minLiquidityZone.notionalUsd, 2),
          unit: "usd",
          status: orderbookAnalytics.minLiquidityZone ? "exact" : "unavailable",
          method: "clustered_book_zone",
          source: "orderbook_snapshot",
          note:
            orderbookAnalytics.minLiquidityZone
              ? `${titleCase(orderbookAnalytics.minLiquidityZone.side)} around ${round(orderbookAnalytics.minLiquidityZone.price, 4)}`
              : null,
        }),
        largestOrder: buildMetric({
          key: "largest_order_volume",
          label: "Largest Order Volume",
          value: metricValue(orderbookAnalytics.largestOrder && orderbookAnalytics.largestOrder.notionalUsd, 2),
          unit: "usd",
          status: orderbookAnalytics.largestOrder ? "exact" : "unavailable",
          method: "largest_visible_book_level",
          source: "orderbook_snapshot",
          note:
            orderbookAnalytics.largestOrder
              ? `${titleCase(orderbookAnalytics.largestOrder.side)} @ ${round(orderbookAnalytics.largestOrder.price, 4)}`
              : null,
        }),
        smallestOrder: buildMetric({
          key: "smallest_order_volume",
          label: "Smallest Order Volume",
          value: metricValue(orderbookAnalytics.smallestOrder && orderbookAnalytics.smallestOrder.notionalUsd, 2),
          unit: "usd",
          status: orderbookAnalytics.smallestOrder ? "exact" : "unavailable",
          method: "smallest_meaningful_visible_book_level",
          source: "orderbook_snapshot",
          note:
            orderbookAnalytics.smallestOrder
              ? `${titleCase(orderbookAnalytics.smallestOrder.side)} @ ${round(orderbookAnalytics.smallestOrder.price, 4)}`
              : null,
        }),
        topZones: orderbookAnalytics.zones.slice(0, 8).map((row) => ({
          side: row.side,
          startPct: round(row.startPct, 2),
          endPct: round(row.endPct, 2),
          centerPrice: round(row.centerPrice, 6),
          notionalUsd: round(row.notionalUsd, 2),
          levelCount: row.levelCount,
        })),
      },
      wallets: {
        activeTrackedWallets: buildMetric({
          key: "tracked_wallets_active",
          label: "Tracked Wallets Active",
          value: metricValue(walletSnapshot.activeWallets, 0),
          unit: "count",
          status: "exact",
          method: "tracked_wallet_universe",
          source: "wallet_explorer_dataset",
          note: "Scope is the tracked wallet universe only.",
        }),
        longWallets: buildMetric({
          key: "tracked_long_wallets",
          label: "Tracked Long Wallets",
          value: metricValue(longWallets, 0),
          unit: "count",
          status: longWallets > 0 || shortWallets > 0 ? "exact" : "unavailable",
          method: "current_open_positions",
          source: "tracked_wallet_positions",
        }),
        shortWallets: buildMetric({
          key: "tracked_short_wallets",
          label: "Tracked Short Wallets",
          value: metricValue(shortWallets, 0),
          unit: "count",
          status: longWallets > 0 || shortWallets > 0 ? "exact" : "unavailable",
          method: "current_open_positions",
          source: "tracked_wallet_positions",
        }),
        longShortRatio: buildMetric({
          key: "tracked_long_short_ratio",
          label: "Long / Short Ratio",
          value: metricValue(longShortRatio, 3),
          unit: "ratio",
          status: longWallets > 0 || shortWallets > 0 ? "exact" : "unavailable",
          method: "current_open_positions",
          source: "tracked_wallet_positions",
          note:
            shortWallets > 0
              ? `${longWallets} tracked long wallets versus ${shortWallets} short wallets.`
              : longWallets > 0
              ? `${longWallets} tracked long wallets and no tracked short wallets.`
              : "No tracked directional wallet split available.",
          extra: {
            longWallets,
            shortWallets,
          },
        }),
        netLongVsShort: buildMetric({
          key: "tracked_wallet_long_short_bias",
          label: "Tracked Long vs Short Bias",
          value: metricValue(walletSnapshot.netWalletBias, 0),
          unit: "count",
          status: "exact",
          method: "current_open_positions",
          source: "tracked_wallet_positions",
          note: "Positive means more tracked wallets are long than short.",
        }),
        holdingWallets: buildMetric({
          key: "holding_wallets",
          label: "Wallets Still Holding",
          value: metricValue(walletSnapshot.holdingWalletCount, 0),
          unit: "count",
          status: "exact",
          method: "current_open_positions",
          source: "tracked_wallet_positions",
        }),
        exitedWallets: buildMetric({
          key: "exited_wallets",
          label: "Recently Exited Wallets",
          value: metricValue(walletSnapshot.exitedWallets, 0),
          unit: "count",
          status: "partial",
          method: "observed_position_lifecycle",
          source: "position_lifecycle_store",
          note: "Observed from tracking start, not full historical lifetime.",
        }),
        trackedExposureUsd: buildMetric({
          key: "tracked_exposure",
          label: "Tracked Exposure",
          value: metricValue(walletSnapshot.totalTrackedExposureUsd, 2),
          unit: "usd",
          status: "exact",
          method: "current_open_positions",
          source: "tracked_wallet_positions",
        }),
        concentrationTop1Pct: buildMetric({
          key: "tracked_concentration_top1",
          label: "Top Holder Share",
          value: metricValue(walletSnapshot.top1SharePct, 4),
          unit: "pct",
          status: "exact",
          method: "current_open_positions",
          source: "tracked_wallet_positions",
        }),
        concentrationTop5Pct: buildMetric({
          key: "tracked_concentration_top5",
          label: "Top 5 Holder Share",
          value: metricValue(walletSnapshot.top5SharePct, 4),
          unit: "pct",
          status: "exact",
          method: "current_open_positions",
          source: "tracked_wallet_positions",
        }),
        averageLongEntryPrice: buildMetric({
          key: "average_long_entry_price",
          label: "Average Buy Price",
          value: metricValue(walletSnapshot.averageLongEntryPrice, 6),
          unit: "price",
          status: Number.isFinite(toNum(walletSnapshot.averageLongEntryPrice, NaN)) ? "exact" : "unavailable",
          method: "position_usd_weighted_open_long_entry",
          source: "tracked_wallet_positions",
        }),
        averageShortEntryPrice: buildMetric({
          key: "average_short_entry_price",
          label: "Average Short Price",
          value: metricValue(walletSnapshot.averageShortEntryPrice, 6),
          unit: "price",
          status: Number.isFinite(toNum(walletSnapshot.averageShortEntryPrice, NaN)) ? "exact" : "unavailable",
          method: "position_usd_weighted_open_short_entry",
          source: "tracked_wallet_positions",
        }),
        totalBuyExposureUsd: buildMetric({
          key: "total_buy_exposure",
          label: "Total Buy",
          value: metricValue(walletSnapshot.longExposureUsd, 2),
          unit: "usd",
          status: "exact",
          method: "current_open_long_positions",
          source: "tracked_wallet_positions",
        }),
        totalShortExposureUsd: buildMetric({
          key: "total_short_exposure",
          label: "Total Short",
          value: metricValue(walletSnapshot.shortExposureUsd, 2),
          unit: "usd",
          status: "exact",
          method: "current_open_short_positions",
          source: "tracked_wallet_positions",
        }),
        recentEntries: walletSnapshot.recentEntries,
        recentExits: walletSnapshot.recentExits,
        topTrackedHolders: walletSnapshot.topHolders,
        topProfitableWallets: {
          status: walletSnapshot.topProfitableWallets.length ? "partial" : "unavailable",
          method: "current_open_position_pnl_only",
          source: "tracked_wallet_positions",
          note: "This ranks open-position unrealized PnL, not full token-lifetime realized PnL.",
          rows: walletSnapshot.topProfitableWallets,
        },
      },
      sentiment: {
        fearGreed: buildMetric({
          key: "fear_greed_proxy",
          label: "Fear & Greed",
          value: metricValue(fearGreedProxy.value, 1),
          unit: "number",
          status: "derived",
          method: "derived_market_proxy",
          source: "price + trades + oi + funding",
          note: `${fearGreedProxy.label} · ${fearGreedProxy.note}`,
        }),
        fundingRate: buildMetric({
          key: "funding_rate_sentiment",
          label: "Funding Rate",
          value: metricValue(fundingNowPct * 100, 6),
          unit: "pct",
          status: Number.isFinite(fundingNowPct) ? "exact" : "unavailable",
          method: "snapshot",
          source: "market_price_snapshot",
        }),
        fundingTrend: buildMetric({
          key: "funding_trend_sentiment",
          label: "Funding Trend",
          value: metricValue(fundingTrendDelta, 6),
          unit: "pct",
          status: fundingCoverage.status,
          method: "derived_from_funding_history",
          source: sourceMeta && sourceMeta.funding ? sourceMeta.funding : "funding_history",
        }),
        longShortRatio: buildMetric({
          key: "long_short_ratio_sentiment",
          label: "Long / Short Ratio",
          value: metricValue(longShortRatio, 3),
          unit: "ratio",
          status: longWallets > 0 || shortWallets > 0 ? "exact" : "unavailable",
          method: "current_open_positions",
          source: "tracked_wallet_positions",
        }),
      },
      smartMoney: {
        smartSentiment: buildMetric({
          key: "smart_sentiment",
          label: "Smart Sentiment",
          value: metricValue(walletSnapshot.smartMoney.sharePct, 2),
          unit: "pct",
          status: walletSnapshot.smartMoney.walletCount > 0 ? "partial" : "unavailable",
          method: "scored_tracked_wallet_model",
          source: "tracked_wallet_positions + wallet_performance",
          note:
            "Share of tracked token exposure held by wallets scoring high on win rate, pnl quality, size, and volume.",
        }),
        smartMoneyUsd: buildMetric({
          key: "smart_money_capital",
          label: "Smart Money Capital",
          value: metricValue(walletSnapshot.smartMoney.exposureUsd, 2),
          unit: "usd",
          status: walletSnapshot.smartMoney.walletCount > 0 ? "partial" : "unavailable",
          method: "scored_tracked_wallet_model",
          source: "tracked_wallet_positions",
          note:
            "Current tracked exposure attached to high-scoring wallets on this market.",
        }),
        smartBias: buildMetric({
          key: "smart_money_bias",
          label: "Smart Money Bias",
          value: metricValue(walletSnapshot.smartMoney.biasPct, 2),
          unit: "pct",
          status: walletSnapshot.smartMoney.walletCount > 0 ? "partial" : "unavailable",
          method: "net_long_vs_short_smart_exposure",
          source: "tracked_wallet_positions",
          note: "Positive means smart-scored wallets lean net long; negative means net short.",
        }),
        smartWallets: buildMetric({
          key: "smart_wallet_count",
          label: "Smart Wallets",
          value: metricValue(walletSnapshot.smartMoney.walletCount, 0),
          unit: "count",
          status: walletSnapshot.smartMoney.walletCount > 0 ? "partial" : "unavailable",
          method: "scored_tracked_wallet_model",
          source: "wallet_explorer_dataset",
        }),
        smartAverageLongEntryPrice: buildMetric({
          key: "smart_average_long_entry_price",
          label: "Smart Avg Long Entry",
          value: metricValue(walletSnapshot.smartMoney.averageLongEntryPrice, 6),
          unit: "price",
          status: Number.isFinite(toNum(walletSnapshot.smartMoney.averageLongEntryPrice, NaN)) ? "exact" : "unavailable",
          method: "position_usd_weighted_open_long_entry",
          source: "smart_wallet_current_positions",
          note: "Weighted by current long exposure across the smart-wallet cohort.",
        }),
        smartAverageShortEntryPrice: buildMetric({
          key: "smart_average_short_entry_price",
          label: "Smart Avg Short Entry",
          value: metricValue(walletSnapshot.smartMoney.averageShortEntryPrice, 6),
          unit: "price",
          status: Number.isFinite(toNum(walletSnapshot.smartMoney.averageShortEntryPrice, NaN)) ? "exact" : "unavailable",
          method: "position_usd_weighted_open_short_entry",
          source: "smart_wallet_current_positions",
          note: "Weighted by current short exposure across the smart-wallet cohort.",
        }),
        smartPositionsOpenedToday: buildMetric({
          key: "smart_positions_opened_today",
          label: "Smart Positions Opened Today",
          value: metricValue(walletSnapshot.smartMoney.positionsOpenedToday, 0),
          unit: "count",
          status: walletSnapshot.smartMoney.walletsOpenedToday > 0 ? "partial" : "unavailable",
          method: "current_opened_at_utc_day",
          source: "smart_wallet_lifecycle_positions",
          note: `${
            walletSnapshot.smartMoney.symbolsOpenedToday
              ? `${walletSnapshot.smartMoney.symbolsOpenedToday} symbols opened today.`
              : "No smart-wallet positions opened today yet."
          } ${
            Array.isArray(walletSnapshot.smartMoney.openedSymbolsToday) &&
            walletSnapshot.smartMoney.openedSymbolsToday.length
              ? `Top symbols: ${walletSnapshot.smartMoney.openedSymbolsToday
                  .slice(0, 5)
                  .map((row) => `${row.symbol} (${row.count})`)
                  .join(", ")}.`
              : ""
          }`,
        }),
        smartWalletsOpenedToday: buildMetric({
          key: "smart_wallets_opened_today",
          label: "Smart Wallets Opened Today",
          value: metricValue(walletSnapshot.smartMoney.walletsOpenedToday, 0),
          unit: "count",
          status: walletSnapshot.smartMoney.walletsOpenedToday > 0 ? "partial" : "unavailable",
          method: "current_opened_at_utc_day",
          source: "smart_wallet_lifecycle_positions",
          note: "Unique smart wallets that opened positions today.",
        }),
        smartSymbolsOpenedToday: buildMetric({
          key: "smart_symbols_opened_today",
          label: "Smart Coins Opened Today",
          value: metricValue(walletSnapshot.smartMoney.symbolsOpenedToday, 0),
          unit: "count",
          status: walletSnapshot.smartMoney.symbolsOpenedToday > 0 ? "partial" : "unavailable",
          method: "current_opened_at_utc_day",
          source: "smart_wallet_lifecycle_positions",
          note:
            Array.isArray(walletSnapshot.smartMoney.openedSymbolsToday) &&
            walletSnapshot.smartMoney.openedSymbolsToday.length
              ? walletSnapshot.smartMoney.openedSymbolsToday
                  .slice(0, 5)
                  .map((row) => `${row.symbol} (${row.count})`)
                  .join(", ")
              : "No coin list available yet.",
        }),
        topWallets: walletSnapshot.smartMoney.topWallets,
      },
      wallets: {
        positions: buildMetric({
          key: `${normalizedSymbol}_wallet_positions`,
          label: `${normalizedSymbol} Wallet Positions`,
          value: metricValue(walletPositionTable.positionCount, 0),
          unit: "count",
          status: walletPositionTable.walletCount > 0 ? "exact" : "unavailable",
          method: "live_current_open_positions",
          source: "live_positions_baseline_current_open_set + wallet_summary",
          note: `${walletPositionTable.walletCount} wallets with ${walletPositionTable.positionCount} open ${normalizedSymbol} positions.`,
        }),
        totalPnL: buildMetric({
          key: `${normalizedSymbol}_wallet_total_pnl`,
          label: "Total PnL",
          value: metricValue(walletPositionTable.totalWalletPnlUsd, 2),
          unit: "usd",
          status: walletPositionTable.walletCount > 0 ? "exact" : "unavailable",
          method: "wallet_total_pnl_merged",
          source: "wallet_records",
        }),
        avgOpenPositions30d: buildMetric({
          key: `${normalizedSymbol}_wallet_avg_open_positions_30d`,
          label: "Avg. Open Positions / 30d",
          value: metricValue(walletPositionTable.avgOpenPositions30d, 3),
          unit: "number",
          status: walletPositionTable.walletCount > 0 ? "derived" : "unavailable",
          method: "opened_positions_last_30d_divided_by_30",
          source: "lifecycle_positions",
        }),
        totalVolumeOnCoin: buildMetric({
          key: `${normalizedSymbol}_wallet_total_coin_volume`,
          label: "Total Volume",
          value: metricValue(walletPositionTable.totalCoinVolumeUsd, 2),
          unit: "usd",
          status: walletPositionTable.walletCount > 0 ? "exact" : "unavailable",
          method: "wallet_symbol_volume_merged",
          source: "wallet_records + symbol_breakdown",
        }),
        positionTable: {
          status: walletPositionTable.walletCount > 0 ? "exact" : "unavailable",
          method: "live_current_open_positions + wallet_summary",
          source: "live_positions_baseline_current_open_set + wallet_records",
          symbol: normalizedSymbol,
          walletCount: walletPositionTable.walletCount,
          positionCount: walletPositionTable.positionCount,
          page: walletPositionTable.page,
          pageSize: walletPositionTable.pageSize,
          totalRows: walletPositionTable.totalRows,
          totalPages: walletPositionTable.totalPages,
          sortKey: walletPositionTable.sortKey,
          sortDir: walletPositionTable.sortDir,
          totalPositionUsd: walletPositionTable.totalPositionUsd,
          totalWalletPnlUsd: walletPositionTable.totalWalletPnlUsd,
          totalCoinVolumeUsd: walletPositionTable.totalCoinVolumeUsd,
          avgOpenPositions30d: walletPositionTable.avgOpenPositions30d,
          stats: walletPositionTable.stats,
          rows: walletPositionTable.rows,
        },
      },
      risk: {
        flags: riskFlags,
      },
      methodology: {
        items: [
          {
            key: "volume",
            label: "Volume windows",
            formula: "Sum absolute public-trade notional inside rolling 5m, 15m, 1h, 4h, and 24h windows.",
          },
          {
            key: "market_depth",
            label: "Market depth",
            formula: "Sum visible bid/ask order-book notional overall and within 1% of the mark. Group levels into 0.25% zones to find liquidity walls.",
          },
          {
            key: "taker_maker",
            label: "Taker / maker",
            formula: "Aggressor side comes from the public trade feed. Passive maker activity is mirrored from the matched counterparty because the public tape does not attribute unique maker wallets.",
          },
          {
            key: "avg_entries",
            label: "Average buy / short price",
            formula: "USD-weighted average entry across currently open tracked long and short positions for the selected market.",
          },
          {
            key: "fear_greed",
            label: "Fear & greed proxy",
            formula: "Composite score from 24h price change, open-interest change, buy/sell imbalance, funding crowding, and realized volatility.",
          },
          {
            key: "smart_money",
            label: "Smart money",
            formula:
              "Tracked wallets are scored on win rate, pnl quality, traded volume, current position size, recency, and repeat activity. Smart sentiment is the share of tracked exposure held by high-scoring wallets; today metrics count positions whose currentOpenedAt falls inside the current UTC day.",
          },
        ],
      },
    },
    charts: {
      price: buildChart(priceHistory.map((row) => ({ timestamp: row.timestamp, value: row.value })), "line", priceCoverage.status, "Price", "Uses retained mark/candle history."),
      volume: buildChart(tradeBuckets5m.map((row) => ({ timestamp: row.timestamp, value: row.volumeUsd })), "bars", tradeCoverage.status, "Volume", "Derived from retained public trades."),
      oi: buildChart(oiSeries, "line", oiChange24h.status === "exact" ? "estimated" : oiChange24h.status, "Open Interest", "Estimated from current OI snapshot scaled by mark history."),
      funding: buildChart(fundingSeries, "line_zero", fundingCoverage.status, "Funding", "Funding history where available; falls back to current snapshot."),
      delta: buildChart(tradeBuckets5m.map((row) => ({ timestamp: row.timestamp, value: row.netUsd })), "diverging", tradeCoverage.status, "Buy/Sell Delta", "Derived from public trade sides."),
      liquidations: buildChart(tradeBuckets5m.map((row) => ({
        timestamp: row.timestamp,
        value: row.longLiquidationsUsd + row.shortLiquidationsUsd,
        longValue: row.longLiquidationsUsd,
        shortValue: row.shortLiquidationsUsd,
      })), "stacked_split", tradeCoverage.status, "Liquidations", "Derived from public trade causes."),
      walletActivity: buildChart(recentWalletActivitySeries, "bars", recentWalletActivitySeries.length ? "partial" : "unavailable", "Tracked Wallet Activity", "Observed latest tracked-wallet position changes only."),
      depth: buildChart(
        orderbookAnalytics.zones.slice(0, 12).map((row, index) => ({
          timestamp: generatedAt + index,
          value: row.notionalUsd,
        })),
        "bars",
        orderbookAnalytics.available ? "exact" : "unavailable",
        "Depth Zones",
        "Visible order-book zones clustered in 0.25% bands from the mark."
      ),
    },
    recentActivity,
    meta: {
      source: sourceMeta || {},
      rows: {
        trades: Array.isArray(tradesRows) ? tradesRows.length : 0,
        funding: Array.isArray(fundingRows) ? fundingRows.length : 0,
        price: Array.isArray(priceHistory) ? priceHistory.length : 0,
        wallets: walletSnapshot.activeWallets,
      },
      semantics: {
        statusValues: ["exact", "estimated", "partial", "unavailable"],
        methodNote:
          "Status describes completeness/confidence. Method describes how the metric was produced.",
      },
    },
    freshness: {
      updatedAt: freshnessUpdatedAt || generatedAt,
      priceTimestamp: priceHistory[priceHistory.length - 1] ? priceHistory[priceHistory.length - 1].timestamp : null,
      tradeTimestamp: tradesRows[tradesRows.length - 1] ? toTimestampMs(tradesRows[tradesRows.length - 1].timestamp) : null,
      fundingTimestamp: fundingSeries[fundingSeries.length - 1] ? fundingSeries[fundingSeries.length - 1].timestamp : null,
    },
    kpis: {
      currentOiUsd: metricValue(oiUsdNow, 2),
      volume24hUsd: metricValue(volume24hUsd, 2),
      fundingPctCurrent: metricValue(fundingNowPct * 100, 6),
      netflow24hUsd: metricValue(netFlow24hUsd, 2),
      liquidations24hUsd: metricValue(totalLiquidations24hUsd, 2),
      activity24h: metricValue(walletSnapshot.activeWallets, 0),
      fearGreedProxy: metricValue(fearGreedProxy.value, 1),
      smartMoneySharePct: metricValue(walletSnapshot.smartMoney.sharePct, 2),
      lastTradeAt: tradesRows[tradesRows.length - 1] ? toTimestampMs(tradesRows[tradesRows.length - 1].timestamp) : null,
      lastFundingAt: fundingSeries[fundingSeries.length - 1] ? fundingSeries[fundingSeries.length - 1].timestamp : null,
    },
    series: {
      price: priceHistory.map((row) => ({ timestamp: row.timestamp, value: row.value })),
      volume: tradeBuckets5m.map((row) => ({ timestamp: row.timestamp, value: row.volumeUsd })),
      oi: oiSeries,
      funding: fundingSeries,
      liquidations: tradeBuckets5m.map((row) => ({
        timestamp: row.timestamp,
        value: row.longLiquidationsUsd + row.shortLiquidationsUsd,
        longValue: row.longLiquidationsUsd,
        shortValue: row.shortLiquidationsUsd,
      })),
      netflow: tradeBuckets5m.map((row) => ({ timestamp: row.timestamp, value: row.netUsd })),
      wallet_activity: recentWalletActivitySeries,
      depth: orderbookAnalytics.zones.slice(0, 12).map((row, index) => ({
        timestamp: generatedAt + index,
        value: row.notionalUsd,
      })),
    },
    events: legacyEvents,
    tradeRows: Array.isArray(tradesRows) ? tradesRows : [],
  };
}

module.exports = {
  buildTokenAnalysisPayload,
};
