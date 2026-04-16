function normalizePaginationSummary(summary = null) {
  if (!summary || typeof summary !== "object") return null;
  return {
    exhausted: Boolean(summary.exhausted),
    frontierCursor:
      summary.frontierCursor !== undefined && summary.frontierCursor !== null
        ? summary.frontierCursor
        : null,
    highestFetchedPage: Number(summary.highestFetchedPage || 0),
    highestKnownPage: Number(summary.highestKnownPage || 0),
    totalKnownPages:
      summary.totalKnownPages !== undefined && summary.totalKnownPages !== null
        ? Number(summary.totalKnownPages || 0)
        : null,
    fetchedPages: Number(summary.fetchedPages || 0),
    verifiedPages: Number(
      summary.verifiedPages !== undefined ? summary.verifiedPages : summary.fetchedPages || 0
    ),
    retryPages: Number(summary.retryPages || 0),
    pendingPages: Number(summary.pendingPages || 0),
    lastSuccessfulPage:
      summary.lastSuccessfulPage !== undefined && summary.lastSuccessfulPage !== null
        ? Number(summary.lastSuccessfulPage || 0)
        : null,
    lastSuccessfulCursor:
      summary.lastSuccessfulCursor !== undefined && summary.lastSuccessfulCursor !== null
        ? summary.lastSuccessfulCursor
        : null,
    lastSuccessfulAt:
      summary.lastSuccessfulAt !== undefined && summary.lastSuccessfulAt !== null
        ? Number(summary.lastSuccessfulAt || 0)
        : null,
    lastAttemptedPage:
      summary.lastAttemptedPage !== undefined && summary.lastAttemptedPage !== null
        ? Number(summary.lastAttemptedPage || 0)
        : null,
    lastAttemptedCursor:
      summary.lastAttemptedCursor !== undefined && summary.lastAttemptedCursor !== null
        ? summary.lastAttemptedCursor
        : null,
    lastAttemptedAt:
      summary.lastAttemptedAt !== undefined && summary.lastAttemptedAt !== null
        ? Number(summary.lastAttemptedAt || 0)
        : null,
    missingPageRanges: Array.isArray(summary.missingPageRanges)
      ? summary.missingPageRanges
      : [],
  };
}

function isPaginationComplete(summary = null) {
  const safe = normalizePaginationSummary(summary);
  if (!safe) return false;
  return Boolean(safe.exhausted) && Number(safe.retryPages || 0) <= 0 && !safe.frontierCursor;
}

function paginationTruthScore(summary = null) {
  const safe = normalizePaginationSummary(summary);
  if (!safe) return -1;
  return (
    (isPaginationComplete(safe) ? 1_000_000 : 0) +
    (Boolean(safe.exhausted) ? 100_000 : 0) +
    Number(safe.verifiedPages || 0) * 1_000 +
    Number(safe.fetchedPages || 0) * 100 +
    Number(safe.highestFetchedPage || 0) * 10 -
    Number(safe.retryPages || 0) * 500 -
    Number(safe.pendingPages || 0) * 5 +
    Number(safe.lastSuccessfulAt || 0) / 1_000_000_000_000
  );
}

function chooseStrongerPaginationSummary(current = null, incoming = null) {
  const currentScore = paginationTruthScore(current);
  const incomingScore = paginationTruthScore(incoming);
  if (incomingScore > currentScore) return normalizePaginationSummary(incoming);
  if (currentScore >= 0) return normalizePaginationSummary(current);
  return normalizePaginationSummary(incoming);
}

function deriveWalletHistoryTruth(row = {}, options = {}) {
  const safeRow = row && typeof row === "object" ? row : {};
  const fileAudit =
    options.fileAudit && typeof options.fileAudit === "object" ? options.fileAudit : null;
  const filePagination =
    fileAudit && fileAudit.pagination && typeof fileAudit.pagination === "object"
      ? fileAudit.pagination
      : {};
  const tradePagination = chooseStrongerPaginationSummary(
    safeRow.tradePagination && typeof safeRow.tradePagination === "object"
      ? safeRow.tradePagination
      : null,
    filePagination.trades
  );
  const fundingPagination = chooseStrongerPaginationSummary(
    safeRow.fundingPagination && typeof safeRow.fundingPagination === "object"
      ? safeRow.fundingPagination
      : null,
    filePagination.funding
  );

  const tradeDone =
    Boolean(safeRow.tradeDone) || isPaginationComplete(tradePagination);
  const fundingDone =
    Boolean(safeRow.fundingDone) || isPaginationComplete(fundingPagination);
  const tradeHasMore = tradeDone ? false : Boolean(safeRow.tradeHasMore);
  const fundingHasMore = fundingDone ? false : Boolean(safeRow.fundingHasMore);
  const tradeCursor = tradeDone ? null : safeRow.tradeCursor || null;
  const fundingCursor = fundingDone ? null : safeRow.fundingCursor || null;
  const retryPending = tradeDone && fundingDone ? false : Boolean(safeRow.retryPending);
  const retryReason = retryPending ? safeRow.retryReason || null : null;
  const remoteHistoryVerified =
    tradeDone &&
    fundingDone &&
    !retryPending &&
    Number((tradePagination && tradePagination.retryPages) || 0) <= 0 &&
    Number((fundingPagination && fundingPagination.retryPages) || 0) <= 0 &&
    !tradeCursor &&
    !fundingCursor &&
    !tradeHasMore &&
    !fundingHasMore;
  const backfillComplete = remoteHistoryVerified;

  return {
    tradePagination,
    fundingPagination,
    tradeDone,
    fundingDone,
    tradeHasMore,
    fundingHasMore,
    tradeCursor,
    fundingCursor,
    retryPending,
    retryReason,
    remoteHistoryVerified,
    backfillComplete,
  };
}

function buildWalletHistoryAudit(row = {}, options = {}) {
  const safeRow = row && typeof row === "object" ? row : {};
  const fileAudit =
    options.fileAudit && typeof options.fileAudit === "object" ? options.fileAudit : null;
  const truth = deriveWalletHistoryTruth(safeRow, { fileAudit });
  const tradesStored = Math.max(
    Number(safeRow.tradeRowsLoaded || 0),
    Number(fileAudit && fileAudit.tradesStored !== undefined ? fileAudit.tradesStored : 0)
  );
  const fundingStored = Math.max(
    Number(safeRow.fundingRowsLoaded || 0),
    Number(fileAudit && fileAudit.fundingStored !== undefined ? fileAudit.fundingStored : 0)
  );
  const pendingReasons = [];
  if (!truth.remoteHistoryVerified) pendingReasons.push("remote_history_verification_pending");
  if (!truth.tradeDone) pendingReasons.push("trade_history_incomplete");
  if (!truth.fundingDone) pendingReasons.push("funding_history_incomplete");
  if (truth.tradeCursor) pendingReasons.push("trade_cursor_pending");
  if (truth.fundingCursor) pendingReasons.push("funding_cursor_pending");
  if (truth.tradeHasMore) pendingReasons.push("trade_pages_pending");
  if (truth.fundingHasMore) pendingReasons.push("funding_pages_pending");
  if (truth.tradePagination && Number(truth.tradePagination.retryPages || 0) > 0) {
    pendingReasons.push("trade_page_retries_pending");
  }
  if (truth.fundingPagination && Number(truth.fundingPagination.retryPages || 0) > 0) {
    pendingReasons.push("funding_page_retries_pending");
  }
  if (
    truth.tradePagination &&
    !Boolean(truth.tradePagination.exhausted) &&
    Number(truth.tradePagination.highestFetchedPage || 0) > 0
  ) {
    pendingReasons.push("trade_page_frontier_pending");
  }
  if (
    truth.fundingPagination &&
    !Boolean(truth.fundingPagination.exhausted) &&
    Number(truth.fundingPagination.highestFetchedPage || 0) > 0
  ) {
    pendingReasons.push("funding_page_frontier_pending");
  }
  if (truth.retryPending) {
    pendingReasons.push(
      `retry_pending:${String(truth.retryReason || safeRow.lastErrorReason || "unknown")}`
    );
  }

  const warnings = [];
  if (
    truth.backfillComplete &&
    (truth.tradeHasMore || truth.fundingHasMore || truth.tradeCursor || truth.fundingCursor)
  ) {
    warnings.push("complete_state_has_pending_markers");
  }
  if (
    fileAudit &&
    Number(fileAudit.tradeSeenKeys || 0) > 0 &&
    Number(fileAudit.tradeSeenKeys || 0) < tradesStored
  ) {
    warnings.push("trade_seen_index_smaller_than_rows");
  }
  if (
    fileAudit &&
    Number(fileAudit.fundingSeenKeys || 0) > 0 &&
    Number(fileAudit.fundingSeenKeys || 0) < fundingStored
  ) {
    warnings.push("funding_seen_index_smaller_than_rows");
  }

  let completenessState = "pending";
  if (truth.backfillComplete) completenessState = "complete";
  else if (tradesStored > 0 || fundingStored > 0) completenessState = "partial_history";
  else if (Number(safeRow.liveTrackingSince || 0) > 0) completenessState = "activation_only";

  return {
    version: 1,
    completenessState,
    backfillComplete: truth.backfillComplete,
    remoteHistoryVerified: truth.remoteHistoryVerified,
    retryPending: truth.retryPending,
    retryReason: truth.retryReason || safeRow.lastErrorReason || safeRow.lastError || null,
    pendingReasons: Array.from(new Set(pendingReasons.filter(Boolean))),
    warnings: Array.from(new Set(warnings.filter(Boolean))),
    lastAttemptMode: safeRow.lastAttemptMode || null,
    lastSuccessMode: safeRow.lastSuccessMode || null,
    lastFailureMode: safeRow.lastFailureMode || null,
    file: fileAudit ? { ...fileAudit } : null,
    pagination: {
      trades: truth.tradePagination ? { ...truth.tradePagination } : null,
      funding: truth.fundingPagination ? { ...truth.fundingPagination } : null,
    },
  };
}

module.exports = {
  buildWalletHistoryAudit,
  chooseStrongerPaginationSummary,
  deriveWalletHistoryTruth,
  isPaginationComplete,
  normalizePaginationSummary,
};
