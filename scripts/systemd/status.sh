#!/usr/bin/env bash
set -euo pipefail

systemctl --no-pager --full status \
  pacifica-flow.target \
  pacifica-live-positions.target \
  pacifica-proxy-refresh.timer \
  pacifica-proxy-refresh.service \
  pacifica-wallet-indexer.service \
  pacifica-onchain-discovery.service \
  pacifica-global-kpi.service \
  pacifica-ui-api.service

systemctl --no-pager --full list-units 'pacifica-live-positions@*.service' 2>/dev/null || true
