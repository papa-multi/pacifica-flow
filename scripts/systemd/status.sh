#!/usr/bin/env bash
set -euo pipefail

systemctl --no-pager --full status \
  pacifica-flow.target \
  pacifica-wallet-indexer.service \
  pacifica-onchain-discovery.service \
  pacifica-global-kpi.service \
  pacifica-ui-api.service
