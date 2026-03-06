# Wallet Source Contract (Optional Merge Input)

On-chain discovery is now the primary global indexer path. Wallet sources are optional merge inputs that can accelerate discovery and recover missed wallets.

Provide a wallet source via `PACIFICA_WALLET_SOURCE_URL`.

Accepted JSON payloads:

```json
["walletA", "walletB"]
```

```json
{"wallets": ["walletA", "walletB"]}
```

```json
{"data": ["walletA", "walletB"]}
```

```json
{"data": {"wallets": ["walletA", "walletB"]}}
```

Object rows are also accepted as long as each row has one of:

- `wallet`
- `address`
- `account`
- `id`

Example:

```json
{"wallets": [{"address": "walletA"}, {"wallet": "walletB"}]}
```

If no URL is configured, fallback source discovery uses:

- `PACIFICA_WALLET_SEEDS`
- `config/wallet-seeds.txt`
- already indexed wallets in `data/indexer/wallets.json`
