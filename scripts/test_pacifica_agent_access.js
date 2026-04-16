#!/usr/bin/env node

const { createPacificaAgentKeypair, createPacificaSignedRequest } = require("../src/services/pacifica/agent_signing");

function parseArgs(argv) {
  const out = {};
  for (let i = 2; i < argv.length; i += 1) {
    const arg = argv[i];
    if (!arg.startsWith("--")) continue;
    const [key, inlineValue] = arg.slice(2).split("=", 2);
    const normalizedKey = key.replace(/-([a-z])/g, (_, ch) => ch.toUpperCase());
    if (inlineValue !== undefined) {
      out[normalizedKey] = inlineValue;
      continue;
    }
    const next = argv[i + 1];
    if (next && !next.startsWith("--")) {
      out[normalizedKey] = next;
      i += 1;
    } else {
      out[normalizedKey] = "true";
    }
  }
  return out;
}

function requireValue(name, value) {
  const text = String(value || "").trim();
  if (!text) {
    throw new Error(`missing_${name}`);
  }
  return text;
}

async function main() {
  const args = parseArgs(process.argv);
  const accountWallet = requireValue("account", args.account || process.env.PACIFICA_AGENT_ACCOUNT);
  const agentWallet = requireValue("agent_wallet", args.agentWallet || process.env.PACIFICA_AGENT_WALLET);
  const agentPrivateKey = requireValue(
    "agent_private_key",
    args.privateKey || process.env.PACIFICA_AGENT_PRIVATE_KEY
  );

  const keypair = createPacificaAgentKeypair(agentPrivateKey);
  const derivedAgentWallet = requireValue("derived_agent_wallet", args.agentWallet || agentWallet);
  const derivedPublic = requireValue("derived_public", derivedAgentWallet);
  const expectedPublic = createPacificaSignedRequest({
    accountWallet,
    agentWallet,
    agentPrivateKey,
    type: "list_api_keys",
    operationData: null,
  }).derivedAgentWallet;

  if (expectedPublic !== derivedPublic) {
    throw new Error("agent_wallet_private_key_mismatch");
  }

  const accountRes = await fetch(`https://api.pacifica.fi/api/v1/account?account=${encodeURIComponent(accountWallet)}`, {
    headers: { Accept: "application/json" },
  });
  const accountJson = await accountRes.json();

  const signed = createPacificaSignedRequest({
    accountWallet,
    agentWallet,
    agentPrivateKey,
    type: "list_api_keys",
    operationData: null,
    expiryWindow: 5000,
  });

  const validateRes = await fetch("https://api.pacifica.fi/api/v1/account/api_keys", {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify(signed.body),
  });

  const validateText = await validateRes.text();
  let validateJson = null;
  try {
    validateJson = JSON.parse(validateText);
  } catch (_error) {
    validateJson = { raw: validateText };
  }

  process.stdout.write(
    JSON.stringify(
      {
        ok: validateRes.ok,
        accountStatus: accountRes.status,
        accountBalance: accountJson && accountJson.data ? accountJson.data.balance : null,
        validateStatus: validateRes.status,
        validatePayload: validateJson,
        derivedAgentWallet: expectedPublic,
        expectedMatches: true,
      },
      null,
      2
    ) + "\n"
  );
}

main().catch((error) => {
  process.stderr.write(`${error && error.stack ? error.stack : error}\n`);
  process.exit(1);
});
