const crypto = require("crypto");

const BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

function base58Decode(input) {
  const text = String(input || "").trim();
  if (!text) return Buffer.alloc(0);

  let num = 0n;
  for (const ch of text) {
    const idx = BASE58_ALPHABET.indexOf(ch);
    if (idx < 0) {
      throw new Error(`invalid_base58_character:${ch}`);
    }
    num = num * 58n + BigInt(idx);
  }

  let hex = num.toString(16);
  if (hex.length % 2) hex = `0${hex}`;
  let body = hex ? Buffer.from(hex, "hex") : Buffer.alloc(0);

  let leadingZeros = 0;
  for (const ch of text) {
    if (ch !== "1") break;
    leadingZeros += 1;
  }
  if (leadingZeros) {
    body = Buffer.concat([Buffer.alloc(leadingZeros), body]);
  }
  return body;
}

function base58Encode(input) {
  const bytes = Buffer.from(input || []);
  if (!bytes.length) return "";

  let num = 0n;
  for (const byte of bytes) {
    num = (num << 8n) + BigInt(byte);
  }

  let out = "";
  while (num > 0n) {
    const mod = Number(num % 58n);
    out = `${BASE58_ALPHABET[mod]}${out}`;
    num /= 58n;
  }

  let leadingZeros = 0;
  for (const byte of bytes) {
    if (byte !== 0) break;
    leadingZeros += 1;
  }
  if (leadingZeros) {
    out = `${"1".repeat(leadingZeros)}${out}`;
  }
  return out || "1".repeat(leadingZeros);
}

function derLength(length) {
  if (length < 0x80) {
    return Buffer.from([length]);
  }
  const bytes = [];
  let value = length;
  while (value > 0) {
    bytes.unshift(value & 0xff);
    value >>= 8;
  }
  return Buffer.from([0x80 | bytes.length, ...bytes]);
}

function derSequence(parts) {
  const body = Buffer.concat(parts);
  return Buffer.concat([Buffer.from([0x30]), derLength(body.length), body]);
}

function derInteger(value) {
  return Buffer.from([0x02, 0x01, value]);
}

function derObjectIdEd25519() {
  return Buffer.from([0x06, 0x03, 0x2b, 0x65, 0x70]);
}

function derOctetString(bytes) {
  const body = Buffer.from(bytes || []);
  return Buffer.concat([Buffer.from([0x04]), derLength(body.length), body]);
}

function pkcs8FromEd25519Seed(seed) {
  const rawSeed = Buffer.from(seed || []);
  if (rawSeed.length !== 32) {
    throw new Error("ed25519_seed_must_be_32_bytes");
  }
  const privateKeyOctetString = derOctetString(Buffer.concat([Buffer.from([0x04, 0x20]), rawSeed]));
  return derSequence([derInteger(0), derSequence([derObjectIdEd25519()]), privateKeyOctetString]);
}

function spkiFromEd25519PublicKey(publicKey) {
  const rawPublicKey = Buffer.from(publicKey || []);
  if (rawPublicKey.length !== 32) {
    throw new Error("ed25519_public_key_must_be_32_bytes");
  }
  return Buffer.concat([
    Buffer.from([0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00]),
    rawPublicKey,
  ]);
}

function sortJson(value) {
  if (Array.isArray(value)) {
    return value.map((item) => sortJson(item));
  }
  if (value && typeof value === "object" && !(value instanceof Buffer)) {
    return Object.keys(value)
      .sort()
      .reduce((acc, key) => {
        acc[key] = sortJson(value[key]);
        return acc;
      }, {});
  }
  return value;
}

function createPacificaAgentKeypair(privateKeyBase58) {
  const raw = base58Decode(privateKeyBase58);
  if (raw.length !== 64) {
    throw new Error("api_agent_private_key_must_decode_to_64_bytes");
  }
  const seed = raw.subarray(0, 32);
  const expectedPublicKey = raw.subarray(32, 64);
  const privateKeyObject = crypto.createPrivateKey({
    key: pkcs8FromEd25519Seed(seed),
    format: "der",
    type: "pkcs8",
  });
  const publicKeyObject = crypto.createPublicKey(privateKeyObject);
  const publicKeyDer = publicKeyObject.export({ format: "der", type: "spki" });
  const derivedPublicKey = Buffer.from(publicKeyDer).subarray(-32);
  return {
    seed,
    expectedPublicKey,
    derivedPublicKey,
    privateKeyObject,
    publicKeyObject,
  };
}

function createPacificaSignedRequest({
  accountWallet,
  agentWallet,
  agentPrivateKey,
  type,
  operationData = null,
  timestamp = Date.now(),
  expiryWindow = 5000,
}) {
  const operationType = String(type || "").trim();
  if (!operationType) {
    throw new Error("operation_type_required");
  }
  const account = String(accountWallet || "").trim();
  const agent = String(agentWallet || "").trim();
  const privateKey = String(agentPrivateKey || "").trim();
  if (!account) throw new Error("account_wallet_required");
  if (!agent) throw new Error("agent_wallet_required");
  if (!privateKey) throw new Error("agent_private_key_required");

  const keypair = createPacificaAgentKeypair(privateKey);
  const derivedAgentWallet = base58Encode(keypair.derivedPublicKey);
  if (derivedAgentWallet !== agent) {
    throw new Error("agent_wallet_private_key_mismatch");
  }

  const signatureHeader = {
    expiry_window: Number(expiryWindow) || 5000,
    timestamp: Number(timestamp) || Date.now(),
    type: operationType,
  };
  const message = operationData && Object.keys(operationData).length
    ? { ...signatureHeader, data: operationData }
    : { ...signatureHeader, data: {} };
  const compactMessage = JSON.stringify(sortJson(message));
  const signature = crypto.sign(null, Buffer.from(compactMessage, "utf8"), keypair.privateKeyObject);
  const signatureBase58 = base58Encode(signature);

  const finalBody = {
    account,
    agent_wallet: agent,
    signature: signatureBase58,
    timestamp: signatureHeader.timestamp,
    expiry_window: signatureHeader.expiry_window,
  };

  if (operationData && Object.keys(operationData).length) {
    Object.assign(finalBody, operationData);
  }

  return {
    account,
    agentWallet: agent,
    derivedAgentWallet,
    timestamp: signatureHeader.timestamp,
    expiryWindow: signatureHeader.expiry_window,
    signature: signatureBase58,
    message: compactMessage,
    body: finalBody,
  };
}

module.exports = {
  base58Decode,
  base58Encode,
  createPacificaAgentKeypair,
  createPacificaSignedRequest,
  sortJson,
};
