const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function main() {
  const version = await (await fetch("http://127.0.0.1:9225/json/version")).json();
  const ws = new WebSocket(version.webSocketDebuggerUrl);
  let nextId = 0;
  const pending = new Map();

  const send = (method, params = {}, sessionId) =>
    new Promise((resolve, reject) => {
      const id = ++nextId;
      pending.set(id, { resolve, reject });
      ws.send(JSON.stringify(sessionId ? { id, method, params, sessionId } : { id, method, params }));
    });

  ws.onmessage = (ev) => {
    const msg = JSON.parse(ev.data);
    if (!msg.id || !pending.has(msg.id)) return;
    const p = pending.get(msg.id);
    pending.delete(msg.id);
    if (msg.error) p.reject(new Error(msg.error.message || "CDP error"));
    else p.resolve(msg.result);
  };

  await new Promise((resolve, reject) => {
    ws.onopen = resolve;
    ws.onerror = reject;
  });

  const { targetId } = await send("Target.createTarget", { url: "about:blank" });
  const { sessionId } = await send("Target.attachToTarget", { targetId, flatten: true });
  const cdp = (method, params = {}) => send(method, params, sessionId);

  await cdp("Page.enable");
  await cdp("Runtime.enable");
  await cdp("Page.navigate", { url: "http://127.0.0.1/wallets-explorer/" });

  let ready = false;
  for (let i = 0; i < 24; i += 1) {
    await delay(5000);
    const res = await cdp("Runtime.evaluate", {
      expression: `({
        booted: !!window.__PF_APP_BOOTED,
        rows: document.querySelectorAll('#wallet-table-body .inspect-btn').length,
        viewActive: document.querySelector('#wallets-view')?.hidden === false
      })`,
      returnByValue: true,
    });
    const value = res.result.value;
    console.log("STATE", value);
    if (value.booted && value.rows > 0) {
      ready = true;
      break;
    }
  }

  if (!ready) {
    throw new Error("wallet explorer not ready");
  }

  await cdp("Runtime.evaluate", {
    expression: `(() => {
      const btn = document.querySelector('#wallet-table-body .inspect-btn');
      if (!btn) return 'no-button';
      btn.click();
      return btn.getAttribute('data-wallet') || 'clicked';
    })()`,
    returnByValue: true,
  });

  await delay(5000);

  const out = await cdp("Runtime.evaluate", {
    expression: `(() => {
      const cards = [...document.querySelectorAll('.wallet-detail-kpi-card')].map((card) => ({
        label: card.querySelector('span')?.textContent?.trim() || '',
        value: card.querySelector('strong')?.textContent?.trim() || ''
      }));
      return {
        hidden: document.getElementById('wallet-profile-panel')?.hidden,
        profileId: document.getElementById('wallet-profile-id')?.textContent || '',
        cards,
        symbolText: document.querySelector('.wallet-symbol-bars')?.innerText?.slice(0, 300) || '',
        emptyState: document.querySelector('.wallet-symbol-bars')?.innerText?.includes('No symbol data yet.') || false,
      };
    })()`,
    returnByValue: true,
  });

  console.log(JSON.stringify(out.result.value, null, 2));
  ws.close();
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
