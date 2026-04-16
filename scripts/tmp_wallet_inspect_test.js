const delay = ms => new Promise(r => setTimeout(r, ms));

async function main() {
  const version = await (await fetch('http://127.0.0.1:9225/json/version')).json();
  const ws = new WebSocket(version.webSocketDebuggerUrl);
  let nextId = 0;
  const pending = new Map();

  const send = (method, params = {}, sessionId) =>
    new Promise((resolve, reject) => {
      const id = ++nextId;
      pending.set(id, { resolve, reject });
      ws.send(JSON.stringify(sessionId ? { id, method, params, sessionId } : { id, method, params }));
    });

  ws.onmessage = ev => {
    const msg = JSON.parse(ev.data);
    if (!msg.id || !pending.has(msg.id)) return;
    const p = pending.get(msg.id);
    pending.delete(msg.id);
    if (msg.error) p.reject(new Error(msg.error.message || 'CDP error'));
    else p.resolve(msg.result);
  };

  await new Promise((resolve, reject) => {
    ws.onopen = resolve;
    ws.onerror = reject;
  });

  const { targetId } = await send('Target.createTarget', { url: 'about:blank' });
  const { sessionId } = await send('Target.attachToTarget', { targetId, flatten: true });
  const cdp = (method, params = {}) => send(method, params, sessionId);

  await cdp('Page.enable');
  await cdp('Runtime.enable');
  await cdp('DOM.enable');
  await cdp('Page.navigate', { url: 'http://127.0.0.1/wallets-explorer/' });

  await delay(18000);

  const before = await cdp('Runtime.evaluate', {
    expression: `({
      rows: document.querySelectorAll('#wallet-table-body .inspect-btn').length,
      hidden: document.getElementById('wallet-profile-panel')?.hidden,
      profileId: document.getElementById('wallet-profile-id')?.textContent || '',
      runtimeError: !!window.__PF_APP_RUNTIME_ERROR_SHOWN,
      booted: !!window.__PF_APP_BOOTED,
      cacheSize: window.__PF_WALLET_ROWS_BY_ID ? Object.keys(window.__PF_WALLET_ROWS_BY_ID).length : 0,
      cacheHit: window.__PF_WALLET_ROWS_BY_ID ? !!window.__PF_WALLET_ROWS_BY_ID[document.querySelector('#wallet-table-body .inspect-btn')?.getAttribute('data-wallet') || ''] : false
    })`,
    returnByValue: true,
  });
  console.log('before', before.result.value);

  const clicked = await cdp('Runtime.evaluate', {
    expression: `(() => {
      const btn = document.querySelector('#wallet-table-body .inspect-btn');
      if (!btn) return 'no-button';
      btn.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window }));
      return btn.getAttribute('data-wallet') || 'clicked';
    })()`,
    returnByValue: true,
  });
  console.log('clicked', clicked.result.value);
  const clickFlag = await cdp('Runtime.evaluate', {
    expression: `window.__PF_LAST_INSPECT_ATTEMPT || ''`,
    returnByValue: true,
  });
  console.log('clickFlag', clickFlag.result.value);
  const inspectState = await cdp('Runtime.evaluate', {
    expression: `({
      helper: typeof window.__PF_OPEN_WALLET_INSPECT,
      rowSeen: window.__PF_ROW_INSPECT_SEEN || '',
      callSeen: window.__PF_INSPECT_CALL_SEEN || '',
      start: window.__PF_INSPECT_START || '',
      rendered: window.__PF_INSPECT_RENDERED || ''
    })`,
    returnByValue: true,
  });
  console.log('inspectState', inspectState.result.value);

  await delay(16000);

  const after = await cdp('Runtime.evaluate', {
    expression: `({
      hidden: document.getElementById('wallet-profile-panel')?.hidden,
      profileId: document.getElementById('wallet-profile-id')?.textContent || '',
      subtitle: document.getElementById('wallet-profile-subtitle')?.textContent || '',
      bodyText: document.getElementById('wallet-profile-body')?.innerText?.slice(0,140) || '',
      bodyHtml: document.getElementById('wallet-profile-body')?.innerHTML?.slice(0,260) || ''
    })`,
    returnByValue: true,
  });
  console.log('after', after.result.value);
  ws.close();
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
