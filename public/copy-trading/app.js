"use strict";(function(){const root=document.getElementById("ct-root");if(!root)return;const STORAGE_KEY="pacificaFlow.copyTrading.selectedLeaderId";const VALID_PAGES=new Set(["overview","settings"]);const state={page:getPageFromPath(),loading:true,refreshing:false,error:null,auth:null,snapshot:null,positionsData:null,activityData:null,balanceData:null,executionLogData:null,reconciliationData:null,selectedLeaderId:loadSelectedLeaderId(),lastUpdatedAt:null};let source=null;let refreshTimer=null;function getPageFromPath(pathname){const clean=String(pathname||window.location.pathname||"").replace(/\/+$/,"");if(clean.endsWith("/settings"))return"settings";if(clean.endsWith("/overview"))return"overview";if(clean==="/copy-trading"||clean==="/copy-trading/"||clean==="")return"overview";return"overview"}function loadSelectedLeaderId(){try{return String(localStorage.getItem(STORAGE_KEY)||"").trim()}catch(_error){return""}}function saveSelectedLeaderId(value){state.selectedLeaderId=String(value||"").trim();try{if(state.selectedLeaderId){localStorage.setItem(STORAGE_KEY,state.selectedLeaderId)}else{localStorage.removeItem(STORAGE_KEY)}}catch(_error){}}function setPage(nextPage){const normalized=VALID_PAGES.has(nextPage)?nextPage:"overview";if(state.page===normalized)return;state.page=normalized;const path=normalized==="settings"?"/copy-trading/settings":"/copy-trading/overview";if(window.location.pathname!==path){window.history.pushState({},"",path)}render()}function escapeHtml(value){return String(value!==null&&value!==undefined?value:"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;").replace(/'/g,"&#39;")}function shortWallet(value){const text=String(value||"").trim();if(!text)return"-";if(text.length<=12)return text;return`${text.slice(0,5)}…${text.slice(-4)}`}function formatUsd(value,digits){const num=Number(value);if(!Number.isFinite(num))return"-";return`${num<0?"-":""}${Math.abs(num).toLocaleString("en-US",{minimumFractionDigits:digits||0,maximumFractionDigits:digits||0})} USDT`}function formatSignedUsd(value,digits){const num=Number(value);if(!Number.isFinite(num))return"-";const body=Math.abs(num).toLocaleString("en-US",{minimumFractionDigits:digits||0,maximumFractionDigits:digits||0});return`${num>0?"+":num<0?"-":""}${body} USDT`}function formatPct(value,digits){const num=Number(value);if(!Number.isFinite(num))return"-";return`${num.toLocaleString("en-US",{minimumFractionDigits:digits||1,maximumFractionDigits:digits||1})}%`}function formatNumber(value,digits){const num=Number(value);if(!Number.isFinite(num))return"-";return num.toLocaleString("en-US",{minimumFractionDigits:digits||0,maximumFractionDigits:digits||0})}function formatTime(value){const num=Number(value);if(!Number.isFinite(num)||!num)return"-";return new Date(num).toLocaleString("en-GB",{day:"2-digit",month:"short",hour:"2-digit",minute:"2-digit"})}function formatRelative(value){const num=Number(value);if(!Number.isFinite(num)||!num)return"-";const delta=Math.max(0,Math.floor((Date.now()-num)/1000));if(delta<60)return`${delta}s ago`;if(delta<3600)return`${Math.floor(delta/60)}m ago`;if(delta<86400)return`${Math.floor(delta/3600)}h ago`;return`${Math.floor(delta/86400)}d ago`}function formatSymbol(value){const text=String(value||"").trim();if(!text)return"-";return text.replace(/USDT$/i,"")}function getTextField(form,name){const el=form.elements&&form.elements[name];if(!el)return"";if(el.length&&el[0]&&typeof el[0].value==="string"){return String(el[0].value||"").trim()}return String(el.value||"").trim()}function getChecked(form,name){const el=form.elements&&form.elements[name];if(!el)return false;if(el.length&&el[0]&&typeof el[0].checked==="boolean"){return Boolean(el[0].checked)}return Boolean(el&&el.checked)}function getNumberField(form,name){const raw=getTextField(form,name);if(!raw)return null;const num=Number(raw);return Number.isFinite(num)?num:null}function chip(text,tone){const klass=tone?`ct-chip ${tone}`:"ct-chip";return`<span class="${klass}">${escapeHtml(text)}</span>`}function rowChip(text,tone){return chip(text,tone)}function statusTone(stateValue){const value=String(stateValue||"").toLowerCase();if(["done","good","active","running","ready","healthy","copied","ok","enabled"].includes(value))return"is-green";if(["current","preview","watch","warning","preview mode"].includes(value))return"is-amber";if(["blocked","bad","failed","error","paused","disabled","stopped","missing"].includes(value))return"is-red";if(["info","neutral","idle"].includes(value))return"is-blue";return"is-muted"}function apiFetch(url,init){return fetch(url,{credentials:"include",...init,headers:{"Content-Type":"application/json",...(init&&init.headers?init.headers:{})}}).then(async res=>{let payload=null;try{payload=await res.json()}catch(_error){}if(!res.ok){const error=new Error(payload&&payload.error||`Request failed (${res.status})`);error.status=res.status;error.payload=payload;throw error}return payload})}function computeModel(){const auth=state.auth||{};const snapshot=state.snapshot||{};const execution=snapshot.execution&&snapshot.execution.config?snapshot.execution:{config:{}};const config=execution.config||{};const runtime=snapshot.runtime||{};const profile=snapshot.profile||{};const risk=snapshot.risk||{global:{},leaders:{}};const leaders=Array.isArray(snapshot.leaders)?snapshot.leaders.slice():[];const positions=Array.isArray(state.positionsData&&(state.positionsData.copiedPositions||state.positionsData.positions))?state.positionsData.copiedPositions||state.positionsData.positions:[];const activity=Array.isArray(state.activityData&&state.activityData.activity)?state.activityData.activity:[];const alerts=Array.isArray(state.activityData&&state.activityData.alerts)?state.activityData.alerts:[];const balanceHistory=Array.isArray(state.balanceData&&state.balanceData.history)?state.balanceData.history:[];const balance=state.balanceData&&state.balanceData.latestBalance?state.balanceData.latestBalance:null;const executionEvents=Array.isArray(state.executionLogData&&state.executionLogData.events)?state.executionLogData.events:[];const reconciliation=state.reconciliationData||null;const accessSets=Array.isArray(snapshot.apiKeys)?snapshot.apiKeys:[];const hasCopyAccess=Boolean(snapshot.pacificaReady||state.positionsData&&state.positionsData.hasConnectedAccessSet);const authenticated=Boolean(auth&&auth.authenticated);const twoFactor=Boolean(auth&&(auth.mfaEnabled||snapshot.twoFactorEnabled));const leadersReady=leaders.length>0;const copyEnabled=Boolean(config.enabled);const dryRun=Boolean(config.dryRun);const copyActive=Boolean(copyEnabled&&runtime.state==="running");const selectedLeader=leaders.find(row=>String(row.id||row.wallet||"").trim()===state.selectedLeaderId)||null;const selectedLeaderRisk=selectedLeader?risk.leaders&&(risk.leaders[String(selectedLeader.wallet||"").toUpperCase()]||risk.leaders[String(selectedLeader.id||"").toUpperCase()])||{}:null;const readiness=[{key:"signedIn",label:"Signed in",done:authenticated,blocked:!authenticated,detail:authenticated?auth.displayName||auth.email||auth.twitterUsername||"Account ready":"Sign in to continue."},{key:"twoFactor",label:"2FA enabled",done:twoFactor,blocked:authenticated&&!twoFactor,detail:twoFactor?"Authenticator is enabled.":"Enable 2FA before live copy trading."},{key:"pacifica",label:"Pacifica connected",done:hasCopyAccess,blocked:authenticated&&twoFactor&&!hasCopyAccess,detail:hasCopyAccess?"Pacifica access is connected.":"Connect a Pacifica access set."},{key:"leaders",label:"Leaders added",done:leadersReady,blocked:authenticated&&twoFactor&&hasCopyAccess&&!leadersReady,detail:leadersReady?`${leaders.length} leader${leaders.length===1?"":"s"} configured.`:"Add at least one leader."},{key:"copy",label:"Copy trading active",done:copyActive,blocked:authenticated&&twoFactor&&hasCopyAccess&&leadersReady&&!copyActive,detail:copyActive?dryRun?"Preview mode is active.":"Live copy trading is running.":"Copy trading is off."},{key:"mode",label:"Mode",done:Boolean(snapshot&&snapshot.execution&&snapshot.execution.config&&snapshot.execution.config.enabled!==undefined),blocked:false,detail:!snapshot||!snapshot.execution||!snapshot.execution.config?"Waiting for saved execution settings.":dryRun?"Dry run is active \u2014 no live orders will be sent.":"Live mode is enabled."}];const missing=readiness.find(step=>step.blocked)||null;let nextAction={label:"Open settings",action:"settings"};let nextTitle="Review your setup";let nextText="Open the settings workspace to confirm execution, leaders, and risk controls.";if(!authenticated){nextTitle="Sign in to continue";nextText="You need to be signed in before copy trading can be configured.";nextAction={label:"Sign in",href:"/"}}else if(!twoFactor){nextTitle="Enable 2FA";nextText="Live copy trading requires Google Authenticator before it can be activated.";nextAction={label:"Enable 2FA",href:"/settings/"}}else if(!hasCopyAccess){nextTitle="Connect Pacifica";nextText="Add a Pacifica access set so the workspace can read balance, positions, and execution state.";nextAction={label:"Manage Pacifica",href:"/settings/"}}else if(!leadersReady){nextTitle="Add a leader";nextText="Copy trading starts by selecting one or more leader wallets.";nextAction={label:"Add leader",href:"/copy-trading/settings"}}else if(!copyEnabled){nextTitle="Enable copy trading";nextText="Everything is ready. Turn copy trading on when you want the worker to start tracking leaders.";nextAction={label:"Enable copy trading",action:"enable"}}else if(dryRun){nextTitle="Preview mode is active";nextText="Dry run is on, so no live orders will be sent. Switch to live in settings when you're ready.";nextAction={label:"Open settings",href:"/copy-trading/settings"}}else{nextTitle="Copy trading is live";nextText="The worker is active and ready to react to new leader positions.";nextAction={label:"Pause copy trading",action:"pause"}}return{auth,snapshot,execution,config,runtime,profile,risk,leaders,positions,activity,alerts,balance,balanceHistory,executionEvents,reconciliation,accessSets,selectedLeader,selectedLeaderRisk,readiness,missing,copyActive,copyEnabled,dryRun,authenticated,twoFactor,hasCopyAccess,leadersReady,nextAction,nextTitle,nextText}}function stepTone(step){if(step.done)return"done";if(step.blocked)return"blocked";return"current"}function renderHeader(model){const page=state.page==="settings"?{title:"Copy Trading Settings",subtitle:"Leader-first workspace for execution, risk, and manual behavior. Only real account state is shown."}:{title:"Overview",subtitle:"Operational dashboard for readiness, leaders, copied positions, and copy-trading activity."};const actions=state.page==="overview"?renderOverviewHeaderActions(model):`
          <button class="ct-btn" type="button" data-action="refresh">Refresh</button>
          <a class="ct-btn ct-btn-primary" href="/copy-trading/overview" data-nav="overview">Back to overview</a>
        `;return`
      <header class="ct-header">
        <div>
          <p class="ct-kicker">PacificaFlow / Copy Trading</p>
          <h1 class="ct-title">${escapeHtml(page.title)}</h1>
          <p class="ct-subtitle">${escapeHtml(page.subtitle)}</p>
        </div>
        <div class="ct-header-actions">${actions}</div>
      </header>
    `}function renderOverviewHeaderActions(model){if(!model.authenticated){return`
        <a class="ct-btn ct-btn-primary" href="/" data-nav="signin">Sign in</a>
        <a class="ct-btn" href="/settings/" data-nav="account">Open account</a>
      `}if(!model.twoFactor){return`
        <a class="ct-btn ct-btn-primary" href="/settings/" data-nav="settings">Enable 2FA</a>
        <a class="ct-btn" href="/copy-trading/settings" data-nav="settings">Open settings</a>
      `}if(!model.hasCopyAccess){return`
        <a class="ct-btn ct-btn-primary" href="/settings/" data-nav="pacifica">Connect Pacifica</a>
        <a class="ct-btn" href="/copy-trading/settings" data-nav="settings">Open settings</a>
      `}if(!model.leadersReady){return`
        <a class="ct-btn ct-btn-primary" href="/copy-trading/settings" data-nav="settings">Add leader</a>
        <a class="ct-btn" href="/copy-trading/settings" data-nav="settings">Review settings</a>
      `}if(!model.copyEnabled){return`
        <button class="ct-btn ct-btn-primary" type="button" data-action="enable">Enable copy trading</button>
        <a class="ct-btn" href="/copy-trading/settings" data-nav="settings">Review settings</a>
      `}if(model.dryRun){return`
        <button class="ct-btn ct-btn-primary" type="button" data-action="enable-live">Switch to live</button>
        <button class="ct-btn" type="button" data-action="pause">Pause copy</button>
      `}return`
      <button class="ct-btn ct-btn-primary" type="button" data-action="pause">Pause copy</button>
      <button class="ct-btn" type="button" data-action="stop-keep">Stop &amp; keep positions</button>
    `}function renderTabs(){return`
      <nav class="ct-tabs" aria-label="Copy Trading sections">
        <a class="ct-tab ${state.page==="overview"?"is-active":""}" href="/copy-trading/overview" data-nav="overview">Overview</a>
        <a class="ct-tab ${state.page==="settings"?"is-active":""}" href="/copy-trading/settings" data-nav="settings">Settings Workspace</a>
      </nav>
    `}function renderBanner(model){const title=model.copyActive?model.dryRun?"Copy trading is enabled in preview mode":"Copy trading is live":!model.authenticated?"Sign in to start copy trading":!model.twoFactor?"Enable 2FA before live trading":!model.hasCopyAccess?"Connect Pacifica before enabling copy trading":!model.leadersReady?"Add a leader to continue":"Copy trading is off";const note=!model.authenticated?"You need an active account before the workspace can show live copy-trading state.":!model.twoFactor?"Google Authenticator is required before copy trading can be turned on.":!model.hasCopyAccess?"Add a Pacifica access set so the app can read balance, positions, and execution state.":!model.leadersReady?"The system is ready, but it still needs at least one tracked leader.":model.copyActive?model.dryRun?"Dry run is active. The worker will preview trades, but it will not submit live orders.":"The worker is active and ready to react to tracked leader changes.":"Everything is ready. Turn copy trading on when you want live tracking to begin.";const modeText=model.copyActive?model.dryRun?"Preview mode":"Live mode":"Inactive";return`
      <section class="ct-banner">
        <div class="ct-banner-main">
          <span class="ct-banner-dot" aria-hidden="true"></span>
          <div class="ct-banner-copy">
            <h2>${escapeHtml(title)}</h2>
            <p>${escapeHtml(note)}</p>
          </div>
        </div>
        <div class="ct-actions">
          <span class="ct-pill">${escapeHtml(modeText)}</span>
          ${model.nextAction&&model.nextAction.action==="enable"?`<button class="ct-btn ct-btn-primary" type="button" data-action="enable">${escapeHtml(model.nextAction.label)}</button>`:model.nextAction&&model.nextAction.action==="pause"?`<button class="ct-btn ct-btn-primary" type="button" data-action="pause">${escapeHtml(model.nextAction.label)}</button>`:model.nextAction&&model.nextAction.action==="enable-live"?`<button class="ct-btn ct-btn-primary" type="button" data-action="enable-live">${escapeHtml(model.nextAction.label)}</button>`:model.nextAction&&model.nextAction.href?`<a class="ct-btn ct-btn-primary" href="${escapeHtml(model.nextAction.href)}">${escapeHtml(model.nextAction.label)}</a>`:`<a class="ct-btn ct-btn-primary" href="/copy-trading/settings" data-nav="settings">${escapeHtml(model.nextAction.label||"Open settings")}</a>`}
        </div>
      </section>
    `}function renderSteps(model){const steps=model.readiness.map((step,index)=>{const tone=stepTone(step);const label=tone==="done"?"Done":tone==="blocked"?"Blocked":"Next";return`
        <div class="ct-step is-${tone}">
          <div class="ct-step-top">
            <div class="ct-step-index">${index+1}</div>
            <span class="ct-step-badge">${escapeHtml(label)}</span>
          </div>
          <strong class="ct-step-title">${escapeHtml(step.label)}</strong>
          <div class="ct-step-note">${escapeHtml(step.detail)}</div>
        </div>
      `});return`
      <section class="ct-strip">
        <div class="ct-strip-head">
          <span>Quick start</span>
          <span class="ct-pill">${model.copyActive?"Active":model.missing?"Setup required":"Ready"}</span>
        </div>
        <div class="ct-steps">${steps.join("")}</div>
      </section>
    `}function renderEmptyState(title,copy,actionHtml){return`
      <div class="ct-empty">
        <strong style="display:block;margin-bottom:6px;color:rgba(245,250,255,0.94);font-size:14px;">${escapeHtml(title)}</strong>
        <div>${escapeHtml(copy)}</div>
        ${actionHtml?`<div class="ct-inline-actions" style="margin-top:12px;">${actionHtml}</div>`:""}
      </div>
    `}function renderReadinessCard(model){const next=model.missing||model.readiness.find(step=>!step.done)||null;const statusRows=model.readiness.map(step=>{const tone=stepTone(step);const value=step.done?"Ready":step.blocked?"Missing":"Pending";return`
        <div class="ct-summary-row">
          <div class="ct-summary-row-head">
            <strong>${escapeHtml(step.label)}</strong>
            <span>${escapeHtml(value)}</span>
          </div>
          <div class="ct-summary-row-meta">
            ${chip(step.detail,tone==="done"?"is-green":tone==="blocked"?"is-red":"is-amber")}
          </div>
        </div>
      `}).join("");const nextAction=model.nextAction&&model.nextAction.href?`<a class="ct-btn ct-btn-primary" href="${escapeHtml(model.nextAction.href)}">${escapeHtml(model.nextAction.label)}</a>`:model.nextAction&&model.nextAction.action?`<button class="ct-btn ct-btn-primary" type="button" data-action="${escapeHtml(model.nextAction.action)}">${escapeHtml(model.nextAction.label)}</button>`:"";return`
      <section class="ct-card">
        <div class="ct-card-head">
          <div>
            <p class="ct-card-kicker">Readiness</p>
            <h2 class="ct-card-title">What is ready right now</h2>
            <p class="ct-card-subtitle">This card only shows the live account state. Missing requirements stay visible until they are resolved.</p>
          </div>
        </div>
        <div class="ct-card-grid">
          <div class="ct-summary-list">${statusRows}</div>
          <div class="ct-baseline-note">
            <strong>Next step:</strong> ${escapeHtml(next?next.detail:"Everything is ready.")}
          </div>
          <div class="ct-inline-actions">${nextAction}</div>
        </div>
      </section>
    `}function renderLeadersCard(model){const leaders=model.leaders;if(!leaders.length){return`
        <section class="ct-card">
          <div class="ct-card-head">
            <div>
              <p class="ct-card-kicker">Active leaders</p>
              <h2 class="ct-card-title">No leaders yet</h2>
              <p class="ct-card-subtitle">Add at least one leader to begin tracking baseline-aware copy trading.</p>
            </div>
          </div>
          ${renderEmptyState("No tracked leaders","You can add the first leader from the settings workspace. The system will snapshot any existing open positions as baseline when tracking begins.",`<a class="ct-btn ct-btn-primary" href="/copy-trading/settings" data-nav="settings">Add leader</a>`)}
        </section>
      `}const rows=leaders.map(leader=>{var _leader$allocationPct,_leader$currentCopied,_leader$pnlUsd,_leader$copiedPositio;const active=String(leader.id||leader.wallet||"")===state.selectedLeaderId;const tone=leader.paused?"is-amber":leader.syncState&&/drift|error|warn/i.test(String(leader.syncState))?"is-red":"is-green";return`
        <button class="ct-row-card ct-leader-select-card ${active?"is-selected":""}" type="button" data-select-leader="${escapeHtml(leader.id||leader.wallet)}">
          <div class="ct-row-main">
            <div class="ct-row-title">
              <div class="ct-avatar">${escapeHtml((leader.label||leader.wallet||"L").slice(0,1).toUpperCase())}</div>
              <div class="ct-name">
                <strong>${escapeHtml(leader.label||leader.walletPreview||shortWallet(leader.wallet))}</strong>
                <span>${escapeHtml(leader.walletPreview||shortWallet(leader.wallet))}${leader.note?` · ${escapeHtml(leader.note)}`:""}</span>
              </div>
            </div>
            ${chip(leader.paused?"Paused":leader.syncState||"Tracking",tone)}
          </div>
          <div class="ct-meta-row">
            ${chip(`${formatPct((_leader$allocationPct=leader.allocationPct)!==null&&_leader$allocationPct!==void 0?_leader$allocationPct:0,0)} allocation`,"is-blue")}
            ${chip(`${formatUsd((_leader$currentCopied=leader.currentCopiedExposureUsd)!==null&&_leader$currentCopied!==void 0?_leader$currentCopied:0,0)} copied`,"is-muted")}
            ${chip(`${formatSignedUsd((_leader$pnlUsd=leader.pnlUsd)!==null&&_leader$pnlUsd!==void 0?_leader$pnlUsd:0,0)}`,leader.pnlUsd>0?"is-green":leader.pnlUsd<0?"is-red":"is-muted")}
            ${chip(`${(_leader$copiedPositio=leader.copiedPositionsCount)!==null&&_leader$copiedPositio!==void 0?_leader$copiedPositio:0} positions`,"is-muted")}
          </div>
        </button>
      `}).join("");return`
      <section class="ct-card">
        <div class="ct-card-head">
          <div>
            <p class="ct-card-kicker">Leaders</p>
            <h2 class="ct-card-title">Tracked leaders</h2>
            <p class="ct-card-subtitle">Select a leader to edit allocation, risk overrides, and behavior. Only current leaders are shown here.</p>
          </div>
        </div>
        <div class="ct-card-grid">
          <form class="ct-card-grid" data-form="add-leader">
            <div class="ct-field-grid cols-2">
              <label class="ct-field">
                <span>Leader wallet</span>
                <input name="wallet" placeholder="Paste leader wallet" autocomplete="off" />
              </label>
              <label class="ct-field">
                <span>Label</span>
                <input name="label" placeholder="Optional label" autocomplete="off" />
              </label>
            </div>
            <div class="ct-actions">
              <button class="ct-btn ct-btn-primary" type="submit">Add leader</button>
              <span class="ct-small-copy">The baseline snapshot is taken when tracking starts.</span>
            </div>
          </form>
          <div class="ct-leader-list">${rows}</div>
        </div>
      </section>
    `}function renderCopiedPositionsCard(model){const positions=model.positions;if(!positions.length){return`
        <section class="ct-card">
          <div class="ct-card-head">
            <div>
              <p class="ct-card-kicker">Copied positions</p>
              <h2 class="ct-card-title">No copied positions yet</h2>
              <p class="ct-card-subtitle">When copy trading starts reacting to new leader positions, they will appear here.</p>
            </div>
          </div>
          ${renderEmptyState("Nothing copied yet","No follower positions are currently tracked. New positions will appear here after copy trading is active.")}
        </section>
      `}const rows=positions.map(position=>{var _ref,_ref2,_position$pnlUsd,_ref3,_ref4,_position$copiedSize,_ref5,_position$entry,_ref6,_position$mark;const symbol=formatSymbol(position.symbol||position.token);const side=String(position.side||"").toLowerCase();const pnl=Number((_ref=(_ref2=(_position$pnlUsd=position.pnlUsd)!==null&&_position$pnlUsd!==void 0?_position$pnlUsd:position.unrealizedPnlUsd)!==null&&_ref2!==void 0?_ref2:position.pnl)!==null&&_ref!==void 0?_ref:0);const status=position.status||position.lifecycleState||"Tracking";return`
        <tr>
          <td class="ct-mono">${escapeHtml(symbol)}</td>
          <td>${escapeHtml(side||"-")}</td>
          <td>${escapeHtml(position.leaderName||position.leaderWallet||"-")}</td>
          <td>${escapeHtml(formatNumber((_ref3=(_ref4=(_position$copiedSize=position.copiedSize)!==null&&_position$copiedSize!==void 0?_position$copiedSize:position.size)!==null&&_ref4!==void 0?_ref4:position.amount)!==null&&_ref3!==void 0?_ref3:0,4))}</td>
          <td>${escapeHtml(formatNumber((_ref5=(_position$entry=position.entry)!==null&&_position$entry!==void 0?_position$entry:position.entryPrice)!==null&&_ref5!==void 0?_ref5:0,4))}</td>
          <td>${escapeHtml(formatNumber((_ref6=(_position$mark=position.mark)!==null&&_position$mark!==void 0?_position$mark:position.markPrice)!==null&&_ref6!==void 0?_ref6:0,4))}</td>
          <td class="${pnl>0?"ct-good":pnl<0?"ct-bad":"ct-muted"}">${escapeHtml(formatSignedUsd(pnl,0))}</td>
          <td>${escapeHtml(status)}</td>
          <td>${escapeHtml(formatTime(position.openedAt||position.createdAt||position.updatedAt))}</td>
          <td>
            <div class="ct-inline-actions">
              <button class="ct-btn" type="button" data-pos-action="close" data-symbol="${escapeHtml(symbol)}" data-side="${escapeHtml(side)}">Close</button>
              <button class="ct-btn" type="button" data-pos-action="protect" data-symbol="${escapeHtml(symbol)}" data-side="${escapeHtml(side)}">Protect</button>
            </div>
          </td>
        </tr>
      `}).join("");return`
      <section class="ct-card">
        <div class="ct-card-head">
          <div>
            <p class="ct-card-kicker">Copied positions</p>
            <h2 class="ct-card-title">Follower positions</h2>
            <p class="ct-card-subtitle">Only real copied positions are shown. Empty rows are not fabricated.</p>
          </div>
        </div>
        <div class="ct-card-grid">
          <div class="ct-table-wrap">
            <table class="ct-table">
              <thead>
                <tr>
                  <th>Symbol</th>
                  <th>Side</th>
                  <th>Leader</th>
                  <th>Copied size</th>
                  <th>Entry</th>
                  <th>Mark</th>
                  <th>PnL</th>
                  <th>Status</th>
                  <th>Opened</th>
                  <th>Action</th>
                </tr>
              </thead>
              <tbody>${rows}</tbody>
            </table>
          </div>
        </div>
      </section>
    `}function renderActivityCard(model){const activity=(model.activity||[]).slice(0,8);const alerts=model.alerts||[];const list=activity.length?activity.map(row=>{const tone=row.status==="copied"||/copied|ok|saved|enabled/i.test(String(row.status||row.message||""))?"is-green":row.status==="blocked"||/block|fail|stop/i.test(String(row.status||row.message||""))?"is-red":"is-amber";return`
            <div class="ct-summary-row">
              <div class="ct-summary-row-head">
                <strong>${escapeHtml(row.action||row.message||"Event")}</strong>
                <span>${escapeHtml(formatRelative(row.createdAt))}</span>
              </div>
              <div class="ct-summary-row-meta">
                ${row.leaderName?chip(row.leaderName,"is-muted"):""}
                ${row.symbol?chip(formatSymbol(row.symbol),"is-blue"):""}
                ${chip(row.reason||row.message||"No details",tone)}
              </div>
            </div>
          `}).join(""):renderEmptyState("No activity yet","Execution events, skips, and manual overrides will appear here once copy trading is active.");const alertChips=alerts.length?alerts.map(row=>chip(row.message||"Alert",row.severity==="good"?"is-green":row.severity==="warn"?"is-amber":"is-red")).join(""):chip("No active alerts","is-muted");return`
      <section class="ct-card">
        <div class="ct-card-head">
          <div>
            <p class="ct-card-kicker">Activity</p>
            <h2 class="ct-card-title">Execution feed</h2>
            <p class="ct-card-subtitle">This feed reflects the actual copy-trading event log. Nothing here is synthetic.</p>
          </div>
        </div>
        <div class="ct-card-grid">
          <div class="ct-summary-list">${list}</div>
          <div class="ct-summary-row">
            <div class="ct-summary-row-head">
              <strong>Alerts</strong>
              <span>${escapeHtml(String(alerts.length))}</span>
            </div>
            <div class="ct-summary-row-meta">${alertChips}</div>
          </div>
        </div>
      </section>
    `}function renderBalanceCard(model){var _ref7,_ref8,_balance$amount;const balance=model.balance;if(!balance){return`
        <section class="ct-card">
          <div class="ct-card-head">
            <div>
              <p class="ct-card-kicker">Balance</p>
              <h2 class="ct-card-title">No balance snapshot</h2>
              <p class="ct-card-subtitle">Connect Pacifica access to show the latest account balance and portfolio history.</p>
            </div>
          </div>
          ${renderEmptyState("Balance not available yet",model.hasCopyAccess?"The backend did not return a balance snapshot for the selected Pacifica access set.":"A Pacifica access set is required before balance can be read.",model.hasCopyAccess?"":`<a class="ct-btn ct-btn-primary" href="/settings/" data-nav="pacifica">Manage access</a>`)}
        </section>
      `}return`
      <section class="ct-card">
        <div class="ct-card-head">
          <div>
            <p class="ct-card-kicker">Balance</p>
            <h2 class="ct-card-title">Follower account balance</h2>
            <p class="ct-card-subtitle">Latest Pacifica snapshot for the connected account.</p>
          </div>
        </div>
        <div class="ct-card-grid">
          <div class="ct-summary-row">
            <div class="ct-summary-row-head">
              <strong>${escapeHtml(formatUsd((_ref7=(_ref8=(_balance$amount=balance.amount)!==null&&_balance$amount!==void 0?_balance$amount:balance.balance)!==null&&_ref8!==void 0?_ref8:balance.pendingBalance)!==null&&_ref7!==void 0?_ref7:0,0))}</strong>
              <span>${escapeHtml(balance.eventType||"balance")}</span>
            </div>
            <div class="ct-summary-row-meta">
              ${balance.pendingBalance!==undefined&&balance.pendingBalance!==null?chip(`Pending ${formatUsd(balance.pendingBalance,0)}`,"is-muted"):""}
              ${balance.createdAt?chip(formatTime(balance.createdAt),"is-muted"):""}
            </div>
          </div>
          <div class="ct-small-copy">Portfolio history points: ${escapeHtml(String((model.balanceHistory||[]).length))}</div>
        </div>
      </section>
    `}function renderSettingsSummaryCard(model){const summary=model.snapshot||{};const modeLabel=summary.execution&&summary.execution.config?summary.execution.config.enabled?summary.execution.config.dryRun?"Dry run":"Live":"Disabled":"Disabled";const copyMode=summary.profile?summary.profile.copyMode:"proportional";return`
      <section class="ct-card">
        <div class="ct-card-head">
          <div>
            <p class="ct-card-kicker">State</p>
            <h2 class="ct-card-title">Current copy-trading state</h2>
            <p class="ct-card-subtitle">This summary is read from the current saved settings and runtime state.</p>
          </div>
        </div>
        <div class="ct-summary-list">
          <div class="ct-summary-row">
            <div class="ct-summary-row-head">
              <strong>Copy mode</strong>
              <span>${escapeHtml(copyMode.replace(/_/g," "))}</span>
            </div>
            <div class="ct-summary-row-meta">
              ${chip(modeLabel,summary.execution&&summary.execution.config&&summary.execution.config.enabled?summary.execution.config.dryRun?"is-amber":"is-green":"is-muted")}
            </div>
          </div>
          <div class="ct-summary-row">
            <div class="ct-summary-row-head">
              <strong>Pacifica access</strong>
              <span>${escapeHtml(summary.pacificaReady?"connected":"missing")}</span>
            </div>
            <div class="ct-summary-row-meta">
              ${chip(summary.pacificaReady?"Connected":"Missing",summary.pacificaReady?"is-green":"is-red")}
            </div>
          </div>
          <div class="ct-summary-row">
            <div class="ct-summary-row-head">
              <strong>Manual close protection</strong>
              <span>${escapeHtml(summary.profile&&summary.profile.allowReopenAfterManualClose?"off":"on")}</span>
            </div>
            <div class="ct-summary-row-meta">
              ${chip(summary.profile&&summary.profile.allowReopenAfterManualClose?"Reopen allowed":"Protection on",summary.profile&&summary.profile.allowReopenAfterManualClose?"is-muted":"is-blue")}
            </div>
          </div>
          <div class="ct-baseline-note">
            Existing leader positions are ignored when tracking starts. Only new positions opened after tracking begins are eligible, and size changes are mirrored only for tracked positions that were opened together from the beginning.
          </div>
        </div>
      </section>
    `}function renderOverview(model){return`
      <div class="ct-frame">
        <div class="ct-frame-inner">
          <div class="ct-shell">
            ${renderHeader(model)}
            ${renderTabs()}
            ${renderBanner(model)}
            ${renderSteps(model)}
            <div class="ct-grid ct-overview-grid">
              <div class="ct-stack">
                ${renderReadinessCard(model)}
                ${renderLeadersCard(model)}
                ${renderCopiedPositionsCard(model)}
              </div>
              <div class="ct-sticky">
                ${renderActivityCard(model)}
                ${renderBalanceCard(model)}
                ${renderSettingsSummaryCard(model)}
              </div>
            </div>
          </div>
        </div>
      </div>
    `}function renderExecutionCard(model){const config=model.execution&&model.execution.config||{};const enabled=Boolean(config.enabled);const dryRun=Boolean(config.dryRun);const accessSet=Array.isArray(model.accessSets)?model.accessSets.find(row=>row&&row.id===config.accessKeyId||row&&row.wallet===config.accessKeyId):null;return`
      <section class="ct-card">
        <div class="ct-card-head">
          <div>
            <p class="ct-card-kicker">Execution</p>
            <h2 class="ct-card-title">Global execution controls</h2>
            <p class="ct-card-subtitle">These controls define whether copy trading is active and which Pacifica access set it uses.</p>
          </div>
          <div class="ct-card-actions">
            <button class="ct-btn ct-btn-primary" type="submit" form="global-execution-form">Save execution</button>
          </div>
        </div>
        <form id="global-execution-form" class="ct-card-grid" data-form="global-execution">
          <div class="ct-toggle-list">
            <label class="ct-toggle">
              <div class="ct-toggle-top">
                <strong>Copy trading enabled</strong>
                <input type="checkbox" name="enabled" ${enabled?"checked":""} />
              </div>
              <div class="ct-toggle-copy">Turn the worker on or off for this account.</div>
            </label>
            <label class="ct-toggle">
              <div class="ct-toggle-top">
                <strong>Dry run</strong>
                <input type="checkbox" name="dryRun" ${dryRun?"checked":""} />
              </div>
              <div class="ct-toggle-copy">Preview orders without sending live execution to Pacifica.</div>
            </label>
            <label class="ct-toggle">
              <div class="ct-toggle-top">
                <strong>Copy only new trades</strong>
                <input type="checkbox" name="copyOnlyNewTrades" ${model.profile&&model.profile.copyOnlyNewTrades?"checked":""} />
              </div>
              <div class="ct-toggle-copy">Ignore positions that were already open when tracking began.</div>
            </label>
            <label class="ct-toggle">
              <div class="ct-toggle-top">
                <strong>Manual close protection</strong>
                <input type="checkbox" name="allowReopenAfterManualClose" ${!(model.profile&&model.profile.allowReopenAfterManualClose)?"checked":""} />
              </div>
              <div class="ct-toggle-copy">Manually closed copied positions stay closed unless you explicitly allow reopen.</div>
            </label>
            <label class="ct-toggle">
              <div class="ct-toggle-top">
                <strong>Pause new opens only</strong>
                <input type="checkbox" name="pauseNewOpensOnly" ${model.profile&&model.profile.pauseNewOpensOnly?"checked":""} />
              </div>
              <div class="ct-toggle-copy">Pause new entries while keeping existing copied positions managed.</div>
            </label>
          </div>
          <div class="ct-field-grid cols-2">
            <label class="ct-field">
              <span>Pacifica access set</span>
              <select name="accessKeyId">
                ${renderAccessSetOptions(model.accessSets,config.accessKeyId)}
              </select>
              <div class="ct-field-help">The access set must belong to this account and contain the account wallet and agent keypair.</div>
            </label>
            <label class="ct-field">
              <span>Copy mode</span>
              <select name="copyMode">
                ${renderCopyModeOptions(model.profile&&model.profile.copyMode)}
              </select>
              <div class="ct-field-help">Choose how leader size is translated into follower orders.</div>
            </label>
          </div>
          <div class="ct-field-grid cols-2">
            <label class="ct-field">
              <span>When stopping</span>
              <select name="stopMode">
                <option value="keep" ${String(model.profile&&model.profile.stopMode||"keep")==="keep"?"selected":""}>Keep positions</option>
                <option value="close_all" ${String(model.profile&&model.profile.stopMode||"keep")==="close_all"?"selected":""}>Close all positions</option>
              </select>
              <div class="ct-field-help">This applies when you stop copy trading from the controls.</div>
            </label>
            <label class="ct-field">
              <span>Leverage policy</span>
              <select name="leveragePolicy">
                <option value="cap" ${String(config.leveragePolicy||"cap")==="cap"?"selected":""}>Cap</option>
                <option value="mirror" ${String(config.leveragePolicy||"cap")==="mirror"?"selected":""}>Mirror</option>
                <option value="fixed" ${String(config.leveragePolicy||"cap")==="fixed"?"selected":""}>Fixed</option>
              </select>
              <div class="ct-field-help">Policy used when sizing copied positions.</div>
            </label>
          </div>
          <div class="ct-actions">
            <span class="ct-pill">${escapeHtml(enabled?dryRun?"Enabled / Preview":"Enabled / Live":"Disabled")}</span>
          </div>
          <div class="ct-inline-actions">
            ${enabled?`<button class="ct-btn ct-btn-warn" type="button" data-action="pause">Pause copy trading</button>`:""}
            ${enabled?`<button class="ct-btn" type="button" data-action="stop-keep">Stop and keep positions</button>`:""}
            ${enabled?`<button class="ct-btn ct-btn-danger" type="button" data-action="stop-close">Stop and close all</button>`:""}
          </div>
          <div class="ct-small-copy">If no access set is selected, the workspace will not be able to fetch balance or positions.</div>
        </form>
      </section>
    `}function renderAccessSetOptions(accessSets,selectedId){const rows=Array.isArray(accessSets)?accessSets:[];if(!rows.length){return`<option value="">No Pacifica access set saved</option>`}return rows.map(row=>{const value=row.id||row.accessKeyId||row.wallet||"";const selected=String(value)===String(selectedId||"");const label=`${shortWallet(row.accountWallet||row.wallet)} · ${shortWallet(row.agentWallet||row.agentWalletPreview||"")}`;return`<option value="${escapeHtml(value)}" ${selected?"selected":""}>${escapeHtml(label)}</option>`}).join("")}function renderCopyModeOptions(selected){const rows=[["proportional","Proportional"],["fixed_amount","Fixed amount"],["fixed_allocation","Fixed allocation"],["smart_sync","Smart sync"]];return rows.map(([value,label])=>`<option value="${value}" ${String(selected||"proportional")===value?"selected":""}>${escapeHtml(label)}</option>`).join("")}function renderLeaderList(model){const leaders=model.leaders;const rows=leaders.length?leaders.map(leader=>{const selected=String(leader.id||leader.wallet||"")===state.selectedLeaderId;return`
            <button class="ct-row-card ct-leader-select-card ${selected?"is-selected":""}" type="button" data-select-leader="${escapeHtml(leader.id||leader.wallet)}">
              <div class="ct-row-main">
                <div class="ct-row-title">
                  <div class="ct-avatar ${leader.paused?"is-warm":"is-green"}">${escapeHtml((leader.label||leader.wallet||"L").slice(0,1).toUpperCase())}</div>
                  <div class="ct-name">
                    <strong>${escapeHtml(leader.label||leader.walletPreview||shortWallet(leader.wallet))}</strong>
                    <span>${escapeHtml(leader.walletPreview||shortWallet(leader.wallet))}</span>
                  </div>
                </div>
                ${chip(leader.paused?"Paused":leader.syncState||"Tracking",leader.paused?"is-amber":/drift|warn|error/i.test(String(leader.syncState||""))?"is-red":"is-green")}
              </div>
              <div class="ct-meta-row">
                ${chip(`Allocation ${formatPct(leader.allocationPct||0,0)}`,"is-blue")}
                ${chip(`Copied ${formatUsd(leader.currentCopiedExposureUsd||0,0)}`,"is-muted")}
                ${chip(`PnL ${formatSignedUsd(leader.pnlUsd||0,0)}`,Number(leader.pnlUsd||0)>=0?"is-green":"is-red")}
              </div>
            </button>
          `}).join(""):renderEmptyState("No leaders added","Add a leader wallet to start baseline-aware copy trading. Only new positions opened after tracking starts will be copied.","");return`
      <section class="ct-card">
        <div class="ct-card-head">
          <div>
            <p class="ct-card-kicker">Leaders</p>
            <h2 class="ct-card-title">Tracked leaders</h2>
            <p class="ct-card-subtitle">Select one leader to edit their allocation, risk overrides, and behavior. The list only shows real saved leaders.</p>
          </div>
        </div>
        <div class="ct-card-grid">
          <form class="ct-card-grid" data-form="add-leader">
            <div class="ct-field-grid cols-2">
              <label class="ct-field">
                <span>Leader wallet</span>
                <input name="wallet" placeholder="Paste leader wallet" autocomplete="off" />
              </label>
              <label class="ct-field">
                <span>Label</span>
                <input name="label" placeholder="Optional label" autocomplete="off" />
              </label>
            </div>
            <div class="ct-actions">
              <button class="ct-btn ct-btn-primary" type="submit">Add leader</button>
              <span class="ct-small-copy">Baseline positions are ignored automatically when a leader is first tracked.</span>
            </div>
          </form>
          <div class="ct-leader-list">${rows}</div>
        </div>
      </section>
    `}function renderSelectedLeaderPanel(model){const leader=model.selectedLeader;if(!leader){return`
        <section class="ct-card">
          <div class="ct-card-head">
            <div>
              <p class="ct-card-kicker">Selected leader</p>
              <h2 class="ct-card-title">Select a leader first</h2>
              <p class="ct-card-subtitle">Select a leader to configure allocation, risk limits, and behavior.</p>
            </div>
          </div>
          ${renderEmptyState("No leader selected","Pick a leader from the list on the left. The selected leader panel will open here with only that leader\u2019s settings.")}
        </section>
      `}const leaderRisk=model.selectedLeaderRisk||{};const selectedWallet=String(leader.wallet||"").toUpperCase();return`
      <section class="ct-card">
        <div class="ct-card-head">
          <div>
            <p class="ct-card-kicker">Selected leader</p>
            <h2 class="ct-card-title">${escapeHtml(leader.label||shortWallet(leader.wallet))}</h2>
            <p class="ct-card-subtitle">${escapeHtml(leader.walletPreview||shortWallet(leader.wallet))} · ${escapeHtml(leader.syncState||"Tracking")}</p>
          </div>
          <div class="ct-card-actions">
            ${chip(leader.paused?"Paused":"Tracking",leader.paused?"is-amber":"is-green")}
          </div>
        </div>
        <form class="ct-card-grid" data-form="save-selected-leader">
          <div class="ct-status-grid">
            <div class="ct-status">
              <span>Copied exposure</span>
              <strong>${escapeHtml(formatUsd(leader.currentCopiedExposureUsd||0,0))}</strong>
            </div>
            <div class="ct-status">
              <span>Current PnL</span>
              <strong class="${Number(leader.pnlUsd||0)>=0?"ct-good":"ct-bad"}">${escapeHtml(formatSignedUsd(leader.pnlUsd||0,0))}</strong>
            </div>
            <div class="ct-status">
              <span>Copied positions</span>
              <strong>${escapeHtml(formatNumber(leader.copiedPositionsCount||0,0))}</strong>
            </div>
            <div class="ct-status">
              <span>Sync state</span>
              <strong>${escapeHtml(leader.syncState||"-")}</strong>
            </div>
          </div>

          <div class="ct-field-grid cols-2">
            <label class="ct-field">
              <span>Allocation USD</span>
              <input name="allocationUsd" type="number" min="0" step="0.01" value="${leader.allocationUsd!==null&&leader.allocationUsd!==undefined?escapeHtml(leader.allocationUsd):""}" />
              <div class="ct-field-help">Total budget assigned to this leader.</div>
            </label>
            <label class="ct-field">
              <span>Allocation %</span>
              <input name="allocationPct" type="number" min="0" step="0.01" value="${leader.allocationPct!==null&&leader.allocationPct!==undefined?escapeHtml(leader.allocationPct):""}" />
              <div class="ct-field-help">Share of the global copy budget reserved for this leader.</div>
            </label>
          </div>

          <div class="ct-field-grid cols-3">
            <label class="ct-field">
              <span>Max exposure %</span>
              <input name="maxExposurePct" type="number" min="0" step="0.01" value="${leaderRisk.maxExposurePct!==null&&leaderRisk.maxExposurePct!==undefined?escapeHtml(leaderRisk.maxExposurePct):""}" />
            </label>
            <label class="ct-field">
              <span>Max drawdown %</span>
              <input name="maxDrawdownPct" type="number" min="0" step="0.01" value="${leaderRisk.maxDrawdownPct!==null&&leaderRisk.maxDrawdownPct!==undefined?escapeHtml(leaderRisk.maxDrawdownPct):""}" />
            </label>
            <label class="ct-field">
              <span>Daily loss cap USDT</span>
              <input name="dailyLossCapUsd" type="number" min="0" step="0.01" value="${leaderRisk.dailyLossCapUsd!==null&&leaderRisk.dailyLossCapUsd!==undefined?escapeHtml(leaderRisk.dailyLossCapUsd):""}" />
            </label>
          </div>

          <div class="ct-field-grid cols-3">
            <label class="ct-field">
              <span>Max leverage</span>
              <input name="leverageCap" type="number" min="0" step="0.01" value="${leaderRisk.leverageCap!==null&&leaderRisk.leverageCap!==undefined?escapeHtml(leaderRisk.leverageCap):""}" />
            </label>
            <label class="ct-field">
              <span>Max open positions</span>
              <input name="maxOpenPositions" type="number" min="0" step="1" value="${leaderRisk.maxOpenPositions!==null&&leaderRisk.maxOpenPositions!==undefined?escapeHtml(leaderRisk.maxOpenPositions):""}" />
            </label>
            <label class="ct-field">
              <span>Stop mode</span>
              <select name="stopMode">
                <option value="keep" ${String(leader.stopMode||"keep")==="keep"?"selected":""}>Keep positions</option>
                <option value="close_all" ${String(leader.stopMode||"keep")==="close_all"?"selected":""}>Close all</option>
              </select>
            </label>
          </div>

          <div class="ct-field-grid cols-2">
            <label class="ct-field">
              <span>Allowed symbols</span>
              <textarea name="allowedSymbols" placeholder="BTC, ETH, SOL">${escapeHtml((leaderRisk.allowedSymbols||[]).join(", "))}</textarea>
              <div class="ct-field-help">Leave blank to allow the global symbol rules to apply.</div>
            </label>
            <label class="ct-field">
              <span>Blocked symbols</span>
              <textarea name="blockedSymbols" placeholder="PUMP, MEME">${escapeHtml((leaderRisk.blockedSymbols||[]).join(", "))}</textarea>
              <div class="ct-field-help">Symbols in this list will never be copied for this leader.</div>
            </label>
          </div>

          <div class="ct-actions">
            <button class="ct-btn ct-btn-primary" type="submit">Save leader</button>
            <button class="ct-btn" type="button" data-leader-action="pause" data-leader-id="${escapeHtml(leader.id||leader.wallet)}">${leader.paused?"Resume leader":"Pause leader"}</button>
            <button class="ct-btn ct-btn-warn" type="button" data-leader-action="stop" data-leader-id="${escapeHtml(leader.id||leader.wallet)}">Stop leader</button>
            <button class="ct-btn ct-btn-danger" type="button" data-leader-action="remove" data-leader-id="${escapeHtml(leader.id||leader.wallet)}">Remove leader</button>
          </div>
          <div class="ct-small-copy">Only tracked positions can mirror increases, reductions, and closes. Baseline positions stay ignored.</div>
        </form>
      </section>
    `}function renderRiskCard(model){const global=model.risk&&model.risk.global||{};return`
      <section class="ct-card">
        <div class="ct-card-head">
          <div>
            <p class="ct-card-kicker">Risk</p>
            <h2 class="ct-card-title">Global risk controls</h2>
            <p class="ct-card-subtitle">These limits apply across the whole copy-trading account.</p>
          </div>
          <div class="ct-card-actions">
            <button class="ct-btn ct-btn-primary" type="submit" form="risk-global-form">Save risk</button>
          </div>
        </div>
        <form id="risk-global-form" class="ct-card-grid" data-form="global-risk">
          <div class="ct-field-grid cols-2">
            <label class="ct-field">
              <span>Total copied exposure cap USDT</span>
              <input name="totalCopiedExposureCapUsd" type="number" min="0" step="0.01" value="${global.totalCopiedExposureCapUsd!==null&&global.totalCopiedExposureCapUsd!==undefined?escapeHtml(global.totalCopiedExposureCapUsd):""}" />
              <div class="ct-field-help">Maximum combined notional across all copied positions.</div>
            </label>
            <label class="ct-field">
              <span>Total daily loss cap USDT</span>
              <input name="totalDailyLossCapUsd" type="number" min="0" step="0.01" value="${global.totalDailyLossCapUsd!==null&&global.totalDailyLossCapUsd!==undefined?escapeHtml(global.totalDailyLossCapUsd):""}" />
              <div class="ct-field-help">Stops copying if the follower account loses more than this in one day.</div>
            </label>
          </div>
          <div class="ct-field-grid cols-3">
            <label class="ct-field">
              <span>Max leverage</span>
              <input name="leverageCap" type="number" min="0" step="0.01" value="${global.leverageCap!==null&&global.leverageCap!==undefined?escapeHtml(global.leverageCap):""}" />
            </label>
            <label class="ct-field">
              <span>Max open positions</span>
              <input name="maxOpenPositions" type="number" min="0" step="1" value="${global.maxOpenPositions!==null&&global.maxOpenPositions!==undefined?escapeHtml(global.maxOpenPositions):""}" />
            </label>
            <label class="ct-field">
              <span>Max allocation %</span>
              <input name="maxAllocationPct" type="number" min="0" step="0.01" value="${global.maxAllocationPct!==null&&global.maxAllocationPct!==undefined?escapeHtml(global.maxAllocationPct):""}" />
            </label>
          </div>
          <label class="ct-toggle">
            <div class="ct-toggle-top">
              <strong>Auto-pause after failures</strong>
              <input type="checkbox" name="autoPauseAfterFailures" ${Number(global.autoPauseAfterFailures||0)>0?"checked":""} />
            </div>
            <div class="ct-toggle-copy">Pause the worker when repeated execution failures cross the configured threshold.</div>
          </label>
          <div class="ct-field-grid cols-2">
            <label class="ct-field">
              <span>Allowed symbols</span>
              <textarea name="allowedSymbols" placeholder="BTC, ETH, SOL">${escapeHtml((global.allowedSymbols||[]).join(", "))}</textarea>
            </label>
            <label class="ct-field">
              <span>Blocked symbols</span>
              <textarea name="blockedSymbols" placeholder="PUMP, MEME">${escapeHtml((global.blockedSymbols||[]).join(", "))}</textarea>
            </label>
          </div>
        </form>
      </section>
    `}function renderBehaviorCard(model){const profile=model.profile||{};const execution=model.execution&&model.execution.config?model.execution.config:{};return`
      <section class="ct-card">
        <div class="ct-card-head">
          <div>
            <p class="ct-card-kicker">Behavior</p>
            <h2 class="ct-card-title">Copy behavior</h2>
            <p class="ct-card-subtitle">These settings decide how copy trading behaves when positions change or manual actions happen.</p>
          </div>
        </div>
        <div class="ct-toggle-list">
          <div class="ct-summary-row">
            <div class="ct-summary-row-head">
              <strong>Manual close protection</strong>
              <span>${escapeHtml(profile.allowReopenAfterManualClose?"off":"on")}</span>
            </div>
            <div class="ct-summary-row-meta">
              ${chip(profile.allowReopenAfterManualClose?"Reopen allowed":"Protection on",profile.allowReopenAfterManualClose?"is-muted":"is-blue")}
              ${chip(profile.pauseNewOpensOnly?"New opens paused":"New opens allowed",profile.pauseNewOpensOnly?"is-amber":"is-green")}
              ${chip(profile.stopMode==="close_all"?"Close all on stop":"Keep positions on stop",profile.stopMode==="close_all"?"is-amber":"is-blue")}
            </div>
          </div>
          <div class="ct-small-copy">
            Copy mode: ${escapeHtml(String(profile.copyMode||execution.copyMode||"proportional").replace(/_/g," "))}. The worker only copies size changes for positions that were opened together from the baseline.
          </div>
        </div>
      </section>
    `}function renderAdvancedCard(model){const execution=model.execution&&model.execution.config?model.execution.config:{};return`
      <details class="ct-advanced">
        <summary>Advanced settings</summary>
        <div class="ct-advanced-body">
          <div class="ct-small-copy">These values are saved with the main execution card above.</div>
          <div class="ct-field-grid cols-2">
            <label class="ct-field">
              <span>Copy ratio</span>
              <input name="copyRatio" type="number" min="0" step="0.01" value="${execution.copyRatio!==null&&execution.copyRatio!==undefined?escapeHtml(execution.copyRatio):""}" form="global-execution-form" />
              <div class="ct-field-help">Scales leader size before risk limits are applied.</div>
            </label>
            <label class="ct-field">
              <span>Fixed notional USDT</span>
              <input name="fixedNotionalUsd" type="number" min="0" step="0.01" value="${execution.fixedNotionalUsd!==null&&execution.fixedNotionalUsd!==undefined?escapeHtml(execution.fixedNotionalUsd):""}" form="global-execution-form" />
              <div class="ct-field-help">Used when fixed amount per trade is selected.</div>
            </label>
          </div>
          <div class="ct-field-grid cols-2">
            <label class="ct-field">
              <span>Min trade size USDT</span>
              <input name="minNotionalUsd" type="number" min="0" step="0.01" value="${execution.minNotionalUsd!==null&&execution.minNotionalUsd!==undefined?escapeHtml(execution.minNotionalUsd):""}" form="global-execution-form" />
            </label>
            <label class="ct-field">
              <span>Slippage %</span>
              <input name="slippagePercent" type="number" min="0" step="0.01" value="${execution.slippagePercent!==null&&execution.slippagePercent!==undefined?escapeHtml(execution.slippagePercent):""}" form="global-execution-form" />
            </label>
          </div>
          <div class="ct-field-grid cols-2">
            <label class="ct-field">
              <span>Retry policy</span>
              <select name="retryPolicy" form="global-execution-form">
                <option value="conservative" ${String(execution.retryPolicy||"balanced")==="conservative"?"selected":""}>Conservative</option>
                <option value="balanced" ${String(execution.retryPolicy||"balanced")==="balanced"?"selected":""}>Balanced</option>
                <option value="aggressive" ${String(execution.retryPolicy||"balanced")==="aggressive"?"selected":""}>Aggressive</option>
              </select>
            </label>
            <label class="ct-field">
              <span>Sync behavior</span>
              <select name="syncBehavior" form="global-execution-form">
                <option value="baseline_only" ${String(execution.syncBehavior||"baseline_only")==="baseline_only"?"selected":""}>Baseline only</option>
                <option value="mirror_all" ${String(execution.syncBehavior||"baseline_only")==="mirror_all"?"selected":""}>Mirror all</option>
                <option value="safe_pause" ${String(execution.syncBehavior||"baseline_only")==="safe_pause"?"selected":""}>Safe pause</option>
              </select>
            </label>
          </div>
          <form class="ct-card-grid" data-form="manual-override">
            <div class="ct-divider"></div>
            <div class="ct-card-head">
              <div>
                <p class="ct-card-kicker">Manual override</p>
                <h3 class="ct-card-title" style="font-size:18px;">Protect or close a copied position</h3>
              </div>
            </div>
            <div class="ct-field-grid cols-3">
              <label class="ct-field">
                <span>Symbol</span>
                <input name="symbol" placeholder="BTCUSDT" />
              </label>
              <label class="ct-field">
                <span>Side</span>
                <select name="side">
                  <option value="long">Long</option>
                  <option value="short">Short</option>
                </select>
              </label>
              <label class="ct-field">
                <span>Leader wallet</span>
                <input name="leaderWallet" placeholder="Optional" />
              </label>
            </div>
            <label class="ct-field">
              <span>Reason</span>
              <textarea name="reason" placeholder="Why this override exists"></textarea>
            </label>
            <div class="ct-toggle-list">
              <label class="ct-toggle">
                <div class="ct-toggle-top">
                  <strong>Close once</strong>
                  <input type="checkbox" name="closeOnce" />
                </div>
                <div class="ct-toggle-copy">Close the copied position once and keep it from being re-opened by the baseline-aware worker.</div>
              </label>
              <label class="ct-toggle">
                <div class="ct-toggle-top">
                  <strong>Protect from reopen</strong>
                  <input type="checkbox" name="protectFromReopen" checked />
                </div>
                <div class="ct-toggle-copy">Prevents the worker from reopening the position unless manual reopen is explicitly allowed.</div>
              </label>
            </div>
            <div class="ct-actions">
              <button class="ct-btn" type="submit">Save override</button>
            </div>
          </form>
        </div>
      </details>
    `}function renderBaselineNote(){return`
      <section class="ct-baseline-note">
        <strong>Baseline-aware copy trading</strong><br />
        Existing leader positions are ignored when tracking starts. Only new positions opened after tracking begins are eligible. Size changes are mirrored only for tracked positions that were opened together from the beginning. Manually closed copied positions stay closed unless reopen is explicitly allowed.
      </section>
    `}function renderSettings(model){return`
      <div class="ct-frame">
        <div class="ct-frame-inner">
          <div class="ct-shell">
            ${renderHeader(model)}
            ${renderTabs()}
            <section class="ct-banner">
              <div class="ct-banner-main">
                <span class="ct-banner-dot" aria-hidden="true"></span>
                <div class="ct-banner-copy">
                  <h2>Settings workspace</h2>
                  <p>Leader-first controls for execution, risk, behavior, and selected leader overrides. Only real saved settings are shown.</p>
                </div>
              </div>
              <div class="ct-actions">
                ${chip(model.copyActive?model.dryRun?"Enabled / Preview":"Enabled / Live":"Disabled",model.copyActive?"is-green":"is-muted")}
                <a class="ct-btn" href="/copy-trading/overview" data-nav="overview">Back to overview</a>
              </div>
            </section>
            <div class="ct-grid ct-settings-grid">
              <div class="ct-stack">
                ${renderExecutionCard(model)}
                ${renderLeaderList(model)}
                ${renderSelectedLeaderPanel(model)}
              </div>
              <div class="ct-sticky">
                ${renderRiskCard(model)}
                ${renderBehaviorCard(model)}
                ${renderAdvancedCard(model)}
                ${renderBaselineNote()}
              </div>
            </div>
          </div>
        </div>
      </div>
    `}function renderLoading(){return`
      <div class="ct-frame">
        <div class="ct-frame-inner">
          <div class="ct-shell">
            <header class="ct-header">
              <div>
                <p class="ct-kicker">PacificaFlow / Copy Trading</p>
                <h1 class="ct-title">Loading workspace</h1>
                <p class="ct-subtitle">Fetching the current account state from Pacifica and the local copy-trading store.</p>
              </div>
            </header>
            <section class="ct-empty">Loading the live workspace state…</section>
          </div>
        </div>
      </div>
    `}function renderError(message){return`
      <div class="ct-frame">
        <div class="ct-frame-inner">
          <div class="ct-shell">
            <header class="ct-header">
              <div>
                <p class="ct-kicker">PacificaFlow / Copy Trading</p>
                <h1 class="ct-title">Workspace error</h1>
                <p class="ct-subtitle">The backend returned an unexpected response while loading the copy-trading workspace.</p>
              </div>
            </header>
            <section class="ct-empty">
              <strong style="display:block;margin-bottom:6px;color:rgba(245,250,255,0.94);font-size:14px;">${escapeHtml(message||"Unable to load the workspace")}</strong>
              <div>Please refresh the page or check the current account and Pacifica access state.</div>
              <div class="ct-inline-actions" style="margin-top:12px;">
                <button class="ct-btn ct-btn-primary" type="button" data-action="refresh">Refresh</button>
              </div>
            </section>
          </div>
        </div>
      </div>
    `}function render(){const model=computeModel();document.body.classList.add("ct-page");document.body.classList.remove("ct-page-overview","ct-page-settings");document.body.classList.add(state.page==="settings"?"ct-page-settings":"ct-page-overview");document.title=`PacificaFlow | Copy Trading - ${state.page==="settings"?"Settings Workspace":"Overview"}`;if(state.loading){root.innerHTML=renderLoading();return}if(state.error){root.innerHTML=renderError(state.error);return}root.innerHTML=state.page==="settings"?renderSettings(model):renderOverview(model)}async function refreshData(options){const silent=Boolean(options&&options.silent);if(!silent){state.loading=true;render()}else{state.refreshing=true}try{const auth=await apiFetch("/api/auth/me").catch(error=>({ok:false,authenticated:false,mfaEnabled:false,copyTradingEligible:false,error:error&&error.message?error.message:"auth unavailable"}));state.auth=auth||{authenticated:false};const eligible=Boolean(state.auth&&state.auth.authenticated&&(state.auth.copyTradingEligible||state.auth.mfaEnabled));if(eligible){const[snapshot,positionsData,activityData,balanceData,executionLogData,reconciliationData]=await Promise.all([apiFetch("/api/copy/settings").catch(()=>null),apiFetch("/api/copy/positions").catch(()=>null),apiFetch("/api/copy/activity").catch(()=>null),apiFetch("/api/copy/balance").catch(()=>null),apiFetch("/api/copy/execution-log").catch(()=>null),apiFetch("/api/copy/reconciliation").catch(()=>null)]);state.snapshot=snapshot&&snapshot.ok?snapshot:null;state.positionsData=positionsData&&positionsData.ok?positionsData:null;state.activityData=activityData&&activityData.ok?activityData:null;state.balanceData=balanceData&&balanceData.ok?balanceData:null;state.executionLogData=executionLogData&&executionLogData.ok?executionLogData:null;state.reconciliationData=reconciliationData&&reconciliationData.ok?reconciliationData:null;state.error=null;const leaders=state.snapshot&&Array.isArray(state.snapshot.leaders)?state.snapshot.leaders:[];if(state.selectedLeaderId&&!leaders.some(row=>String(row.id||row.wallet||"")===state.selectedLeaderId)){saveSelectedLeaderId("")}}else{state.snapshot=null;state.positionsData=null;state.activityData=null;state.balanceData=null;state.executionLogData=null;state.reconciliationData=null;state.error=null}}catch(error){state.error=error&&error.message?error.message:"Failed to load copy trading data"}finally{state.loading=false;state.refreshing=false;state.lastUpdatedAt=Date.now();render();reconnectStream();scheduleRefresh()}}function scheduleRefresh(){if(refreshTimer)clearTimeout(refreshTimer);refreshTimer=setTimeout(()=>{if(!document.hidden){refreshData({silent:true})}},30000)}function reconnectStream(){if(source){try{source.close()}catch(_error){}source=null}if(!state.auth||!state.auth.authenticated||!(state.auth.copyTradingEligible||state.auth.mfaEnabled))return;try{source=new EventSource("/api/copy/stream");source.addEventListener("message",()=>{if(!state.refreshing)refreshData({silent:true})});source.addEventListener("copy-event",()=>{if(!state.refreshing)refreshData({silent:true})});source.addEventListener("leaders-changed",()=>{if(!state.refreshing)refreshData({silent:true})});source.addEventListener("settings-updated",()=>{if(!state.refreshing)refreshData({silent:true})})}catch(_error){}}async function runCopyAction(action){const routes={enable:"/api/copy/enable",pause:"/api/copy/pause","stop-keep":"/api/copy/stop-keep","stop-close":"/api/copy/stop-close-all","enable-live":"/api/copy/settings"};if(action==="enable-live"){const model=computeModel();const payload=buildGlobalExecutionPayload(model);payload.enabled=true;payload.dryRun=false;await apiFetch(routes.enableLive||"/api/copy/settings",{method:"PATCH",body:JSON.stringify(payload)});await refreshData({silent:true});return}const route=routes[action];if(!route)return;await apiFetch(route,{method:"POST",body:"{}"});await refreshData({silent:true})}function buildGlobalExecutionPayload(model,form){const execution=model.execution&&model.execution.config?model.execution.config:{};const profile=model.profile||{};const accessKeyId=form?getTextField(form,"accessKeyId"):String(execution.accessKeyId||"");return{enabled:form?getChecked(form,"enabled"):Boolean(execution.enabled),dryRun:form?getChecked(form,"dryRun"):Boolean(execution.dryRun),accessKeyId,copyMode:form?getTextField(form,"copyMode"):String(profile.copyMode||execution.copyMode||"proportional"),copyOnlyNewTrades:form?getChecked(form,"copyOnlyNewTrades"):Boolean(profile.copyOnlyNewTrades),allowReopenAfterManualClose:form?!getChecked(form,"allowReopenAfterManualClose"):Boolean(profile.allowReopenAfterManualClose),pauseNewOpensOnly:form?getChecked(form,"pauseNewOpensOnly"):Boolean(profile.pauseNewOpensOnly),stopMode:form?getTextField(form,"stopMode")||"keep":String(profile.stopMode||"keep"),copyRatio:form?getNumberField(form,"copyRatio"):Number(execution.copyRatio||1),fixedNotionalUsd:form?getNumberField(form,"fixedNotionalUsd"):Number(execution.fixedNotionalUsd||0),minNotionalUsd:form?getNumberField(form,"minNotionalUsd"):Number(execution.minNotionalUsd||0),slippagePercent:form?getNumberField(form,"slippagePercent"):Number(execution.slippagePercent||0.5),leveragePolicy:form?getTextField(form,"leveragePolicy"):String(execution.leveragePolicy||"cap"),retryPolicy:form?getTextField(form,"retryPolicy"):String(execution.retryPolicy||"balanced"),syncBehavior:form?getTextField(form,"syncBehavior"):String(execution.syncBehavior||"baseline_only")}}async function saveGlobalExecution(form){const model=computeModel();const payload=buildGlobalExecutionPayload(model,form);await apiFetch("/api/copy/settings",{method:"PATCH",body:JSON.stringify(payload)});await refreshData({silent:true})}async function saveGlobalRisk(form){const payload={global:{totalCopiedExposureCapUsd:getNumberField(form,"totalCopiedExposureCapUsd"),totalDailyLossCapUsd:getNumberField(form,"totalDailyLossCapUsd"),leverageCap:getNumberField(form,"leverageCap"),maxOpenPositions:getNumberField(form,"maxOpenPositions"),maxAllocationPct:getNumberField(form,"maxAllocationPct"),autoPauseAfterFailures:getChecked(form,"autoPauseAfterFailures")?3:0,allowedSymbols:String(getTextField(form,"allowedSymbols")||"").split(/[,\s]+/g).map(item=>item.trim().toUpperCase()).filter(Boolean),blockedSymbols:String(getTextField(form,"blockedSymbols")||"").split(/[,\s]+/g).map(item=>item.trim().toUpperCase()).filter(Boolean)},leaders:{}};await apiFetch("/api/copy/risk",{method:"PATCH",body:JSON.stringify(payload)});await refreshData({silent:true})}async function saveSelectedLeader(form){const model=computeModel();const leader=model.selectedLeader;if(!leader)return;const leaderId=String(leader.id||leader.wallet||"").trim();const walletKey=String(leader.wallet||"").trim().toUpperCase();const leaderPatch={allocationUsd:getNumberField(form,"allocationUsd"),allocationPct:getNumberField(form,"allocationPct"),stopMode:getTextField(form,"stopMode")||"keep",paused:Boolean(leader.paused)};const pausedBtn=form.querySelector(`[data-leader-action="pause"][data-leader-id="${CSS.escape(leaderId)}"]`);if(pausedBtn){leaderPatch.paused=false}const riskPatch={global:{},leaders:{[walletKey]:{maxExposurePct:getNumberField(form,"maxExposurePct"),maxDrawdownPct:getNumberField(form,"maxDrawdownPct"),dailyLossCapUsd:getNumberField(form,"dailyLossCapUsd"),leverageCap:getNumberField(form,"leverageCap"),maxOpenPositions:getNumberField(form,"maxOpenPositions"),allowedSymbols:String(getTextField(form,"allowedSymbols")||"").split(/[,\s]+/g).map(item=>item.trim().toUpperCase()).filter(Boolean),blockedSymbols:String(getTextField(form,"blockedSymbols")||"").split(/[,\s]+/g).map(item=>item.trim().toUpperCase()).filter(Boolean)}}};await apiFetch(`/api/copy/leaders/${encodeURIComponent(leaderId)}`,{method:"PATCH",body:JSON.stringify(leaderPatch)});await apiFetch("/api/copy/risk",{method:"PATCH",body:JSON.stringify(riskPatch)});await refreshData({silent:true})}async function addLeader(form){const payload={wallet:getTextField(form,"wallet"),label:getTextField(form,"label")};const response=await apiFetch("/api/copy/leaders",{method:"POST",body:JSON.stringify(payload)});const created=response&&response.leader?response.leader:null;if(created&&(created.id||created.wallet)){saveSelectedLeaderId(created.id||created.wallet)}form.reset();await refreshData({silent:true})}async function saveManualOverride(form){const payload={symbol:getTextField(form,"symbol"),side:getTextField(form,"side"),leaderWallet:getTextField(form,"leaderWallet"),reason:getTextField(form,"reason"),closeOnce:getChecked(form,"closeOnce"),protectFromReopen:getChecked(form,"protectFromReopen")};await apiFetch("/api/copy/manual-override",{method:"POST",body:JSON.stringify(payload)});form.reset();await refreshData({silent:true})}async function leaderAction(action,leaderId){const model=computeModel();const leader=model.leaders.find(row=>String(row.id||row.wallet||"")===String(leaderId||""));if(!leader)return;const id=String(leader.id||leader.wallet||"");if(action==="remove"){if(!window.confirm(`Remove leader ${leader.label||shortWallet(leader.wallet)}?`))return;await apiFetch(`/api/copy/leaders/${encodeURIComponent(id)}`,{method:"DELETE"});if(state.selectedLeaderId===id)saveSelectedLeaderId("");await refreshData({silent:true});return}if(action==="pause"){await apiFetch(`/api/copy/leaders/${encodeURIComponent(id)}`,{method:"PATCH",body:JSON.stringify({paused:!leader.paused,label:leader.label||"",note:leader.note||"",allocationPct:leader.allocationPct,allocationUsd:leader.allocationUsd,stopMode:leader.stopMode||"keep"})});await refreshData({silent:true});return}if(action==="stop"){await apiFetch(`/api/copy/leaders/${encodeURIComponent(id)}`,{method:"PATCH",body:JSON.stringify({paused:true,stopMode:"close_all",label:leader.label||"",note:leader.note||"",allocationPct:leader.allocationPct,allocationUsd:leader.allocationUsd})});await refreshData({silent:true})}}function onClick(event){const target=event.target.closest("button, a");if(!target||!root.contains(target))return;const nav=target.getAttribute("data-nav");if(nav){event.preventDefault();setPage(nav);return}const action=target.getAttribute("data-action");if(action){if(action==="refresh"){event.preventDefault();refreshData({silent:true});return}if(action==="enable"||action==="pause"||action==="stop-keep"||action==="stop-close"||action==="enable-live"){event.preventDefault();runCopyAction(action).catch(error=>{state.error=error&&error.message?error.message:"Copy trading action failed";render()});return}}const leaderId=target.getAttribute("data-select-leader");if(leaderId){event.preventDefault();saveSelectedLeaderId(leaderId);render();return}const leaderActionType=target.getAttribute("data-leader-action");const leaderActionId=target.getAttribute("data-leader-id");if(leaderActionType&&leaderActionId){event.preventDefault();leaderAction(leaderActionType,leaderActionId).catch(error=>{state.error=error&&error.message?error.message:"Leader action failed";render()});return}const posAction=target.getAttribute("data-pos-action");if(posAction){event.preventDefault();const symbol=target.getAttribute("data-symbol")||"";const side=target.getAttribute("data-side")||"";const actionText=posAction==="close"?"Close copied position":"Protect copied position";if(posAction==="close"&&!window.confirm(`Close copied position ${symbol} ${side}?`))return;apiFetch("/api/copy/manual-override",{method:"POST",body:JSON.stringify({symbol,side,reason:actionText,closeOnce:posAction==="close",protectFromReopen:posAction==="protect"})}).then(()=>posAction==="close"?apiFetch("/api/copy/run",{method:"POST",body:"{}"}):Promise.resolve()).then(()=>refreshData({silent:true})).catch(error=>{state.error=error&&error.message?error.message:"Position action failed";render()})}}function onSubmit(event){const form=event.target;if(!(form instanceof HTMLFormElement)||!root.contains(form))return;const formName=form.getAttribute("data-form");if(!formName)return;event.preventDefault();const submitter=event.submitter||null;const mode=submitter&&submitter.getAttribute("data-submit-mode");const promise=formName==="global-execution"?saveGlobalExecution(form):formName==="global-risk"?saveGlobalRisk(form):formName==="save-selected-leader"?saveSelectedLeader(form):formName==="add-leader"?addLeader(form):formName==="manual-override"?saveManualOverride(form):Promise.resolve();promise.catch(error=>{state.error=error&&error.message?error.message:"Save failed";render()})}function renderOverviewLayout(model){return`
      <div class="ct-frame">
        <div class="ct-frame-inner">
          <div class="ct-shell">
            ${renderHeader(model)}
            ${renderTabs()}
            ${renderBanner(model)}
            ${renderSteps(model)}
            <div class="ct-grid ct-overview-grid">
              <div class="ct-stack">
                ${renderReadinessCard(model)}
                ${renderLeadersCard(model)}
                ${renderCopiedPositionsCard(model)}
              </div>
              <div class="ct-sticky">
                ${renderActivityCard(model)}
                ${renderBalanceCard(model)}
                ${renderSettingsSummaryCard(model)}
              </div>
            </div>
          </div>
        </div>
      </div>
    `}function renderSettingsLayout(model){return`
      <div class="ct-frame">
        <div class="ct-frame-inner">
          <div class="ct-shell">
            ${renderHeader(model)}
            ${renderTabs()}
            <section class="ct-banner">
              <div class="ct-banner-main">
                <span class="ct-banner-dot" aria-hidden="true"></span>
                <div class="ct-banner-copy">
                  <h2>Settings workspace</h2>
                  <p>Only real saved state is shown here. Leader settings appear only after a leader is selected.</p>
                </div>
              </div>
              <div class="ct-actions">
                ${chip(model.copyActive?model.dryRun?"Enabled / Preview":"Enabled / Live":"Disabled",model.copyActive?"is-green":"is-muted")}
                <a class="ct-btn" href="/copy-trading/overview" data-nav="overview">Back to overview</a>
              </div>
            </section>
            <div class="ct-grid ct-settings-grid">
              <div class="ct-stack">
                ${renderExecutionCard(model)}
                ${renderLeaderList(model)}
                ${renderSelectedLeaderPanel(model)}
              </div>
              <div class="ct-sticky">
                ${renderRiskCard(model)}
                ${renderBehaviorCard(model)}
                ${renderAdvancedCard(model)}
                ${renderBaselineNote()}
              </div>
            </div>
          </div>
        </div>
      </div>
    `}function renderBody(){if(state.loading)return renderLoading();if(state.error)return renderError(state.error);const model=computeModel();return state.page==="settings"?renderSettingsLayout(model):renderOverviewLayout(model)}function renderLoading(){return`
      <div class="ct-frame">
        <div class="ct-frame-inner">
          <div class="ct-shell">
            <header class="ct-header">
              <div>
                <p class="ct-kicker">PacificaFlow / Copy Trading</p>
                <h1 class="ct-title">Loading workspace</h1>
                <p class="ct-subtitle">Fetching the live account snapshot, copy settings, leaders, positions, and activity.</p>
              </div>
            </header>
            <section class="ct-empty">Loading the current copy-trading state…</section>
          </div>
        </div>
      </div>
    `}function renderError(message){return`
      <div class="ct-frame">
        <div class="ct-frame-inner">
          <div class="ct-shell">
            <header class="ct-header">
              <div>
                <p class="ct-kicker">PacificaFlow / Copy Trading</p>
                <h1 class="ct-title">Workspace error</h1>
                <p class="ct-subtitle">The backend did not return a usable copy-trading snapshot.</p>
              </div>
            </header>
            <section class="ct-empty">
              <strong style="display:block;margin-bottom:6px;color:rgba(245,250,255,0.94);font-size:14px;">${escapeHtml(message||"Unable to load workspace")}</strong>
              <div>Refresh the page, or check whether the account is signed in and has 2FA enabled.</div>
              <div class="ct-inline-actions" style="margin-top:12px;">
                <button class="ct-btn ct-btn-primary" type="button" data-action="refresh">Refresh</button>
              </div>
            </section>
          </div>
        </div>
      </div>
    `}function render(){document.body.classList.add("ct-page");document.body.classList.toggle("ct-page-settings",state.page==="settings");document.body.classList.toggle("ct-page-overview",state.page==="overview");document.title=`PacificaFlow | Copy Trading - ${state.page==="settings"?"Settings Workspace":"Overview"}`;if(state.error){root.innerHTML=renderError(state.error);return}root.innerHTML=renderBody()}function bind(){root.addEventListener("click",onClick);root.addEventListener("submit",onSubmit);window.addEventListener("popstate",()=>{state.page=getPageFromPath();render()});document.addEventListener("visibilitychange",()=>{if(!document.hidden){refreshData({silent:true})}})}bind();render();refreshData({silent:true})})();
