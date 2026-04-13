const $ = (id) => document.getElementById(id);
const state = { map: null, markers: [], allRows: [], currentTakeover: null, jobs: [], activeLegendBand: '' };
const DEST_COORDS = {
  Antalya:[36.8969,30.7133], Dalaman:[36.7658,28.8028], Bodrum:[37.0344,27.4305], Burgas:[42.5048,27.4626],
  Hurghada:[27.2579,33.8116], 'Sharm El Sheikh':[27.9158,34.3300], Rhodes:[36.4349,28.2176], Kos:[36.8930,27.2877],
  Heraklion:[35.3387,25.1442], Larnaca:[34.9003,33.6232], Paphos:[34.7754,32.4218], Palma:[39.5696,2.6502],
  Tenerife:[28.2916,-16.6291], Faro:[37.0194,-7.9304], Enfidha:[36.1324,10.3807], Corfu:[39.6243,19.9217],
  Zakynthos:[37.7870,20.8984], Malta:[35.9375,14.3754]
};

function fmtMoney(v){ return v===null || v===undefined || Number.isNaN(Number(v)) ? '—' : new Intl.NumberFormat('en-GB',{style:'currency',currency:'GBP',maximumFractionDigits:0}).format(Number(v)); }
function fmtDuration(seconds){ if(seconds===null || seconds===undefined || Number.isNaN(Number(seconds))) return '—'; const s=Math.max(0, Number(seconds)); const m=Math.floor(s/60); const sec=s%60; if(m>=60){ const h=Math.floor(m/60); const mm=m%60; return `${h}h ${mm}m`; } return m ? `${m}m ${sec}s` : `${sec}s`; }
function text(v, fallback='—'){ return (v===null||v===undefined||String(v).trim()==='') ? fallback : String(v); }
function bucketClass(value=''){ return 'fit-' + String(value).toLowerCase().replace(/[^a-z0-9]+/g,'-'); }
function safeMd(v=''){ return String(v || '').slice(0,12000); }
async function api(url, options){ const r = await fetch(url, options); if(!r.ok) throw new Error(`${r.status} ${r.statusText}`); return r.json(); }

function setSlider(id, value, suffix=''){ $(id).value = value; $(`${id}Value`).textContent = suffix === '£' ? fmtMoney(value) : `${value}${suffix}`; }
function wireSlider(id, suffix=''){ $(id).addEventListener('input', () => setSlider(id, $(id).value, suffix)); }

function profilePayload(){
  return {
    overview_prompt: $('overviewPrompt').value,
    origin_airport: $('originAirport').value,
    group: {
      adults: Number($('adults').value || 4),
      children: Number($('children').value || 0),
      child_age: Number($('childAge').value || 5),
      child_ages: [Number($('childAge').value || 5)],
      infants: Number($('infants').value || 0),
      rooms: Number($('rooms').value || 1),
    },
    search_preferences: {
      months: ($('months').value || '').split(',').map(v => Number(v.trim())).filter(Boolean),
      trip_lengths: ($('tripLengths').value || '').split(',').map(v => Number(v.trim())).filter(Boolean),
      min_temp_c: Number($('heatFloor').value || 26),
      target_beach_minutes: Number($('beachTarget').value || 10),
      prefer_free_child_places: $('prefFreeChild').checked,
      prefer_family_room: $('prefFamilyRoom').checked,
      prefer_breakfast_or_better: $('prefBreakfast').checked,
    },
    strategy: { buy_line_gbp: Number($('buyLine').value || 3000), heat_floor_c: Number($('heatFloor').value || 26) },
    preference_scales: {
      strictness: Number($('strictness').value || 72),
      price_slack_gbp: 250,
      beach_slack_minutes: 5,
      temp_slack_c: 2,
      price_weight: 48,
      beach_weight: 22,
      heat_weight: 16,
      family_weight: 10,
      trend_weight: 4,
    },
    orchestration: { target_search_volume: Number($('searchVolume').value || 180), max_live_rows: Number($('searchVolume').value || 180), deep_package_passes: Number($('checkoutDepth').value || 4) },
    provider_execution: { max_checkout_attempts: Number($('checkoutDepth').value || 4) },
  };
}

async function loadConfig(){
  const cfg = await api('/api/config');
  $('overviewPrompt').value = cfg.overview_prompt || '';
  $('originAirport').value = cfg.origin_airport || 'Newcastle';
  $('months').value = (cfg.search_preferences?.months || [6,7]).join(',');
  $('tripLengths').value = (cfg.search_preferences?.trip_lengths || [7,10]).join(',');
  $('rooms').value = cfg.group?.rooms || 2;
  $('adults').value = cfg.group?.adults || 4;
  $('children').value = cfg.group?.children || 1;
  $('childAge').value = cfg.group?.child_age || cfg.group?.child_ages?.[0] || 5;
  $('infants').value = cfg.group?.infants || 1;
  setSlider('buyLine', cfg.strategy?.buy_line_gbp || 3000, '£');
  setSlider('heatFloor', cfg.search_preferences?.min_temp_c || 26, '°C');
  setSlider('beachTarget', cfg.search_preferences?.target_beach_minutes || 10, ' min');
  setSlider('searchVolume', cfg.orchestration?.target_search_volume || 180, '');
  setSlider('strictness', cfg.preference_scales?.strictness || 72, '');
  setSlider('checkoutDepth', cfg.provider_execution?.max_checkout_attempts || 4, '');
  $('prefFreeChild').checked = !!cfg.search_preferences?.prefer_free_child_places;
  $('prefFamilyRoom').checked = !!cfg.search_preferences?.prefer_family_room;
  $('prefBreakfast').checked = !!cfg.search_preferences?.prefer_breakfast_or_better;
  $('profileBanner').textContent = cfg.package_profile_text || 'Ready';
}

async function saveProfile(){
  const current = await api('/api/config');
  const payload = { ...current, ...profilePayload(), group: { ...(current.group || {}), ...(profilePayload().group || {}) }, search_preferences: { ...(current.search_preferences || {}), ...(profilePayload().search_preferences || {}) }, strategy: { ...(current.strategy || {}), ...(profilePayload().strategy || {}) }, preference_scales: { ...(current.preference_scales || {}), ...(profilePayload().preference_scales || {}) }, orchestration: { ...(current.orchestration || {}), ...(profilePayload().orchestration || {}) }, provider_execution: { ...(current.provider_execution || {}), ...(profilePayload().provider_execution || {}) } };
  const res = await api('/api/config', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload) });
  $('profileBanner').textContent = res.package_profile_text || 'Saved';
  await refreshDashboard();
}

function renderStats(stats={}){
  const rows = [
    ['Total results', stats.total_results || 0],
    ['Exact fits', stats.exact_fit || 0],
    ['Near misses', stats.near_miss || 0],
    ['Basket-ish prices', stats.basket_like || 0],
    ['Drop alerts', stats.drop_alerts || 0],
  ];
  $('statsGrid').innerHTML = rows.map(([label, value]) => `<article class="stat-card"><div class="label">${label}</div><div class="value">${value}</div></article>`).join('');
}

function renderProviderPlan(rows=[]){
  $('providerPlan').innerHTML = rows.length ? rows.map((r,i)=>`<div class="plan-row"><div class="rank">Priority ${i+1}</div><strong>${text(r.provider || r.name, 'Provider')}</strong><div class="meta">${text(r.reason || r.execution_reason, 'Planned provider pass')}</div></div>`).join('') : '<div class="empty-card">No provider plan yet.</div>';
  $('planMeta').textContent = rows.length ? `${rows.length} providers planned` : 'No plan';
}

function renderRail(target, rows=[]){
  $(target).innerHTML = rows.length ? rows.map(r => `
    <article class="mini-rec-card">
      <div class="pill-row"><span class="pill ${bucketClass(r.recommendation_bucket || r.fit_label || 'candidate')}">${text(r.recommendation_bucket || r.fit_label, 'Candidate')}</span><span class="pill">${text(r.source_site, 'Source')}</span></div>
      <strong>${text(r.hotel_name || r.destination, 'Unnamed option')}</strong>
      <span>${fmtMoney(r.price_reference_gbp ?? r.basket_price_gbp ?? r.price_total_gbp)}</span>
      <small>${text(r.shortlist_reason || r.recommendation || r.fit_summary, 'Worth a look')}</small>
    </article>`).join('') : '<div class="empty-card">Nothing here yet.</div>';
}

function resultCard(r){
  const takeover = encodeURIComponent(JSON.stringify({source_url:r.source_url||'', evidence_html:r.html_capture_file||'', screenshot_file:r.screenshot_file||'', session_id:r.session_id||'', hotel_name:r.hotel_name||r.destination||'Candidate'}));
  const openAttrs = r.source_url ? `data-open-url="${r.source_url}" role="link" tabindex="0"` : '';
  return `<article class="result-card" ${openAttrs}>
    <div class="result-head">
      <div>
        <div class="pill-row"><span class="pill ${bucketClass(r.fit_label || r.recommendation_bucket || 'candidate')}">${text(r.fit_label || r.recommendation_bucket,'Candidate')}</span><span class="pill">${text(r.pricing_truth_label,'Headline price')}</span><span class="pill">${text(r.source_site,'Unknown')}</span></div>
        <h4>${text(r.hotel_name || r.destination,'Unnamed option')}</h4>
        <div class="subline">${text(r.destination)}${r.country ? ` · ${r.country}` : ''}${r.nights ? ` · ${r.nights} nights` : ''}</div>
      </div>
      <div class="price-box"><div class="price-main">${fmtMoney(r.price_reference_gbp ?? r.basket_price_gbp ?? r.price_total_gbp)}</div><div class="price-sub">headline ${fmtMoney(r.price_total_gbp)} · near-final ${fmtMoney(r.basket_price_gbp)}</div></div>
    </div>
    <p>${text(r.shortlist_reason || r.fit_summary || r.recommendation, 'Candidate for review')}</p>
    <div class="details-grid">
      <div><strong>Heat</strong><span>${r.estimated_temp_c ? `${r.estimated_temp_c}°C` : '—'}</span></div>
      <div><strong>Beach</strong><span>${r.beach_minutes !== undefined && r.beach_minutes !== null ? `${r.beach_minutes} min` : '—'}</span></div>
      <div><strong>Board</strong><span>${text(r.board_basis)}</span></div>
      <div><strong>Pool</strong><span>${r.pool === true ? 'Yes' : r.pool === false ? 'No' : '—'}</span></div>
      <div><strong>Bags</strong><span>${r.bags_included === true ? 'Included' : r.bags_included === false ? 'Unclear' : '—'}</span></div>
      <div><strong>Transfers</strong><span>${r.transfers_included === true ? 'Included' : r.transfers_included === false ? 'Unclear' : '—'}</span></div>
      <div><strong>Rooms</strong><span>${text(r.family_room_type || r.basket_room_text || (r.rooms_matched ? `${r.rooms_matched}/${r.rooms_requested} matched` : '—'))}</span></div>
      <div><strong>Child saver</strong><span>${r.free_child_place ? 'Free child cue' : text(r.child_pricing_note)}</span></div>
      <div><strong>Confidence</strong><span>${r.true_price_confidence ? `${Math.round(r.true_price_confidence*100)}%` : '—'}</span></div>
    </div>
    <div class="reason-row">${text(r.likely_price_direction, '')}${r.likely_price_reason ? ` · ${r.likely_price_reason}` : ''}</div>
    <div class="card-actions">
      <span>${text(r.checkout_stage || r.provider_state, 'landing')} · ${text(r.evidence_note, 'No evidence note')}</span>
      <div class="drawer-actions">
        ${r.source_url ? `<a href="${r.source_url}" target="_blank" rel="noreferrer" class="card-link">View deal</a>` : ''}
        ${(r.session_id || r.html_capture_file || r.screenshot_file) ? `<button class="secondary small" data-takeover="${takeover}">Resume / inspect</button>` : ''}
      </div>
    </div>
  </article>`;
}

function renderResults(rows=[]){
  state.allRows = rows;
  $('resultsGrid').innerHTML = rows.length ? rows.map(resultCard).join('') : '<div class="empty-card">No matching results yet. Run a scan or relax the filters.</div>';
  document.querySelectorAll('[data-takeover]').forEach(btn => btn.addEventListener('click', (ev) => { ev.stopPropagation(); openTakeover(JSON.parse(decodeURIComponent(btn.dataset.takeover))); }));
  document.querySelectorAll('.card-link').forEach(link => link.addEventListener('click', ev => ev.stopPropagation()));
  document.querySelectorAll('[data-open-url]').forEach(card => {
    const open = () => window.open(card.dataset.openUrl, '_blank', 'noopener,noreferrer');
    card.addEventListener('click', (ev) => {
      if (ev.target.closest('button, a, input, select, textarea, label')) return;
      open();
    });
    card.addEventListener('keydown', (ev) => {
      if (ev.key === 'Enter' || ev.key === ' ') { ev.preventDefault(); open(); }
    });
  });
  renderMap(rows);
  const sources = [...new Set(rows.map(r => r.source_site).filter(Boolean))].sort();
  $('sourceSite').innerHTML = '<option value="">All sources</option>' + sources.map(s => `<option value="${s}">${s}</option>`).join('');
  const best = rows[0];
  $('bestPriceHeadline').textContent = best ? fmtMoney(best.price_reference_gbp ?? best.basket_price_gbp ?? best.price_total_gbp) : '—';
}

function ensureMap(){
  if (state.map || !window.L) return;
  state.map = L.map('map', { zoomControl:true }).setView([38, 18], 4);
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { maxZoom:12, attribution:'&copy; OpenStreetMap contributors' }).addTo(state.map);
}

function aggregateDestinations(rows=[]){
  const groups = {};
  rows.forEach(r => {
    const key = r.destination || r.country;
    if (!key || !DEST_COORDS[key]) return;
    const price = Number(r.price_reference_gbp ?? r.basket_price_gbp ?? r.price_total_gbp ?? NaN);
    if (!groups[key]) groups[key] = { key, coords: DEST_COORDS[key], rows:[], bestPrice:null, bestHeat:null, bestBeach:null, providers:new Set() };
    groups[key].rows.push(r); groups[key].providers.add(r.source_site || 'Unknown');
    if (!Number.isNaN(price) && (groups[key].bestPrice === null || price < groups[key].bestPrice)) groups[key].bestPrice = price;
    if (r.estimated_temp_c !== undefined && r.estimated_temp_c !== null && (groups[key].bestHeat === null || r.estimated_temp_c > groups[key].bestHeat)) groups[key].bestHeat = r.estimated_temp_c;
    if (r.beach_minutes !== undefined && r.beach_minutes !== null && (groups[key].bestBeach === null || r.beach_minutes < groups[key].bestBeach)) groups[key].bestBeach = r.beach_minutes;
  });
  let out = Object.values(groups).sort((a,b) => (a.bestPrice ?? 1e9) - (b.bestPrice ?? 1e9));
  if(state.activeLegendBand){
    out = out.filter(dest => {
      const v = dest.bestPrice;
      if(v===null || v===undefined) return state.activeLegendBand === "unknown";
      if(state.activeLegendBand === "budget") return v <= 2200;
      if(state.activeLegendBand === "value") return v > 2200 && v <= 3000;
      if(state.activeLegendBand === "premium") return v > 3000 && v <= 3600;
      if(state.activeLegendBand === "high") return v > 3600;
      return true;
    });
  }
  return out;
}
function priceColor(v){ if (v===null || v===undefined) return '#94a3b8'; if (v<=2200) return '#22c55e'; if (v<=3000) return '#84cc16'; if (v<=3600) return '#f59e0b'; return '#ef4444'; }
function focusDestination(name){ $('query').value = name; refreshResults(); }
window.focusDestination = focusDestination;
function renderMap(rows=[]){
  if (!window.L) return;
  ensureMap();
  state.markers.forEach(m=>m.remove()); state.markers=[];
  const destinations = aggregateDestinations(rows);
  const bounds=[];
  destinations.slice(0,40).forEach(dest => {
    const marker = L.circleMarker(dest.coords,{ radius:Math.max(7,Math.min(18,6+dest.rows.length)), color:priceColor(dest.bestPrice), fillColor:priceColor(dest.bestPrice), fillOpacity:.55, weight:2 }).addTo(state.map);
    marker.bindPopup(`<strong>${dest.key}</strong><br>Best seen ${fmtMoney(dest.bestPrice)}<br>${dest.rows.length} options · ${dest.providers.size} providers<br><button onclick="focusDestination('${String(dest.key).replace(/'/g,"&#39;")}')">Show these deals</button>`);
    marker.on('click',()=>focusDestination(dest.key));
    state.markers.push(marker); bounds.push(dest.coords);
  });
  $('mapCaption').textContent = destinations.length ? `${destinations.length} destinations on the map` : 'No mapped destinations yet';
  $('mapLegend').innerHTML = [
    ['budget','#22c55e','Budget sweet spot','Best seen up to £2,200'],
    ['value','#84cc16','Strong value','Best seen from £2,201 to £3,000'],
    ['premium','#f59e0b','Stretch but interesting','Best seen from £3,001 to £3,600'],
    ['high','#ef4444','Higher price','Best seen above £3,600']
  ].map(([key,color,title,desc]) => `<button class="legend-chip ${state.activeLegendBand===key?'active':''}" data-legend-key="${key}"><i class="map-dot" style="background:${color}"></i><span><strong>${title}</strong><small>${desc}</small></span></button>`).join('') + `<button class="legend-chip ${state.activeLegendBand===''?'active':''}" data-legend-key=""><span><strong>Show all</strong><small>Clear the map filter</small></span></button>`;
  $('mapList').innerHTML = destinations.slice(0,10).map(dest => `<div class="map-destination" onclick="focusDestination('${String(dest.key).replace(/'/g,"&#39;")}')"><div><strong>${dest.key}</strong><small>${dest.rows.length} options · ${dest.providers.size} providers · beach ${dest.bestBeach ?? '—'} min · heat ${dest.bestHeat ?? '—'}°C</small></div><div><strong>${fmtMoney(dest.bestPrice)}</strong><small>tap to show deals</small></div></div>`).join('') || '<div class="empty-card">No destination summaries yet.</div>';
  document.querySelectorAll('[data-legend-key]').forEach(btn => btn.addEventListener('click', () => { state.activeLegendBand = btn.dataset.legendKey; renderMap(state.allRows); }));
  if (bounds.length) state.map.fitBounds(bounds, { padding:[24,24] });
}

function jobLabel(job){ return `${job.kind || 'job'} · ${job.status || 'queued'}`; }
function setActiveJob(job){
  if(!job){
    $('activeJobSummary').textContent = 'Idle';
    $('jobStageLabel').textContent = 'Waiting to start';
    $('jobPctLabel').textContent = '0%';
    $('jobProgressBar').style.width = '0%';
    $('jobStatusText').textContent = 'No search running right now.';
    $('jobTiming').textContent = '—';
    return;
  }
  const pct = Math.max(0, Math.min(100, Number(job.progress_pct || 0)));
  $('activeJobSummary').textContent = `${text(job.kind,'run')} ${job.status === 'running' ? 'running' : job.status}`;
  $('jobStageLabel').textContent = text(job.progress_label || job.status_message || job.status, 'Running');
  $('jobPctLabel').textContent = `${Math.round(pct)}%`;
  $('jobProgressBar').style.width = `${pct}%`;
  $('jobStatusText').textContent = text(job.status_message || job.progress_label || 'Running');
  $('jobTiming').textContent = `Elapsed ${fmtDuration(job.elapsed_seconds)}${job.eta_seconds !== null && job.eta_seconds !== undefined ? ` · about ${fmtDuration(job.eta_seconds)} left` : ''}`;
}

function renderJobs(rows=[]){
  state.jobs = rows;
  const active = rows.find(r => r.status === 'running') || rows.find(r => r.status === 'queued') || null;
  setActiveJob(active);
  $('jobsList').innerHTML = rows.length ? rows.map(job => `<div class="job-card"><strong>${jobLabel(job)}</strong><div class="meta">${text(job.status_message || job.progress_label || 'Working')} · ${job.progress_pct !== undefined ? `${Math.round(Number(job.progress_pct)||0)}%` : ''}</div><div class="job-progress-mini"><div style="width:${Math.max(0, Math.min(100, Number(job.progress_pct || 0)))}%"></div></div><div class="meta">Elapsed ${fmtDuration(job.elapsed_seconds)}${job.eta_seconds !== null && job.eta_seconds !== undefined ? ` · ${fmtDuration(job.eta_seconds)} left` : ''}</div><div class="drawer-actions">${job.status === 'running' ? `<button class="ghost small" data-cancel-job="${job.id}">Cancel</button>` : ''}</div></div>`).join('') : '<div class="empty-card">No background runs yet.</div>';
  document.querySelectorAll('[data-cancel-job]').forEach(btn => btn.addEventListener('click', async()=>{ await api(`/api/jobs/${btn.dataset.cancelJob}/cancel`, {method:'POST'}); await refreshJobs(); }));
}

function renderOutputs(outputs={}){
  $('out_truth').textContent = safeMd(outputs['truth_ranked_shortlist.md'] || 'No shortlist yet.');
  $('out_brief').textContent = safeMd(outputs['operator_briefing_pack.md'] || 'No operator pack yet.');
  $('out_tuning').textContent = safeMd(outputs['provider_tuning_report.md'] || 'No provider tuning report yet.');
  $('out_history').textContent = safeMd(outputs['historical_pricing_report.md'] || 'No history report yet.');
}

async function refreshDashboard(){
  const dash = await api('/api/dashboard');
  renderStats(dash.stats || {});
  renderProviderPlan(dash.provider_plan || []);
  renderRail('bestRail', dash.best_now || []);
  renderRail('worthRail', dash.worth_looking_at || []);
  renderOutputs(dash.outputs || {});
  renderJobs(dash.jobs || []);
  $('heroTitle').textContent = dash.best_now?.length ? `Best now: ${text(dash.best_now[0].hotel_name || dash.best_now[0].destination, 'Holiday shortlist ready')}` : 'Holiday shortlist ready';
  $('heroText').textContent = dash.best_now?.length ? text(dash.best_now[0].shortlist_reason || dash.best_now[0].recommendation || dash.best_now[0].fit_summary, 'The engine has fresh options ready.') : 'Run a scan to generate fresh results.';
}

async function refreshJobs(){ const jobs = await api('/api/jobs'); renderJobs(jobs.rows || []); }

async function refreshResults(){
  const payload = {
    query: $('query').value,
    max_price: Number($('buyLine').value || 3000),
    min_temp: Number($('heatFloor').value || 26),
    beach_max_minutes: Number($('beachTarget').value || 10),
    require_pool: $('poolOnly').checked,
    breakfast_or_better: $('breakfastOnly').checked,
    alerts_only: $('alertsOnly').checked,
    free_child_only: $('freeChildOnly').checked,
    family_room_only: $('familyRoomOnly').checked,
    source_site: $('sourceSite').value,
    sort_by: $('sortBy').value,
  };
  const data = await api('/api/query-deals', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload) });
  renderResults(data.rows || []);
}

async function startBackground(endpoint, body){
  const res = await api(endpoint, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) });
  await refreshJobs();
  await refreshDashboard();
  return res;
}

function openTakeover(data){
  state.currentTakeover = data;
  $('takeoverDrawer').classList.remove('hidden');
  $('takeoverTitle').textContent = `Resume ${data.hotel_name || 'provider flow'}`;
  $('takeoverMeta').textContent = [data.session_id ? `Session ${data.session_id}` : '', data.evidence_html ? `HTML ${data.evidence_html}` : '', data.screenshot_file ? `Screenshot ${data.screenshot_file}` : ''].filter(Boolean).join(' · ');
}
async function doTakeover(){
  if (!state.currentTakeover) return;
  await api('/api/manual-takeover/open', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(state.currentTakeover) });
}

function bind(){
  wireSlider('buyLine', '£'); wireSlider('heatFloor', '°C'); wireSlider('beachTarget', ' min'); wireSlider('searchVolume', ''); wireSlider('strictness', ''); wireSlider('checkoutDepth', '');
  $('saveProfileBtn').addEventListener('click', saveProfile);
  $('demoBtn').addEventListener('click', async()=>{ await saveProfile(); await startBackground('/api/live-scan-background', { mode:'cheapest', limit:60, max_flight_queries:12, headed:false }); });
  $('liveBtn').addEventListener('click', async()=>{ await saveProfile(); await startBackground('/api/live-scan-background', { mode:'cheapest', limit:Number($('searchVolume').value || 180), max_flight_queries:40, headed:false }); });
  $('autoBtn').addEventListener('click', async()=>{ await saveProfile(); await startBackground('/api/autopilot-background', { mode:'cheapest', limit:Number($('searchVolume').value || 180), max_flight_queries:40, headed:false }); });
  $('notifyBtn').addEventListener('click', async()=>{ await startBackground('/api/notify-alerts-background', { threshold: Number($('buyLine').value || 3000) }); });
  $('reloadBtn').addEventListener('click', async()=>{ await refreshDashboard(); await refreshResults(); });
  $('clearBtn').addEventListener('click', ()=>{ $('query').value=''; $('sourceSite').value=''; $('sortBy').value='best'; ['poolOnly','breakfastOnly','alertsOnly','freeChildOnly','familyRoomOnly'].forEach(id => $(id).checked=false); refreshResults(); });
  ['query','sourceSite','sortBy','poolOnly','breakfastOnly','alertsOnly','freeChildOnly','familyRoomOnly'].forEach(id => $(id).addEventListener('input', refreshResults));
  ['buyLine','heatFloor','beachTarget'].forEach(id => $(id).addEventListener('change', refreshResults));
  $('closeTakeover').addEventListener('click', ()=> $('takeoverDrawer').classList.add('hidden'));
  $('takeoverOpen').addEventListener('click', doTakeover);
}

async function init(){
  bind();
  await loadConfig();
  await refreshDashboard();
  await refreshResults();
  setInterval(async()=>{ await refreshJobs(); await refreshDashboard(); }, 5000);
}

init().catch(err => {
  console.error(err);
  $('heroTitle').textContent = 'The interface hit a snag loading.';
  $('heroText').textContent = err.message || 'Unknown error';
});
