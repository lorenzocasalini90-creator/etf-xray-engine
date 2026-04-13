/**
 * AI Analysis premium card — two-path funnel (no auto-modal).
 */

const FORM_BASE =
  'https://docs.google.com/forms/d/e/' +
  '1FAIpQLSd-bFJg9H5OyeeAmZXJTdSBen-EEyX9UkeUuLS-nXl_r2V5AQ/' +
  'viewform?entry.1367586493=';

/**
 * @param {HTMLElement} container
 * @param {object} analysisData — full API response
 * @param {Array} positions — original positions array
 */
export function renderAICard(container, analysisData, positions) {
  container.textContent = '';

  // ── Navy CTA card (always visible, no modal on load) ──
  const card = document.createElement('div');
  card.className = 'card ai-premium-card';
  card.style.cssText =
    'background:var(--navy);color:#fff;padding:24px;' +
    'border-radius:12px;position:relative;overflow:hidden;';

  // Header row
  const hdrRow = document.createElement('div');
  hdrRow.style.cssText = 'display:flex;align-items:center;gap:10px;margin-bottom:6px;';

  const title = document.createElement('span');
  title.style.cssText = 'font-size:18px;font-weight:700;';
  title.textContent = '\uD83E\uDD16 Analisi AI';

  const badge = document.createElement('span');
  badge.style.cssText =
    'background:#22C55E;color:#fff;font-size:10px;font-weight:700;' +
    'padding:2px 8px;border-radius:99px;letter-spacing:0.5px;';
  badge.textContent = 'PRO';

  hdrRow.append(title, badge);
  card.appendChild(hdrRow);

  const desc = document.createElement('p');
  desc.style.cssText = 'font-size:13px;color:rgba(255,255,255,0.7);margin-bottom:16px;';
  desc.textContent = 'Interpretazione personalizzata del tuo portafoglio generata da AI';
  card.appendChild(desc);

  const btn = document.createElement('button');
  btn.style.cssText =
    'background:rgba(255,255,255,0.15);color:#fff;border:1px solid rgba(255,255,255,0.25);' +
    'padding:10px 24px;border-radius:8px;font-size:14px;font-weight:600;' +
    'cursor:pointer;transition:background 0.2s;';
  btn.textContent = 'Scopri Analisi AI \u2192';
  btn.addEventListener('mouseenter', () => { btn.style.background = 'rgba(255,255,255,0.25)'; });
  btn.addEventListener('mouseleave', () => { btn.style.background = 'rgba(255,255,255,0.15)'; });

  // Click → expand inline options (no modal)
  btn.addEventListener('click', () => {
    btn.remove();
    _showFunnelOptions(card, container, analysisData, positions);
  });

  card.appendChild(btn);
  container.appendChild(card);
}


// ── Two-path funnel (inline, inside the navy card) ──

function _showFunnelOptions(card, parentContainer, analysisData, positions) {
  const optionsWrap = document.createElement('div');
  optionsWrap.style.cssText =
    'display:flex;gap:16px;flex-wrap:wrap;margin-top:4px;';

  // ── Left column: "Hai già accesso?" ──
  const leftCol = document.createElement('div');
  leftCol.style.cssText = 'flex:1;min-width:200px;';

  const leftLabel = document.createElement('p');
  leftLabel.style.cssText =
    'font-size:12px;color:rgba(255,255,255,0.6);margin-bottom:8px;font-weight:600;';
  leftLabel.textContent = 'Hai già accesso?';
  leftCol.appendChild(leftLabel);

  const input = document.createElement('input');
  input.type = 'email';
  input.placeholder = 'La tua email';
  input.style.cssText =
    'width:100%;padding:9px 12px;border:1px solid rgba(255,255,255,0.3);' +
    'border-radius:8px;font-size:13px;background:rgba(255,255,255,0.1);' +
    'color:#fff;outline:none;margin-bottom:8px;';
  input.addEventListener('focus', () => { input.style.borderColor = 'rgba(255,255,255,0.6)'; });
  input.addEventListener('blur', () => { input.style.borderColor = 'rgba(255,255,255,0.3)'; });
  leftCol.appendChild(input);

  const submitBtn = document.createElement('button');
  submitBtn.style.cssText =
    'width:100%;padding:9px;border-radius:8px;border:none;' +
    'background:#22C55E;color:#fff;font-size:13px;font-weight:600;cursor:pointer;';
  submitBtn.textContent = 'Accedi \u2192';
  leftCol.appendChild(submitBtn);

  const msgArea = document.createElement('div');
  msgArea.style.cssText = 'margin-top:8px;';
  leftCol.appendChild(msgArea);

  // ── Right column: "Non hai ancora accesso?" ──
  const rightCol = document.createElement('div');
  rightCol.style.cssText = 'flex:1;min-width:200px;';

  const rightLabel = document.createElement('p');
  rightLabel.style.cssText =
    'font-size:12px;color:rgba(255,255,255,0.6);margin-bottom:8px;font-weight:600;';
  rightLabel.textContent = 'Non hai ancora accesso?';
  rightCol.appendChild(rightLabel);

  const waitlistBtn = document.createElement('a');
  waitlistBtn.href = FORM_BASE;
  waitlistBtn.target = '_blank';
  waitlistBtn.rel = 'noopener noreferrer';
  waitlistBtn.style.cssText =
    'display:block;text-align:center;padding:9px;border-radius:8px;' +
    'border:1px solid rgba(255,255,255,0.3);background:transparent;' +
    'color:#fff;font-size:13px;font-weight:600;text-decoration:none;' +
    'cursor:pointer;transition:background 0.2s;';
  waitlistBtn.textContent = 'Unisciti alla lista Pro \u2192';
  waitlistBtn.addEventListener('mouseenter', () => {
    waitlistBtn.style.background = 'rgba(255,255,255,0.1)';
  });
  waitlistBtn.addEventListener('mouseleave', () => {
    waitlistBtn.style.background = 'transparent';
  });
  rightCol.appendChild(waitlistBtn);

  optionsWrap.append(leftCol, rightCol);
  card.appendChild(optionsWrap);

  // Focus input
  input.focus();

  // Submit handlers
  submitBtn.addEventListener('click', () => {
    const email = input.value.trim();
    if (!email) {
      input.style.borderColor = 'var(--coral)';
      return;
    }
    _handleSubmit(email, msgArea, waitlistBtn, analysisData, positions, parentContainer);
  });

  input.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') submitBtn.click();
  });
}


async function _handleSubmit(email, msgArea, waitlistBtn, analysisData, positions, parentContainer) {
  msgArea.textContent = '';
  const loadingP = document.createElement('p');
  loadingP.style.cssText = 'font-size:12px;color:rgba(255,255,255,0.7);';
  loadingP.textContent = 'Verifico accesso...';
  msgArea.appendChild(loadingP);

  try {
    const checkRes = await fetch('/api/check-premium', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email }),
    });
    const checkData = await checkRes.json();

    if (!checkData.access) {
      _showNotInList(msgArea, waitlistBtn, email);
      return;
    }

    // Build portfolio_summary from analysis data
    const kpis = analysisData.kpis || {};
    const redundancy = analysisData.redundancy || [];
    const factors = analysisData.factors || {};
    const holdings = analysisData.holdings || [];
    const countryExposure = analysisData.country_exposure || [];

    const highRedundancy = redundancy
      .filter(r => r.redundancy_pct > 50)
      .map(r => r.etf_ticker);

    const factorTilts = (factors.dimensions || [])
      .filter(d => d.tilt && d.tilt !== 'Neutral')
      .map(d => d.name + ' ' + d.tilt);

    const topHolding = holdings.length > 0 ? holdings[0] : null;

    const usEntry = countryExposure.find(
      c => c.label === 'United States' || c.label === 'US' || c.label === 'Stati Uniti'
    );

    const summary = {
      unique_securities: kpis.unique_securities || null,
      hhi: kpis.hhi || null,
      active_share: kpis.active_share || null,
      high_redundancy_etfs: highRedundancy.length ? highRedundancy : null,
      top_holding: topHolding ? topHolding.name : null,
      top_weight: topHolding ? topHolding.weight_pct : null,
      us_weight: usEntry ? usEntry.portfolio_pct : null,
      factor_profile: factorTilts.length ? factorTilts.join(', ') : null,
    };

    loadingP.textContent = 'Genero analisi AI... \u23F3';

    const aiRes = await fetch('/api/ai-analysis', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email, portfolio_summary: summary }),
    });

    if (!aiRes.ok) {
      const errBody = await aiRes.json().catch(() => ({}));
      const errDetail = errBody.detail || 'Errore sconosciuto';
      console.error('[AI Analysis] HTTP', aiRes.status, errDetail);
      msgArea.textContent = '';
      const errP = document.createElement('p');
      errP.style.cssText = 'font-size:12px;color:var(--coral);';
      errP.textContent = aiRes.status === 503
        ? 'Servizio AI non ancora configurato.'
        : 'Errore nell\'analisi AI. Riprova.';
      msgArea.appendChild(errP);
      return;
    }

    const aiData = await aiRes.json();
    _showAIResult(parentContainer, aiData);

  } catch {
    msgArea.textContent = '';
    const errP = document.createElement('p');
    errP.style.cssText = 'font-size:12px;color:var(--coral);';
    errP.textContent = 'Errore di connessione. Riprova.';
    msgArea.appendChild(errP);
  }
}


function _showNotInList(msgArea, waitlistBtn, email) {
  const encoded = encodeURIComponent(email);
  msgArea.textContent = '';

  const msg = document.createElement('p');
  msg.style.cssText = 'font-size:12px;color:rgba(255,255,255,0.8);margin-bottom:6px;';
  msg.textContent = 'La tua email non ha accesso. Vuoi unirti alla lista?';
  msgArea.appendChild(msg);

  // Update waitlist button to include email
  waitlistBtn.href = FORM_BASE + encoded;

  const link = document.createElement('a');
  link.href = FORM_BASE + encoded;
  link.target = '_blank';
  link.rel = 'noopener noreferrer';
  link.style.cssText =
    'display:inline-block;background:rgba(255,255,255,0.15);color:#fff;' +
    'padding:8px 16px;border-radius:8px;font-size:12px;font-weight:600;' +
    'text-decoration:none;border:1px solid rgba(255,255,255,0.3);';
  link.textContent = 'Unisciti alla lista Pro \u2192';
  msgArea.appendChild(link);
}


function _showAIResult(container, aiData) {
  container.textContent = '';

  const resultCard = document.createElement('div');
  resultCard.className = 'card';
  resultCard.style.cssText =
    'border:2px solid var(--navy);border-radius:12px;padding:20px;';

  // Header
  const hdr = document.createElement('div');
  hdr.style.cssText = 'display:flex;align-items:center;gap:10px;margin-bottom:14px;';

  const hdrTitle = document.createElement('span');
  hdrTitle.style.cssText = 'font-size:16px;font-weight:700;';
  hdrTitle.textContent = '\uD83E\uDD16 Analisi AI';

  const hdrBadge = document.createElement('span');
  hdrBadge.style.cssText =
    'background:#22C55E;color:#fff;font-size:10px;font-weight:700;' +
    'padding:2px 8px;border-radius:99px;letter-spacing:0.5px;';
  hdrBadge.textContent = 'PRO';

  hdr.append(hdrTitle, hdrBadge);
  resultCard.appendChild(hdr);

  // Summary
  if (aiData.summary) {
    const sum = document.createElement('p');
    sum.style.cssText = 'font-size:14px;line-height:1.6;margin-bottom:16px;color:var(--text-p);';
    sum.textContent = aiData.summary;
    resultCard.appendChild(sum);
  }

  // Action cards
  const PRIO_COLORS = {
    alta: { border: 'var(--coral)', bg: 'var(--red-pale)' },
    media: { border: 'var(--amber)', bg: 'var(--amber-pale)' },
    bassa: { border: 'var(--teal)', bg: 'var(--green-pale)' },
  };

  const actions = aiData.actions || [];
  for (const action of actions) {
    const prio = PRIO_COLORS[action.priority] || PRIO_COLORS.bassa;
    const ac = document.createElement('div');
    ac.style.cssText =
      'border-left:4px solid ' + prio.border + ';' +
      'background:' + prio.bg + ';' +
      'padding:12px 14px;border-radius:0 8px 8px 0;margin-bottom:8px;';

    const atitle = document.createElement('div');
    atitle.style.cssText = 'font-size:13px;font-weight:700;margin-bottom:4px;';
    atitle.textContent = action.title;

    const detail = document.createElement('div');
    detail.style.cssText = 'font-size:12px;color:var(--text-s);line-height:1.5;';
    detail.textContent = action.detail;

    ac.append(atitle, detail);
    resultCard.appendChild(ac);
  }

  container.appendChild(resultCard);
}
