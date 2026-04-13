/**
 * AI Analysis premium card + modal + result rendering.
 */

const FORM_BASE =
  'https://docs.google.com/forms/d/e/' +
  '1FAIpQLSd-bFJg9H5OyeeAmZXJTdSBen-EEyX9UkeUuLS-nXl_r2V5AQ/' +
  'viewform?entry.1367586493=';

/**
 * @param {HTMLElement} container
 * @param {object} analysisData — full API response (kpis, redundancy, factors, etc.)
 * @param {Array} positions — original positions array
 */
export function renderAICard(container, analysisData, positions) {
  container.textContent = '';

  // ── Premium CTA card ──
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
  title.textContent = '🤖 Analisi AI';

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
  btn.textContent = '🔒 Analisi AI';
  btn.addEventListener('mouseenter', () => { btn.style.background = 'rgba(255,255,255,0.25)'; });
  btn.addEventListener('mouseleave', () => { btn.style.background = 'rgba(255,255,255,0.15)'; });
  btn.addEventListener('click', () => _showModal(container, analysisData, positions));
  card.appendChild(btn);

  container.appendChild(card);
}


function _showModal(parentContainer, analysisData, positions) {
  const existing = document.getElementById('ai-modal-overlay');
  if (existing) existing.remove();

  const overlay = document.createElement('div');
  overlay.id = 'ai-modal-overlay';
  overlay.style.cssText =
    'position:fixed;inset:0;background:rgba(0,0,0,0.5);z-index:1000;' +
    'display:flex;align-items:center;justify-content:center;padding:16px;';
  overlay.addEventListener('click', (e) => {
    if (e.target === overlay) overlay.remove();
  });

  const modal = document.createElement('div');
  modal.style.cssText =
    'background:#fff;border-radius:14px;padding:28px 24px;max-width:420px;' +
    'width:100%;box-shadow:0 20px 60px rgba(0,0,0,0.3);';

  const h3 = document.createElement('h3');
  h3.style.cssText = 'font-size:18px;font-weight:700;margin-bottom:4px;';
  h3.textContent = '🤖 Analisi AI';
  modal.appendChild(h3);

  const subtext = document.createElement('p');
  subtext.style.cssText = 'font-size:13px;color:var(--text-s);margin-bottom:20px;';
  subtext.textContent = 'Inserisci la tua email per accedere';
  modal.appendChild(subtext);

  const input = document.createElement('input');
  input.type = 'email';
  input.placeholder = 'La tua email';
  input.style.cssText =
    'width:100%;padding:10px 14px;border:1px solid var(--border);' +
    'border-radius:8px;font-size:14px;margin-bottom:12px;' +
    'outline:none;text-align:left;';
  input.addEventListener('focus', () => { input.style.borderColor = 'var(--navy)'; });
  input.addEventListener('blur', () => { input.style.borderColor = 'var(--border)'; });
  modal.appendChild(input);

  const submitBtn = document.createElement('button');
  submitBtn.className = 'btn-cta';
  submitBtn.style.cssText =
    'width:100%;padding:12px;font-size:14px;font-weight:600;' +
    'border-radius:8px;border:none;background:var(--navy);color:#fff;cursor:pointer;';
  submitBtn.textContent = 'Accedi →';
  modal.appendChild(submitBtn);

  const msgArea = document.createElement('div');
  msgArea.style.cssText = 'margin-top:14px;';
  modal.appendChild(msgArea);

  submitBtn.addEventListener('click', () => {
    const email = input.value.trim();
    if (!email) {
      input.style.borderColor = 'var(--coral)';
      return;
    }
    _handleSubmit(email, msgArea, analysisData, positions, parentContainer, overlay);
  });

  input.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') submitBtn.click();
  });

  overlay.appendChild(modal);
  document.body.appendChild(overlay);
  input.focus();
}


async function _handleSubmit(email, msgArea, analysisData, positions, parentContainer, overlay) {
  msgArea.textContent = '';
  const loadingP = document.createElement('p');
  loadingP.style.cssText = 'font-size:13px;color:var(--text-s);text-align:center;';
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
      _showNotInList(msgArea, email);
      return;
    }

    // Build portfolio_summary from analysis data
    const kpis = analysisData.kpis || {};
    const redundancy = analysisData.redundancy || [];
    const factors = analysisData.factors || {};

    const highRedundancy = redundancy
      .filter(r => r.redundancy_pct > 50)
      .map(r => r.etf_ticker);

    const factorBadges = (factors.badges || []).map(b => b.label || b).join(', ');

    const summary = {
      unique_securities: kpis.unique_securities || null,
      hhi: kpis.hhi || null,
      active_share: kpis.active_share || null,
      high_redundancy_etfs: highRedundancy.length ? highRedundancy : null,
      top_holding: kpis.top_holding || null,
      top_weight: kpis.top_weight || null,
      us_weight: kpis.us_weight || null,
      factor_profile: factorBadges || null,
    };

    loadingP.textContent = 'Genero analisi AI... ⏳';

    const aiRes = await fetch('/api/ai-analysis', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email, portfolio_summary: summary }),
    });

    if (!aiRes.ok) {
      msgArea.textContent = '';
      const errP = document.createElement('p');
      errP.style.cssText = 'font-size:13px;color:var(--coral);text-align:center;';
      errP.textContent = 'Errore nell\'analisi AI. Riprova.';
      msgArea.appendChild(errP);
      return;
    }

    const aiData = await aiRes.json();
    overlay.remove();
    _showAIResult(parentContainer, aiData);

  } catch {
    msgArea.textContent = '';
    const errP = document.createElement('p');
    errP.style.cssText = 'font-size:13px;color:var(--coral);text-align:center;';
    errP.textContent = 'Errore di connessione. Riprova.';
    msgArea.appendChild(errP);
  }
}


function _showNotInList(msgArea, email) {
  const encoded = encodeURIComponent(email);
  msgArea.textContent = '';

  const wrap = document.createElement('div');
  wrap.style.cssText = 'text-align:center;padding:8px 0;';

  const msg = document.createElement('p');
  msg.style.cssText = 'font-size:14px;font-weight:600;margin-bottom:12px;';
  msg.textContent = 'Non sei ancora nella lista Pro.';

  const link = document.createElement('a');
  link.href = FORM_BASE + encoded;
  link.target = '_blank';
  link.rel = 'noopener noreferrer';
  link.style.cssText =
    'display:inline-block;background:var(--navy);color:#fff;' +
    'padding:10px 20px;border-radius:8px;font-size:13px;font-weight:600;' +
    'text-decoration:none;';
  link.textContent = 'Avvisami al lancio →';

  wrap.append(msg, link);
  msgArea.appendChild(wrap);
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
  hdrTitle.textContent = '🤖 Analisi AI';

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
