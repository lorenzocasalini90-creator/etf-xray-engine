/**
 * "Cosa fare" — free actionable suggestions based on portfolio analysis.
 */
import { fmtPct, fmtEur } from './sanitize.js';

/**
 * @param {HTMLElement} container
 * @param {object} data — { redundancy, kpis, positions }
 */
export function renderSuggestions(container, data) {
  const { redundancy = [], kpis = {}, positions = [] } = data;
  container.textContent = '';
  container.classList.add('fade-in');

  // Header (same pattern as other sections)
  const header = document.createElement('div');
  header.className = 'section-header';

  const numEl = document.createElement('div');
  numEl.className = 'section-num';
  numEl.textContent = '⑤';

  const titleGroup = document.createElement('div');
  titleGroup.className = 'section-title-group';

  const h2 = document.createElement('h2');
  h2.className = 'section-title';
  h2.textContent = 'Cosa fare';

  const sub = document.createElement('p');
  sub.className = 'section-sub';
  sub.textContent = 'Suggerimenti operativi basati sulla tua analisi';

  titleGroup.append(h2, sub);
  header.append(numEl, titleGroup);
  container.appendChild(header);

  const cards = [];

  // 1. High redundancy (>70%)
  const highRedundancy = redundancy.filter(r => r.redundancy_pct > 70);
  for (const r of highRedundancy) {
    const ticker = r.etf_ticker || '?';
    const pct = r.redundancy_pct;
    const pos = positions.find(p => p.ticker === ticker);
    const amount = pos ? pos.amount_eur : 0;
    const ter = r.ter || 0;
    const terWaste = Math.round(amount * (ter / 100) * (pct / 100));

    let text = '♻️ Ridondanza elevata — ' + ticker +
      ' ha oltre il 70% di holdings già coperte.';
    if (terWaste > 0) {
      text += ' Stai pagando ~' + fmtEur(terWaste) + '/anno in commissioni duplicate.';
    }
    cards.push({ text, priority: 'alta' });
  }

  // 2. Active Share < 20%
  const activeShare = kpis.active_share;
  if (activeShare != null && activeShare < 20) {
    cards.push({
      text: '📋 Closet indexing — Il portafoglio è quasi identico al benchmark (' +
        fmtPct(activeShare) + '). Considera un singolo ETF globale con TER inferiore.',
      priority: 'media',
    });
  }

  // 3. Top 10 concentration > 40%
  const top10 = kpis.top10_weight;
  if (top10 != null && top10 > 40) {
    cards.push({
      text: '⚖️ Alta concentrazione — I top 10 titoli pesano il ' +
        fmtPct(top10) + ' del portafoglio. Verifica sia intenzionale.',
      priority: 'media',
    });
  }

  // 4. No issues
  if (cards.length === 0) {
    cards.push({
      text: '✅ Portafoglio equilibrato — Nessuna criticità rilevata.',
      priority: 'info',
    });
  }

  // Render cards
  const BORDER_COLORS = {
    alta: 'var(--coral)',
    media: 'var(--amber)',
    info: 'var(--teal)',
  };

  const wrap = document.createElement('div');
  wrap.style.cssText = 'display:flex;flex-direction:column;gap:12px;';

  for (const c of cards) {
    const card = document.createElement('div');
    card.className = 'card';
    card.style.cssText =
      'border-left:4px solid ' + (BORDER_COLORS[c.priority] || 'var(--border)') + ';' +
      'padding:14px 16px;font-size:13px;line-height:1.6;';
    card.textContent = c.text;
    wrap.appendChild(card);
  }

  container.appendChild(wrap);
}
