/**
 * Overlap & Redundancy section — redundancy cards + heatmap.
 */
import { sanitize, fmtEur, fmtPct } from './sanitize.js';
import { renderHeatmap } from '../charts/heatmap.js';

export function renderOverlap(container, data) {
  const { redundancy, overlap, insights } = data;
  container.textContent = '';
  container.classList.add('fade-in');

  const header = _makeHeader('2', 'Overlap & Ridondanza', 'Quanto si sovrappongono i tuoi ETF');
  container.appendChild(header);

  // Edge case: 1 ETF — show empty state
  if (!redundancy || redundancy.length < 2) {
    const empty = document.createElement('div');
    empty.className = 'card';
    empty.style.textAlign = 'center';
    empty.style.padding = '40px 20px';
    const msg = document.createElement('p');
    msg.style.cssText = 'color:var(--text-t);font-size:13px';
    msg.textContent = 'Aggiungi almeno 2 ETF per vedere l\u2019analisi di overlap e ridondanza.';
    empty.appendChild(msg);
    container.appendChild(empty);
    return;
  }

  // Alert banner for TER waste
  const totalWaste = redundancy.reduce((s, r) => s + r.ter_waste_eur, 0);
  if (totalWaste > 0) {
    const banner = document.createElement('div');
    banner.className = 'alert-banner warning';
    const icon = document.createElement('span');
    icon.className = 'alert-icon';
    icon.textContent = '\uD83D\uDCB8';
    const text = document.createElement('span');
    text.textContent = 'Stai pagando circa ' + fmtEur(totalWaste) +
      '/anno in commissioni su holdings duplicate tra i tuoi ETF.';
    banner.append(icon, text);
    container.appendChild(banner);
  }

  // Redundancy cards
  if (redundancy.length > 0) {
    const cardTitle = document.createElement('div');
    cardTitle.className = 'card-title';
    cardTitle.textContent = 'Ridondanza per ETF';
    cardTitle.style.marginBottom = '14px';
    container.appendChild(cardTitle);

    const grid = document.createElement('div');
    grid.className = 'redundancy-grid';
    redundancy.forEach(r => {
      const verdict = r.redundancy_pct < 30 ? 'green' : r.redundancy_pct < 70 ? 'yellow' : 'red';
      const pctClass = r.redundancy_pct < 30 ? 'green' : r.redundancy_pct < 70 ? 'amber' : 'red';
      const card = document.createElement('div');
      card.className = 'red-card verdict-' + verdict;

      const ticker = document.createElement('div');
      ticker.className = 'red-ticker';
      ticker.textContent = _displayName(r.etf_ticker);
      ticker.title = r.etf_ticker;

      const pct = document.createElement('div');
      pct.className = 'red-pct ' + pctClass;
      pct.textContent = fmtPct(r.redundancy_pct);

      const progWrap = document.createElement('div');
      progWrap.className = 'progress';
      const progFill = document.createElement('div');
      progFill.className = 'progress-fill ' + pctClass;
      progFill.style.width = Math.min(r.redundancy_pct, 100) + '%';
      progWrap.appendChild(progFill);

      const detail = document.createElement('div');
      detail.className = 'red-detail';
      const coveredBy = r.covered_by.map(obj => {
        const entries = Object.entries(obj);
        return entries.map(([k, v]) => sanitize(k) + ' ' + fmtPct(v)).join(', ');
      }).join(', ');
      detail.textContent = (coveredBy ? 'Coperto da: ' + coveredBy : '') +
        (r.ter_waste_eur > 0 ? ' \u00B7 TER sprecato: ' + fmtEur(r.ter_waste_eur) : '');

      card.append(ticker, pct, progWrap, detail);
      if (r.etf_name) {
        const nameSmall = document.createElement('div');
        nameSmall.style.cssText =
          'font-size:10px;color:var(--text-t);margin-bottom:6px;' +
          'white-space:nowrap;overflow:hidden;text-overflow:ellipsis;';
        nameSmall.textContent = sanitize(r.etf_name);
        card.insertBefore(nameSmall, pct);
      }
      grid.appendChild(card);
    });
    container.appendChild(grid);
  }

  // Heatmap
  if (overlap.matrix && overlap.matrix.length > 1) {
    const hmTitle = document.createElement('div');
    hmTitle.className = 'card-title';
    hmTitle.textContent = 'Matrice Overlap (Jaccard pesato)';
    hmTitle.style.marginTop = '24px';
    container.appendChild(hmTitle);

    const legend = document.createElement('div');
    legend.style.cssText =
      'display:flex;gap:12px;align-items:center;' +
      'margin-bottom:10px;flex-wrap:wrap;';
    const items = [
      { color: '#F0FDF4', border: '#BBF7D0', label: 'Minima (<15%)' },
      { color: '#FEF3C7', border: '#FDE68A', label: 'Bassa (15-35%)' },
      { color: '#FEE2E2', border: '#FECACA', label: 'Alta (35-50%)' },
      { color: '#FF6B6B', border: '#FF6B6B', label: 'Critica (>50%)' },
    ];
    items.forEach(item => {
      const el = document.createElement('span');
      el.style.cssText = 'display:flex;align-items:center;gap:5px;' +
        'font-size:11px;color:var(--text-s);';
      const dot = document.createElement('span');
      dot.style.cssText =
        'width:12px;height:12px;border-radius:3px;flex-shrink:0;' +
        'background:' + item.color + ';border:1px solid ' + item.border + ';';
      el.append(dot, document.createTextNode(item.label));
      legend.appendChild(el);
    });
    container.appendChild(legend);

    const hmCard = document.createElement('div');
    hmCard.className = 'card';
    const hmContainer = document.createElement('div');
    hmContainer.id = 'heatmap-container';
    hmContainer.className = 'plotly-chart';
    hmCard.appendChild(hmContainer);
    container.appendChild(hmCard);

    requestAnimationFrame(() => {
      renderHeatmap('heatmap-container', overlap.matrix, overlap.tickers);
    });
  }
}

function _displayName(ticker) {
  if (!ticker) return '—';
  if (ticker.length <= 10) return ticker;
  return ticker.substring(0, 8) + '…';
}

function _makeHeader(num, title, desc) {
  const header = document.createElement('div');
  header.className = 'section-header';
  const numEl = document.createElement('span');
  numEl.className = 'section-num';
  numEl.textContent = num;
  const titleEl = document.createElement('span');
  titleEl.className = 'section-title';
  titleEl.textContent = title;
  const descEl = document.createElement('span');
  descEl.className = 'section-desc';
  descEl.textContent = desc;
  header.append(numEl, titleEl, descEl);
  return header;
}
