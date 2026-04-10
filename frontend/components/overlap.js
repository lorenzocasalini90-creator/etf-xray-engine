/**
 * Overlap & Redundancy section — redundancy cards + heatmap.
 */
import { sanitize, fmtEur, fmtPct } from './sanitize.js';
import { renderHeatmap } from '../charts/heatmap.js';
import { makeInfoIcon } from './tooltip.js';

export function renderOverlap(container, data) {
  const { redundancy, overlap, insights } = data;
  container.textContent = '';
  container.classList.add('fade-in');

  const header = _makeHeader('2', 'Overlap & Ridondanza', 'Quanto si sovrappongono i tuoi ETF');
  container.appendChild(header);

  const introRow = document.createElement('div');
  introRow.style.cssText =
    'margin-bottom:20px;padding:12px 16px;' +
    'background:var(--navy-pale);border-radius:8px;' +
    'font-size:12px;color:var(--text-s);line-height:1.6;';
  introRow.textContent =
    'La ridondanza misura quanta parte di un ETF è già coperta ' +
    'dagli altri ETF nel portafoglio. Un ETF con ridondanza 99% ' +
    'significa che quasi tutte le sue holdings sono già presenti ' +
    'altrove — stai pagando le commissioni senza aggiungere ' +
    'diversificazione reale. La percentuale è calcolata come ' +
    'peso sovrapposto normalizzato sul peso totale dell\u2019ETF.';
  container.appendChild(introRow);

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

      const isinSmall = document.createElement('div');
      isinSmall.style.cssText =
        'font-size:9px;color:var(--text-t);' +
        'font-family:monospace;margin-bottom:4px;' +
        'letter-spacing:0.3px;word-break:break-all;';
      isinSmall.textContent = r.etf_ticker;

      const pct = document.createElement('div');
      pct.className = 'red-pct ' + pctClass;
      pct.textContent = fmtPct(r.redundancy_pct);

      const pctRow = document.createElement('div');
      pctRow.style.cssText = 'display:flex;align-items:center;gap:4px;';
      const pctTip = makeInfoIcon(
        'Percentuale del peso di questo ETF già coperta dagli ' +
        'altri ETF nel portafoglio. Più alto = più ridondante.',
        { dark: false }
      );
      pctRow.append(pct, pctTip);

      const progWrap = document.createElement('div');
      progWrap.className = 'progress';
      const progFill = document.createElement('div');
      progFill.className = 'progress-fill ' + pctClass;
      progFill.style.width = Math.min(r.redundancy_pct, 100) + '%';
      progWrap.appendChild(progFill);

      const detail = document.createElement('div');
      detail.className = 'red-detail';
      const coveredBy = r.covered_by.map(obj => {
        return Object.entries(obj)
          .map(([k, v]) => _displayName(sanitize(k)) + ' ' + fmtPct(v))
          .join(', ');
      }).join(', ');
      detail.textContent = (coveredBy ? 'Coperto da: ' + coveredBy : '') +
        (r.ter_waste_eur > 0 ? ' \u00B7 TER sprecato: ' + fmtEur(r.ter_waste_eur) : '');

      card.append(ticker, isinSmall, pctRow, progWrap, detail);
      grid.appendChild(card);
    });
    container.appendChild(grid);
  }

  // Heatmap
  if (overlap.matrix && overlap.matrix.length > 1) {
    const hmTitleRow = document.createElement('div');
    hmTitleRow.style.cssText =
      'display:flex;align-items:center;gap:6px;' +
      'margin-top:24px;margin-bottom:10px;';
    const hmTitleEl = document.createElement('div');
    hmTitleEl.className = 'card-title';
    hmTitleEl.style.margin = '0';
    hmTitleEl.textContent = 'Matrice Overlap (Jaccard pesato)';
    const jaccardTip = makeInfoIcon(
      'L\u2019indice di Jaccard pesato misura la % di esposizione ' +
      'condivisa tra due ETF, pesata per i pesi nel portafoglio. ' +
      'Diverso dal semplice conteggio di holdings comuni: due ETF ' +
      'con molte holdings condivise ma di piccolo peso avranno ' +
      'Jaccard basso.',
      { dark: false }
    );
    hmTitleRow.append(hmTitleEl, jaccardTip);
    container.appendChild(hmTitleRow);

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

function _displayName(id) {
  if (!id) return '—';
  if (id.length <= 6) return id;
  return id.substring(0, 8) + '…';
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
