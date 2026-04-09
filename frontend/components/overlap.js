/**
 * Overlap & Redundancy section — redundancy cards + heatmap.
 * Uses DOM API for safe rendering.
 */
import { renderHeatmap } from '../charts/heatmap.js';

export function renderOverlap(container, data) {
  const { redundancy, overlap, insights } = data;
  container.textContent = '';
  container.classList.add('fade-in');

  // Header
  const header = _makeHeader('2', 'Overlap & Ridondanza', 'Quanto si sovrappongono i tuoi ETF');
  container.appendChild(header);

  // Alert banner for TER waste
  const totalWaste = redundancy.reduce((s, r) => s + r.ter_waste_eur, 0);
  if (totalWaste > 0) {
    const banner = document.createElement('div');
    banner.className = 'alert-banner warning';
    const icon = document.createElement('span');
    icon.className = 'alert-icon';
    icon.textContent = '\uD83D\uDCB8';
    const text = document.createElement('span');
    text.textContent = 'Stai pagando circa \u20AC' + totalWaste.toFixed(0) +
      '/anno in commissioni su holdings duplicate tra i tuoi ETF.';
    banner.append(icon, text);
    container.appendChild(banner);
  }

  // Redundancy cards grid
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
      ticker.textContent = r.etf_ticker;

      const pct = document.createElement('div');
      pct.className = 'red-pct ' + pctClass;
      pct.textContent = r.redundancy_pct.toFixed(1) + '%';

      const progWrap = document.createElement('div');
      progWrap.className = 'progress';
      const progFill = document.createElement('div');
      progFill.className = 'progress-fill ' + pctClass;
      progFill.style.width = Math.min(r.redundancy_pct, 100) + '%';
      progWrap.appendChild(progFill);

      const detail = document.createElement('div');
      detail.className = 'red-detail';
      // Covered by info
      const coveredBy = r.covered_by.map(obj => {
        const entries = Object.entries(obj);
        return entries.map(([k, v]) => k + ' ' + v.toFixed(1) + '%').join(', ');
      }).join(', ');
      detail.textContent = (coveredBy ? 'Coperto da: ' + coveredBy : '') +
        (r.ter_waste_eur > 0 ? ' \u00B7 TER sprecato: \u20AC' + r.ter_waste_eur.toFixed(2) : '');

      card.append(ticker, pct, progWrap, detail);
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

    const hmCard = document.createElement('div');
    hmCard.className = 'card';
    const hmContainer = document.createElement('div');
    hmContainer.id = 'heatmap-container';
    hmContainer.className = 'plotly-chart';
    hmCard.appendChild(hmContainer);
    container.appendChild(hmCard);

    // Render after DOM insertion
    requestAnimationFrame(() => {
      renderHeatmap('heatmap-container', overlap.matrix, overlap.tickers);
    });
  }
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
