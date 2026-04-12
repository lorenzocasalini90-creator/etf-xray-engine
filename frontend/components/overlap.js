/**
 * Overlap & Redundancy section — redundancy cards + heatmap.
 */
import { sanitize, fmtEur, fmtPct } from './sanitize.js';
import { renderHeatmap } from '../charts/heatmap.js';
import { makeInfoIcon } from './tooltip.js';

export function renderOverlap(container, data) {
  const { redundancy, overlap, insights } = data;
  const nameMap = (overlap && overlap.ticker_names) || {};
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

    const isMobileRed = window.innerWidth < 600;
    const withOverlap = isMobileRed
      ? redundancy.filter(r => r.redundancy_pct > 0) : redundancy;
    const zeroOverlap = isMobileRed
      ? redundancy.filter(r => r.redundancy_pct === 0) : [];

    withOverlap.forEach(r => {
      const verdict = r.redundancy_pct < 30 ? 'green' : r.redundancy_pct < 70 ? 'yellow' : 'red';
      const pctClass = r.redundancy_pct < 30 ? 'green' : r.redundancy_pct < 70 ? 'amber' : 'red';
      const card = document.createElement('div');
      card.className = 'red-card verdict-' + verdict;

      const ticker = document.createElement('div');
      ticker.className = 'red-ticker';
      ticker.textContent = _displayName(r.etf_ticker, nameMap);
      ticker.title = nameMap[r.etf_ticker]
        ? nameMap[r.etf_ticker] + ' (' + r.etf_ticker + ')'
        : r.etf_ticker;

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

      const nonZero = r.covered_by
        .flatMap(obj => Object.entries(obj))
        .filter(([, v]) => Number(v) > 0)
        .sort((a, b) => b[1] - a[1]);

      if (nonZero.length === 0) {
        detail.textContent = 'Nessun overlap con gli altri ETF del portafoglio';
        detail.style.color = 'var(--text-t)';
      } else {
        nonZero.forEach(([k, v], i) => {
          if (i > 0) {
            const sep = document.createElement('span');
            sep.textContent = ' \u00B7 ';
            sep.style.color = 'var(--text-t)';
            sep.style.fontSize = '10px';
            detail.appendChild(sep);
          }
          const span = document.createElement('span');
          const style = _overlapColor(Number(v));
          span.style.color = style.color;
          span.style.fontWeight = style.fontWeight;
          span.textContent = _shortName(sanitize(k), nameMap) + ' ' + fmtPct(Number(v));
          detail.appendChild(span);
        });
        if (r.ter_waste_eur > 0) {
          const ter = document.createElement('span');
          ter.style.cssText = 'color:var(--text-t);display:block;margin-top:4px;';
          ter.textContent = 'TER sprecato: ' + fmtEur(r.ter_waste_eur);
          detail.appendChild(ter);
        }
      }

      card.append(ticker, isinSmall, pctRow, progWrap, detail);
      grid.appendChild(card);
    });

    // Mobile: compact summary for zero-overlap ETFs
    if (zeroOverlap.length > 0) {
      const zeroSection = document.createElement('div');
      zeroSection.style.cssText =
        'margin-top:12px;padding:10px 14px;' +
        'background:var(--bg);border-radius:8px;' +
        'border:0.5px solid var(--border);';
      const zeroTitle = document.createElement('div');
      zeroTitle.style.cssText =
        'font-size:11px;font-weight:600;' +
        'color:var(--text-s);margin-bottom:6px;';
      zeroTitle.textContent =
        'ETF senza overlap (' + zeroOverlap.length + ')';
      zeroSection.appendChild(zeroTitle);
      const zeroList = document.createElement('div');
      zeroList.style.cssText =
        'font-size:11px;color:var(--text-t);line-height:1.8;';
      zeroList.textContent = zeroOverlap
        .map(r => _shortName(r.etf_ticker, nameMap))
        .join(' \u00B7 ');
      zeroSection.appendChild(zeroList);
      grid.appendChild(zeroSection);
    }

    container.appendChild(grid);
  }

  // Heatmap (desktop) or pair list (mobile)
  if (overlap.matrix && overlap.matrix.length > 1) {
    const isMobile = window.innerWidth < 600;

    if (isMobile) {
      // Mobile: show sorted pair list instead of heatmap
      const pairs = [];
      const n = overlap.tickers.length;
      overlap.matrix.forEach((row, i) => {
        row.forEach((val, j) => {
          if (j > i && val > 0) {
            pairs.push({ a: overlap.tickers[i], b: overlap.tickers[j], val });
          }
        });
      });
      pairs.sort((a, b) => b.val - a.val);

      const listCard = document.createElement('div');
      listCard.className = 'card';
      listCard.style.marginTop = '16px';

      const listTitle = document.createElement('div');
      listTitle.className = 'card-title';
      listTitle.style.marginBottom = '12px';
      listTitle.textContent = 'Overlap tra ETF (Jaccard)';
      listCard.appendChild(listTitle);

      if (pairs.length === 0) {
        const noData = document.createElement('p');
        noData.style.cssText = 'color:var(--text-t);font-size:12px;';
        noData.textContent = 'Nessun overlap significativo rilevato.';
        listCard.appendChild(noData);
      } else {
        pairs.forEach(p => {
          const row = document.createElement('div');
          row.style.cssText =
            'display:flex;justify-content:space-between;' +
            'align-items:center;padding:8px 0;' +
            'border-bottom:0.5px solid var(--border);' +
            'font-size:12px;';
          const names = document.createElement('span');
          names.style.cssText =
            'flex:1;color:var(--text-p);' +
            'overflow:hidden;text-overflow:ellipsis;' +
            'white-space:nowrap;margin-right:8px;';
          names.textContent =
            _shortName(p.a, nameMap) + ' \u00B7 ' +
            _shortName(p.b, nameMap);
          const val = document.createElement('span');
          const style = _overlapColor(p.val);
          val.style.cssText =
            'color:' + style.color + ';' +
            'font-weight:' + style.fontWeight + ';' +
            'white-space:nowrap;font-size:13px;';
          val.textContent = p.val.toFixed(1) + '%';
          row.append(names, val);
          listCard.appendChild(row);
        });
      }
      container.appendChild(listCard);
    } else {
      // Desktop: Plotly heatmap
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
        renderHeatmap('heatmap-container', overlap.matrix, overlap.tickers, nameMap);
      });
    }
  }
}

function _displayName(id, nameMap = {}) {
  if (!id) return '—';
  if (nameMap[id]) return nameMap[id];
  if (id.length <= 6) return id;
  return id.substring(0, 8) + '…';
}

function _shortName(id, nameMap = {}) {
  if (!id) return '—';
  if (!nameMap[id]) {
    if (id.length <= 6) return id;
    return id.substring(0, 8) + '…';
  }
  const full = nameMap[id];
  const clean = full
    .replace(/\s+(UCITS|ETF|USD|EUR|GBP|Acc|Inc|Distributing|Accumulating)\b.*/gi, '')
    .trim();
  const name = clean || full;
  return name.length > 22 ? name.substring(0, 21) + '…' : name;
}

function _overlapColor(pct) {
  if (pct > 50)  return { color: '#B91C1C', fontWeight: '700' };
  if (pct > 35)  return { color: '#FF6B6B', fontWeight: '600' };
  if (pct > 15)  return { color: '#F97316', fontWeight: '600' };
  if (pct > 0)   return { color: '#F59E0B', fontWeight: '500' };
  return { color: 'var(--text-t)', fontWeight: '400' };
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
