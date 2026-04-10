/**
 * Factor Fingerprint section — tilt badges, factor bars, coverage.
 * Uses DOM API for safe rendering.
 */
import { makeInfoIcon } from './tooltip.js';

const FACTOR_TOOLTIPS = {
  'Value/Growth':
    'Punteggio basso = orientato al Value (titoli economici ' +
    'rispetto agli utili). Punteggio alto = orientato al Growth ' +
    '(aziende in forte crescita).',
  'Quality':
    'Misura la solidità finanziaria media dei titoli: ROE, debito, ' +
    'stabilità degli utili.',
  'Size':
    'Capitalizzazione media dei titoli. Vicino a 0 = molte small ' +
    'cap. Vicino a 100 = prevalenza di large cap.',
  'Dividend Yield':
    'Rendimento da dividendi medio ponderato del portafoglio.',
};

export function renderFactor(container, data) {
  const { factors } = data;
  container.textContent = '';
  container.classList.add('fade-in');

  // Header
  const header = _makeHeader('4', 'Factor Fingerprint',
    'Profilo fattoriale del portafoglio');
  container.appendChild(header);

  const intro = document.createElement('p');
  intro.style.cssText =
    'font-size:12px;color:var(--text-s);margin-bottom:16px;' +
    'line-height:1.6;max-width:700px;';
  intro.textContent =
    'Il profilo fattoriale descrive le caratteristiche dei titoli ' +
    'nel tuo portafoglio. Value/Growth misura se i titoli tendono ' +
    'ad essere "a buon prezzo" (Value) o ad alta crescita (Growth). ' +
    'Size misura la capitalizzazione media: Small Cap = aziende ' +
    'piccole, Large Cap = grandi. Quality misura solidità dei bilanci. ' +
    'I punteggi vanno da 0 a 100 — 50 è neutro.';
  container.appendChild(intro);

  if (!factors || !factors.dimensions || factors.dimensions.length === 0) {
    const noData = document.createElement('p');
    noData.style.cssText = 'color:var(--text-t);font-size:13px;text-align:center;padding:30px 0';
    noData.textContent = 'Dati fattoriali non disponibili per questo portafoglio.';
    container.appendChild(noData);
    return;
  }

  // Tilt badges
  const badgesDiv = document.createElement('div');
  badgesDiv.className = 'factor-tilt-badges';
  factors.dimensions.forEach(d => {
    const badge = document.createElement('span');
    badge.className = 'badge badge-navy';
    badge.textContent = d.name + ': ' + d.tilt;
    badgesDiv.appendChild(badge);
  });
  // Reliability badge
  const relBadge = document.createElement('span');
  const relClass = factors.reliability === 'high' ? 'badge-green' :
                   factors.reliability === 'medium' ? 'badge-amber' : 'badge-red';
  const relIcon = factors.reliability === 'high' ? '\u2705' :
                  factors.reliability === 'medium' ? '\u26A0\uFE0F' : '\u274C';
  relBadge.className = 'badge ' + relClass;
  relBadge.textContent = relIcon + ' Affidabilit\u00E0 ' + factors.reliability;
  badgesDiv.appendChild(relBadge);
  container.appendChild(badgesDiv);

  // Factor bars (portfolio vs benchmark)
  const grid = document.createElement('div');
  grid.className = 'grid-2';

  const barsCard = document.createElement('div');
  barsCard.className = 'card';
  const barsTitle = document.createElement('div');
  barsTitle.className = 'card-title';
  barsTitle.textContent = 'Punteggi Fattoriali';
  barsCard.appendChild(barsTitle);

  factors.dimensions.forEach(d => {
    const row = document.createElement('div');
    row.className = 'factor-bar-row';
    row.style.marginBottom = '24px';

    const label = document.createElement('span');
    label.className = 'factor-bar-label';
    label.textContent = d.name;
    const tip = FACTOR_TOOLTIPS[d.name];
    if (tip) label.appendChild(makeInfoIcon(tip, { dark: false }));

    const track = document.createElement('div');
    track.style.cssText =
      'flex:1;position:relative;height:24px;' +
      'display:flex;align-items:center;margin:0 8px;';

    const bg = document.createElement('div');
    bg.style.cssText =
      'position:absolute;left:0;right:0;height:6px;' +
      'background:var(--border);border-radius:3px;';
    track.appendChild(bg);

    if (d.benchmark_score > 0) {
      const bMark = document.createElement('div');
      bMark.style.cssText =
        'position:absolute;width:2px;height:14px;' +
        'background:#0D5E4C;border-radius:1px;' +
        'top:50%;transform:translateY(-50%);' +
        'left:calc(' + d.benchmark_score.toFixed(1) + '% - 1px);';
      bMark.title = 'Benchmark: ' + d.benchmark_score.toFixed(1);
      track.appendChild(bMark);
    }

    const pct = Math.min(Math.max(d.portfolio_score, 0), 100);
    const pMark = document.createElement('div');
    pMark.style.cssText =
      'position:absolute;width:14px;height:14px;' +
      'background:#1B2A4A;border-radius:50%;' +
      'border:2px solid white;' +
      'box-shadow:0 1px 4px rgba(0,0,0,0.2);' +
      'top:50%;transform:translate(-50%,-50%);' +
      'left:' + pct.toFixed(1) + '%;';
    pMark.title = 'Portafoglio: ' + pct.toFixed(1);
    track.appendChild(pMark);

    const scaleWrap = document.createElement('div');
    scaleWrap.style.cssText =
      'position:absolute;left:0;right:0;top:16px;' +
      'display:flex;justify-content:space-between;';
    const scaleMin = document.createElement('span');
    scaleMin.style.cssText = 'font-size:9px;color:var(--text-t);';
    scaleMin.textContent = d.name === 'Value/Growth' ? 'Value' : '0';
    const scaleMax = document.createElement('span');
    scaleMax.style.cssText = 'font-size:9px;color:var(--text-t);';
    scaleMax.textContent = d.name === 'Value/Growth' ? 'Growth' : '100';
    scaleWrap.append(scaleMin, scaleMax);
    track.appendChild(scaleWrap);

    const valueEl = document.createElement('span');
    valueEl.className = 'factor-bar-value';
    valueEl.style.cssText =
      'min-width:36px;font-size:13px;font-weight:700;' +
      'color:var(--text-p);text-align:right;';
    valueEl.textContent = pct.toFixed(0);

    row.append(label, track, valueEl);
    barsCard.appendChild(row);
  });

  // Legend: portfolio dot vs benchmark tick
  const legend = document.createElement('div');
  legend.style.cssText =
    'display:flex;gap:16px;margin-top:20px;' +
    'font-size:11px;color:var(--text-s);align-items:center;';

  const legP = document.createElement('span');
  legP.style.cssText = 'display:flex;align-items:center;gap:5px;';
  const dotP = document.createElement('span');
  dotP.style.cssText =
    'width:12px;height:12px;background:#1B2A4A;' +
    'border-radius:50%;border:2px solid white;' +
    'box-shadow:0 1px 3px rgba(0,0,0,0.2);' +
    'display:inline-block;flex-shrink:0;';
  legP.append(dotP, document.createTextNode(' Portafoglio'));

  const legB = document.createElement('span');
  legB.style.cssText = 'display:flex;align-items:center;gap:5px;';
  const tickB = document.createElement('span');
  tickB.style.cssText =
    'width:2px;height:14px;background:#0D5E4C;' +
    'border-radius:1px;display:inline-block;flex-shrink:0;';
  legB.append(tickB, document.createTextNode(' Benchmark'));

  legend.append(legP, legB);
  barsCard.appendChild(legend);
  grid.appendChild(barsCard);

  // Coverage card
  const covCard = document.createElement('div');
  covCard.className = 'card';
  const covTitle = document.createElement('div');
  covTitle.className = 'card-title';
  covTitle.textContent = 'Copertura Dati';
  covCard.appendChild(covTitle);

  const cov = factors.coverage;
  const covBar = document.createElement('div');
  covBar.className = 'coverage-bar';

  const segments = [
    { label: 'L1', pct: cov.l1_pct, cls: 'seg-l1' },
    { label: 'L2', pct: cov.l2_pct, cls: 'seg-l2' },
    { label: 'L3', pct: cov.l3_pct, cls: 'seg-l3' },
    { label: 'L4', pct: cov.l4_pct, cls: 'seg-l4' },
  ];

  segments.forEach(s => {
    if (s.pct > 0) {
      const seg = document.createElement('div');
      seg.className = 'seg ' + s.cls;
      seg.style.width = s.pct.toFixed(1) + '%';
      seg.textContent = s.pct >= 8 ? s.pct.toFixed(0) + '%' : '';
      covBar.appendChild(seg);
    }
  });
  covCard.appendChild(covBar);

  // Legend
  const covLegend = document.createElement('div');
  covLegend.className = 'coverage-legend';
  const legendItems = [
    { label: 'L1 Settore', color: '#1B2A4A' },
    { label: 'L2 Fondamentali', color: '#0D5E4C' },
    { label: 'L3 Proxy', color: '#F59E0B' },
    { label: 'L4 Non classificato', color: '#D1D5DB' },
  ];
  legendItems.forEach(l => {
    const item = document.createElement('div');
    item.className = 'coverage-legend-item';
    const dot = document.createElement('span');
    dot.className = 'coverage-legend-dot';
    dot.style.background = l.color;
    const text = document.createTextNode(l.label);
    item.append(dot, text);
    covLegend.appendChild(item);
  });
  covCard.appendChild(covLegend);

  // Explanation text
  const explText = document.createElement('p');
  explText.style.cssText = 'font-size:12px;color:var(--text-t);margin-top:16px;line-height:1.5';
  const l12 = cov.l1_pct + cov.l2_pct;
  if (l12 >= 80) {
    explText.textContent = '\u2705 Oltre l\'80% del portafoglio ha dati fondamentali reali. I punteggi fattoriali sono affidabili.';
  } else if (l12 >= 60) {
    explText.textContent = '\u26A0\uFE0F Il 60-80% del portafoglio ha dati reali. I punteggi sono indicativi ma potrebbero variare.';
  } else {
    explText.textContent = '\u274C Meno del 60% del portafoglio ha dati fondamentali. I punteggi fattoriali hanno bassa affidabilit\u00E0.';
  }
  covCard.appendChild(explText);

  grid.appendChild(covCard);
  container.appendChild(grid);
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
