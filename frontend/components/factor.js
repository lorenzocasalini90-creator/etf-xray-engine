/**
 * Factor Fingerprint section — tilt badges, factor bars, coverage.
 * Uses DOM API for safe rendering.
 */

export function renderFactor(container, data) {
  const { factors } = data;
  container.textContent = '';
  container.classList.add('fade-in');

  // Header
  const header = _makeHeader('4', 'Factor Fingerprint',
    'Profilo fattoriale del portafoglio');
  container.appendChild(header);

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

    const label = document.createElement('span');
    label.className = 'factor-bar-label';
    label.textContent = d.name;

    const barWrap = document.createElement('div');
    barWrap.className = 'factor-bar-wrap';

    // Portfolio bar
    const maxVal = Math.max(d.portfolio_score, d.benchmark_score, 1);
    const pBar = document.createElement('div');
    pBar.className = 'factor-bar-fill';
    pBar.style.background = '#1B2A4A';
    pBar.style.width = (d.portfolio_score / maxVal * 80).toFixed(1) + '%';
    pBar.style.position = 'absolute';
    pBar.style.top = '0';
    barWrap.style.position = 'relative';

    // Benchmark bar (thinner, underneath)
    const bBar = document.createElement('div');
    bBar.style.cssText = 'position:absolute;top:6px;height:12px;border-radius:4px;background:rgba(13,94,76,0.25);';
    bBar.style.width = (d.benchmark_score / maxVal * 80).toFixed(1) + '%';

    barWrap.append(bBar, pBar);

    const valueEl = document.createElement('span');
    valueEl.className = 'factor-bar-value';
    valueEl.textContent = d.portfolio_score.toFixed(1);

    row.append(label, barWrap, valueEl);
    barsCard.appendChild(row);
  });

  // Legend
  const legend = document.createElement('div');
  legend.style.cssText = 'display:flex;gap:16px;margin-top:12px;font-size:11px;color:var(--text-s)';
  const legP = document.createElement('span');
  legP.style.cssText = 'display:flex;align-items:center;gap:4px';
  const dotP = document.createElement('span');
  dotP.style.cssText = 'width:10px;height:10px;border-radius:2px;background:#1B2A4A';
  legP.append(dotP, document.createTextNode(' Portafoglio'));
  const legB = document.createElement('span');
  legB.style.cssText = 'display:flex;align-items:center;gap:4px';
  const dotB = document.createElement('span');
  dotB.style.cssText = 'width:10px;height:10px;border-radius:2px;background:rgba(13,94,76,0.25)';
  legB.append(dotB, document.createTextNode(' Benchmark'));
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
