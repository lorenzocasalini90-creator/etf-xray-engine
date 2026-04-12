/**
 * Plotly heatmap wrapper for overlap matrix.
 */

function _abbr(ticker) {
  if (!ticker) return '—';
  if (ticker.length <= 6) return ticker;
  return ticker.substring(0, 6) + '…';
}

function _label(ticker, nameMap) {
  if (nameMap && nameMap[ticker]) {
    const n = nameMap[ticker];
    // "iShares Core MSCI World UCITS ETF USD Acc" → "iShares Core MSCI World"
    // Strip boilerplate tails starting at UCITS/ETF/USD/EUR/Acc/Inc/…
    const clean = n
      .replace(/\s+(UCITS|ETF|USD|EUR|GBP|Acc|Inc|Distributing|Accumulating)\b.*/gi, '')
      .trim();
    const pick = clean || n;
    return pick.length > 16 ? pick.substring(0, 15) + '…' : pick;
  }
  return _abbr(ticker);
}

export function renderHeatmap(containerId, matrix, tickers, nameMap = {}) {
  const el = document.getElementById(containerId);
  if (!el || !matrix || matrix.length === 0) return;

  const shortTickers = tickers.map(t => _label(t, nameMap));

  // Build text annotations: only the value, color carries the severity
  const textMatrix = matrix.map((row, i) => row.map((val, j) => {
    if (i === j) return shortTickers[i];
    return val.toFixed(1) + '%';
  }));

  // customdata preserves the full tickers for the hover tooltip
  const customdata = matrix.map((row, ri) =>
    row.map((_, ci) => ({
      fy: tickers[ri] || '',
      fx: tickers[ci] || '',
    }))
  );

  const data = [{
    z: matrix,
    x: shortTickers,
    y: shortTickers,
    type: 'heatmap',
    text: textMatrix,
    texttemplate: '%{text}',
    customdata: customdata,
    hovertemplate:
      '<b>%{customdata.fy}</b><br>' +
      'vs <b>%{customdata.fx}</b><br>' +
      'Overlap: %{z:.1f}%<extra></extra>',
    colorscale: [
      [0, '#F0FDF4'],
      [0.15, '#FEF3C7'],
      [0.35, '#FEE2E2'],
      [0.50, '#FF6B6B'],
      [1.0, '#B91C1C'],
    ],
    showscale: false,
    zmin: 0,
    zmax: 100,
  }];

  const layout = {
    paper_bgcolor: 'white',
    plot_bgcolor: 'white',
    font: { family: 'DM Sans, sans-serif', size: 12, color: '#111827' },
    margin: { l: 70, r: 20, t: 50, b: 60 },
    xaxis: {
      side: 'bottom',
      tickangle: -35,
      tickfont: { size: 9, family: 'DM Sans, sans-serif' },
    },
    yaxis: {
      autorange: 'reversed',
      tickfont: { size: 9, family: 'DM Sans, sans-serif' },
    },
    height: Math.max(300, tickers.length * 80 + 80),
  };

  const config = { displayModeBar: false, responsive: true };

  Plotly.newPlot(el, data, layout, config);

  // Remove any old print table (was inside el in previous version)
  const oldInner = el.querySelector('.heatmap-print');
  if (oldInner) oldInner.remove();

  // Create print table as SIBLING after the Plotly container (not inside it)
  const oldSibling = el.nextElementSibling;
  if (oldSibling && oldSibling.classList.contains('heatmap-print-wrapper')) {
    oldSibling.remove();
  }
  const printWrapper = document.createElement('div');
  printWrapper.className = 'heatmap-print-wrapper';
  el.parentNode.insertBefore(printWrapper, el.nextSibling);
  _renderPrintTable(printWrapper, matrix, tickers, nameMap);
}

function _renderPrintTable(wrapper, matrix, tickers, nameMap) {
  while (wrapper.firstChild) wrapper.removeChild(wrapper.firstChild);

  const table = document.createElement('table');
  table.style.cssText = [
    'width:100%',
    'border-collapse:collapse',
    'font-size:6.5px',
    'font-family:DM Sans,sans-serif',
    'table-layout:fixed',
    'page-break-inside:avoid',
  ].join(';');

  // Header — horizontal labels (vertical ones get truncated in print)
  const thead = document.createElement('thead');
  const hrow = document.createElement('tr');
  const corner = document.createElement('th');
  corner.style.cssText = 'width:80px;border:none;';
  hrow.appendChild(corner);

  tickers.forEach(t => {
    const th = document.createElement('th');
    th.style.cssText = [
      'font-size:6px',
      'font-weight:600',
      'padding:2px 1px',
      'text-align:center',
      'border:0.5px solid #E5E7EB',
      'background:#F9FAFB',
      'overflow:hidden',
      'white-space:nowrap',
      'text-overflow:ellipsis',
      'max-width:50px',
    ].join(';');
    th.textContent = _label(t, nameMap);
    th.title = nameMap[t] || t;
    hrow.appendChild(th);
  });
  thead.appendChild(hrow);
  table.appendChild(thead);

  // Body
  const tbody = document.createElement('tbody');
  matrix.forEach((row, ri) => {
    const tr = document.createElement('tr');

    const tdLabel = document.createElement('td');
    tdLabel.style.cssText = [
      'font-size:6px',
      'font-weight:600',
      'padding:2px 3px',
      'white-space:nowrap',
      'overflow:hidden',
      'text-overflow:ellipsis',
      'max-width:80px',
      'border:0.5px solid #E5E7EB',
      'background:#F9FAFB',
    ].join(';');
    tdLabel.textContent = _label(tickers[ri], nameMap);
    tr.appendChild(tdLabel);

    row.forEach((val, ci) => {
      const td = document.createElement('td');
      const isdiag = (ri === ci);
      let bg, color, text;

      if (isdiag) {
        bg = '#1B2A4A'; color = '#fff'; text = '\u25A0';
      } else if (val > 50) {
        bg = '#FF6B6B'; color = '#fff'; text = val.toFixed(1) + '%';
      } else if (val > 35) {
        bg = '#FEE2E2'; color = '#B91C1C'; text = val.toFixed(1) + '%';
      } else if (val > 15) {
        bg = '#FEF3C7'; color = '#92400E'; text = val.toFixed(1) + '%';
      } else if (val > 0) {
        bg = '#F0FDF4'; color = '#15803D'; text = val.toFixed(1) + '%';
      } else {
        bg = '#FFFFFF'; color = '#D1D5DB'; text = '0';
      }

      td.style.cssText = [
        'text-align:center',
        'padding:1.5px 1px',
        'font-size:6px',
        'border:0.5px solid #E5E7EB',
        'background:' + bg,
        'color:' + color,
        isdiag ? 'font-weight:700' : 'font-weight:400',
      ].join(';');
      td.textContent = text;
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
  wrapper.appendChild(table);
}
