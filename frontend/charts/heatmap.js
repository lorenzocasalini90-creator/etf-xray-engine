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

  // Generate HTML table for print (Plotly SVG gets clipped on A4)
  _renderPrintTable(el, matrix, tickers, nameMap);
}

function _renderPrintTable(container, matrix, tickers, nameMap) {
  const prev = container.querySelector('.heatmap-print');
  if (prev) prev.remove();

  const wrap = document.createElement('div');
  wrap.className = 'heatmap-print';

  const table = document.createElement('table');
  table.style.cssText =
    'width:100%;border-collapse:collapse;font-size:7px;table-layout:fixed;';

  // Header row
  const thead = document.createElement('thead');
  const hrow = document.createElement('tr');
  const th0 = document.createElement('th');
  th0.style.cssText = 'width:70px;';
  hrow.appendChild(th0);

  tickers.forEach(t => {
    const th = document.createElement('th');
    th.style.cssText =
      'writing-mode:vertical-rl;text-orientation:mixed;' +
      'transform:rotate(180deg);max-height:60px;overflow:hidden;' +
      'font-size:7px;font-weight:600;padding:2px;text-align:left;' +
      'white-space:nowrap;';
    th.textContent = _label(t, nameMap);
    hrow.appendChild(th);
  });
  thead.appendChild(hrow);
  table.appendChild(thead);

  // Body rows
  const tbody = document.createElement('tbody');
  matrix.forEach((row, ri) => {
    const tr = document.createElement('tr');

    const td0 = document.createElement('td');
    td0.style.cssText =
      'font-size:7px;font-weight:600;padding:2px 4px;' +
      'white-space:nowrap;overflow:hidden;text-overflow:ellipsis;' +
      'max-width:70px;';
    td0.textContent = _label(tickers[ri], nameMap);
    tr.appendChild(td0);

    row.forEach((val, ci) => {
      const td = document.createElement('td');
      td.style.cssText =
        'text-align:center;padding:1px;font-size:7px;' +
        'font-weight:' + (ri === ci ? '700' : '400') + ';';

      if (ri === ci) {
        td.style.background = '#1B2A4A';
        td.style.color = '#fff';
        td.textContent = '—';
      } else if (val > 50) {
        td.style.background = '#FF6B6B';
        td.style.color = '#fff';
        td.textContent = val.toFixed(1) + '%';
      } else if (val > 35) {
        td.style.background = '#FEE2E2';
        td.style.color = '#B91C1C';
        td.textContent = val.toFixed(1) + '%';
      } else if (val > 15) {
        td.style.background = '#FEF3C7';
        td.style.color = '#92400E';
        td.textContent = val.toFixed(1) + '%';
      } else if (val > 0) {
        td.style.background = '#F0FDF4';
        td.style.color = '#15803D';
        td.textContent = val.toFixed(1) + '%';
      } else {
        td.style.background = '#F9FAFB';
        td.style.color = '#9CA3AF';
        td.textContent = '0%';
      }
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
  wrap.appendChild(table);
  container.appendChild(wrap);
}
