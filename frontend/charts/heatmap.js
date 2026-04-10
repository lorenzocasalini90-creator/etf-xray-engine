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
}
