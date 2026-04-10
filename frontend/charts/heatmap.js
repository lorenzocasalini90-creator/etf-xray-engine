/**
 * Plotly heatmap wrapper for overlap matrix.
 */

export function renderHeatmap(containerId, matrix, tickers) {
  const el = document.getElementById(containerId);
  if (!el || !matrix || matrix.length === 0) return;

  // Build text annotations: only the value, color carries the severity
  const textMatrix = matrix.map((row, i) => row.map((val, j) => {
    if (i === j) return tickers[i];
    return val.toFixed(1) + '%';
  }));

  const data = [{
    z: matrix,
    x: tickers,
    y: tickers,
    type: 'heatmap',
    text: textMatrix,
    texttemplate: '%{text}',
    hoverinfo: 'skip',
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
    margin: { l: 60, r: 20, t: 20, b: 60 },
    xaxis: { side: 'bottom', tickangle: 0 },
    yaxis: { autorange: 'reversed' },
    height: Math.max(300, tickers.length * 80 + 80),
  };

  const config = { displayModeBar: false, responsive: true };

  Plotly.newPlot(el, data, layout, config);
}
