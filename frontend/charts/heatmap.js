/**
 * Plotly heatmap wrapper for overlap matrix.
 */

export function renderHeatmap(containerId, matrix, tickers) {
  const el = document.getElementById(containerId);
  if (!el || !matrix || matrix.length === 0) return;

  // Build text annotations
  const textMatrix = matrix.map((row, i) => row.map((val, j) => {
    if (i === j) return tickers[i];
    const v = val.toFixed(1);
    let label = '';
    if (val < 15) label = 'MIN';
    else if (val < 35) label = 'BASSA';
    else if (val < 50) label = 'ALTA';
    else label = 'CRITICA';
    return v + '%\n' + label;
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
