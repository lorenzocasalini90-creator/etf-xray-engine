/**
 * Plotly horizontal bars wrapper.
 */

export function renderBars(containerId, labels, values, colors, options = {}) {
  const el = document.getElementById(containerId);
  if (!el) return;

  const textVals = values.map(v => v.toFixed(1) + '%');

  const data = [{
    x: values,
    y: labels,
    type: 'bar',
    orientation: 'h',
    marker: { color: colors || '#1B2A4A', line: { width: 0 } },
    text: textVals,
    textposition: 'outside',
    textfont: { family: 'DM Sans, sans-serif', size: 11, color: '#6B7280' },
    hoverinfo: 'skip',
    cliponaxis: false,
  }];

  const layout = {
    paper_bgcolor: 'white',
    plot_bgcolor: 'white',
    font: { family: 'DM Sans, sans-serif', size: 11, color: '#111827' },
    margin: { l: 120, r: 50, t: 10, b: 20 },
    xaxis: {
      showgrid: true,
      gridcolor: '#F3F4F6',
      gridwidth: 1,
      griddash: 'dot',
      zeroline: false,
      showticklabels: false,
    },
    yaxis: {
      automargin: true,
    },
    height: options.height || 300,
    bargap: 0.3,
  };

  const config = { displayModeBar: false, responsive: true };

  Plotly.newPlot(el, data, layout, config);
}
