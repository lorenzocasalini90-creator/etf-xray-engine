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
    textfont: { family: 'DM Sans, sans-serif', size: 12, color: '#374151' },
    hoverinfo: 'skip',
    cliponaxis: false,
  }];

  const isMobile = window.innerWidth < 600;
  const layout = {
    paper_bgcolor: 'white',
    plot_bgcolor: 'white',
    font: {
      family: 'DM Sans, sans-serif',
      size: isMobile ? 9 : 11,
      color: '#111827',
    },
    margin: isMobile
      ? { l: 90, r: 40, t: 10, b: 20 }
      : { l: 140, r: 70, t: 10, b: 20 },
    xaxis: {
      showgrid: true,
      gridcolor: '#D1D5DB',
      gridwidth: 1,
      griddash: 'dash',
      zeroline: true,
      zerolinecolor: '#9CA3AF',
      showticklabels: false,
    },
    yaxis: {
      automargin: true,
    },
    height: isMobile
      ? Math.max(200, labels.length * 22)
      : (options.height || 300),
    bargap: 0.3,
  };

  const config = { displayModeBar: false, responsive: true };

  Plotly.newPlot(el, data, layout, config);
}
