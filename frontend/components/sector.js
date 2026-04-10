/**
 * Sector & Country exposure section.
 * Uses DOM API + Plotly bars.
 */
import { renderBars } from '../charts/bars.js';

export function renderSector(container, data) {
  const { sector_exposure, country_exposure } = data;
  container.textContent = '';
  container.classList.add('fade-in');

  // Header
  const header = _makeHeader('3', 'Esposizione Settoriale & Geografica',
    'Dove sono investiti i tuoi soldi');
  container.appendChild(header);

  // Two-column grid
  const grid = document.createElement('div');
  grid.className = 'grid-2';

  // Sector bars
  const sectorCard = document.createElement('div');
  sectorCard.className = 'card';
  const sectorTitle = document.createElement('div');
  sectorTitle.className = 'card-title';
  sectorTitle.textContent = 'Settori';
  sectorCard.appendChild(sectorTitle);
  const sectorChart = document.createElement('div');
  sectorChart.id = 'chart-sectors';
  sectorChart.className = 'plotly-chart';
  sectorCard.appendChild(sectorChart);
  grid.appendChild(sectorCard);

  // Country bars
  const countryCard = document.createElement('div');
  countryCard.className = 'card';
  const countryTitle = document.createElement('div');
  countryTitle.className = 'card-title';
  countryTitle.textContent = 'Paesi (Top 10)';
  countryCard.appendChild(countryTitle);
  const countryChart = document.createElement('div');
  countryChart.id = 'chart-countries';
  countryChart.className = 'plotly-chart';
  countryCard.appendChild(countryChart);
  grid.appendChild(countryCard);

  container.appendChild(grid);

  // Render charts after DOM insertion
  requestAnimationFrame(() => {
    // Merge "Financials" + "Financial Services" into a single bucket
    const mergedMap = {};
    (sector_exposure || []).forEach(s => {
      const key = (s.label === 'Financial Services' || s.label === 'Financials')
        ? 'Financials'
        : s.label;
      mergedMap[key] = (mergedMap[key] || 0) + s.portfolio_pct;
    });
    const sectors = Object.entries(mergedMap)
      .map(([label, portfolio_pct]) => ({ label, portfolio_pct }))
      .sort((a, b) => a.portfolio_pct - b.portfolio_pct)
      .slice(-15);

    const sLabels = sectors.map(s => s.label);
    const sValues = sectors.map(s => s.portfolio_pct);
    const sColors = sLabels.map(l =>
      l === 'Unknown' ? '#D1D5DB' : '#1B2A4A'
    );
    renderBars('chart-sectors', sLabels, sValues, sColors, {
      height: Math.max(320, sectors.length * 34),
    });

    const countries = (country_exposure || []).slice(0, 10);
    const cLabels = countries.map(c => c.label).reverse();
    const cValues = countries.map(c => c.portfolio_pct).reverse();
    const cColors = cLabels.map(l =>
      l === 'Unknown' ? '#D1D5DB' : '#0D5E4C'
    );
    renderBars('chart-countries', cLabels, cValues, cColors, {
      height: Math.max(280, countries.length * 34),
    });
  });
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
