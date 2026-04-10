/**
 * Hero bar — navy strip with KPI stats.
 */
import { fmtEur, fmtPct, fmtNum } from './sanitize.js';
import { makeInfoIcon } from './tooltip.js';

const HERO_TOOLTIPS = {
  Copertura:
    'Percentuale del portafoglio per cui abbiamo dati ' +
    'completi di holdings dagli emittenti ETF.',
  'Effective N':
    'Numero equivalente di titoli equi-pesati. ' +
    'Un portafoglio da 130 titoli con Eff. N=71 ha una ' +
    'diversificazione reale pari a 71 titoli identici.',
};

export function renderHero(container, kpis, fetchMetadata, totalEur) {
  container.textContent = '';

  const stats = [
    { label: 'Portafoglio', value: fmtEur(totalEur), cls: '' },
    { label: 'Titoli Unici', value: fmtNum(kpis.unique_securities), cls: '' },
    { label: 'HHI', value: (kpis.hhi * 10000).toFixed(0),
      cls: kpis.hhi > 0.15 ? 'coral' : kpis.hhi > 0.05 ? 'amber' : 'green' },
    { label: 'Effective N', value: kpis.effective_n.toFixed(0), cls: '' },
    { label: 'Active Share', value: fmtPct(kpis.active_share),
      cls: kpis.active_share < 20 ? 'coral' : kpis.active_share > 60 ? 'green' : 'amber' },
    { label: 'Top 10', value: fmtPct(kpis.top10_concentration),
      cls: kpis.top10_concentration > 50 ? 'coral' : kpis.top10_concentration > 30 ? 'amber' : 'green' },
    { label: 'Copertura', value: fetchMetadata.coverage_pct.toFixed(0) + '%', cls: '' },
  ];

  stats.forEach(s => {
    const div = document.createElement('div');
    div.className = 'hero-stat';
    const lbl = document.createElement('span');
    lbl.className = 'hero-label';
    lbl.textContent = s.label;
    const tip = HERO_TOOLTIPS[s.label];
    if (tip) lbl.appendChild(makeInfoIcon(tip));
    const val = document.createElement('span');
    val.className = 'hero-value' + (s.cls ? ' ' + s.cls : '');
    val.textContent = s.value;
    div.append(lbl, val);
    container.appendChild(div);
  });

  const editBtn = document.createElement('button');
  editBtn.className = 'hero-edit';
  editBtn.textContent = '\u270F Modifica';
  editBtn.addEventListener('click', () => {
    document.getElementById('report').hidden = true;
    document.getElementById('portfolio-input').hidden = false;
    document.getElementById('topbar-nav').hidden = true;
    document.getElementById('topbar-chip').hidden = true;
    document.getElementById('topbar-pdf').hidden = true;
    window.scrollTo({ top: 0, behavior: 'smooth' });
  });
  container.appendChild(editBtn);
}
