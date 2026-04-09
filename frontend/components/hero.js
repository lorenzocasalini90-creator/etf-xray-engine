/**
 * Hero bar — navy strip with KPI stats.
 * Uses DOM API (no innerHTML with untrusted data).
 */

export function renderHero(container, kpis, fetchMetadata, totalEur) {
  container.textContent = '';

  const stats = [
    { label: 'Portafoglio', value: '\u20AC ' + totalEur.toLocaleString('it-IT'), cls: '' },
    { label: 'Titoli Unici', value: kpis.unique_securities.toLocaleString(), cls: '' },
    { label: 'HHI', value: (kpis.hhi * 10000).toFixed(0),
      cls: kpis.hhi > 0.15 ? 'coral' : kpis.hhi > 0.05 ? 'amber' : 'green' },
    { label: 'Effective N', value: kpis.effective_n.toFixed(0), cls: '' },
    { label: 'Active Share', value: kpis.active_share.toFixed(1) + '%',
      cls: kpis.active_share < 20 ? 'amber' : kpis.active_share > 60 ? 'green' : '' },
    { label: 'Top 10', value: kpis.top10_concentration.toFixed(1) + '%',
      cls: kpis.top10_concentration > 50 ? 'coral' : kpis.top10_concentration > 30 ? 'amber' : 'green' },
    { label: 'Copertura', value: fetchMetadata.coverage_pct.toFixed(0) + '%', cls: '' },
  ];

  stats.forEach(s => {
    const div = document.createElement('div');
    div.className = 'hero-stat';
    const lbl = document.createElement('span');
    lbl.className = 'hero-label';
    lbl.textContent = s.label;
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
    window.scrollTo({ top: 0, behavior: 'smooth' });
  });
  container.appendChild(editBtn);
}
