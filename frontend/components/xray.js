/**
 * X-Ray section — holdings table + active bets + KPI cards.
 */
import { sanitize, fmtEur, fmtPct, fmtNum } from './sanitize.js';
import { makeInfoIcon } from './tooltip.js';

const TOOLTIPS = {
  'Titoli unici':
    'Numero totale di titoli distinti nel portafoglio aggregato, ' +
    'eliminando i duplicati tra ETF.',
  'Active Share':
    'Quanto il tuo portafoglio differisce dal benchmark. ' +
    '0% = identico al benchmark. 100% = completamente diverso.',
  'HHI (concentrazione)':
    'Indice di concentrazione Herfindahl-Hirschman. ' +
    'Sotto 0.01 = molto diversificato. Sopra 0.05 = concentrato. ' +
    'Più è basso, meglio è distribuito il rischio.',
  'TER inefficienza':
    'Costo annuo stimato delle commissioni duplicate: ' +
    'holdings presenti in più ETF che paghi più volte.',
};

export function renderXRay(container, data) {
  const { holdings, active_bets, insights, kpis } = data;
  const benchLabel = data.benchmark_label || 'benchmark';
  container.textContent = '';
  container.classList.add('fade-in');

  const header = _makeHeader('1', 'X-Ray', 'Composizione aggregata del portafoglio');
  container.appendChild(header);

  // Critical alerts
  const criticals = (insights || []).filter(i => i.severity === 'critical');
  criticals.forEach(ins => {
    const banner = document.createElement('div');
    banner.className = 'alert-banner critical';
    const icon = document.createElement('span');
    icon.className = 'alert-icon';
    icon.textContent = '\u26A0';
    const text = document.createElement('span');
    text.textContent = sanitize(ins.body);
    banner.append(icon, text);
    container.appendChild(banner);
  });

  // KPI cards with semantic colors
  const terWaste = data.redundancy
    ? data.redundancy.reduce((s, r) => s + r.ter_waste_eur, 0)
    : 0;
  const asClass = kpis.active_share > 40 ? 'kpi-green' : kpis.active_share > 20 ? 'kpi-amber' : 'kpi-coral';
  const asValue = kpis.active_share < 1
    ? 'Identico al benchmark'
    : fmtPct(kpis.active_share);
  const hhiClass = kpis.hhi > 0.05 ? 'kpi-coral'
                 : kpis.hhi > 0.01 ? 'kpi-amber'
                 : 'kpi-green';
  const isMobile = window.innerWidth < 600;
  const kpiData = [
    { label: 'Titoli unici', value: fmtNum(kpis.unique_securities), cls: '' },
    { label: 'Active Share', value: asValue, cls: asClass },
    { label: isMobile ? 'HHI' : 'HHI (concentrazione)', value: Number(kpis.hhi).toFixed(4), cls: hhiClass },
    { label: 'TER inefficienza', value: fmtEur(terWaste) + '/anno', cls: 'kpi-coral' },
  ];
  const kpiGrid = document.createElement('div');
  kpiGrid.className = 'kpi-grid';
  kpiData.forEach((k, i) => {
    const card = document.createElement('div');
    card.className = 'kpi-card stagger-' + (i + 1);
    const lbl = document.createElement('div');
    lbl.className = 'kpi-label';
    lbl.textContent = k.label;
    const tooltipKey = k.label === 'HHI' ? 'HHI (concentrazione)' : k.label;
    const info = TOOLTIPS[tooltipKey];
    if (info) lbl.appendChild(makeInfoIcon(info, {dark: false}));
    const val = document.createElement('div');
    val.className = 'kpi-value' + (k.cls ? ' ' + k.cls : '');
    val.textContent = k.value;
    card.append(lbl, val);
    kpiGrid.appendChild(card);
  });
  container.appendChild(kpiGrid);

  // Two-column layout
  const grid = document.createElement('div');
  grid.className = 'grid-2';

  // Left: holdings table — NAME primary, ticker secondary
  const holdingsCard = document.createElement('div');
  holdingsCard.className = 'card';
  const holdingsTitle = document.createElement('div');
  holdingsTitle.className = 'card-title';
  holdingsTitle.textContent = 'Top Holdings';
  holdingsCard.appendChild(holdingsTitle);

  const table = document.createElement('table');
  table.className = 'data-table';
  const thead = document.createElement('thead');
  const headRow = document.createElement('tr');
  ['#', 'Titolo', 'Peso', '', 'Valore', 'Settore'].forEach(h => {
    const th = document.createElement('th');
    th.textContent = h;
    if (['Peso', 'Valore', '#'].includes(h)) th.className = 'num';
    headRow.appendChild(th);
  });
  thead.appendChild(headRow);
  table.appendChild(thead);

  const tbody = document.createElement('tbody');
  const maxWeight = holdings.length > 0 ? holdings[0].weight_pct : 1;
  const displayCount = 10;
  const visibleHoldings = holdings.slice(0, displayCount);
  const remainingHoldings = holdings.slice(displayCount, 30);

  visibleHoldings.forEach(h => tbody.appendChild(_makeHoldingRow(h, maxWeight)));
  table.appendChild(tbody);
  holdingsCard.appendChild(table);

  if (remainingHoldings.length > 0) {
    const expandBtn = document.createElement('button');
    expandBtn.className = 'expander-btn';
    expandBtn.textContent = 'Mostra altri ' + remainingHoldings.length + ' titoli \u25BC';
    let expanded = false;
    expandBtn.addEventListener('click', () => {
      if (!expanded) {
        remainingHoldings.forEach(h => tbody.appendChild(_makeHoldingRow(h, maxWeight)));
        expandBtn.textContent = 'Nascondi \u25B2';
        expanded = true;
      } else {
        while (tbody.children.length > displayCount) tbody.removeChild(tbody.lastChild);
        expandBtn.textContent = 'Mostra altri ' + remainingHoldings.length + ' titoli \u25BC';
        expanded = false;
      }
    });
    holdingsCard.appendChild(expandBtn);
  }
  grid.appendChild(holdingsCard);

  // Right: active bets
  const betsCard = document.createElement('div');
  betsCard.className = 'card';
  const betsTitle = document.createElement('div');
  betsTitle.className = 'card-title';
  betsTitle.textContent = 'Active Bets vs ' + benchLabel;
  betsCard.appendChild(betsTitle);

  const betsExpl = document.createElement('p');
  betsExpl.style.cssText =
    'font-size:11px;color:var(--text-t);margin-bottom:12px;line-height:1.5;';
  betsExpl.textContent =
    'Titoli in cui sei più (sovrappeso) o meno (sottopeso) ' +
    'esposto rispetto al ' + benchLabel + '. ' +
    '+X pp = hai X punti percentuale in più rispetto al benchmark.';
  betsCard.appendChild(betsExpl);

  if (active_bets.overweight.length === 0 && active_bets.underweight.length === 0) {
    const noData = document.createElement('p');
    noData.style.cssText = 'color:var(--text-t);font-size:12px;text-align:center;padding:20px 0';
    noData.textContent = 'Nessun benchmark selezionato';
    betsCard.appendChild(noData);
  } else {
    if (active_bets.overweight.length > 0) {
      const owLabel = document.createElement('div');
      owLabel.style.cssText = 'font-size:11px;font-weight:600;color:var(--text-t);margin-bottom:6px';
      owLabel.textContent = 'SOVRAPPESO';
      betsCard.appendChild(owLabel);
      active_bets.overweight.slice(0, 8).forEach(b => betsCard.appendChild(_makeBetRow(b, true)));
    }
    if (active_bets.underweight.length > 0) {
      const uwLabel = document.createElement('div');
      uwLabel.style.cssText = 'font-size:11px;font-weight:600;color:var(--text-t);margin:14px 0 6px';
      uwLabel.textContent = 'SOTTOPESO';
      betsCard.appendChild(uwLabel);
      active_bets.underweight.slice(0, 5).forEach(b => betsCard.appendChild(_makeBetRow(b, false)));
    }
  }
  grid.appendChild(betsCard);
  container.appendChild(grid);
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

function _makeHoldingRow(h, maxWeight) {
  const tr = document.createElement('tr');

  const tdRank = document.createElement('td');
  tdRank.className = 'num cell-muted';
  tdRank.textContent = h.rank;

  // Combined name + ticker cell (B3: name primary)
  const tdTitle = document.createElement('td');
  tdTitle.style.maxWidth = '220px';
  const nameSpan = document.createElement('span');
  nameSpan.className = 'holding-name';
  nameSpan.textContent = sanitize(h.name);
  const tickerSpan = document.createElement('span');
  tickerSpan.className = 'holding-ticker';
  tickerSpan.textContent = h.ticker || '';
  tdTitle.append(nameSpan, tickerSpan);

  const tdWeight = document.createElement('td');
  tdWeight.className = 'num';
  tdWeight.textContent = fmtPct(h.weight_pct);

  const tdBar = document.createElement('td');
  tdBar.style.width = '60px';
  const bar = document.createElement('span');
  bar.className = 'mini-bar-wrap';
  const fill = document.createElement('span');
  fill.className = 'mini-bar-fill';
  fill.style.width = (h.weight_pct / maxWeight * 100).toFixed(0) + '%';
  bar.appendChild(fill);
  tdBar.appendChild(bar);

  const tdVal = document.createElement('td');
  tdVal.className = 'num';
  tdVal.textContent = fmtEur(h.value_eur);

  const tdSector = document.createElement('td');
  tdSector.className = 'cell-muted';
  tdSector.textContent = sanitize(h.sector) || '';
  tdSector.style.maxWidth = '120px';
  tdSector.style.overflow = 'hidden';
  tdSector.style.textOverflow = 'ellipsis';
  tdSector.style.whiteSpace = 'nowrap';

  tr.append(tdRank, tdTitle, tdWeight, tdBar, tdVal, tdSector);
  return tr;
}

function _makeBetRow(bet, isOver) {
  const row = document.createElement('div');
  row.className = 'bet-row';
  const name = document.createElement('span');
  name.className = 'bet-name';
  name.textContent = sanitize(bet.name) || bet.ticker;
  const delta = document.createElement('span');
  delta.className = 'bet-delta ' + (isOver ? 'pos' : 'neg');
  const rounded = Math.round(Math.abs(bet.delta_pct * 100));
  delta.textContent = (isOver ? '+' : '−') + rounded + ' pp';
  row.append(name, delta);
  return row;
}
