/**
 * CheckMyETFs — main application entry point.
 * Orchestrates form -> API call -> report rendering.
 */

import { renderTopbar, showNav, setOverlapAlert } from './components/topbar.js';
import { renderPortfolioForm } from './components/portfolio_form.js';
import { renderHero } from './components/hero.js';
import { renderXRay } from './components/xray.js';
import { renderOverlap } from './components/overlap.js';
import { renderSector } from './components/sector.js';
import { renderFactor } from './components/factor.js';
import { fmtEur } from './components/sanitize.js';

// Progress messages shown during loading
const PROGRESS_MSGS = [
  { t: 0,  msg: 'Scarico composizione ETF...' },
  { t: 5,  msg: 'Calcolo esposizione reale...' },
  { t: 10, msg: 'Analizzo overlap e ridondanza...' },
  { t: 15, msg: 'Calcolo factor fingerprint...' },
  { t: 20, msg: 'Quasi pronto...' },
];

document.addEventListener('DOMContentLoaded', () => {
  renderTopbar(document.getElementById('topbar'));
  renderPortfolioForm(document.getElementById('portfolio-input'), onAnalyze);
});

async function onAnalyze(positions, benchmark) {
  const loading = document.getElementById('loading-overlay');
  const loadingMsg = document.getElementById('loading-msg');
  const inputSection = document.getElementById('portfolio-input');
  const report = document.getElementById('report');

  // Show loading with progress messages
  loading.hidden = false;
  loadingMsg.textContent = PROGRESS_MSGS[0].msg;
  let elapsed = 0;
  const msgInterval = setInterval(() => {
    elapsed += 1;
    const step = PROGRESS_MSGS.filter(m => m.t <= elapsed).pop();
    if (step && loadingMsg) loadingMsg.textContent = step.msg;
  }, 1000);

  try {
    const body = { positions };
    if (benchmark) body.benchmark = benchmark;

    const res = await fetch('/api/analyze', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });

    if (!res.ok) {
      const err = await res.json().catch(() => ({ detail: 'Errore sconosciuto' }));
      throw new Error(err.detail || 'Errore ' + res.status);
    }

    const data = await res.json();

    inputSection.hidden = true;
    report.hidden = false;

    const totalEur = positions.reduce((s, p) => s + p.amount_eur, 0);

    renderHero(document.getElementById('hero-bar'), data.kpis, data.fetch_metadata, totalEur);

    renderXRay(document.getElementById('s-xray'), {
      holdings: data.holdings,
      active_bets: data.active_bets,
      insights: data.insights,
      kpis: data.kpis,
      redundancy: data.redundancy,
    });

    renderOverlap(document.getElementById('s-overlap'), {
      redundancy: data.redundancy,
      overlap: data.overlap,
      insights: data.insights,
    });

    renderSector(document.getElementById('s-sector'), {
      sector_exposure: data.sector_exposure,
      country_exposure: data.country_exposure,
    });

    renderFactor(document.getElementById('s-factor'), { factors: data.factors });

    // Update topbar
    const nEtfs = positions.length;
    const label = nEtfs + ' ETF \u00B7 ' + fmtEur(totalEur);
    showNav(label);

    const maxRedundancy = data.redundancy.length > 0
      ? Math.max(...data.redundancy.map(r => r.redundancy_pct))
      : 0;
    setOverlapAlert(maxRedundancy > 70);

    requestAnimationFrame(() => {
      document.getElementById('s-xray').scrollIntoView({ behavior: 'smooth', block: 'start' });
    });

    if (typeof gtag === 'function') {
      gtag('event', 'analyze_portfolio', {
        n_etfs: nEtfs,
        total_eur: totalEur,
        benchmark: benchmark || 'none',
      });
    }

  } catch (err) {
    report.hidden = true;
    inputSection.hidden = false;
    _showError(inputSection, err.message);
  } finally {
    clearInterval(msgInterval);
    loading.hidden = true;
  }
}

function _showError(container, message) {
  const prev = container.querySelector('.error-card');
  if (prev) prev.remove();

  const card = document.createElement('div');
  card.className = 'error-card card';
  const h3 = document.createElement('h3');
  h3.textContent = 'Analisi non riuscita';
  const p = document.createElement('p');
  p.textContent = message;
  const btn = document.createElement('button');
  btn.className = 'btn-retry';
  btn.textContent = 'Riprova';
  btn.addEventListener('click', () => card.remove());
  card.append(h3, p, btn);
  container.appendChild(card);
}
