/**
 * CheckMyETFs — main application entry point.
 * Orchestrates form → API call → report rendering.
 */

import { renderTopbar, showNav, setOverlapAlert } from './components/topbar.js';
import { renderPortfolioForm } from './components/portfolio_form.js';
import { renderHero } from './components/hero.js';
import { renderXRay } from './components/xray.js';
import { renderOverlap } from './components/overlap.js';
import { renderSector } from './components/sector.js';
import { renderFactor } from './components/factor.js';

// Initialize
document.addEventListener('DOMContentLoaded', () => {
  renderTopbar(document.getElementById('topbar'));
  renderPortfolioForm(document.getElementById('portfolio-input'), onAnalyze);
});

async function onAnalyze(positions, benchmark) {
  const loading = document.getElementById('loading-overlay');
  const inputSection = document.getElementById('portfolio-input');
  const report = document.getElementById('report');

  // Show loading
  loading.hidden = false;

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

    // Hide form, show report
    inputSection.hidden = true;
    report.hidden = false;

    // Calculate total EUR
    const totalEur = positions.reduce((s, p) => s + p.amount_eur, 0);

    // Render all sections
    renderHero(
      document.getElementById('hero-bar'),
      data.kpis,
      data.fetch_metadata,
      totalEur,
    );

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

    renderFactor(document.getElementById('s-factor'), {
      factors: data.factors,
    });

    // Update topbar
    const nEtfs = positions.length;
    const label = nEtfs + ' ETF \u00B7 \u20AC' + totalEur.toLocaleString('it-IT');
    showNav(label);

    // Check overlap alert
    const maxRedundancy = data.redundancy.length > 0
      ? Math.max(...data.redundancy.map(r => r.redundancy_pct))
      : 0;
    setOverlapAlert(maxRedundancy > 70);

    // Scroll to X-Ray
    requestAnimationFrame(() => {
      document.getElementById('s-xray').scrollIntoView({ behavior: 'smooth', block: 'start' });
    });

    // Track in GA4
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
    loading.hidden = true;
  }
}

function _showError(container, message) {
  // Remove any previous error
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
