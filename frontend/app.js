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
import { renderFeedback } from './components/feedback.js';
import { renderSuggestions } from './components/suggestions.js';
import { renderAICard } from './components/ai_analysis.js';
import { fmtEur } from './components/sanitize.js';

// Print: hide Plotly container with visibility:hidden, show HTML table wrapper
window.addEventListener('beforeprint', () => {
  document.querySelectorAll('#heatmap-container').forEach(el => {
    el.style.setProperty('visibility', 'hidden', 'important');
    el.style.setProperty('height', '0px', 'important');
    el.style.setProperty('overflow', 'hidden', 'important');
    el.style.setProperty('margin', '0', 'important');
    el.style.setProperty('padding', '0', 'important');
  });
  document.querySelectorAll('.heatmap-print-wrapper').forEach(el => {
    el.style.setProperty('display', 'block', 'important');
  });
});

window.addEventListener('afterprint', () => {
  document.querySelectorAll('#heatmap-container').forEach(el => {
    el.style.removeProperty('visibility');
    el.style.removeProperty('height');
    el.style.removeProperty('overflow');
    el.style.removeProperty('margin');
    el.style.removeProperty('padding');
  });
  document.querySelectorAll('.heatmap-print-wrapper').forEach(el => {
    el.style.removeProperty('display');
  });
  window.dispatchEvent(new Event('resize'));
});

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

  async function _fetchWithRetry(body, maxRetries = 1) {
    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        const res = await fetch('/api/analyze', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body),
        });
        if (res.status === 504 && attempt < maxRetries) {
          if (loadingMsg) {
            loadingMsg.textContent =
              'Prima analisi completata in background. Carico i risultati...';
          }
          await new Promise(r => setTimeout(r, 1500));
          continue;
        }
        return res;
      } catch (err) {
        if (attempt < maxRetries) {
          await new Promise(r => setTimeout(r, 1500));
          continue;
        }
        throw err;
      }
    }
  }

  try {
    // Nascondi landing hero quando parte l'analisi
    const landingHero = document.getElementById('landing-hero');
    if (landingHero) landingHero.hidden = true;

    const body = { positions };
    if (benchmark) body.benchmark = benchmark;

    const res = await _fetchWithRetry(body);

    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      const detail = err.detail || '';

      if (res.status === 504) {
        _gtagError('504');
        throw new Error(
          'Prima analisi pi\u00F9 lenta (~40s), attendere... ' +
          'Riprova tra qualche secondo \u2014 la prossima analisi sar\u00E0 pi\u00F9 veloce.'
        );
      }
      if (res.status === 422) {
        _gtagError('validation');
        throw new Error(detail || 'Dati non validi. Controlla ticker e importi.');
      }
      if (res.status === 429) {
        _gtagError('rate_limit');
        throw new Error(detail || 'Troppe richieste. Attendi 1 minuto.');
      }
      if (res.status === 502 || res.status === 503) {
        _gtagError('server_overload');
        _showCountdown(inputSection, 30);
        throw new Error('Il server \u00E8 occupato. Riprova tra 30 secondi.');
      }

      _gtagError(String(res.status));
      throw new Error(detail || 'Errore ' + res.status);
    }

    const data = await res.json();

    inputSection.hidden = true;
    report.hidden = false;

    const totalEur = positions.reduce((s, p) => s + p.amount_eur, 0);

    renderHero(document.getElementById('hero-bar'), data.kpis, data.fetch_metadata, totalEur);

    const BENCH_LABELS = {
      MSCI_WORLD: 'MSCI World',
      SP500: 'S&P 500',
      MSCI_EM: 'MSCI EM',
      FTSE_ALL_WORLD: 'FTSE All-World',
    };
    const benchmarkLabel = benchmark ? (BENCH_LABELS[benchmark] || benchmark) : 'benchmark';

    renderXRay(document.getElementById('s-xray'), {
      holdings: data.holdings,
      active_bets: data.active_bets,
      insights: data.insights,
      kpis: data.kpis,
      redundancy: data.redundancy,
      benchmark_label: benchmarkLabel,
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

    renderSuggestions(document.getElementById('s-suggestions'), {
      redundancy: data.redundancy,
      kpis: data.kpis,
      positions,
    });

    renderAICard(document.getElementById('s-ai'), data, positions);

    // Feedback widget (after Factor Fingerprint, before mobile PDF)
    const feedbackWrap = document.createElement('div');
    feedbackWrap.className = 'report-section';
    feedbackWrap.style.paddingTop = '0';
    document.getElementById('report').appendChild(feedbackWrap);
    renderFeedback(feedbackWrap);

    // Update topbar
    const nEtfs = positions.length;
    const label = nEtfs + ' ETF \u00B7 ' + fmtEur(totalEur);
    showNav(label);

    const maxRedundancy = data.redundancy.length > 0
      ? Math.max(...data.redundancy.map(r => r.redundancy_pct))
      : 0;
    setOverlapAlert(maxRedundancy > 70);

    // Add mobile PDF button at bottom of report (if not already present)
    if (!document.getElementById('btn-pdf-mobile')) {
      const mobilePdf = document.createElement('div');
      mobilePdf.style.cssText = 'padding:16px 12px 32px;display:none;';
      mobilePdf.className = 'mobile-pdf-wrap';
      const btn = document.createElement('button');
      btn.id = 'btn-pdf-mobile';
      btn.className = 'btn-cta';
      btn.style.cssText =
        'width:100%;font-size:14px;padding:13px;' +
        'display:flex;align-items:center;' +
        'justify-content:center;gap:8px;';
      btn.textContent = '\uD83D\uDCC4 Esporta PDF';
      btn.addEventListener('click', () => { window.print(); });
      mobilePdf.appendChild(btn);
      report.appendChild(mobilePdf);
    }

    requestAnimationFrame(() => {
      document.getElementById('s-xray').scrollIntoView({ behavior: 'smooth', block: 'start' });
    });

    if (typeof gtag === 'function') {
      gtag('event', 'analysis_complete', {
        etf_count: nEtfs,
        has_overlap: (data.overlap?.pairs?.length || 0) > 0,
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

function _gtagError(errorType) {
  if (typeof gtag === 'function') {
    gtag('event', 'analysis_error', { error_type: errorType });
  }
}

function _showCountdown(container, seconds) {
  const prev = container.querySelector('.countdown-banner');
  if (prev) prev.remove();

  const banner = document.createElement('div');
  banner.className = 'countdown-banner alert-banner warning';
  let remaining = seconds;
  banner.textContent = 'Il server \u00E8 occupato. Riprova tra ' + remaining + 's...';
  const timer = setInterval(() => {
    remaining -= 1;
    if (remaining <= 0) {
      clearInterval(timer);
      banner.remove();
      return;
    }
    banner.textContent =
      'Il server \u00E8 occupato. Riprova tra ' + remaining + 's...';
  }, 1000);
  container.prepend(banner);
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
