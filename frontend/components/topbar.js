/**
 * Topbar — sticky nav with logo, anchor links, portfolio chip.
 * All dynamic text is escaped via sanitize.js to prevent XSS.
 */
import { esc } from './sanitize.js';

const SECTIONS = [
  { id: 's-xray',    label: '\u2460 X-Ray' },
  { id: 's-overlap', label: '\u2461 Overlap' },
  { id: 's-sector',  label: '\u2462 Settori' },
  { id: 's-factor',  label: '\u2463 Factor' },
];

let _observer = null;

export function renderTopbar(container) {
  // Build DOM safely
  container.textContent = '';

  const logo = document.createElement('div');
  logo.className = 'topbar-logo';
  logo.innerHTML = 'Check<span>My</span>ETFs';
  container.appendChild(logo);

  const nav = document.createElement('div');
  nav.className = 'topbar-nav';
  nav.id = 'topbar-nav';
  nav.hidden = true;
  SECTIONS.forEach(s => {
    const a = document.createElement('a');
    a.href = '#' + s.id;
    a.dataset.section = s.id;
    a.textContent = s.label;
    a.addEventListener('click', e => {
      e.preventDefault();
      const el = document.getElementById(s.id);
      if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' });
    });
    nav.appendChild(a);
  });
  container.appendChild(nav);

  const chip = document.createElement('div');
  chip.className = 'topbar-chip';
  chip.id = 'topbar-chip';
  chip.hidden = true;
  container.appendChild(chip);
}

export function showNav(portfolioLabel) {
  const nav = document.getElementById('topbar-nav');
  const chip = document.getElementById('topbar-chip');
  if (nav) nav.hidden = false;
  if (chip) {
    chip.hidden = false;
    chip.textContent = portfolioLabel;  // textContent is safe
    chip.onclick = () => {
      document.getElementById('report').hidden = true;
      document.getElementById('portfolio-input').hidden = false;
      if (nav) nav.hidden = true;
      chip.hidden = true;
      window.scrollTo({ top: 0, behavior: 'smooth' });
    };
  }
  _setupObserver();
}

export function setOverlapAlert(hasAlert) {
  const links = document.querySelectorAll('.topbar-nav a');
  links.forEach(a => {
    if (a.dataset.section === 's-overlap') {
      if (hasAlert) {
        a.classList.add('alert');
        a.textContent = '\u2461 Overlap !';
      } else {
        a.classList.remove('alert');
        a.textContent = '\u2461 Overlap';
      }
    }
  });
}

function _setupObserver() {
  if (_observer) _observer.disconnect();
  const links = document.querySelectorAll('.topbar-nav a');
  const sections = SECTIONS.map(s => document.getElementById(s.id)).filter(Boolean);

  _observer = new IntersectionObserver(entries => {
    entries.forEach(entry => {
      if (entry.isIntersecting) {
        links.forEach(a => a.classList.remove('active'));
        const active = document.querySelector(`.topbar-nav a[data-section="${entry.target.id}"]`);
        if (active) active.classList.add('active');
      }
    });
  }, { rootMargin: '-80px 0px -60% 0px', threshold: 0 });

  sections.forEach(s => _observer.observe(s));
}
