/**
 * Topbar — sticky nav with logo, anchor links, portfolio chip, PDF button.
 */

const SECTIONS = [
  { id: 's-xray',    label: '\u2460 X-Ray' },
  { id: 's-overlap', label: '\u2461 Overlap' },
  { id: 's-sector',  label: '\u2462 Settori' },
  { id: 's-factor',  label: '\u2463 Factor' },
];

let _observer = null;

export function renderTopbar(container) {
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

  // PDF button (hidden until report visible)
  const pdfBtn = document.createElement('button');
  pdfBtn.className = 'btn-pdf no-print';
  pdfBtn.id = 'topbar-pdf';
  pdfBtn.textContent = '\uD83D\uDCC4 Esporta PDF';
  pdfBtn.hidden = true;
  pdfBtn.addEventListener('click', () => window.print());
  container.appendChild(pdfBtn);

  // Modify button — jumps back to the portfolio form
  const modBtn = document.createElement('button');
  modBtn.className = 'btn-pdf no-print';
  modBtn.id = 'topbar-mod';
  modBtn.textContent = '\u270F Modifica';
  modBtn.hidden = true;
  modBtn.addEventListener('click', () => {
    document.getElementById('report').hidden = true;
    document.getElementById('portfolio-input').hidden = false;
    const nav = document.getElementById('topbar-nav');
    const chipEl = document.getElementById('topbar-chip');
    const pdf = document.getElementById('topbar-pdf');
    if (nav) nav.hidden = true;
    if (chipEl) chipEl.hidden = true;
    if (pdf) pdf.hidden = true;
    modBtn.hidden = true;
    window.scrollTo({ top: 0, behavior: 'smooth' });
  });
  container.appendChild(modBtn);

  const chip = document.createElement('div');
  chip.className = 'topbar-chip';
  chip.id = 'topbar-chip';
  chip.hidden = true;
  container.appendChild(chip);
}

export function showNav(portfolioLabel) {
  const nav = document.getElementById('topbar-nav');
  const chip = document.getElementById('topbar-chip');
  const pdf = document.getElementById('topbar-pdf');
  const mod = document.getElementById('topbar-mod');
  if (nav) nav.hidden = false;
  if (pdf) pdf.hidden = false;
  if (mod) mod.hidden = false;
  if (chip) {
    chip.hidden = false;
    chip.textContent = portfolioLabel;
    chip.onclick = () => {
      document.getElementById('report').hidden = true;
      document.getElementById('portfolio-input').hidden = false;
      if (nav) nav.hidden = true;
      if (pdf) pdf.hidden = true;
      if (mod) mod.hidden = true;
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
        a.textContent = '\u2461 Overlap ';
        const dot = document.createElement('span');
        dot.className = 'pulse-dot';
        a.appendChild(dot);
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
        const active = document.querySelector(
          `.topbar-nav a[data-section="${entry.target.id}"]`
        );
        if (active) active.classList.add('active');
      }
    });
  }, {
    rootMargin: '-60px 0px -40% 0px',
    threshold: 0.1,
  });

  sections.forEach(s => _observer.observe(s));

  // Fallback: near the bottom of the page always highlight the last
  // section (Factor) — the IntersectionObserver can miss it when the
  // section is shorter than the viewport.
  window.addEventListener('scroll', () => {
    const nearBottom =
      window.scrollY + window.innerHeight >
      document.documentElement.scrollHeight - 120;
    if (nearBottom) {
      links.forEach(a => a.classList.remove('active'));
      const factorLink = document.querySelector(
        '.topbar-nav a[data-section="s-factor"]'
      );
      if (factorLink) factorLink.classList.add('active');
    }
  }, { passive: true });
}
