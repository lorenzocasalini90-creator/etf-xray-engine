/**
 * Sanitization + formatting utilities.
 * All user/API data must pass through esc()/sanitize() before DOM insertion.
 */

/** Escape HTML special characters to prevent XSS. */
export function esc(val) {
  if (val === null || val === undefined) return '';
  const str = String(val);
  const div = document.createElement('div');
  div.textContent = str;
  return div.innerHTML;
}

/** Sanitize a string: remove non-printable chars, keep unicode letters. */
export function sanitize(str) {
  if (!str) return '';
  return String(str).replace(/[\x00-\x1F\x7F]/g, '').trim();
}

/** Format EUR with Italian locale (€ 40.000). */
export function fmtEur(n) {
  if (n === null || n === undefined || isNaN(n)) return '\u2014';
  return new Intl.NumberFormat('it-IT', {
    style: 'currency', currency: 'EUR', maximumFractionDigits: 0,
  }).format(n);
}

/** Format percentage with 2 decimals (14.50%). */
export function fmtPct(n) {
  if (n === null || n === undefined || isNaN(n)) return '\u2014';
  return Number(n).toFixed(2) + '%';
}

/** Format integer with Italian locale separators (4.340). */
export function fmtNum(n) {
  if (n === null || n === undefined || isNaN(n)) return '\u2014';
  return new Intl.NumberFormat('it-IT').format(n);
}
