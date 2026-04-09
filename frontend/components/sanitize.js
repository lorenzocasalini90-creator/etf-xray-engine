/**
 * HTML text sanitization utility.
 * All user/API data must pass through esc() before insertion into innerHTML.
 */

/**
 * Escape HTML special characters to prevent XSS.
 * @param {*} val — value to escape (converted to string)
 * @returns {string} — safe HTML string
 */
export function esc(val) {
  if (val === null || val === undefined) return '';
  const str = String(val);
  const div = document.createElement('div');
  div.textContent = str;
  return div.innerHTML;
}

/**
 * Format a number as EUR with Italian locale.
 * @param {number} n
 * @returns {string} — escaped formatted string
 */
export function fmtEur(n) {
  return esc(Number(n).toLocaleString('it-IT'));
}

/**
 * Format a number with fixed decimals.
 * @param {number} n
 * @param {number} decimals
 * @returns {string}
 */
export function fmtNum(n, decimals = 1) {
  if (n === null || n === undefined || isNaN(n)) return '—';
  return esc(Number(n).toFixed(decimals));
}
