/**
 * Unified tooltip popup utility.
 * Click on the ⓘ icon toggles a small popup with explanation.
 * Popup is appended to document.body with position:fixed and
 * placed via getBoundingClientRect() so it never gets clipped
 * by parent overflow or pushed off-viewport.
 */

export function makeInfoIcon(text, opts = {}) {
  const wrap = document.createElement('span');
  wrap.style.cssText =
    'position:relative;display:inline-flex;' +
    'align-items:center;margin-left:4px;vertical-align:middle;';

  const icon = document.createElement('span');
  icon.textContent = 'ⓘ';
  icon.style.cssText =
    'font-size:11px;cursor:pointer;line-height:1;' +
    'user-select:none;' +
    (opts.dark === false
      ? 'color:var(--text-t);'
      : 'color:rgba(255,255,255,0.55);');

  const popup = document.createElement('div');
  popup.setAttribute('data-tooltip-popup', '1');
  popup.style.cssText =
    'position:fixed;background:#1B2A4A;color:#fff;' +
    'font-size:12px;font-weight:400;line-height:1.6;' +
    'text-transform:none;text-align:left;' +
    'padding:10px 14px;border-radius:8px;width:240px;' +
    'z-index:9999;pointer-events:none;opacity:0;' +
    'box-shadow:0 4px 20px rgba(0,0,0,0.25);' +
    'transition:opacity 0.15s;';
  popup.textContent = text;
  document.body.appendChild(popup);

  icon.addEventListener('click', (e) => {
    e.stopPropagation();
    document.querySelectorAll('[data-tooltip-popup]').forEach(p => {
      p.style.opacity = '0';
      p.style.pointerEvents = 'none';
    });
    const rect = icon.getBoundingClientRect();
    const spaceAbove = rect.top;
    const spaceBelow = window.innerHeight - rect.bottom;
    if (spaceAbove < 140 || spaceBelow > spaceAbove) {
      popup.style.top = (rect.bottom + 6) + 'px';
      popup.style.bottom = 'auto';
    } else {
      const h = popup.offsetHeight || 130;
      popup.style.top = (rect.top - 6 - h) + 'px';
      popup.style.bottom = 'auto';
    }
    let left = rect.left + rect.width / 2 - 120;
    left = Math.max(8, Math.min(left, window.innerWidth - 248));
    popup.style.left = left + 'px';
    popup.style.opacity = '1';
    popup.style.pointerEvents = 'auto';
  });

  document.addEventListener('click', () => {
    popup.style.opacity = '0';
    popup.style.pointerEvents = 'none';
  });

  wrap.appendChild(icon);
  return wrap;
}
