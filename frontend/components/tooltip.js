/**
 * Unified tooltip popup utility.
 * Click on the ⓘ icon toggles a small popup with explanation.
 * Click outside or on another icon closes any open popup.
 */

export function makeInfoIcon(text) {
  const wrap = document.createElement('span');
  wrap.style.cssText = 'position:relative;display:inline-flex;' +
    'align-items:center;margin-left:4px;';

  const icon = document.createElement('span');
  icon.textContent = 'ⓘ';
  icon.style.cssText = 'font-size:11px;color:var(--text-t);' +
    'cursor:pointer;line-height:1;user-select:none;';

  const popup = document.createElement('div');
  popup.style.cssText =
    'position:absolute;bottom:calc(100% + 6px);left:50%;' +
    'transform:translateX(-50%);background:#1B2A4A;color:#fff;' +
    'font-size:11px;line-height:1.5;padding:8px 12px;' +
    'border-radius:8px;width:220px;z-index:500;' +
    'box-shadow:0 4px 16px rgba(0,0,0,0.2);' +
    'pointer-events:none;opacity:0;transition:opacity 0.15s;';
  popup.textContent = text;

  icon.addEventListener('click', (e) => {
    e.stopPropagation();
    const isVisible = popup.style.opacity === '1';
    document.querySelectorAll('[data-tooltip-popup]').forEach(p => {
      p.style.opacity = '0';
      p.style.pointerEvents = 'none';
      p.removeAttribute('data-tooltip-popup');
    });
    if (!isVisible) {
      popup.style.opacity = '1';
      popup.style.pointerEvents = 'auto';
      popup.setAttribute('data-tooltip-popup', '1');
    }
  });

  document.addEventListener('click', () => {
    popup.style.opacity = '0';
    popup.style.pointerEvents = 'none';
    popup.removeAttribute('data-tooltip-popup');
  });

  wrap.append(icon, popup);
  return wrap;
}
