/**
 * Feedback widget — shown once per session after Factor Fingerprint.
 * Opens Google Form in new tab.
 */

const FORM_URL = 'https://docs.google.com/forms/d/' +
  'e/1FAIpQLSfRt3wavrE70n5P1pbVvMZ3-shzXu8_wy6MD4X' +
  'KVhPtAIlfhA/viewform';

export function renderFeedback(container) {
  container.textContent = '';

  const card = document.createElement('div');
  card.className = 'feedback-card';

  // Bottone chiudi X
  const closeBtn = document.createElement('button');
  closeBtn.className = 'feedback-close';
  closeBtn.textContent = '\u00D7';
  closeBtn.setAttribute('aria-label', 'Chiudi');
  closeBtn.addEventListener('click', () => {
    card.style.opacity = '0';
    setTimeout(() => card.remove(), 500);
  });

  const title = document.createElement('div');
  title.className = 'feedback-title';
  title.textContent =
    'Hai 90 secondi? Il tuo feedback ci aiuta' +
    ' a migliorare \uD83D\uDE4F';

  const btn = document.createElement('button');
  btn.className = 'feedback-btn-primary';
  btn.textContent = 'Dai il tuo feedback \u2192';
  btn.addEventListener('click', () => {
    window.open(FORM_URL, '_blank',
      'noopener,noreferrer');
  });

  card.append(closeBtn, title, btn);
  container.appendChild(card);

}
