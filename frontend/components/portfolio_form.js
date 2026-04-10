/**
 * Portfolio form — ETF input, autocomplete, list management.
 * All dynamic text escaped via sanitize.js.
 */
import { esc, fmtEur } from './sanitize.js';

const ETF_COLORS = ['#1B2A4A','#0D5E4C','#6366F1','#EC4899','#F59E0B',
                     '#8B5CF6','#14B8A6','#EF4444','#3B82F6','#84CC16'];
const BENCHMARKS = [
  { value: 'MSCI_WORLD',     label: 'MSCI World (SWDA)' },
  { value: 'SP500',          label: 'S&P 500 (CSPX)' },
  { value: 'MSCI_EM',        label: 'MSCI EM (EIMI)' },
  { value: 'FTSE_ALL_WORLD', label: 'FTSE All-World (VWCE)' },
  { value: '',               label: 'Nessun benchmark' },
];

let _positions = [];
let _onAnalyze = null;
let _container = null;
let _debounceTimer = null;
let _jsonInput = null;

export function renderPortfolioForm(container, onAnalyze) {
  _container = container;
  _onAnalyze = onAnalyze;

  if (!_jsonInput) {
    _jsonInput = document.createElement('input');
    _jsonInput.type = 'file';
    _jsonInput.accept = '.json';
    _jsonInput.hidden = true;
    _jsonInput.addEventListener('change', _onJsonLoad);
    document.body.appendChild(_jsonInput);
  }

  const saved = localStorage.getItem('cmf_portfolio');
  if (saved) { try { _positions = JSON.parse(saved); } catch(e) { _positions = []; } }
  _positions = _positions.filter(p =>
    p && p.ticker && typeof p.ticker === 'string' &&
    p.ticker.length >= 2 && p.ticker.charCodeAt(0) >= 32 &&
    Number(p.capital) > 0
  ).map(p => ({ ticker: p.ticker, capital: Number(p.capital), name: p.name || '' }));
  _render();
}

function _render() {
  const total = _positions.reduce((s, p) => s + p.capital, 0);
  const c = _container;
  c.textContent = '';

  const card = document.createElement('div');
  card.className = 'form-card fade-in';

  // Title
  const title = document.createElement('div');
  title.className = 'form-title';
  title.textContent = 'Analizza il tuo portafoglio ETF';
  card.appendChild(title);

  const sub = document.createElement('div');
  sub.className = 'form-subtitle';
  sub.textContent = 'Inserisci i tuoi ETF per ottenere un report completo: overlap, ridondanza, esposizione settoriale e factor fingerprint.';
  card.appendChild(sub);

  // Input row
  const row = document.createElement('div');
  row.className = 'form-row';

  // Ticker field
  const tickerWrap = document.createElement('div');
  tickerWrap.className = 'form-field autocomplete-wrap';
  tickerWrap.style.flex = '2';
  const tickerLabel = document.createElement('label');
  tickerLabel.textContent = 'Ticker o ISIN';
  const tickerInput = document.createElement('input');
  tickerInput.type = 'text';
  tickerInput.id = 'etf-input';
  tickerInput.placeholder = 'es. SWDA, VWCE, IE00B5BMR087';
  tickerInput.autocomplete = 'off';
  const acList = document.createElement('div');
  acList.className = 'autocomplete-list';
  acList.id = 'ac-list';
  acList.hidden = true;
  tickerWrap.append(tickerLabel, tickerInput, acList);

  // Amount field
  const amountWrap = document.createElement('div');
  amountWrap.className = 'form-field';
  amountWrap.style.flex = '1';
  const amountLabel = document.createElement('label');
  amountLabel.textContent = 'Importo EUR';
  const amountInput = document.createElement('input');
  amountInput.type = 'number';
  amountInput.id = 'amount-input';
  amountInput.value = '10000';
  amountInput.min = '1';
  amountInput.step = '500';
  amountWrap.append(amountLabel, amountInput);

  // Add button
  const btnAdd = document.createElement('button');
  btnAdd.className = 'btn-add';
  btnAdd.textContent = '+ Aggiungi';

  row.append(tickerWrap, amountWrap, btnAdd);
  card.appendChild(row);

  // ETF list
  const listDiv = document.createElement('div');
  listDiv.className = 'etf-list';
  if (_positions.length === 0) {
    const empty = document.createElement('p');
    empty.style.cssText = 'color:var(--text-t);font-size:12px;text-align:center;padding:20px 0';
    empty.textContent = 'Nessun ETF aggiunto. Inizia cercando un ticker.';
    listDiv.appendChild(empty);
  } else {
    _positions.forEach((p, i) => {
      const erow = document.createElement('div');
      erow.className = 'etf-row';
      const dot = document.createElement('span');
      dot.className = 'etf-dot';
      dot.style.background = ETF_COLORS[i % ETF_COLORS.length];
      const ticker = document.createElement('span');
      ticker.className = 'etf-ticker';
      ticker.textContent = p.ticker;
      const name = document.createElement('span');
      name.className = 'etf-name';
      name.textContent = p.name || '';
      const amount = document.createElement('span');
      amount.className = 'etf-amount';
      amount.textContent = fmtEur(Number(p.capital));
      const removeBtn = document.createElement('button');
      removeBtn.className = 'etf-remove';
      removeBtn.textContent = '\u00D7';
      removeBtn.title = 'Rimuovi';
      removeBtn.addEventListener('click', () => { _positions.splice(i, 1); _render(); });
      erow.append(dot, ticker, name, amount, removeBtn);
      listDiv.appendChild(erow);
    });
  }
  card.appendChild(listDiv);

  if (_positions.length > 0) {
    const clearBtn = document.createElement('button');
    clearBtn.style.cssText =
      'background:none;border:none;color:var(--text-t);' +
      'font-size:11px;cursor:pointer;padding:4px 0;' +
      'display:block;margin-left:auto;margin-bottom:8px;';
    clearBtn.textContent = '× Svuota portafoglio';
    clearBtn.addEventListener('click', () => {
      _positions = [];
      localStorage.removeItem('cmf_portfolio');
      _render();
    });
    card.appendChild(clearBtn);
  }

  // Distribution bar
  if (_positions.length > 0) {
    const distBar = document.createElement('div');
    distBar.className = 'dist-bar';
    _positions.forEach((p, i) => {
      const seg = document.createElement('div');
      seg.className = 'dist-segment';
      seg.style.width = (p.capital / total * 100).toFixed(1) + '%';
      seg.style.background = ETF_COLORS[i % ETF_COLORS.length];
      distBar.appendChild(seg);
    });
    card.appendChild(distBar);

    const totalDiv = document.createElement('div');
    totalDiv.className = 'form-total';
    totalDiv.innerHTML = 'Totale: <strong>' + esc(fmtEur(total)) + '</strong>';
    card.appendChild(totalDiv);
  }

  // Benchmark
  const benchRow = document.createElement('div');
  benchRow.className = 'bench-row';
  const benchLabel = document.createElement('label');
  benchLabel.textContent = 'Benchmark:';
  const benchSelect = document.createElement('select');
  benchSelect.id = 'bench-select';
  BENCHMARKS.forEach(b => {
    const opt = document.createElement('option');
    opt.value = b.value;
    opt.textContent = b.label;
    benchSelect.appendChild(opt);
  });
  benchRow.append(benchLabel, benchSelect);
  card.appendChild(benchRow);

  // CTA
  const btnCta = document.createElement('button');
  btnCta.className = 'btn-cta';
  btnCta.id = 'btn-analyze';
  btnCta.textContent = 'Analizza Portafoglio';
  btnCta.disabled = _positions.length === 0;
  card.appendChild(btnCta);

  // Secondary actions
  const actions = document.createElement('div');
  actions.className = 'form-actions';
  const btnSave = document.createElement('button');
  btnSave.className = 'btn-secondary';
  btnSave.textContent = '\uD83D\uDCBE Salva';
  const btnLoad = document.createElement('button');
  btnLoad.className = 'btn-secondary';
  btnLoad.textContent = '\uD83D\uDCC2 Carica';
  const fileLabel = document.createElement('label');
  fileLabel.className = 'btn-secondary';
  fileLabel.style.cssText = 'text-align:center;cursor:pointer';
  fileLabel.textContent = '\uD83D\uDCE4 Importa CSV';
  const fileInput = document.createElement('input');
  fileInput.type = 'file';
  fileInput.accept = '.csv,.xlsx,.xls';
  fileInput.hidden = true;
  fileLabel.appendChild(fileInput);
  actions.append(btnSave, btnLoad, fileLabel);
  card.appendChild(actions);

  c.appendChild(card);

  // Event listeners
  btnAdd.addEventListener('click', _addETF);
  tickerInput.addEventListener('keydown', e => { if (e.key === 'Enter') _addETF(); });
  tickerInput.addEventListener('input', _onSearchInput);
  btnCta.addEventListener('click', _submit);
  btnSave.addEventListener('click', () => {
    // Save to localStorage AND download as JSON file
    localStorage.setItem('cmf_portfolio', JSON.stringify(_positions));
    const blob = new Blob([JSON.stringify(_positions, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'portafoglio_checkmyetfs.json';
    a.click();
    URL.revokeObjectURL(url);
    btnSave.textContent = '\u2713 Salvato';
    setTimeout(() => { btnSave.textContent = '\uD83D\uDCBE Salva'; }, 1500);
  });
  btnLoad.addEventListener('click', () => _jsonInput && _jsonInput.click());
  fileInput.addEventListener('change', _onFileUpload);

  // Close autocomplete on outside click
  document.addEventListener('click', e => {
    if (!tickerWrap.contains(e.target)) acList.hidden = true;
  });
}

function _addETF() {
  const input = _container.querySelector('#etf-input');
  const amountInput = _container.querySelector('#amount-input');
  const ticker = input.value.trim().toUpperCase();
  const amount = parseFloat(amountInput.value);
  if (!ticker || ticker.length < 2 || isNaN(amount) || amount <= 0) return;
  if (_positions.length >= 10 || _positions.some(p => p.ticker === ticker)) return;
  _positions.push({ ticker, capital: Number(amount), name: '' });
  input.value = '';
  const acList = _container.querySelector('#ac-list');
  if (acList) acList.hidden = true;
  _render();
}

function _onSearchInput(e) {
  const q = e.target.value.trim();
  clearTimeout(_debounceTimer);
  if (q.length < 2) { const l = _container.querySelector('#ac-list'); if(l) l.hidden = true; return; }
  _debounceTimer = setTimeout(async () => {
    try {
      const res = await fetch('/api/search?q=' + encodeURIComponent(q) + '&limit=5');
      if (!res.ok) return;
      const results = await res.json();
      _showAutocomplete(results);
    } catch(err) { /* ignore */ }
  }, 200);
}

function _showAutocomplete(results) {
  const list = _container.querySelector('#ac-list');
  if (!results || results.length === 0 || !list) { if(list) list.hidden = true; return; }
  list.textContent = '';
  results.forEach(r => {
    const item = document.createElement('div');
    item.className = 'autocomplete-item';
    const tickerSpan = document.createElement('span');
    tickerSpan.className = 'ac-ticker';
    tickerSpan.textContent = r.ticker || '';
    const nameSpan = document.createElement('span');
    nameSpan.className = 'ac-name';
    nameSpan.textContent = (r.name || '') + (r.ter_pct ? ' (TER ' + r.ter_pct + '%)' : '');
    item.append(tickerSpan, nameSpan);
    item.addEventListener('click', () => {
      _container.querySelector('#etf-input').value = r.ticker;
      list.hidden = true;
    });
    list.appendChild(item);
  });
  list.hidden = false;
}

function _submit() {
  if (_positions.length === 0) return;
  const bench = _container.querySelector('#bench-select').value || null;
  const positions = _positions.map(p => ({ ticker: p.ticker, amount_eur: p.capital }));
  if (_onAnalyze) _onAnalyze(positions, bench);
}

function _onFileUpload(e) {
  const file = e.target.files[0];
  if (!file) return;
  const ext = file.name.split('.').pop().toLowerCase();

  if (ext === 'xlsx' || ext === 'xls') {
    const reader = new FileReader();
    reader.onload = (evt) => {
      try {
        const wb = XLSX.read(evt.target.result, { type: 'array' });
        const ws = wb.Sheets[wb.SheetNames[0]];
        const rows = XLSX.utils.sheet_to_json(ws, { header: 1 });
        for (let i = 1; i < rows.length && _positions.length < 10; i++) {
          const ticker = String(rows[i][0] || '').trim().toUpperCase();
          const raw = String(rows[i][1] || '').replace(/[^\d.]/g, '');
          const amount = parseFloat(raw);
          if (ticker && ticker.length >= 2 &&
              !isNaN(amount) && amount > 0 &&
              !_positions.some(p => p.ticker === ticker)) {
            _positions.push({ ticker, capital: Number(amount), name: '' });
          }
        }
      } catch (err) {
        console.error('XLSX parse error:', err);
      }
      _render();
    };
    reader.readAsArrayBuffer(file);
  } else {
    const reader = new FileReader();
    reader.onload = (evt) => {
      const lines = evt.target.result.split('\n').filter(l => l.trim());
      for (let i = 1; i < lines.length && _positions.length < 10; i++) {
        const parts = lines[i].split(/[,;\t]/);
        if (parts.length >= 2) {
          const ticker = parts[0].trim().toUpperCase();
          const amount = parseFloat(parts[1].replace(/[^\d.]/g, ''));
          if (ticker && !isNaN(amount) && amount > 0 && !_positions.some(p => p.ticker === ticker)) {
            _positions.push({ ticker, capital: Number(amount), name: '' });
          }
        }
      }
      _render();
    };
    reader.readAsText(file, 'UTF-8');
  }

  e.target.value = '';
}

function _onJsonLoad(e) {
  const file = e.target.files[0];
  if (!file) return;
  const reader = new FileReader();
  reader.onload = (evt) => {
    try {
      const data = JSON.parse(evt.target.result);
      // Accept both array format [{ticker, capital}] and wrapped {positions: [...]}
      const arr = Array.isArray(data) ? data : (data.positions || []);
      if (!Array.isArray(arr) || arr.length === 0) return;
      _positions = arr.filter(p => p.ticker && Number(p.capital) > 0).slice(0, 10).map(p => ({
        ticker: String(p.ticker).trim().toUpperCase(),
        capital: Number(p.capital) || 0,
        name: p.name || '',
      }));
      _render();
    } catch (err) {
      /* ignore malformed JSON */
    }
  };
  reader.readAsText(file, 'UTF-8');
  // Reset so the same file can be loaded again
  e.target.value = '';
}

export function getPositions() { return _positions; }
