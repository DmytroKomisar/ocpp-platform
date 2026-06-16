// Fleet Monitoring Portal
(function () {
  'use strict';

  const API_BASE = window.location.origin;
  const FLEET_INTERVAL = 5000;
  const DETAIL_INTERVAL = 3000;

  let currentView = 'fleet'; // 'fleet' or 'detail'
  let currentChargerId = null;
  let refreshTimer = null;
  let lastData = null;

  // --- Helpers ---

  function formatPower(watts) {
    const w = parseFloat(watts) || 0;
    if (w >= 1000000) return (w / 1000000).toFixed(2) + ' MW';
    if (w >= 1000) return (w / 1000).toFixed(1) + ' kW';
    return w.toFixed(0) + ' W';
  }

  function formatEnergy(wh) {
    const v = parseFloat(wh) || 0;
    if (v >= 1000000) return (v / 1000000).toFixed(1) + ' MWh';
    if (v >= 1000) return (v / 1000).toFixed(1) + ' kWh';
    return v.toFixed(0) + ' Wh';
  }

  function timeAgo(isoStr) {
    if (!isoStr) return 'n/a';
    const diff = (Date.now() - new Date(isoStr).getTime()) / 1000;
    if (diff < 0) return 'just now';
    if (diff < 60) return Math.floor(diff) + 's ago';
    if (diff < 3600) return Math.floor(diff / 60) + 'm ago';
    if (diff < 86400) return Math.floor(diff / 3600) + 'h ago';
    return Math.floor(diff / 86400) + 'd ago';
  }

  function statusClass(status) {
    return 'status-' + (status || 'Unavailable');
  }

  // --- Fleet Stats ---

  function computeStats(chargers) {
    const stats = {
      total: chargers.length,
      online: 0,
      charging: 0,
      totalPowerW: 0,
      totalEnergyWh: 0,
      faulted: 0,
    };
    for (const c of chargers) {
      if (c.online) stats.online++;
      for (const conn of c.connectors || []) {
        if (conn.status === 'Charging') {
          stats.charging++;
          stats.totalPowerW += parseFloat(conn.power_w) || 0;
          stats.totalEnergyWh += parseFloat(conn.energy_wh) || 0;
        }
        if (conn.status === 'Faulted') stats.faulted++;
      }
    }
    return stats;
  }

  // --- Rendering: Fleet View ---

  function renderFleet(data) {
    const chargers = data.chargers || [];
    lastData = chargers;

    const stats = computeStats(chargers);

    document.getElementById('stat-total').textContent = stats.total;
    document.getElementById('stat-online').textContent = stats.online + ' / ' + stats.total;
    document.getElementById('stat-charging').textContent = stats.charging;
    document.getElementById('stat-power').textContent = formatPower(stats.totalPowerW);
    document.getElementById('stat-energy').textContent = formatEnergy(stats.totalEnergyWh);

    const faultEl = document.getElementById('stat-faulted');
    faultEl.textContent = stats.faulted;
    faultEl.className = 'stat-value ' + (stats.faulted > 0 ? 'red' : 'green');

    renderChargerGrid(chargers);
  }

  function renderChargerGrid(chargers) {
    const filter = document.getElementById('filter-status').value;
    const sortBy = document.getElementById('sort-by').value;
    const search = (document.getElementById('search-box').value || '').toLowerCase();

    let filtered = chargers;

    if (filter !== 'All') {
      filtered = filtered.filter(function (c) {
        return (c.connectors || []).some(function (conn) { return conn.status === filter; });
      });
    }

    if (search) {
      filtered = filtered.filter(function (c) {
        return c.charger_id.toLowerCase().includes(search) ||
          (c.vendor || '').toLowerCase().includes(search) ||
          (c.model || '').toLowerCase().includes(search);
      });
    }

    filtered.sort(function (a, b) {
      if (sortBy === 'id') return a.charger_id.localeCompare(b.charger_id);
      if (sortBy === 'status') {
        const order = { Charging: 0, Faulted: 1, Preparing: 2, Finishing: 3, Available: 4, Unavailable: 5 };
        const sa = Math.min.apply(null, (a.connectors || []).map(function (c) { return order[c.status] != null ? order[c.status] : 9; }));
        const sb = Math.min.apply(null, (b.connectors || []).map(function (c) { return order[c.status] != null ? order[c.status] : 9; }));
        return sa - sb;
      }
      if (sortBy === 'power') {
        const pa = (a.connectors || []).reduce(function (s, c) { return s + (parseFloat(c.power_w) || 0); }, 0);
        const pb = (b.connectors || []).reduce(function (s, c) { return s + (parseFloat(c.power_w) || 0); }, 0);
        return pb - pa;
      }
      return 0;
    });

    const grid = document.getElementById('charger-grid');
    grid.innerHTML = filtered.map(function (c) { return chargerCardHTML(c); }).join('');
  }

  function chargerCardHTML(c) {
    const badge = c.online
      ? '<span class="online-badge">Online</span>'
      : '<span class="offline-badge">Offline</span>';

    const connectors = (c.connectors || []).map(function (conn) {
      const power = conn.status === 'Charging' ? formatPower(conn.power_w) : '';
      const soc = conn.soc_percent ? 'SoC: ' + parseFloat(conn.soc_percent).toFixed(0) + '%' : '';
      return '<div class="connector-row">' +
        '<span class="connector-label">Conn ' + conn.connector_id + '</span>' +
        '<span class="status-badge ' + statusClass(conn.status) + '">' + (conn.status || '?') + '</span>' +
        (power ? '<span class="connector-power">' + power + '</span>' : '') +
        (soc ? '<span class="connector-soc">' + soc + '</span>' : '') +
        '</div>';
    }).join('');

    return '<div class="charger-card" onclick="window.navigateToCharger(\'' + c.charger_id + '\')">' +
      '<div class="charger-card-header">' +
      '<div><span class="charger-id">' + c.charger_id + '</span> ' +
      '<span class="charger-model">' + (c.vendor || '') + ' ' + (c.model || '') + '</span></div>' +
      badge +
      '</div>' +
      connectors +
      '<div class="charger-card-footer">Last seen: ' + timeAgo(c.last_seen) + '</div>' +
      '</div>';
  }

  // --- Rendering: Detail View ---

  function renderDetail(c) {
    const app = document.getElementById('app');

    const meta = [
      c.vendor ? 'Vendor: ' + c.vendor : '',
      c.model ? 'Model: ' + c.model : '',
      c.firmware ? 'FW: ' + c.firmware : '',
      c.serial ? 'S/N: ' + c.serial : '',
      'Last boot: ' + timeAgo(c.last_boot),
    ].filter(Boolean).map(function (s) { return '<span>' + s + '</span>'; }).join('');

    const badge = c.online
      ? '<span class="online-badge">Online</span>'
      : '<span class="offline-badge">Offline</span>';

    const connCards = (c.connectors || []).map(function (conn) {
      const socPct = parseFloat(conn.soc_percent) || 0;
      const socBar = conn.soc_percent
        ? '<div class="soc-bar"><div class="soc-fill" style="width:' + socPct + '%"></div></div>'
        : '';

      return '<div class="connector-card">' +
        '<h3>Connector ' + conn.connector_id +
        ' <span class="status-badge ' + statusClass(conn.status) + '">' + (conn.status || '?') + '</span></h3>' +
        '<div class="detail-row"><span class="detail-label">Error Code</span><span class="detail-value' +
        (conn.error_code && conn.error_code !== 'NoError' ? ' red' : '') + '">' + (conn.error_code || 'NoError') + '</span></div>' +
        '<div class="detail-row"><span class="detail-label">Transaction</span><span class="detail-value">' +
        (conn.active_transaction ? 'Active' : 'None') + '</span></div>' +
        '<div class="detail-row"><span class="detail-label">Power</span><span class="detail-value">' +
        formatPower(conn.power_w) + '</span></div>' +
        '<div class="detail-row"><span class="detail-label">Energy</span><span class="detail-value">' +
        formatEnergy(conn.energy_wh) + '</span></div>' +
        (conn.soc_percent ? '<div class="detail-row"><span class="detail-label">SoC</span><span class="detail-value">' +
          socPct.toFixed(0) + '%</span></div>' + socBar : '') +
        '<div class="detail-row"><span class="detail-label">Status since</span><span class="detail-value">' +
        timeAgo(conn.status_timestamp) + '</span></div>' +
        '<div class="detail-row"><span class="detail-label">Meter updated</span><span class="detail-value">' +
        timeAgo(conn.meter_timestamp) + '</span></div>' +
        '</div>';
    }).join('');

    app.innerHTML =
      '<div class="detail-page">' +
      '<a href="#" class="back-link" onclick="window.navigateToFleet(); return false;">&larr; Back to Fleet</a>' +
      '<div class="detail-header">' +
      '<h2>' + c.charger_id + ' ' + badge + '</h2>' +
      '<div class="detail-meta">' + meta + '</div>' +
      '<div style="font-size:12px; color:var(--text-muted); margin-top:6px;">Last seen: ' + timeAgo(c.last_seen) + '</div>' +
      '</div>' +
      '<div class="connector-cards">' + connCards + '</div>' +
      '</div>';
  }

  // --- Data Fetching ---

  function fetchFleet() {
    fetch(API_BASE + '/chargers')
      .then(function (r) { return r.json(); })
      .then(function (data) {
        if (currentView === 'fleet') renderFleet(data);
      })
      .catch(function (err) {
        console.error('Fleet fetch error:', err);
      });
  }

  function fetchDetail(id) {
    fetch(API_BASE + '/chargers/' + id + '/state')
      .then(function (r) { return r.json(); })
      .then(function (data) {
        if (currentView === 'detail' && currentChargerId === id) renderDetail(data);
      })
      .catch(function (err) {
        console.error('Detail fetch error:', err);
      });
  }

  // --- Navigation ---

  window.navigateToCharger = function (id) {
    currentView = 'detail';
    currentChargerId = id;
    clearInterval(refreshTimer);

    const app = document.getElementById('app');
    app.innerHTML = '<div class="loading">Loading charger ' + id + '...</div>';

    document.getElementById('fleet-controls').style.display = 'none';

    fetchDetail(id);
    refreshTimer = setInterval(function () { fetchDetail(id); }, DETAIL_INTERVAL);

    history.pushState({ view: 'detail', id: id }, '', '/dashboard/chargers/' + id);
  };

  window.navigateToFleet = function () {
    currentView = 'fleet';
    currentChargerId = null;
    clearInterval(refreshTimer);

    const app = document.getElementById('app');
    app.innerHTML = '<div id="charger-grid" class="charger-grid"><div class="loading">Loading fleet...</div></div>';

    document.getElementById('fleet-controls').style.display = '';

    fetchFleet();
    refreshTimer = setInterval(fetchFleet, FLEET_INTERVAL);

    history.pushState({ view: 'fleet' }, '', '/dashboard/');
  };

  // Handle browser back/forward
  window.addEventListener('popstate', function (e) {
    if (e.state && e.state.view === 'detail') {
      window.navigateToCharger(e.state.id);
    } else {
      window.navigateToFleet();
    }
  });

  // --- Init ---

  function init() {
    // Toolbar events
    document.getElementById('filter-status').addEventListener('change', function () {
      if (lastData) renderChargerGrid(lastData);
    });
    document.getElementById('sort-by').addEventListener('change', function () {
      if (lastData) renderChargerGrid(lastData);
    });
    document.getElementById('search-box').addEventListener('input', function () {
      if (lastData) renderChargerGrid(lastData);
    });

    // Check if URL has a charger ID
    const pathMatch = window.location.pathname.match(/\/dashboard\/chargers\/(.+)/);
    if (pathMatch) {
      window.navigateToCharger(decodeURIComponent(pathMatch[1]));
    } else {
      window.navigateToFleet();
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
