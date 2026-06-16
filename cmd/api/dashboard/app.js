// Fleet Monitoring Portal
(function () {
  'use strict';

  const API_BASE = window.location.origin;
  const FLEET_INTERVAL = 5000;
  const DETAIL_INTERVAL = 3000;
  const SESSIONS_INTERVAL = 5000;

  let currentView = 'fleet'; // 'fleet', 'detail', or 'sessions'
  let currentChargerId = null;
  let refreshTimer = null;
  let lastData = null;
  let lastSessionIds = new Set();

  // --- Helpers ---

  function formatPower(watts) {
    var w = parseFloat(watts) || 0;
    if (w >= 1000000) return (w / 1000000).toFixed(2) + ' MW';
    if (w >= 1000) return (w / 1000).toFixed(1) + ' kW';
    return w.toFixed(0) + ' W';
  }

  function formatEnergy(wh) {
    var v = parseFloat(wh) || 0;
    if (v >= 1000000) return (v / 1000000).toFixed(1) + ' MWh';
    if (v >= 1000) return (v / 1000).toFixed(1) + ' kWh';
    return v.toFixed(0) + ' Wh';
  }

  function formatDuration(seconds) {
    var s = parseInt(seconds) || 0;
    if (s < 60) return s + 's';
    var m = Math.floor(s / 60);
    var sec = s % 60;
    if (m < 60) return m + 'm ' + sec + 's';
    var h = Math.floor(m / 60);
    m = m % 60;
    return h + 'h ' + m + 'm';
  }

  function formatTime(isoStr) {
    if (!isoStr) return 'n/a';
    var d = new Date(isoStr);
    return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  }

  function timeAgo(isoStr) {
    if (!isoStr) return 'n/a';
    var diff = (Date.now() - new Date(isoStr).getTime()) / 1000;
    if (diff < 0) return 'just now';
    if (diff < 60) return Math.floor(diff) + 's ago';
    if (diff < 3600) return Math.floor(diff / 60) + 'm ago';
    if (diff < 86400) return Math.floor(diff / 3600) + 'h ago';
    return Math.floor(diff / 86400) + 'd ago';
  }

  function statusClass(status) {
    return 'status-' + (status || 'Unavailable');
  }

  function setActiveNav(view) {
    document.getElementById('nav-fleet').className = (view === 'fleet' || view === 'detail') ? 'active' : '';
    document.getElementById('nav-sessions').className = view === 'sessions' ? 'active' : '';
  }

  // --- Fleet Stats ---

  function computeStats(chargers) {
    var stats = {
      total: chargers.length,
      online: 0,
      charging: 0,
      totalPowerW: 0,
      totalEnergyWh: 0,
      faulted: 0,
    };
    for (var i = 0; i < chargers.length; i++) {
      var c = chargers[i];
      if (c.online) stats.online++;
      var conns = c.connectors || [];
      for (var j = 0; j < conns.length; j++) {
        var conn = conns[j];
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
    var chargers = data.chargers || [];
    lastData = chargers;

    var stats = computeStats(chargers);

    document.getElementById('stat-total').textContent = stats.total;
    document.getElementById('stat-online').textContent = stats.online + ' / ' + stats.total;
    document.getElementById('stat-charging').textContent = stats.charging;
    document.getElementById('stat-power').textContent = formatPower(stats.totalPowerW);
    document.getElementById('stat-energy').textContent = formatEnergy(stats.totalEnergyWh);

    var faultEl = document.getElementById('stat-faulted');
    faultEl.textContent = stats.faulted;
    faultEl.className = 'stat-value ' + (stats.faulted > 0 ? 'red' : 'green');

    renderChargerGrid(chargers);
  }

  function renderChargerGrid(chargers) {
    var filter = document.getElementById('filter-status').value;
    var sortBy = document.getElementById('sort-by').value;
    var search = (document.getElementById('search-box').value || '').toLowerCase();

    var filtered = chargers;

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
        var order = { Charging: 0, Faulted: 1, Preparing: 2, Finishing: 3, SuspendedEV: 3.5, Available: 4, Unavailable: 5 };
        var sa = Math.min.apply(null, (a.connectors || []).map(function (c) { return order[c.status] != null ? order[c.status] : 9; }));
        var sb = Math.min.apply(null, (b.connectors || []).map(function (c) { return order[c.status] != null ? order[c.status] : 9; }));
        return sa - sb;
      }
      if (sortBy === 'power') {
        var pa = (a.connectors || []).reduce(function (s, c) { return s + (parseFloat(c.power_w) || 0); }, 0);
        var pb = (b.connectors || []).reduce(function (s, c) { return s + (parseFloat(c.power_w) || 0); }, 0);
        return pb - pa;
      }
      return 0;
    });

    var grid = document.getElementById('charger-grid');
    grid.innerHTML = filtered.map(function (c) { return chargerCardHTML(c); }).join('');
  }

  function chargerCardHTML(c) {
    var badge = c.online
      ? '<span class="online-badge">Online</span>'
      : '<span class="offline-badge">Offline</span>';

    var connectors = (c.connectors || []).map(function (conn) {
      var power = conn.status === 'Charging' ? formatPower(conn.power_w) : '';
      var soc = conn.soc_percent ? 'SoC: ' + parseFloat(conn.soc_percent).toFixed(0) + '%' : '';
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
    var app = document.getElementById('app');

    var meta = [
      c.vendor ? 'Vendor: ' + c.vendor : '',
      c.model ? 'Model: ' + c.model : '',
      c.firmware ? 'FW: ' + c.firmware : '',
      c.serial ? 'S/N: ' + c.serial : '',
      'Last boot: ' + timeAgo(c.last_boot),
    ].filter(Boolean).map(function (s) { return '<span>' + s + '</span>'; }).join('');

    var badge = c.online
      ? '<span class="online-badge">Online</span>'
      : '<span class="offline-badge">Offline</span>';

    var connCards = (c.connectors || []).map(function (conn) {
      var socPct = parseFloat(conn.soc_percent) || 0;
      var socBar = conn.soc_percent
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

  // --- Rendering: Sessions View ---

  function renderSessions(data) {
    var sessions = data.sessions || [];
    var app = document.getElementById('app');

    var newIds = new Set(sessions.map(function (s) { return s.session_id; }));

    var rows = sessions.map(function (s) {
      var isNew = !lastSessionIds.has(s.session_id);
      var reasonClass = 'reason-' + (s.stop_reason || 'Other');
      if (!['EVDisconnected', 'Local', 'Remote'].includes(s.stop_reason)) reasonClass = 'reason-Other';

      return '<tr class="' + (isNew ? 'session-new' : '') + '">' +
        '<td>' + formatTime(s.end_time) + '</td>' +
        '<td><span class="charger-link" onclick="window.navigateToCharger(\'' + s.charger_id + '\')">' + s.charger_id + '</span></td>' +
        '<td>' + s.connector_id + '</td>' +
        '<td>' + formatDuration(s.duration_sec) + '</td>' +
        '<td>' + formatEnergy(s.energy_wh) + '</td>' +
        '<td>' + s.id_tag + '</td>' +
        '<td><span class="reason-badge ' + reasonClass + '">' + (s.stop_reason || 'Unknown') + '</span></td>' +
        '</tr>';
    }).join('');

    lastSessionIds = newIds;

    app.innerHTML =
      '<div class="sessions-page">' +
      '<h2>Completed Charging Sessions (CDRs)</h2>' +
      '<table class="sessions-table">' +
      '<thead><tr>' +
      '<th>Ended</th>' +
      '<th>Charger</th>' +
      '<th>Conn</th>' +
      '<th>Duration</th>' +
      '<th>Energy</th>' +
      '<th>ID Tag</th>' +
      '<th>Stop Reason</th>' +
      '</tr></thead>' +
      '<tbody>' + (rows || '<tr><td colspan="7" style="text-align:center; color:var(--text-muted); padding:40px;">No sessions yet...</td></tr>') +
      '</tbody></table></div>';
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

  function fetchSessions() {
    fetch(API_BASE + '/sessions?limit=100')
      .then(function (r) { return r.json(); })
      .then(function (data) {
        if (currentView === 'sessions') renderSessions(data);
      })
      .catch(function (err) {
        console.error('Sessions fetch error:', err);
      });
  }

  // --- Navigation ---

  window.navigateToCharger = function (id) {
    currentView = 'detail';
    currentChargerId = id;
    clearInterval(refreshTimer);

    var app = document.getElementById('app');
    app.innerHTML = '<div class="loading">Loading charger ' + id + '...</div>';

    document.getElementById('fleet-controls').style.display = 'none';
    setActiveNav('detail');

    fetchDetail(id);
    refreshTimer = setInterval(function () { fetchDetail(id); }, DETAIL_INTERVAL);

    history.pushState({ view: 'detail', id: id }, '', '/dashboard/chargers/' + id);
  };

  window.navigateToFleet = function () {
    currentView = 'fleet';
    currentChargerId = null;
    clearInterval(refreshTimer);

    var app = document.getElementById('app');
    app.innerHTML = '<div id="charger-grid" class="charger-grid"><div class="loading">Loading fleet...</div></div>';

    document.getElementById('fleet-controls').style.display = '';
    setActiveNav('fleet');

    fetchFleet();
    refreshTimer = setInterval(fetchFleet, FLEET_INTERVAL);

    history.pushState({ view: 'fleet' }, '', '/dashboard/');
  };

  window.navigateToSessions = function () {
    currentView = 'sessions';
    currentChargerId = null;
    clearInterval(refreshTimer);
    lastSessionIds = new Set();

    var app = document.getElementById('app');
    app.innerHTML = '<div class="loading">Loading sessions...</div>';

    document.getElementById('fleet-controls').style.display = 'none';
    setActiveNav('sessions');

    fetchSessions();
    refreshTimer = setInterval(fetchSessions, SESSIONS_INTERVAL);

    history.pushState({ view: 'sessions' }, '', '/dashboard/sessions');
  };

  // Handle browser back/forward
  window.addEventListener('popstate', function (e) {
    if (e.state && e.state.view === 'detail') {
      window.navigateToCharger(e.state.id);
    } else if (e.state && e.state.view === 'sessions') {
      window.navigateToSessions();
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

    // Check URL for routing
    var path = window.location.pathname;
    if (path.match(/\/dashboard\/chargers\/(.+)/)) {
      var id = decodeURIComponent(path.match(/\/dashboard\/chargers\/(.+)/)[1]);
      window.navigateToCharger(id);
    } else if (path.match(/\/dashboard\/sessions/)) {
      window.navigateToSessions();
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
