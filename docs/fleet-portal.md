# Fleet Monitoring Portal

Real-time web dashboard for monitoring the EV charger fleet. Embedded in the API binary — no separate build step, no npm, no framework.

**URL:** `http://localhost:8081/dashboard/`

## Features

### Fleet Dashboard (`/dashboard/`)

Aggregate stats bar at the top showing:

| Stat | Description |
|------|-------------|
| Total Chargers | Count of all registered chargers |
| Online | Chargers currently connected (`online == true`) |
| Charging Now | Connectors in `Charging` status |
| Total Power | Sum of active power across all charging connectors |
| Total Energy | Sum of energy delivered across all active sessions |
| Faulted | Connectors in `Faulted` status (green when 0, red otherwise) |

Below the stats, a card grid shows every charger with:
- Charger ID, vendor, model
- Online/offline badge
- Per-connector status with color-coded badges
- Live power and SoC for charging connectors
- Relative "last seen" timestamp

**Toolbar controls:**
- **Filter** by connector status (All / Charging / Available / Preparing / Finishing / Faulted / Unavailable)
- **Sort** by charger ID, status priority, or power (descending)
- **Search** by charger ID, vendor, or model

### Charger Detail (`/dashboard/chargers/{id}`)

Click any charger card to drill down. Shows:
- Charger metadata (vendor, model, firmware, serial, last boot)
- Online/offline status with last seen time
- Per-connector detail cards:
  - Status with color badge
  - Error code (highlighted red if not `NoError`)
  - Active transaction indicator
  - Live power and cumulative energy
  - SoC percentage with progress bar
  - Status-since and meter-updated relative timestamps

### Charging Sessions (`/dashboard/sessions`)

Live-updating table of completed charging sessions (CDRs). Newest sessions appear at the top with a subtle highlight animation.

| Column | Description |
|--------|-------------|
| Ended | Time the session ended |
| Charger | Charger ID (clickable — navigates to charger detail) |
| Conn | Connector number |
| Duration | Session length (e.g., `3m 5s`) |
| Energy | Total energy delivered (e.g., `4.9 kWh`) |
| ID Tag | RFID tag used for authorization |
| Stop Reason | Why the session ended, color-coded badge |

**Stop reason colors:**
- Green: `EVDisconnected` (normal — driver unplugged)
- Blue: `Local` (stopped at the charger)
- Yellow: `Remote` (stopped remotely via CSMS)

Data source: `GET /sessions` endpoint querying the `charge_sessions` table in PostgreSQL.

## Live Updates

- Fleet view polls `GET /chargers` every **5 seconds**
- Detail view polls `GET /chargers/{id}/state` every **3 seconds**
- Pulsing green "Live" indicator in the header
- Only changed values update — no full-page flash

## Status Color Legend

| Status | Color |
|--------|-------|
| Available | Green |
| Preparing / Finishing | Yellow/Amber |
| Charging | Blue |
| SuspendedEV / SuspendedEVSE | Orange |
| Faulted | Red |
| Unavailable / Reserved | Grey |

## Technical Implementation

| Aspect | Detail |
|--------|--------|
| Frontend | Vanilla HTML + CSS + JS, no framework |
| Serving | Embedded in Go API binary via `//go:embed dashboard/*` |
| Data source | Existing REST API (`/chargers`, `/chargers/{id}/state`) |
| Routing | SPA — all non-asset paths serve `index.html`, JS handles client-side routing |
| Theme | Dark theme, responsive grid layout |
| Dependencies | Zero external dependencies (no CDN, no npm) |

### File Structure

```
cmd/api/
  dashboard/
    index.html       # SPA shell with stats bar, toolbar, app container
    style.css        # Dark theme, card layout, status badges, responsive grid
    app.js           # Fetch logic, rendering, auto-refresh, filtering, sorting, SPA navigation
  main.go            # Embeds dashboard/*, serves at /dashboard/
```

### Go Integration

The dashboard files are embedded at compile time:

```go
//go:embed dashboard/*
var dashboardFS embed.FS
```

Routes are registered on the existing `http.ServeMux`:
- `/dashboard/*.css`, `/dashboard/*.js` — served as static assets with correct MIME types
- `/dashboard/**` (everything else) — serves `index.html` for SPA client-side routing

No new containers, no CORS configuration, no additional ports.

## Screenshots

_Launch the platform and navigate to `http://localhost:8081/dashboard/` to see the portal in action._
