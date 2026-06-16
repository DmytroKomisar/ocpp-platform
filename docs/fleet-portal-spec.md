# Fleet Monitoring Portal — Technical Specification (PoC)

## Overview

Real-time web dashboard for monitoring the EV charger fleet. Shows aggregate fleet stats, per-station drill-down, and live updates. PoC level — no auth, no production hardening.

## Tech Stack

| Layer | Choice | Rationale |
|-------|--------|-----------|
| Frontend | Single-page HTML + vanilla JS (or lightweight — Alpine.js/htmx) | Zero build step, embed in Go binary, PoC speed |
| Data source | Existing REST API (`/chargers`, `/chargers/{id}/state`) | Already built, already has all data |
| Real-time updates | Polling every 3-5s (or SSE from API) | Simple, good enough for PoC. WebSocket overkill here. |
| Serving | Embedded in `cmd/api` (Go `embed`) | No extra container, no CORS issues |

## Pages

### 1. Fleet Dashboard (`/` or `/dashboard`)

Main page. Shows the entire fleet at a glance.

**Aggregate Stats Bar (top):**

| Stat | Source | Format |
|------|--------|--------|
| Total Chargers | count from `/chargers` | `50` |
| Online | count where `online == true` | `48 / 50` |
| Charging Now | count connectors where `status == "Charging"` | `32` |
| Total Power | sum of all `power_w` where status is Charging | `1.2 MW` |
| Total Energy (session) | sum of all `energy_wh` | `45.2 MWh` |
| Faulted | count connectors where `status == "Faulted"` | `0` (green) / `3` (red) |

**Charger Grid/Table (below stats):**

Each charger shown as a card or table row:

```
┌─────────────────────────────────────────────┐
│ SPI-00001  ●  Online     Spirii S3-50kW     │
│                                             │
│  Conn 1: ⚡ Charging   22.5 kW   SoC: 67%  │
│  Conn 2: ○ Available                        │
│                                             │
│  Last seen: 2s ago                          │
└─────────────────────────────────────────────┘
```

**Features:**
- Color-coded status indicators (green=Available, blue=Charging, yellow=Finishing/Preparing, red=Faulted, grey=Unavailable)
- Sort by: charger ID, status, power
- Filter by: status (All / Charging / Available / Faulted)
- Click card/row to navigate to detail page
- Auto-refresh every 5 seconds (visual indicator showing countdown)

### 2. Charger Detail Page (`/dashboard/chargers/{id}`)

Drill-down into a single charger.

**Header:**
- Charger ID, vendor, model, firmware version
- Online/offline indicator with `last_seen` relative time
- Last boot time

**Connector Cards:**

For each connector, show:

| Field | Value |
|-------|-------|
| Status | Badge with color (e.g., `Charging`) |
| Error Code | `NoError` or error with red highlight |
| Active Transaction | Yes/No |
| Power | `22.5 kW` (live, updates every refresh) |
| Energy | `15.2 kWh` (cumulative this session) |
| SoC | `67%` with progress bar |
| Status since | relative time (`2m ago`) |
| Meter updated | relative time (`5s ago`) |

**Nice-to-have (if time permits):**
- Mini sparkline chart showing power over last N data points (stored client-side from polling)
- Status transition history (client-side, accumulated from polling — not from PG)

## API Requirements

The existing API already provides everything needed:
- `GET /chargers` — fleet list with all connector data
- `GET /chargers/{id}/state` — single charger detail

**No new API endpoints required.**

Optional enhancement: `GET /chargers/stats` endpoint returning pre-aggregated fleet stats to avoid computing them client-side. But for PoC, computing in JS from the `/chargers` response is fine (50 chargers = trivial).

## Implementation Plan

### Approach: Embedded SPA in Go API

The portal is a set of static HTML/CSS/JS files embedded into the `cmd/api` binary using Go's `embed` package. Served at `/dashboard`. No build step, no npm, no framework.

### File Structure

```
cmd/api/
  dashboard/
    index.html       # Fleet dashboard
    charger.html     # Charger detail (loaded via JS or separate page)
    style.css        # All styles
    app.js           # Fetch logic, rendering, auto-refresh
```

### UI Design Constraints

- **Dark theme** — easier on the eyes for monitoring screens
- **Responsive** — works on desktop and tablet
- **No external dependencies** — all CSS/JS inline or embedded (except optional CDN for icons)
- **Minimal JS** — vanilla DOM manipulation, no virtual DOM, no framework
- **Status colors are consistent** across the portal and match industry conventions:
  - `Available` → green
  - `Preparing` / `Finishing` → yellow/amber
  - `Charging` → blue
  - `SuspendedEV` / `SuspendedEVSE` → orange
  - `Faulted` → red
  - `Unavailable` / `Reserved` → grey

### Auto-Refresh

- Poll `GET /chargers` every 5 seconds
- Show a subtle refresh indicator (pulsing dot or countdown bar)
- On detail page: poll `GET /chargers/{id}/state` every 3 seconds
- Smooth transitions — don't flash the entire page on each update, only update changed values

### Implementation Tasks

1. Create `cmd/api/dashboard/style.css` — dark theme, card layout, status badges, responsive grid
2. Create `cmd/api/dashboard/app.js` — fetch, render fleet grid, render detail, auto-refresh, filtering, sorting
3. Create `cmd/api/dashboard/index.html` — fleet dashboard shell
4. Add Go embed + serve routes in `cmd/api/main.go` — `//go:embed dashboard/*`, serve at `/dashboard/`
5. Rebuild, test end-to-end

### Non-Goals (explicitly out of scope)

- Authentication / authorization
- Historical data / time-series charts (use Grafana for that)
- Charger control (RemoteStart, Reset) — read-only portal
- Mobile-optimized layout (responsive is enough, not mobile-first)
- Persistent state (no localStorage, no cookies)
- Internationalization
- Accessibility (PoC)
