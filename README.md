# Oringen Simple Monitoring (Go)

A Go rewrite of the original PowerShell monitoring server (`check.ps1` + `server.ps1` merged into one binary). It performs parallel ICMP pings and HTTP checks against configured targets, and serves an HTML dashboard.

## What it does

- Loads monitoring targets from `cis.json`
- Runs all checks in parallel every 10 seconds (one goroutine per target)
- **ICMP** targets: single shared socket with reply dispatcher — no per-goroutine socket racing
- **OLA** targets: HTTP GET, checks for HTTP 200
- **FUEL** targets: HTTP GET to a JSON API, checks fuel level thresholds
- Writes JSON monitoring results to `result/result.json`
- Serves the dashboard at `http://localhost:8080/`
- Logs alerts and slow responses (>500 ms) to `monitor-logs/YYYY-MM-DD-monitoring-log.csv`
- Rotates log files older than 30 days automatically

## Status codes

| Code | Color  | Meaning                                     |
|------|--------|---------------------------------------------|
| 10   | Green  | OK                                          |
| 20   | Yellow | Failing, below down-threshold (< 4 polls)   |
| 40   | Red    | Down (≥ 4 consecutive failures)             |
| 0    | Green  | Not yet checked / unknown                   |

## Build and run

```bash
go build -o monitoring .
./monitoring
```

The dashboard is at `http://localhost:8080/`.

On macOS the binary tries a privileged raw socket first, then falls back to the unprivileged datagram socket — **no `sudo` required** on macOS 10.15+.

On Linux you may need `sudo` or the `CAP_NET_RAW` capability for ICMP:

```bash
sudo setcap cap_net_raw+ep ./monitoring
./monitoring
```

## Directory layout

```
.
├── cis.json            monitored targets (input)
├── result/
│   └── result.json     latest check results (written by monitor)
├── wwwroot/
│   └── monitoring.html dashboard HTML + JS (unchanged from original)
├── monitor-logs/       daily CSV alert logs, rotated after 30 days
└── monitoring          compiled binary
```

## Configuration — cis.json

Each entry in `cis.json` has these fields:

| Field      | Description                                        |
|------------|----------------------------------------------------|
| `index`    | Unique integer identifier                          |
| `type`     | `icmp`, `OLA`, or `FUEL` (case-insensitive)        |
| `ip`       | IP address (icmp) or URL (OLA/FUEL)                |
| `name`     | Display name in the dashboard                      |
| `status`   | Initial status — set to `"0"`                      |
| `count`    | Failure counter — set to `0`                       |
| `downtime` | Timestamp of first down event — set to `""`        |

## Optional fuel API credentials

For `FUEL` targets set these environment variables before starting:

```bash
export FUEL_API_USER=root
export FUEL_API_PASSWORD=yourpassword
./monitoring
```

## Key differences from the PowerShell version

| Feature                  | PowerShell             | Go                                    |
|--------------------------|------------------------|---------------------------------------|
| ICMP pinging             | Sequential per entry   | Parallel, single shared socket        |
| HTTP server              | Separate `server.ps1`  | Merged into one binary                |
| Hard-coded paths         | `C:\monitoring\...`    | Relative to executable directory      |
| Log rotation             | Yes (30 days)          | Yes (30 days)                         |
| Root required for ICMP   | No (Windows)           | No on macOS 10.15+, optional on Linux |
