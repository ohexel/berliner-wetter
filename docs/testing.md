# Testing strategy

> **Built on the Berlin capstone.** The Germany layer is the main deliverable;
> its tests come first below. It grew out of the Berlin capstone and runs on
> the capstone's installation, and it follows the capstone's testing approach:
> tests protect contracts, real database behavior is tested against
> PostgreSQL/PostGIS, and markers separate database-free, integration and
> acceptance tests. Capstone-specific tests are at the end:
> [Berlin capstone tests](#berlin-capstone-tests).

The tests protect operational contracts, not a coverage percentage. Start with
the database-free suite, add real PostgreSQL/PostGIS checks when a database is
available, run the read-only acceptance tests on an installed database, and
reserve the optional historical-rebuild tests for work on that workflow.

## Run the default suite

```bash
uv run python -m pytest
```

The default selection requires neither a running database nor a live external
source. It covers both parts of the repository: for the Germany layer, the
Germany DDL boundary, source parsers and their refusal of layout changes, the
Zensus token handling, the polite BA client, the release sensors, the sector
source-total rules and the map page; for the capstone, forecast identifiers,
source acquisition boundaries, snapshot handling, restart safety, static
contracts, and the public forecast runner. Optional historical-rebuild modules
are excluded before collection. Optional Dagster-container tests are collected
only when the active checkout includes `docker/dagster.yml`.

For a focused review of the most important database-free contracts:

```bash
uv run python -m pytest \
  -m "contract and not integration and not acceptance and not historical_rebuild"
```

Markers are assigned centrally in `tests/conftest.py` (capstone files) or in
the Germany test modules themselves; their definitions and the default
selection live in `pyproject.toml`.

## Germany layer tests

The Germany tests were cut to the ones that protect a decision or an observed
property of the data (test triage of 14 Nov 2026): 49 cases in eight files,
33 of them in the default suite. The essential tests are listed with what
they prove in the README under [Key tests](../README.md#key-tests).

### Integration tests

They need a database with the [base installation](../runbook.md#base-installation).
Each test applies the Germany schema and loads small synthetic geographies,
cells or workbooks inside a transaction that is always rolled back, so
nothing is left behind:

```bash
uv run --env-file .env python -m pytest -m integration tests/test_germany_*.py
```

They cover population statuses and checks 1–3, a repeated static load,
area weighting with hand-computed values, run switching and retention, the
tie rule of the 24-hour summaries, and the employment and sector refreshes
with their checks.

### Acceptance tests

They read the installed database and write nothing (about 100 s). Run them
after `run_static`, at least one complete `run_forecast`, `run_employment`
and `run_sectors`:

```bash
uv run --env-file .env python -m pytest -m acceptance tests/test_germany_acceptance.py
```

They check that the `germany` group is registered with all checks, that all
15 Germany asset checks pass, that the current views are complete at all four
levels and roll up consistently, that the employment and sector views cover
every region with an explicit status, that the Regionalreport counts
employees at their place of work, and that the map exports for all three
themes can be built completely.

### Germany contracts worth reviewing

| Concern | Tests | Protected assumption |
| --- | --- | --- |
| Berlin stays untouched | `test_germany_contracts.py`, `test_germany_forecast.py` | Germany DDL only adds `de_` objects; the Berlin sensor starts no Germany job; no Germany data is committed. |
| Zensus source | `test_germany_raw.py` | The Zensus token is sent only as a header and never appears in an error; "-" (exactly zero) and 12-digit keys with leading zeros survive into raw. |
| Hierarchy and population | `test_germany_normalized.py` | Every key case gets a status, missing population is NULL never 0, checks 1–3 hold, and `run_static` can be repeated safely. |
| Forecast aggregation | `test_germany_forecast.py` | Area weighting matches hand-computed values, views switch only to complete runs, two runs are retained, and the earliest hour wins a tie. |
| Employment theme | `test_germany_employment.py` | Suppressed values are kept as `*`, a changed BA layout stops the load, published Kreis and Land values are served with explicit statuses, the release sensor asks only for the next vintage. |
| Sector theme | `test_germany_sectors.py` | Parsers read five quarters and refuse changed layouts; the BA client is considerate and stops on rate limits; reviewed discrepancies pass only at their documented size; each level shows only what it publishes. |
| Map | `test_germany_contracts.py`, `test_germany_dashboard.py`, `test_germany_sectors.py` | Required attribution, pinned map libraries without a basemap, maxima and minima kept apart, Range requests in the preview, the sector theme states its limits. |
| Installed data | `test_germany_acceptance.py` | All checks pass and every view and export is complete on the installed database. |

## Why some tests use `monkeypatch`

`monkeypatch` is a pytest fixture that temporarily replaces an external
boundary for a single test and automatically restores it afterward. Here it is
appropriate for a network request, database connector, subprocess, or Dagster
entry point when the test needs to verify production control flow without
calling the real service.

For example, the BA client test replaces the network and the clock with small
fakes and asserts that HTTP 429 leads to waits, a shared cool-down and a stop; a
bootstrap-order test replaces external operations with tiny recording
functions and asserts that the archive is verified before expensive static
processing. Both test actual operational guarantees. A fabricated dataframe
that merely reproduces an expected answer without calling project code does
not, and should not be retained merely to increase test counts.

Real PostgreSQL/PostGIS behavior belongs in integration tests; external live
services belong in explicit operational checks. Prefer small fakes and clear
contracts over chains of mocks or tests of implementation trivia.

---

## Berlin capstone tests

### Run PostgreSQL/PostGIS integration tests

Initialize the canonical schema first. Then run:

```bash
uv run --env-file .env python -m pytest \
  -m "integration and not acceptance and not historical_rebuild"
```

These tests exercise real database functions, constraints, PostGIS operations,
temperature transformations, population quality gates, and the final serving
contract. They do not assert behavior through a Python imitation of SQL. This
selection also collects the Germany integration tests.

### Run operational acceptance tests

The acceptance suite requires completed static initialization, a verified
HOSTRADA reference import, and any forecast partitions expected by the specific
tests:

```bash
uv run --env-file .env python -m pytest \
  -m "acceptance and not historical_rebuild"
```

This selection also collects the Germany acceptance tests. On an
installation without Germany data, add
`--ignore=tests/test_germany_acceptance.py`.

For the strongest handoff proof, run the suite after materializing one live
forecast in a freshly bootstrapped database. The clean-room acceptance
criterion is described in [../runbook.md](../runbook.md).

### Optional historical-rebuild tests

The ordinary operational installation never downloads or reconstructs the
1995–2025 historical archive. Run these tests only when changing that optional
workflow:

```bash
uv run python -m pytest \
  -m "historical_rebuild and not integration"
```

For the database-backed historical checks:

```bash
uv run --env-file .env python -m pytest \
  -m "historical_rebuild and integration"
```

Some historical integration checks require historical spatial prerequisites or
materialized source partitions. Follow
[historical-rebuild.md](historical-rebuild.md) before interpreting failures in
that optional environment.

### Capstone contracts worth reviewing

| Concern | Representative tests | Protected assumption |
| --- | --- | --- |
| Forecast identity and source publication | `test_forecast_key.py`, `test_run_forecast.py`, `test_weather_source_contract.py` | Forecast runs are UTC, have supported lead times, and fail clearly when unpublished or expired. |
| Complete forecast horizons | `test_forecast_horizon.py`, `test_forecast_horizon_sql.py`, `test_dagster_partitions.py` | Exactly 25 hourly leads use existing historical medians; signed differences, earliest-time ties, rejected population, restart behavior, and incomplete newer runs remain correct. |
| Optional historical-year trajectories | `test_historical_temperature_trajectories.py`, `test_historical_temperature_trajectories_sql.py` | Exactly 31 historical years of temperature and apparent temperature align with all 25 Berlin-local lead hours; month boundaries, indexed source access, explicit opt-in, atomic refresh, and compact-snapshot isolation remain correct. |
| Static temperature dashboard | `test_temperature_dashboard.py` | Complete PLR geometry, summary, temperature/apparent-temperature forecast and historical exports; lazy per-PLR files; coordinated chart modes; snapshot-only fallback; dependency-free browser assets; generated data excluded from Git. |
| Geography and forecast-grid compatibility | `test_icon_grid_contract.py`, `test_spatial_bridge_sql.py` | The declared grid, PLR geometry, and area-weighted bridge remain compatible. |
| Population quality | `test_population_sql_quality_gate.py`, `test_bootstrap.py` | Malformed source records are rejected explicitly; the checked-in CSV fallback remains usable. |
| Analyst-facing PLR names | `test_plr_display_names.py`, `test_bootstrap_acceptance.py` | All 542 official labels match the PLR geography; leading zeroes, German characters, duplicate names, and offline fallback remain correct. |
| Temperature calculation | `test_apparent_temperature_sql.py`, `test_weather_contract_sql.py` | The shade apparent-temperature formula and forecast data grain remain stable. |
| Early forecast filtering | `test_weather_early_filtering.py`, `test_weather_mask_state.py` | Full fields remain validated while only ordered, in-range Berlin-mask cells cross into PostgreSQL. |
| HOSTRADA references | `test_hostrada_reference.py`, `test_hostrada_reference_sql.py`, `test_hostrada_snapshot.py` | Berlin-local calendar rules, sample counts, geography fingerprint, and the lean serving contract remain intact. |
| Bootstrap and release artifacts | `test_bootstrap.py`, `test_bootstrap_snapshot_sql.py`, `test_static_snapshot.py`, `test_distribution_manifest.py` | Canonical initialization is non-destructive, restores are verified, and published manifests match runtime assumptions. |
| End-to-end readiness | `test_bootstrap_acceptance.py` | A clean installation serves all 542 PLRs with valid references, 540 accepted populations, and two explicit rejections without historical backfill data. |
| Forecast retention | `test_weather_raw_retention.py`, `test_dagster_partitions.py`, `test_run_forecast.py` | Forecast files, forecast-only PostgreSQL tables, manual runs, and visible Dagster partitions share an hourly window capped at 24 hours; HOSTRADA data remains untouched. |
