# Operational runbook

> **Built on the Berlin capstone.** The Germany layer is the main deliverable
> of this repository. It grew out of the Berlin neighborhood-temperature
> capstone (tag `v1.0-capstone-submitted`) and runs on top of it: the
> capstone's base installation provides the database, the ICON grid, the DWD
> forecast download and the Dagster set-up, and the Germany layer follows the
> same operating rules (pinned source contracts, asset checks as quality
> gates, sensors off by default). Capstone-specific operation is at the end of
> this document: [Berlin capstone operation](#berlin-capstone-operation).

This document covers the base installation, the operation of the Germany
layer, shared Dagster and database tasks, and then the Berlin capstone.
Reconstructing the 1995–2025 historical archive is a separate, optional
procedure documented in [docs/historical-rebuild.md](docs/historical-rebuild.md).
Checks, data flow and measurements of the Germany layer are described in
[docs/germany-layer.md](docs/germany-layer.md) and
[docs/germany-layer-facts.md](docs/germany-layer-facts.md).

## Operating requirements

- Docker Engine and the Docker Compose plugin.
- `uv` and Python 3.11.
- The repository's tracked `pyproject.toml`, `uv.lock`, `.env.example`,
  `docker/postgres.yml`, canonical SQL schema, and HOSTRADA reference manifest.
- The externally distributed
  [HOSTRADA reference archive](https://www.ohexel.com/files/hostrada-reference-1995-2025.pgcustom),
  required by the base installation.
- Optionally, the externally distributed static-input archive.
- A persistent Docker volume for PostgreSQL and a persistent `DAGSTER_HOME`
  directory for Dagster run history.
- For the Germany layer: a free Zensus 2022 API token (`ZENSUS2022_TOKEN` in
  `.env`) while the Zensus files are not yet under `data/raw/germany/`;
  `tippecanoe` once, to build the map's boundary tiles; network access to
  BKG, the Zensus database and the BA statistics portal.

## Base installation

The base installation is the capstone's bootstrap. It loads Berlin's
geography and population, the ICON grid (which the Germany layer reuses) and
the historical reference. The first verified clean-room installation
completed in approximately ten minutes. It created 542,040 ICON grid cells and
imported approximately 4.76 million reference rows without reconstructing
historical temperature.

From the repository root:

```bash
cp .env.example .env
```

Edit `.env`, including `POSTGRES_PASSWORD` and, for the Germany layer,
`ZENSUS2022_TOKEN`. Do not commit `.env`.

```bash
uv sync --frozen
```

```bash
docker compose --env-file .env -f docker/postgres.yml up -d
```

```bash
export DAGSTER_HOME="$PWD/.dagster_home"
```

Verify the external HOSTRADA archive before starting the expensive spatial
bootstrap:

```bash
uv run --env-file .env python -m src.hostrada_snapshot verify \
  --archive /path/to/hostrada-reference-1995-2025.pgcustom
```

Initialize all operational dependencies:

```bash
uv run --env-file .env python -m src.bootstrap \
  --reference-archive /path/to/hostrada-reference-1995-2025.pgcustom
```

To preserve automatic acquisition while providing a fallback:

```bash
uv run --env-file .env python -m src.bootstrap \
  --reference-archive /path/to/hostrada-reference-1995-2025.pgcustom \
  --static-snapshot /path/to/static-inputs.tar.xz
```

To avoid static-source network requests entirely:

```bash
uv run --env-file .env python -m src.bootstrap \
  --reference-archive /path/to/hostrada-reference-1995-2025.pgcustom \
  --static-snapshot /path/to/static-inputs.tar.xz \
  --offline
```

`--offline` applies to static data acquisition, not the initial Docker/Python
dependency installation or subsequent live DWD forecast processing.

The successful JSON summary must report:

```json
{
  "status": "ready",
  "hostrada_snapshot": {
    "status": "imported"
  },
  "plr_display_names": {
    "status": "installed",
    "plr_count": 542
  },
  "weather_sensor_default_status": "STOPPED"
}
```

An already initialized installation reports
`"hostrada_snapshot": {"status": "already_installed"}` instead. The PLR-name
lookup likewise reports `"already_installed"` on subsequent runs. The counts
the bootstrap must produce are listed under
[expected static and reference state](#expected-static-and-reference-state).

### If the base installation fails

| Symptom | Meaning | Action |
| --- | --- | --- |
| Missing `.env` or required PostgreSQL variable | Local configuration is incomplete. | Copy `.env.example`, fill all required values, and retry. |
| Reference checksum or archive-size mismatch | Archive does not match the committed manifest. | Obtain the correct archive; do not bypass verification. |
| PLR geography fingerprint mismatch | Installed LOR geography differs from the reference snapshot. | Restore the matching geography or rebuild the reference deliberately. |
| Static source unavailable | An official endpoint or network connection is unavailable. | Supply `--static-snapshot`; use `--offline` when appropriate. |
| Population direct download fails | The provider's opaque direct URL may have changed. | The downloader restores the verified bundled CSV automatically; see [docs/data-sources.md](docs/data-sources.md). |
| Existing schema rejected | The database is partial or incompatible. | Inspect the target; do not reset the wrong volume. |

## Germany layer operation

Every Germany job first applies the idempotent schema files in
`sql/germany/`, so there is no separate install step. Germany objects carry
the prefix `de_`; no Germany job changes a Berlin table, view or sensor.

### Static set-up

Once, after the base installation:

```bash
uv run --env-file .env python -m src.germany.run_static
```

This runs the Dagster job `germany_static`: VG250 and Zensus raw loads, the
Bund → Land → Kreis → Gemeinde hierarchy, population, the ICON grid subset
for Germany and the cell-to-Gemeinde bridge (about 2½ minutes on an
installed database). It needs `ZENSUS2022_TOKEN` only when the Zensus files
are not yet present locally. The command takes no arguments.

It is safe to repeat: a geography version that already exists from the same
VG250 file is left unchanged (a different file needs a new
`geography_version` and is refused); population and the bridge are rebuilt.
Checks 1–4 and the two raw-file contracts must pass.

### Forecast runs

```bash
uv run --env-file .env python -m src.germany.run_forecast \
  --run-time "$(date -u -d '3 hours ago' +%Y%m%dT%H00)"
```

Run labels are **UTC**; DWD keeps runs for about 24 hours. The runner
materializes the Dagster job `germany_forecast` once per lead hour 0–24 (the
four raw DWD GRIB fields, shared with Berlin and written only to files, plus
the Germany aggregation), then `germany_forecast_publish`. The serving views
switch to a run only when every Gemeinde has all 25 lead hours.

There is no sensor for Germany forecast runs, and the Berlin availability
sensor does not start Germany jobs: start `run_forecast` for each run to be
served.

### Employment theme (yearly)

```bash
# newest known vintage: 2025-06-30
uv run --env-file .env python -m src.germany.run_employment
# a later vintage
uv run --env-file .env python -m src.germany.run_employment --reference-date 2026-06-30
```

This runs the Dagster job `germany_employment` on the BA *Gemeindedaten*
(needs the hierarchy from `run_static`). It is safe to repeat: raw rows and
indicator values of the reference date are replaced. A vintage not yet in
the contract loads if its structure matches; its key-coverage check (10)
then fails until its territorial changes are reviewed and added to
`src/germany/contract.py`.

### Sector theme (quarterly)

```bash
# newest known quarter: 2026-03-31
uv run --env-file .env python -m src.germany.run_sectors
# a later quarter
uv run --env-file .env python -m src.germany.run_sectors --file-date 2026-06-30
```

This runs the Dagster job `germany_sectors`: the 17 WZ booklets for the
quarter and for a year earlier, then one Regionalreport per Kreis, one
request at a time with a 3 s pause (434 files, about 22 minutes for a new
quarter; at most 600 requests per run). Files already on disk are not
downloaded again, and all are kept. Every file is checked against the
structural contract before it is kept; one bad file stops the run.

**When the BA throttles.** The BA limits the volume of requests. HTTP 429 is
answered with waits of 1, 5 and 15 minutes (or the server's `Retry-After`);
if it persists, a cool-down of at least an hour is written to
`data/raw/germany/ba_rate_limit.json`, which every download and the sensor
respect before sending anything. `run_sectors` then stops with exit status 2
and a message such as "312 of 400 Kreis files for 2026-03-31 are on disk …
Run the same command again after 2026-11-14T23:05+01:00". Running it again
later continues with the missing files. Avoid back-to-back full downloads
from the same network. To copy the files from another checkout instead, put
them under `data/raw/germany/ba_regionalreport/` and
`data/raw/germany/ba_wz_heft/` with the same names: they are checked like
downloads and not requested again.

### Release sensors

Two sensors watch for new BA publications. Both are **stopped by default**;
switch them on under Automation in the Dagster interface (see
[Dagster monitoring and automation](#dagster-monitoring-and-automation)).

| Sensor | Starts | What it sends |
| --- | --- | --- |
| `ba_gemeindedaten_release_sensor` | `germany_employment` | one HEAD request a day for the vintage after the newest one loaded |
| `ba_sectors_release_sensor` | `germany_sectors` | two HEAD requests a day for the next quarter; respects the BA cool-down file |

### Map export and upload

```bash
# export (and build the boundary tiles on first use), then preview locally
uv run --env-file .env python -m src.export_germany_dashboard --serve
# -> http://127.0.0.1:8766/
```

The export writes `values.json` (temperature), `employment.json` and
`sectors.json` into `web/germany-dashboard/data/`; a theme whose data is not
loaded is left out. The boundary tiles (`vg250-2022-12-31.pmtiles`) are built
once per geography version with `tippecanoe`; `--rebuild-tiles` forces a new
build. Other options: `--output-dir`, `--host`, `--port`.

To publish, copy the whole `web/germany-dashboard/` folder, including
`data/`, to the web host. The host must answer HTTP Range requests for the
`.pmtiles` file, and the page must be served from the same origin as the
tiles. After a new forecast run, re-export and upload `data/values.json`
only; after a new BA vintage `data/employment.json`, after a new quarter
`data/sectors.json`. Details: [docs/germany-layer.md](docs/germany-layer.md#map).

### Inspect the Germany serving layer

Load the project environment into the current trusted shell when using the
database user/name variables directly:

```bash
set -a
source .env
set +a
```

Check the served forecast run:

```bash
docker compose --env-file .env -f docker/postgres.yml \
  exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
    SELECT run_time_utc, gemeinde_count, lead_time_count, population_total
    FROM analytical.de_current_bund_summary;
  "
```

Expected: one row, 10,990 Gemeinden, 25 lead times and a population of
82,711,282 (the sum of the Länder; the published Germany total is 8,258
higher, see check 3).

Inspect the 24-hour series of one Land, area- and population-weighted:

```bash
docker compose --env-file .env -f docker/postgres.yml \
  exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
    SELECT
      lead_hour,
      valid_time_berlin,
      ROUND(apparent_temperature_shade_area_weighted_c::numeric, 1) AS area_c,
      ROUND(apparent_temperature_shade_population_weighted_c::numeric, 1)
        AS population_c
    FROM analytical.de_current_land_forecast_25h
    WHERE land_name = 'Bayern'
    ORDER BY lead_hour;
  "
```

Check that the employment and sector views cover every region:

```bash
docker compose --env-file .env -f docker/postgres.yml \
  exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
    SELECT 'employment' AS theme, reference_date, level, COUNT(*) AS regions
    FROM analytical.de_current_employment
    GROUP BY reference_date, level
    UNION ALL
    SELECT 'sectors', reference_date, level, COUNT(*)
    FROM analytical.de_current_sectors
    GROUP BY reference_date, level
    ORDER BY 1, 3;
  "
```

Expected: employment `gem` 10,990, `krs` 400, `lan` 16, `sta` 1; sectors
`krs` 400, `lan` 16, `sta` 1. Values the BA suppresses or does not report
are rows with a `*_status` other than `value`, never missing rows.

Run the read-only acceptance tests on the installed data (about 100 s):

```bash
uv run --env-file .env python -m pytest -m acceptance tests/test_germany_acceptance.py
```

They include `test_all_germany_checks_pass_on_installed_data`, which runs all
15 Germany asset checks.

### Germany retention

Database rows of the newest complete forecast run and the complete run
before it are kept, plus a newer run while it is still loading
(`normalized.prune_de_forecast`, applied on publish). The per-cell values of
those runs take about 588 MB. The Germany layer never deletes GRIB files;
they share Berlin's directory layout and its
[file retention rule](#forecast-retention). BA and Zensus raw files under
`data/raw/germany/` are kept.

### Germany failure handling

| Symptom | Meaning | Action |
| --- | --- | --- |
| `run_static` reports `ZENSUS2022_TOKEN is not set` | The Zensus files are not on disk yet and no token is configured. | Set `ZENSUS2022_TOKEN` in `.env` and retry. |
| `run_static` refuses the VG250 file | A different file than the one the geography version was built from. | Keep the original file, or load the new one under a new `geography_version`. |
| Germany bridge is empty | The base installation has not loaded the ICON grid. | Run the base installation first. |
| Requested forecast run unavailable | Run is unpublished, incomplete, older than about 24 hours, or entered in local time. | Choose another UTC run. |
| Check 5 (`de_forecast_completeness`) warns | A newer run was loaded but never completed, e.g. a failed download or a model field with missing values. | The views keep serving the previous complete run; run the next forecast. |
| `run_sectors` exits with status 2 | The BA throttled the downloads; a cool-down is in force. | Run the same command again after the time it prints. |
| Check 10 (`de_ba_key_coverage`) fails on a new vintage | The BA's territorial status changed. | Review the changes and add them to `src/germany/contract.py`. |
| A BA file is refused by its contract | The BA changed the file's layout. | Do not bypass the contract; adapt the parser and contract deliberately. |
| Tile build fails | `tippecanoe` is not installed. | Install it once; later exports reuse the tiles. |

## Dagster monitoring and automation

Use the same `DAGSTER_HOME` in every shell:

```bash
export DAGSTER_HOME="$PWD/.dagster_home"
```

List registered definitions:

```bash
uv run --env-file .env dg list defs
```

Start Dagster:

```bash
uv run --env-file .env dagster dev \
  -m src.dagster_pipeline.definitions
```

Open the local URL shown by Dagster. Sensors are under Automation:

| Sensor | Theme | Default |
| --- | --- | --- |
| `ba_gemeindedaten_release_sensor` | Germany employment | stopped |
| `ba_sectors_release_sensor` | Germany sectors | stopped |
| `dwd_icon_d2_ruc_availability_sensor` | Berlin forecast | stopped |
| `historical_trajectories_sensor` | Berlin historical lines | running |
| `dashboard_export_sensor` | Berlin dashboard export | running |

The command-line runners do not require any sensor to be started. Run
history and asset lineage are kept under `DAGSTER_HOME`.

## Database lifecycle

Stop the container while preserving the volume:

```bash
docker compose --env-file .env -f docker/postgres.yml stop
```

Restart it:

```bash
docker compose --env-file .env -f docker/postgres.yml up -d
```

`scripts/bootstrap_database.sh` is the internal, idempotent schema operation
used by `python -m src.bootstrap`; it is not a separate required installation
step. The two named SQL files under `sql/` are the complete supported database
initialization surface for the base installation; the Germany jobs apply
`sql/germany/` themselves.

### Destructive reset

`scripts/reset_database.sh` exists as an explicit convenience for local
development. It **deletes the current Compose project's PostgreSQL volume and
all database state**, including imported references, Germany tables and
forecast results.

Only run it after confirming that `.env` selects the intended Compose project:

```bash
bash scripts/reset_database.sh
```

The script requires the confirmation text `RESET`. Afterward, rerun the base
installation and the Germany static set-up, not the historical backfill.

### Isolated second installation

To run a clean-room installation beside an existing database, set different
values in the second checkout's `.env`:

```dotenv
POSTGRES_PORT=55432
POSTGRES_CONTAINER_NAME=capstone_cleanroom_postgres
COMPOSE_PROJECT_NAME=capstone_cleanroom
```

Set `DAGSTER_HOME` to the second checkout's own directory. Docker volumes,
containers, exposed ports, and Dagster run history remain independent. Copy
the BA files under `data/raw/germany/` rather than downloading them a second
time from the same network.

For test selection, see [docs/testing.md](docs/testing.md).

---

## Berlin capstone operation

The capstone serves Berlin's 542 planning areas (PLRs) with forecast
temperature, historical references and population. Its installation is the
[base installation](#base-installation) above.

### Expected static and reference state

| Contract | Expected |
| --- | ---: |
| Berlin PLRs | 542 |
| Analyst-facing PLR display names | 542 |
| Raw population rows | 542 |
| Accepted population rows | 540 |
| Rejected population rows | 2 |
| ICON grid cells | 542,040 |
| ICON-to-PLR bridge rows | 1,678 |
| Berlin forecast-mask cells | 465 |
| PLR historical-reference rows | 4,747,920 |
| Berlin historical-reference rows | 8,760 |
| Included historical Berlin observations | 271,559 |

The bridge and forecast-mask counts reflect the committed geography and the
default 5,000 m mask buffer. Deliberately changing the spatial configuration
may change those values.

### Manual forecast processing

Inspect supported arguments and lead times:

```bash
uv run --env-file .env python -m src.run_forecast --help
```

Process an explicit UTC model run:

```bash
uv run --env-file .env python -m src.run_forecast \
  --run-time 20260824T1600 \
  --lead-time PT000H00M
```

The example timestamp documents the format; old example runs should not be
expected to remain available upstream. On Debian/GNU/Linux, choose a recent
candidate dynamically:

```bash
uv run --env-file .env python -m src.run_forecast \
  --run-time "$(date -u -d '2 hours ago' +%Y%m%dT%H00)"
```

Run labels represent **UTC**. For example, 18:00 UTC is 20:00 Berlin-local
time during daylight saving. Very recent runs may be incomplete; old runs may
have been removed from DWD. The command reports missing source fields and a
representative upstream URL before starting a failing Dagster materialization.

Existing validated local GRIB files can still be reprocessed after upstream
removal. If all four fields are already retained, the command does not contact
DWD for that partition.

#### Materialize a complete 25-point forecast

An existing database needs the additive forecast-serving views once; this
command preserves all existing data:

```bash
bash scripts/bootstrap_database.sh
```

Process lead hours 0 through 24 sequentially for the same UTC model run:

```bash
uv run --env-file .env python -m src.run_forecast_horizon \
  --run-time "$(date -u -d '2 hours ago' +%Y%m%dT%H00)"
```

The command downloads up to four forecast fields per lead, skips partitions
that already pass the final PostgreSQL quality gate, and can be restarted
after an interruption. Its final quality result must report:

```text
plr_count:              542
lead_hour_count:         25
forecast_row_count:  13,550
summary_row_count:      542
```

It requires the existing compact historical-reference snapshot but does not
query or rebuild historical hourly HOSTRADA observations.

#### Optionally extract individual historical-year trajectories

This extension requires the original populated
`analytical.hostrada_plr_hourly` table. The ordinary reference snapshot stores
only aggregate statistics and cannot recover individual historical years.

After a complete 25-point forecast is available, extract just its matching
Berlin-local calendar hours for historical years 1995–2025:

```bash
uv run --env-file .env python -m src.historical_temperature_trajectories
```

Alternatively, materialize a forecast and extract its historical lines in one
explicitly opted-in command:

```bash
uv run --env-file .env python -m src.run_forecast_horizon \
  --run-time "$(date -u -d '2 hours ago' +%Y%m%dT%H00)" \
  --historical-trajectories
```

The expected result is:

```text
plr_count:                  542
historical_year_count:       31
lead_hour_count:             25
historical_row_count:   420,050
```

The extraction makes 775 targeted lookups against the existing
`(source_month_utc, valid_time_utc, plr_id)` primary key. A complete existing
cache is reused, and a failed refresh preserves the previous cache. Only the
current forecast's historical lines are retained.

### Berlin forecast sensor

For automatic Berlin ingestion, find `dwd_icon_d2_ruc_availability_sensor`
under Automation in the Dagster interface and switch it on.

- Stopped by default; starting the webserver does not enable it.
- Polls during minutes `:30`–`:59` of each hour.
- Requires all four forecast fields for the same run and lead time.
- Skips partitions whose final temperature/population contract is already valid.
- Discovers and submits at most five ready forecast partitions per evaluation.
- Preserves forecast run history and asset lineage under `DAGSTER_HOME`.
- Starts no Germany job.

The manual forecast runner does not require the sensor to be started.

### Inspect the Berlin serving layer

With the project environment loaded (see
[Inspect the Germany serving layer](#inspect-the-germany-serving-layer)),
check the current partition and population-quality accounting:

```bash
docker compose --env-file .env -f docker/postgres.yml \
  exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
    SELECT
      run_time_utc,
      lead_time,
      COUNT(*) AS plr_count,
      COUNT(*) FILTER (
        WHERE population_status = 'available'
      ) AS accepted_population,
      COUNT(*) FILTER (
        WHERE population_status = 'rejected_source_record'
      ) AS rejected_population
    FROM analytical.current_plr_weather_context
    GROUP BY run_time_utc, lead_time;
  "
```

Expected: one partition, 542 PLRs, 540 available population records, and two
explicitly rejected records.

Inspect values:

```bash
docker compose --env-file .env -f docker/postgres.yml \
  exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
    SELECT
      plr_id,
      plr_name,
      valid_time_utc,
      valid_time_berlin,
      temperature_c,
      apparent_temperature_shade_c,
      plr_apparent_temperature_median_c,
      berlin_apparent_temperature_median_c,
      population_65plus,
      population_status
    FROM analytical.current_plr_weather_context
    ORDER BY plr_id
    LIMIT 10;
  "
```

Inspect the complete current temperature series for one planning area:

```bash
docker compose --env-file .env -f docker/postgres.yml \
  exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
    SELECT
      lead_hour,
      valid_time_berlin,
      ROUND(forecast_temperature_c::numeric, 1) AS forecast_c,
      ROUND(historical_temperature_median_c::numeric, 1) AS median_c,
      ROUND(temperature_difference_c::numeric, 1) AS difference_c
    FROM analytical.current_plr_temperature_forecast_25h
    WHERE plr_id = '01100101'
    ORDER BY lead_hour;
  "
```

Inspect the compact one-row-per-PLR summary:

```bash
docker compose --env-file .env -f docker/postgres.yml \
  exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
    SELECT
      plr_id,
      plr_name,
      max_forecast_temperature_c,
      max_forecast_temperature_at_berlin,
      max_temperature_difference_c,
      max_temperature_difference_at_berlin,
      sum_temperature_difference_c,
      population_65plus,
      population_status
    FROM analytical.current_plr_temperature_summary_25h
    ORDER BY max_temperature_difference_c DESC
    LIMIT 10;
  "
```

Inspect the historical-year plotting lines for one neighborhood:

```bash
docker compose --env-file .env -f docker/postgres.yml \
  exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
    SELECT
      plr_name,
      historical_year,
      lead_hour,
      valid_time_berlin,
      ROUND(historical_temperature_c::numeric, 1) AS historical_c,
      ROUND(forecast_temperature_c::numeric, 1) AS forecast_c
    FROM analytical.current_plr_temperature_history_25h
    WHERE plr_id = '01100101'
    ORDER BY historical_year, lead_hour
    LIMIT 30;
  "
```

Validate installed historical references without requiring historical source
files:

```bash
uv run --env-file .env python -m src.hostrada_snapshot validate
```

### Static-source fallback maintenance

Create an optional archive from an existing installation's retained static
source files:

```bash
uv run --env-file .env python -m src.static_snapshot create \
  --output /path/to/static-inputs.tar.xz
```

Inspect its internal source manifest:

```bash
uv run --env-file .env python -m src.static_snapshot inspect \
  --archive /path/to/static-inputs.tar.xz
```

Restore one source manually if necessary:

```bash
uv run --env-file .env python -m src.static_snapshot restore \
  --archive /path/to/static-inputs.tar.xz \
  --source lor_plr
```

The restore operation verifies the selected file and refuses to overwrite an
existing conflicting file.

### Forecast retention

Forecast model runs are retained for a maximum of 24 hours. The same rolling
UTC cutoff applies to downloaded GRIB files, their metadata sidecars, and the
seven Berlin forecast-only PostgreSQL tables. The GRIB files are shared with
the Germany layer, which keeps its own database rows as described under
[Germany retention](#germany-retention). HOSTRADA observations, historical
references, geography, population, and other static inputs are never included.

Each successful forecast materialization automatically removes expired forecast
rows and files. To inspect the current candidates without deleting anything,
run:

```bash
uv run --env-file .env python -m src.retention.weather_raw
```

To apply cleanup immediately, use an explicit `--apply`:

```bash
uv run --env-file .env python -m src.retention.weather_raw --apply
```

The default window is 24 hours. Set `FORECAST_RETENTION_HOURS` in `.env` to a
whole number from 1 through 24 if a shorter window is appropriate. Values over
24 are rejected, and expired partitions cannot be pinned. The previous
`WEATHER_RAW_RETENTION_DAYS` and `WEATHER_RAW_PINNED_PARTITIONS` settings no
longer apply.

Dagster computes its visible forecast partition window when the code location
loads. Restart Dagster, or reload its code location in the interface, to drop
older partitions from the display. The sensor and database/file cleanup always
calculate their current cutoff when they run, even if the interface has not
been reloaded.

PostgreSQL makes deleted space available for reuse through normal vacuuming;
an immediate reduction in the database file size is not guaranteed. Deleted
GRIB files and database forecast rows cannot be recovered unless the same
upstream forecast is still available and reprocessed.

### Berlin failure handling

| Symptom | Meaning | Action |
| --- | --- | --- |
| Requested forecast run unavailable | Run is unpublished, incomplete, outside retention, or entered in local time. | Choose another UTC run; inspect missing fields shown by the runner. |
| Sensor shows no runs | The sensor is stopped or outside its polling window. | Enable it explicitly or use the manual forecast runner. |

For source reconstruction, see
[docs/historical-rebuild.md](docs/historical-rebuild.md).
