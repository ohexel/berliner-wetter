# Architecture

> **Built on the Berlin capstone.** The Germany layer is the main deliverable
> of this repository. It grew out of the Berlin neighborhood-temperature
> capstone (tag `v1.0-capstone-submitted`) and is built on top of it: it runs
> in the capstone's database and Dagster code location, reuses its ICON grid,
> forecast download and apparent-temperature function, and keeps its
> architecture: `raw` → `normalized` → `analytical` layers, versioned
> area-weighted spatial bridges, pinned source contracts and asset checks as
> quality gates. What it reuses is listed under
> [What the Germany layer reuses](#what-the-germany-layer-reuses); the
> capstone's own architecture is described at the end:
> [Berlin capstone](#berlin-capstone).

## Problem and serving grain

Analysts who compare regions in Germany need values on the official
administrative hierarchy: Bund → 16 Länder → 400 Kreise → 10,990 Gemeinden.
The Germany layer serves measurements from official sources at the levels the
sources publish, with an explicit status wherever a value is suppressed or not
reported. It does not classify values or set thresholds.

| Theme | Serving grain |
| --- | --- |
| Temperature forecast | One region (Gemeinde, Kreis, Land or Bund) and one lead hour 0–24 of the current complete run; one summary row per region with the 24-hour maximum and minimum. |
| Employment | One region and level for the newest BA reference date (yearly). |
| Sectors | One Kreis, Land or Germany for the newest quarter. |

## System overview

```mermaid
flowchart TD
    BASE["Capstone base installation: ICON grid cells"] --> STATIC["germany_static"]
    VG["BKG VG250 boundaries"] --> STATIC
    ZEN["Zensus 2022 population"] --> STATIC
    STATIC --> HIER["Hierarchy and population"]
    STATIC --> BRIDGE["Cell-to-Gemeinde bridge"]

    DWD["DWD ICON-D2-RUC fields"] --> FC["germany_forecast, one partition per lead hour"]
    BRIDGE --> FC
    FC --> PUB["germany_forecast_publish"]
    HIER --> PUB
    PUB --> FV["Forecast and summary views, four levels"]

    BAG["BA Gemeindedaten"] --> EMP["germany_employment"]
    BAS["BA Regionalreport and WZ booklets"] --> SEC["germany_sectors"]
    SENS["BA release sensors, off by default"] --> EMP
    SENS --> SEC
    HIER --> EMP
    HIER --> SEC
    EMP --> IND["Indicator tables"]
    SEC --> IND
    IND --> EV["Employment and sector views"]

    FV --> MAP["Static map: PMTiles and JSON"]
    EV --> MAP
```

Python owns source-specific acquisition, GRIB/NetCDF and workbook decoding,
and operational coordination. PostgreSQL/PostGIS owns durable relational
state, spatial intersections, constraints, set-based transformations,
roll-ups and serving views. Dagster owns asset lineage, partition execution,
quality checks, run history, and source-aware triggering. Docker isolates
PostgreSQL/PostGIS; the application uses a reproducible `uv` environment.

This is a source-aware **batch pipeline**: hourly forecast partitions, yearly
and quarterly BA vintages. It is not a streaming platform, an operated
service, or a decision system.

## What the Germany layer reuses

The Germany layer adds only `de_`-prefixed relations in the same three
schemas and changes no Berlin relation, view, sensor or test. It reads from
the capstone:

| Capstone part | Used for |
| --- | --- |
| Base installation (`python -m src.bootstrap`) | Database, canonical schema, ICON grid; no separate Germany install step. |
| `normalized.icon_cell` (EPSG:25833, read-only) | Cell polygons, transformed to EPSG:3035 for the Germany grid subset (ADR 014). |
| DWD ICON-D2-RUC download and GRIB file layout | The four forecast fields per lead hour, shared with Berlin and written only to files. |
| `normalized.calculate_apparent_temperature_shade_c` | Shade apparent temperature with Berlin's units. |
| `normalized.icon_plr_area_bridge` (read-only) | Check 7: Berlin as one Gemeinde against the roll-up of its 542 planning areas. |
| Dagster code location and `DAGSTER_HOME` | Five Germany jobs in the group `germany`, two sensors. |
| Forecast file retention | GRIB files follow the capstone's rolling 24-hour file rule. |

Germany DDL lives in `sql/germany/` and is applied idempotently by every
Germany job, not by the capstone bootstrap, so the Berlin bootstrap stays
unchanged.

## Sources

| Source | Grain | Refresh behavior | Operational role |
| --- | --- | --- | --- |
| BKG VG250 | Staat, Länder, Kreise, Gemeinden; status 31 Dec 2022, EPSG:25832 | Static (one vintage, checksum pinned) | Administrative hierarchy and geometry. |
| Zensus 2022 table 1000A-1003 | Länder, Kreise, Gemeinden; reference date 15 May 2022 | Static (one census, checksum pinned) | Total population per region and the published totals for the source checks. |
| DWD ICON-D2-RUC | Forecast run, lead time, grid cell | Rolling publication (shared with Berlin) | Temperature, humidity and wind fields. |
| BA Regionalreport über Beschäftigte (product 709) | Kreise; quarterly, five quarters per file | Quarterly (about six months after the quarter); one file per Kreis; structure contract | WZ sections and three manufacturing groups. |
| BA Beschäftigte nach Wirtschaftszweigen | Länder and Germany; quarterly | Quarterly; one file per Land; structure contract | Every WZ division (motor vehicles, mechanical engineering, chemicals, steel). |
| BA Gemeindedaten (product 229) | Germany, Länder, Kreise, Gemeinden; reference date 30 June, territorial status of that date | Yearly (January); structure contract for every vintage, checksum pinned for the known one | Employees by place of residence and of work, commuters, establishments. |

Source contracts, licences and attribution: [docs/data-sources.md](docs/data-sources.md).

## Relations by layer

| Schema | Relations |
| --- | --- |
| `raw` | `de_vg250`, `de_zensus2022_population`, `de_ba_gemeindedaten`, `de_ba_regionalreport`, `de_ba_wz_heft` |
| `normalized` | `de_land`, `de_kreis`, `de_gemeinde` (hierarchy, EPSG:3035 geometry, area), `de_gemeinde_population`, `de_zensus_published_population`, `de_icon_cell`, `de_icon_cell_gemeinde_bridge`, `de_forecast_run`, `de_icon_cell_value`, `de_gemeinde_forecast`; `de_indicator_source`, `de_indicator`, `de_indicator_value` (theme-independent indicators, ADR 019), `de_indicator_sum_component` (declared sums), `de_indicator_quality_note` (reviewed source discrepancies) |
| `analytical` | `de_current_run`; `de_current_{gemeinde,kreis,land,bund}_forecast_25h`; `de_current_{gemeinde,kreis,land,bund}_summary`; `de_current_employment`; `de_current_sectors` |

## Forecast path

The forecast key is the capstone's `run_time_utc + lead_time`, with UTC run
labels. For each lead hour, the four GRIB fields are decoded for the Germany
cell subset; temperature and shade apparent temperature are stored per cell
for the two retained runs (`de_icon_cell_value`, ADR 018), so that checks 6
and 7 can run on them. SQL aggregates cells to Gemeinden by the fraction of
each Gemeinde's area that a cell covers (`refresh_de_forecast_partition`),
the same area weighting as Berlin's planning areas, computed in the
equal-area projection EPSG:3035 (ADR 014).

Kreis, Land and Bund values are rolled up from Gemeinden with an area and a
population weighting (ADR 015). The `_summary` views hold the 24-hour maximum
and minimum of each measure and its time, with one rule for all four levels
(highest or lowest value, earliest time on a tie). The
`de_current_*` views show the newest run in which every Gemeinde has all 25
lead times, the same switching rule as Berlin's 25-hour views. Gemeinden
without a census record contribute no population weight; missing population
is never treated as zero.

## Indicator model

Employment and sector values share one theme-independent model (ADR 019):
catalogue rows (`de_indicator_source`, `de_indicator`), and one row per
geography version, level, region, indicator and reference date in
`de_indicator_value`, with `value_status` = `value`, `suppressed` or
`not_reported` (and a reason). The serving views read the BA's own published
Kreis, Land and Germany values rather than sums of suppressed Gemeinden;
derived values (the jobs ratio, sector shares, Land sums of groups) are
marked as own calculations. The sector theme uses the same tables for two
sources at the levels they publish (ADR 020).

## Orchestration

Five jobs in the group `germany`:

| Job | Command | What it does |
| --- | --- | --- |
| `germany_static` | `python -m src.germany.run_static` | VG250 and Zensus raw loads, hierarchy, population, grid subset and bridge, checks 1–4. Safe to repeat: an existing geography version from the same file is left unchanged. |
| `germany_forecast` | (one partition per lead time, started by the next command) | The four raw DWD fields (shared with Berlin, files only) and the Germany aggregation in SQL. |
| `germany_forecast_publish` | `python -m src.germany.run_forecast --run-time …` | Runs all 25 lead times, then publishes, applies retention and runs checks 5–7. |
| `germany_sectors` | `python -m src.germany.run_sectors [--file-date …]` | Downloads one quarter of both sector products (434 files, one request at a time with a 3 s pause; on HTTP 429 it waits minutes, then records a cool-down that every download and the sensor respect, and stops so the next run resumes), loads them raw, refreshes the sector values and runs checks 12–15. Started by `ba_sectors_release_sensor` (off by default) or by hand. |
| `germany_employment` | `python -m src.germany.run_employment [--reference-date …]` | Downloads one BA vintage, loads it raw, refreshes the indicator values and runs checks 8–11. Started by `ba_gemeindedaten_release_sensor` (daily HEAD request for the next vintage; off by default) or by hand. |

There is no sensor for Germany forecast runs. The Berlin availability sensor
does not trigger these jobs and stays stopped by default. Retention keeps the
newest complete run and the one before it, as database rows only; GRIB files
follow the capstone's file rule.

## Checks

Seven asset checks compare the layer with its sources and itself (README,
"Checks against the sources"): key coverage and hierarchy integrity on the
normalized hierarchy, source totals on the population, bridge coverage on the
bridge, and completeness, plausibility and Berlin cross-layer consistency on
the published forecast. Two further checks pin the raw files to their
checksums and counts. The employment theme adds four: the BA file's
structure, identities between its columns, its keys against the hierarchy,
and its Gemeinde, Kreis and Land values against its own published totals.
The sector theme adds four more: the files' structure, identities within each
Kreis, totals across Kreise, Länder, both products and the Gemeindedaten, and
coverage of all Kreise and Länder. Rules and thresholds:
[docs/germany-layer.md](docs/germany-layer.md#checks-and-thresholds).

## Map

`python -m src.export_germany_dashboard` writes a static folder in
`web/germany-dashboard/`: a PMTiles archive of Land, Kreis and Gemeinde
outlines (built once per geography version), one `values.json` per run and
one `employment.json` per BA vintage and one `sectors.json` per quarter,
joined in the browser by regional key (ADR 016). The page is in German and has
three themes: industry (share of metal, electrical and steel per Kreis and
Land, 0 to 40 %; the page opens with it), employment (jobs at place of work
per employed resident on a fixed logarithmic scale) and temperature (24-hour
maxima or minima on a fixed scale from -20 to +30 °C, legend limited to the
displayed range). The host must answer HTTP Range requests; the local preview
(`src/germany/preview_server.py`) does. There is no database connection from
the page and no basemap or API key.

## Things to know

- Every job applies the idempotent Germany schema files first, so a
  forecast run applies them once per lead time. This is simple and safe; it is
  why no separate install step exists.
- Building tiles needs the program `tippecanoe`, once per geography version.
- The serving views compute on read; a query takes about 1.6 to 2.7 seconds.

## Adding a further theme

A theme that publishes values per region uses the indicator tables (ADR 019):
catalogue rows in `de_indicator_source` and `de_indicator`, a raw table as
delivered, a refresh function that writes `de_indicator_value` with an
explicit status for every region, a serving view, a source contract and
checks against the source's own published totals. The hierarchy and the
indicator tables do not change. A theme that is computed from a grid, like
the forecast, keeps its own tables and roll-ups.

## Failure boundaries and restart behavior

- `germany_static` can run again: the same VG250 file leaves the geography
  version unchanged, a different file is refused; population and the bridge
  are rebuilt.
- The forecast views switch only to a run in which every Gemeinde has all 25
  lead hours; a newer incomplete run never displaces the previous complete
  one (check 5 warns about it).
- Every BA file is checked against its structural contract; a changed layout
  stops the load rather than being guessed. In the sector theme each file is
  checked before it is kept, and one bad file stops the run.
- `run_sectors` resumes from the files on disk after the BA throttles, and a
  shared cool-down file keeps every client and the sensor from sending
  requests until it has passed.
- Dagster asset checks prevent incomplete or inconsistent loads from being
  reported as successful.

Decisions of the Germany layer are recorded in ADRs
[014](docs/adr/014-equal-area-projection-for-germany.md) to
[020](docs/adr/020-sector-data-kreis-groups-and-land-divisions.md);
operation and measurements are in [docs/germany-layer.md](docs/germany-layer.md)
and [docs/germany-layer-facts.md](docs/germany-layer-facts.md).

---

## Berlin capstone

The capstone, *24h temperature forecast for Berlin neighborhoods*, is the
project the Germany layer grew out of. Its installation is the base the
Germany layer runs on.

### Problem and serving grain

Urban-planning and public-health analysts need neighborhood-level temperature
observations, historical context, and population counts on the same spatial
and temporal grain. The pipeline serves facts from which analysts can make
their own comparisons; it does not assert a medical risk classification.

One final serving row represents one current Berlin planning area, one forecast
run, and one forecast lead time. Forecast valid time is derived from run time
plus lead time. Historical reference values are matched to the corresponding
Berlin-local calendar month, day, and hour.

### System overview

```mermaid
flowchart TD
    LOR["Berlin LOR geometry"] --> STATIC["Static bootstrap"]
    POP["AfS population CSV"] --> STATIC
    GRID["DWD ICON grid"] --> STATIC
    SNAP["HOSTRADA reference snapshot"] --> REF["Reference import"]

    STATIC --> SPATIAL["PostGIS spatial bridge"]
    STATIC --> QUALITY["Population quality gate"]
    DWD["DWD forecast fields"] --> SENSOR["Dagster availability sensor"]
    SENSOR --> INGEST["Partitioned forecast ingestion"]
    SPATIAL --> INGEST
    INGEST --> TEMP["PLR temperature facts"]
    QUALITY --> VIEW["Temperature context serving view"]
    TEMP --> VIEW
    REF --> VIEW

    REBUILD["Optional historical rebuild"] --> SNAP
```

The division of responsibilities between Python, PostgreSQL/PostGIS, Dagster
and Docker is the one described in the [system overview](#system-overview)
above; the capstone established it.

### Source systems

| Source | Grain | Refresh behavior | Operational role |
| --- | --- | --- | --- |
| Berlin LOR planning areas | 542 polygons, geography dated 2023-01-01 | Slowly changing | Stable PLR identifiers and geometry. |
| Berlin PLR name directory | 542 planning-area identifiers and names | Slowly changing | Analyst-facing neighborhood labels only. |
| AfS population register | PLR, reference date 2025-12-31 | Slowly changing | Total residents and residents aged 65+. |
| DWD ICON grid 0047 | 542,040 cells and 272,089 vertices | Static model grid | Forecast-cell geometry and spatial bridge. |
| DWD ICON-D2-RUC | Forecast run, lead time, grid cell, indicator | Rolling forecast publication | Temperature, humidity, and wind fields. |
| DWD HOSTRADA | Historical hour and 1 km grid cell | Historical reference, 1995–2025 | Neighborhood and Berlin historical comparisons. |

Operational installation imports the completed HOSTRADA reference. Original
historical NetCDF files, the HOSTRADA spatial bridge, and historical hourly
tables are required only when deliberately reconstructing that reference.

### Durable data layers

| PostgreSQL schema | Responsibility | Representative relations |
| --- | --- | --- |
| `raw` | Source-faithful records and ingestion provenance. | `lor_plr`, `afs_population`, `icon_grid_source`, `icon_d2_ruc_field` |
| `normalized` | Validated geometry, accepted/rejected population, spatial weights, and normalized forecast fields. | `plr`, `plr_population_65plus`, `plr_population_rejected`, `icon_cell`, `icon_plr_area_bridge`, `icon_weather_mask`, `icon_d2_ruc_weather` |
| `analytical` | PLR-level temperature facts, historical reference tables, analyst-facing labels, and consumer-facing views. | `plr_weather_population`, `plr_display_name`, `hostrada_plr_hourly_reference`, `hostrada_berlin_hourly_reference`, `current_plr_weather_context` |

Raw forecast GRIB files also remain on the filesystem. Their stable directory
layout mirrors `(run_time_utc, lead_time, indicator)` and permits reprocessing
after DWD removes an older run from its rolling source window.

### Static initialization

The supported operational entry point, which is also the base installation
for the Germany layer, is:

```bash
uv run --env-file .env python -m src.bootstrap \
  --reference-archive /path/to/hostrada-reference-1995-2025.pgcustom
```

It first checks the archive bytes against the committed manifest, before
starting expensive static processing. It then:

1. Applies `sql/bootstrap_schema.sql` once to an empty database, or verifies
   that an existing database already satisfies the canonical schema contract.
2. Acquires LOR geometry, population data, and the ICON grid; an optional
   static-source archive supplies a verified fallback.
3. Runs the Dagster `operational_static_bootstrap` job and validates the
   resulting geometry, population, spatial bridge, and Berlin forecast mask.
4. Confirms that installed PLR identifiers match the reference geography
   fingerprint.
5. Restores the two historical-reference tables in one PostgreSQL transaction.
6. Validates the imported reference independently of historical source
   manifests or 1995–2025 hourly observations.
7. Installs 542 verified PLR display names for the final analyst-facing views.

On the development laptop, the first clean-room installation took
approximately ten minutes. Its largest one-time operation was processing the
ICON grid: 272,089 vertices, 542,040 cells, and 1,626,120 topology records.

### Forecast identity and ingestion

The canonical forecast key is:

```text
run_time_utc + lead_time
```

`valid_time_utc = run_time_utc + lead_time`. Run labels such as
`20260824T1600` always represent UTC. A Dagster sensor examines only recent
advertised DWD runs and triggers a partition when all four required fields are
present:

- `T_2M`: air temperature.
- `RELHUM_2M`: relative humidity.
- `U_10M`: east-west wind component.
- `V_10M`: north-south wind component.

The sensor checks during minutes `:30`–`:59`, is stopped by default, and must
be enabled explicitly. Manual execution performs the same source-completeness
preflight while still allowing complete retained raw partitions to be
reprocessed without contacting DWD.

The forecast pipeline retains and validates full GRIB source fields, selects
the 465 Berlin forecast-mask cell indices from each decoded array in Python,
and copies only those values into PostgreSQL. It then computes shade apparent
temperature and area-weights both temperature measures to PLRs. Full-grid
point counts and missing-value accounting remain recorded in the source
manifest even though nationwide values do not cross the database boundary.

### Spatial and population contracts

Forecast grid cells and PLRs use different geometries. PostGIS constructs a
many-to-many intersection bridge in EPSG:25833; PLR temperature is weighted by the
fraction of each planning area's area represented by each intersecting cell.
Bridge checks protect complete PLR coverage and weight totals.

Population records are validated before entering the analytical layer. The
observed source contains 542 PLR records: 540 accepted and two rejected. A
rejected record remains represented in the final view with null population
counts and `population_status = 'rejected_source_record'`. Missing population
is never represented as zero.

### Historical reference contract

The historical reference is built from HOSTRADA version 1.0 for 1995–2025,
using the current PLR geography rather than historical boundary versions.

For each local month/day/hour, the reference retains the median, 90th
percentile, and observed maximum of both air temperature and shade apparent
temperature. PLR-specific and Berlin-wide results occupy separate tables:

| Relation | Expected rows |
| --- | ---: |
| `analytical.hostrada_plr_hourly_reference` | 4,747,920 |
| `analytical.hostrada_berlin_hourly_reference` | 8,760 |

Calendar groups are derived using `Europe/Berlin`. February 29 is excluded;
repeated autumn UTC observations remain separate samples; missing spring
hours are not fabricated. The 8,760 Berlin groups contain 271,559 historical
observations in total. Group sample counts vary according to actual historical
timezone rules.

Forecast and historical temperature values are produced on different source
grids. The
serving join uses the shared, versioned PLR geography and local calendar key;
it never incorrectly equates the ICON and HOSTRADA grid identifiers.

### Final serving contract

`analytical.plr_weather_context` exposes every forecast partition.
`analytical.current_plr_weather_context` exposes the current partition.
Both views have the same 23 columns:

| Category | Columns |
| --- | --- |
| Geography | `plr_id`, `plr_name` |
| Forecast time | `run_time_utc`, `lead_time`, `valid_time_utc`, `valid_time_berlin` |
| Forecast observations | `temperature_c`, `apparent_temperature_shade_c` |
| PLR air-temperature reference | `plr_temperature_median_c`, `plr_temperature_p90_c`, `plr_temperature_max_c` |
| PLR apparent-temperature reference | `plr_apparent_temperature_median_c`, `plr_apparent_temperature_p90_c`, `plr_apparent_temperature_max_c` |
| Berlin air-temperature reference | `berlin_temperature_median_c`, `berlin_temperature_p90_c`, `berlin_temperature_max_c` |
| Berlin apparent-temperature reference | `berlin_apparent_temperature_median_c`, `berlin_apparent_temperature_p90_c`, `berlin_apparent_temperature_max_c` |
| Population | `population_total`, `population_65plus`, `population_status` |

Display names are presentation-only and may be duplicated: PLR identifiers
remain the sole engineering and spatial join key. Missing names cannot remove
forecast rows because the final label lookup uses a left join.

Historical joins are left joins. Forecasts occurring on Berlin-local February
29 remain visible with null historical comparisons. Geography version,
population reference date, source checksums, sample counts, threshold flags,
and exposure classifications stay out of the serving contract.

#### Complete 25-point forecast and neighborhood summary

The feature branch adds two separate, presentation-oriented views without
changing the existing 23-column single-partition contract:

| Relation | Grain | Expected rows |
| --- | --- | ---: |
| `analytical.current_plr_temperature_forecast_25h` | One PLR and one forecast lead hour, 0–24. | 13,550 |
| `analytical.current_plr_temperature_summary_25h` | One PLR across the complete forecast horizon. | 542 |
| `analytical.current_plr_temperature_history_25h` | One PLR, forecast lead hour, and historical year, 1995–2025; optional. | 420,050 |

Both views select the newest model run containing all 25 lead hours for all
installed PLRs. A newer incomplete run never displaces a previous complete
horizon. Public forecast timestamps are expressed in Berlin local time.

For each PLR, the summary reports the highest forecast temperature and its
earliest local occurrence, the largest **signed** forecast-minus-historical-
median difference and its earliest occurrence, the sum of all 25 signed
difference, the largest apparent-minus-forecast temperature difference, and
the existing population values/status. A negative maximum
difference remains negative when every forecast hour is below its historical
median; absolute differences are never substituted.

The detailed plotting view reads only the established forecast context and its
already-installed reference medians. It does not read, rebuild, or retain the
147-million-row HOSTRADA historical observation table.

Individual historical-year trajectories are a separate, explicitly opted-in
extension. They require the original populated
`analytical.hostrada_plr_hourly` table, rather than the compact reference
snapshot. PostgreSQL resolves each forecast hour to the same Berlin-local
calendar hour in historical years 1995–2025, then uses both leading columns of
the source table's existing `(source_month_utc, valid_time_utc, plr_id)`
primary key for 775 targeted timestamp lookups.

The resulting `analytical.plr_temperature_history_25h` table retains only the
current horizon's 420,050 rows, each containing temperature and apparent
temperature. Its plotting view adds the existing analyst-facing PLR name,
corresponding forecasts, and historical medians. Historical extraction is
transactional, repeatable, and
never changes the original hourly observations, compact reference tables,
existing forecast view, or neighborhood summary.

#### Static temperature dashboard

The optional presentation export reads the three 25-hour serving views and
`normalized.plr`, simplifies the EPSG:25833 boundaries in PostGIS, and writes
a self-contained static folder. One map file contains 542 geometries and their
compact summaries. One small detail file per PLR contains 25 forecast and
median points for temperature and apparent temperature, plus 31 historical-year
trajectories for both indicators; the browser requests a detail only when that
neighborhood is selected.

The browser draws projected PLR boundaries and line charts directly in SVG
using local HTML, CSS, and JavaScript. It uses no map tiles, frontend framework,
CDN, API endpoint, or live database connection. This keeps PostgreSQL
credentials out of the website and makes the exported folder deployable on an
ordinary static host.

### Failure boundaries and restart behavior

- The database bootstrap applies one canonical schema to an empty database and
  never truncates an existing compatible installation.
- Static-source fallback restores only verified files and refuses to overwrite
  conflicting local data.
- HOSTRADA reference import is atomic and rejects incompatible PLR geography.
- Dagster asset checks prevent incomplete partitions from being reported as
  successful.
- Forecast files and forecast-only PostgreSQL rows share a rolling retention
  window of at most 24 hours; retained files remain reprocessable within that
  window without relying on upstream availability.
- The 25-point forecast runner processes one lead at a time, resumes validated
  partitions, and publishes a horizon only after every PLR and lead is present.
- The optional historical backfill commits one validated month at a time and
  removes its large source files only after durable outputs pass validation.

Architectural tradeoffs of both parts are recorded under
[docs/adr/](docs/adr/README.md).
