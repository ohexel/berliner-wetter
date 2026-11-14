"""Germany layer: contracts that hold across all themes.

- The Germany DDL only adds de_ objects, so Berlin is never touched (D2).
- No source or generated data is tracked in Git (D8).
- The public map carries the attribution the licences and terms of use
  require, and loads only pinned, integrity-checked libraries without a
  basemap or key (D6).
"""

import re
from pathlib import Path

import pytest

from src.export_germany_dashboard import DASHBOARD_SOURCE, PUBLIC_ASSETS
from src.germany.contract import GERMANY_RAW_ROOT


PROJECT_ROOT = Path(__file__).resolve().parents[1]
GERMANY_DDL = sorted((PROJECT_ROOT / "sql" / "germany").glob("*.sql"))


def _code(path: Path) -> str:
    """SQL without comments, upper case."""
    lines = path.read_text(encoding="utf-8").splitlines()
    return "\n".join(line.split("--", 1)[0] for line in lines).upper()


def _page() -> tuple[str, str]:
    html = (DASHBOARD_SOURCE / "index.html").read_text(encoding="utf-8")
    script = (DASHBOARD_SOURCE / "app.js").read_text(encoding="utf-8")
    return html, script


@pytest.mark.contract
@pytest.mark.parametrize("ddl", GERMANY_DDL, ids=lambda path: path.name)
def test_germany_ddl_only_adds_de_objects(ddl: Path) -> None:
    code = _code(ddl)
    for forbidden in ("DROP ", "TRUNCATE", "ALTER ", "UPDATE ", "TEMPORARY TABLE", "TEMP TABLE"):
        assert forbidden not in code, forbidden
    for target in re.findall(r"(?:DELETE FROM|INSERT INTO) (\S+)", code):
        assert target.split(".")[-1].startswith("DE_"), target
    for name in re.findall(r"CREATE (?:TABLE|INDEX) IF NOT EXISTS (\S+)", code):
        assert name.split(".")[-1].startswith("DE_"), name
    assert "CREATE TABLE " not in code.replace("CREATE TABLE IF NOT EXISTS", "")
    for name in re.findall(r"CREATE OR REPLACE (?:VIEW|FUNCTION) (\S+?)[\s(]", code):
        assert "DE_" in name, name


@pytest.mark.contract
def test_no_germany_data_in_git() -> None:
    gitignore = (PROJECT_ROOT / ".gitignore").read_text(encoding="utf-8")
    assert re.search(r"(?m)^data/$", gitignore)
    assert GERMANY_RAW_ROOT.parts[0] == "data"  # sources are downloaded under data/
    ignore = (DASHBOARD_SOURCE / "data" / ".gitignore").read_text(encoding="utf-8")
    assert ignore.splitlines() == ["*", "!.gitignore"]  # map exports are generated
    assert DASHBOARD_SOURCE != PROJECT_ROOT / "web" / "temperature-dashboard"
    assert all("temperature-dashboard" not in (DASHBOARD_SOURCE / name).read_text(encoding="utf-8")
               for name in PUBLIC_ASSETS)


@pytest.mark.contract
def test_public_page_shows_the_required_attribution() -> None:
    html, script = _page()
    # BKG VG250 and Zensus 2022: dl-de/by-2-0 with the prescribed wording.
    assert "© GeoBasis-DE / BKG 2022 (Daten verändert)" in html
    assert "https://www.bkg.bund.de" in html
    assert "Zensus 2022, © Statistische Ämter des Bundes und der Länder" in html
    assert html.count("dl-de/by-2-0") >= 2
    # DWD forecast.
    assert "Deutscher Wetterdienst" in html
    # BA terms of use: name the source, link its statistics homepage, mark
    # own calculations as such.
    assert "Statistik der Bundesagentur für Arbeit" in html
    assert 'href="https://statistik.arbeitsagentur.de/"' in html
    for product in ("Gemeindedaten", "Regionalreport über Beschäftigte",
                    "Beschäftigte nach Wirtschaftszweigen"):
        assert product in html
    assert "eigene Berechnung" in html and "eigene Berechnung" in script


@pytest.mark.contract
def test_map_libraries_are_pinned_and_there_is_no_basemap() -> None:
    html, script = _page()
    external = re.findall(r'(?:src|href)="(https://cdn[^"]+)"', html)
    assert external, "MapLibre and PMTiles are loaded from a CDN"
    for url in external:
        assert re.search(r"@\d+\.\d+\.\d+/", url), url
    for tag in re.findall(r"<(?:script|link)[^>]+https://cdn[^>]+>", html, flags=re.S):
        assert 'integrity="sha384-' in tag and 'crossorigin="anonymous"' in tag, tag
    page = (html + script).lower()
    for word in ("api_key", "apikey", "access_token", "mapbox://", "maptiler",
                 "tiles.openstreetmap", "{z}/{x}/{y}", "glyphs", "sprite"):
        assert word not in page, word
