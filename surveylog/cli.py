"""
surveylog CLI

Befehle:
  surveylog collect  --port tcp://localhost:4444 --db feld.db
  surveylog import   punkte.csv --db feld.db
  surveylog build    feld.db ausgabe.gpkg
  surveylog info     feld.db
  surveylog validate feld.db
  surveylog codes
"""

import click
import logging

from .connection import ConnectionConfig
from .collector import run_collector
from .csv_collector import import_csv
from .feature_builder import build_geopackage
from .staging import StagingDB
from .code_table import CODE_TABLE
from .pid_parser import validate_pid_sequence


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Detailliertes Logging")
def cli(verbose):
    """surveylog — Vermessungs-Datenpipeline für Archäologie"""
    if verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)


# ── collect ───────────────────────────────────────────────────────────────────

@cli.command()
@click.option("--port", default="tcp://localhost:4444",
              show_default=True,
              help="Verbindungsport: tcp://localhost:4444 (Android) oder /dev/rfcomm0 (Linux)")
@click.option("--db", default="surveylog.db", show_default=True,
              help="Pfad zur Staging-Datenbank")
@click.option("--timeout", default=5.0, show_default=True,
              help="Verbindungs-Timeout in Sekunden")
@click.option("--wait", default=0.8, show_default=True,
              help="Wartezeit nach Messauslösung (Sekunden)")
@click.option("--prism/--no-prism", default=False,
              help="Mit Reflektor messen (Standard: reflektorlos)")
def collect(port, db, timeout, wait, prism):
    """Live-Aufnahme von einer Totalstation (GeoCOM)."""
    config = ConnectionConfig(
        port=port,
        timeout=timeout,
        measure_wait=wait,
        reflectorless=not prism,
    )
    run_collector(config, db)


# ── import ────────────────────────────────────────────────────────────────────

@cli.command("import")
@click.argument("csv_file", type=click.Path(exists=True))
@click.option("--db", default="surveylog.db", show_default=True,
              help="Pfad zur Staging-Datenbank")
@click.option("--delimiter", default=",", show_default=True,
              help="CSV-Trennzeichen")
def import_cmd(csv_file, db, delimiter):
    """Importiert eine GNSS-CSV-Datei in die Staging-Datenbank."""
    count = import_csv(csv_file, db, delimiter=delimiter)
    click.echo(f"✓ {count} Punkte importiert aus '{csv_file}'")


# ── build ─────────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("db_file", type=click.Path(exists=True))
@click.argument("output", default="ausgabe.gpkg")
@click.option("--crs", default=4326, show_default=True,
              help="Koordinatenreferenzsystem (EPSG-Code)")
def build(db_file, output, crs):
    """Baut ein GeoPackage (.gpkg) aus der Staging-Datenbank."""
    db = StagingDB(db_file)
    points = db.all_points()
    if not points:
        click.echo("✗ Keine Punkte in der Datenbank.")
        return
    result = build_geopackage(points, output, crs=crs)
    click.echo(f"✓ GeoPackage erstellt: {output}")
    click.echo(f"  Punkte:   {result.get('points', 0)}")
    click.echo(f"  Linien:   {result.get('lines', 0)}")
    click.echo(f"  Flächen:  {result.get('polygons', 0)}")


# ── info ──────────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("db_file", type=click.Path(exists=True))
def info(db_file):
    """Zeigt Statistiken über die Staging-Datenbank."""
    db = StagingDB(db_file)
    points = db.all_points()
    if not points:
        click.echo("Keine Punkte in der Datenbank.")
        return

    from collections import Counter
    codes = Counter(p.pid[:2] for p in points)
    sources = Counter(p.source for p in points)

    click.echo(f"\n  Datenbank: {db_file}")
    click.echo(f"  Punkte gesamt: {len(points)}\n")
    click.echo("  Nach Code:")
    for code, count in sorted(codes.items()):
        name = CODE_TABLE.get(code, {}).get("name_de", code)
        click.echo(f"    {code}  {name:<20} {count}")
    click.echo("\n  Nach Quelle:")
    for source, count in sorted(sources.items()):
        click.echo(f"    {source:<12} {count}")
    click.echo()


# ── validate ──────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("db_file", type=click.Path(exists=True))
def validate(db_file):
    """Prüft PID-Sequenzen auf Lücken oder Fehler."""
    db = StagingDB(db_file)
    points = db.all_points()
    pids = [p.pid for p in points]
    issues = validate_pid_sequence(pids)
    if not issues:
        click.echo("✓ Alle PID-Sequenzen vollständig.")
    else:
        click.echo(f"✗ {len(issues)} Problem(e) gefunden:\n")
        for issue in issues:
            click.echo(f"  • {issue}")


# ── codes ─────────────────────────────────────────────────────────────────────

@cli.command()
def codes():
    """Zeigt alle verfügbaren Vermessungs-Codes."""
    click.echo("\n  Code  Typ       Deutsch              Englisch")
    click.echo("  " + "─" * 55)
    for code, info in sorted(CODE_TABLE.items()):
        geom = info.get("geometry", "?")
        name_de = info.get("name_de", "")
        name_en = info.get("name_en", "")
        geom_sym = {"point": "Punkt  ", "line": "Linie  ", "polygon": "Fläche "}.get(geom, "?      ")
        click.echo(f"  {code}    {geom_sym}  {name_de:<20} {name_en}")
    click.echo()
