"""
Interaktiver GeoCOM Collector für surveylog.

Ablauf:
  1. Benutzer gibt PID ein (z.B. FP00010001)
  2. surveylog misst (TMC_DO_MEASURE + TMC_GET_COORDINATE)
  3. Punkt wird in Staging-DB gespeichert
  4. Weiter mit nächstem Punkt

Leere Eingabe → wiederholt letzten PID + 1 (Sequenznummer +1)
'quit' oder Ctrl+C → beendet
"""

import time
import logging
from typing import Optional

from .connection import TotalstationConnection, ConnectionConfig
from .geocom_constants import RC, RPC, TMC_MODE
from .pid_parser import parse_pid, next_pid
from .staging import StagingDB, StagingPoint

logger = logging.getLogger(__name__)


def measure_point(conn: TotalstationConnection, config: ConnectionConfig) -> Optional[dict]:
    """
    Führt eine Messung durch:
      1. TMC_DO_MEASURE (Messung auslösen)
      2. Warten
      3. TMC_GET_COORDINATE (Koordinaten abfragen)

    Gibt {"x": ..., "y": ..., "z": ...} zurück oder None bei Fehler.
    """
    # Messung auslösen
    mode = TMC_MODE.REFLLESS if config.reflectorless else TMC_MODE.DEF_DIST
    result = conn.send_command(RPC.TMC_DO_MEASURE, mode, 1)

    rc = result.get("rc", -1)
    if RC.is_fatal(rc):
        logger.error(f"TMC_DO_MEASURE fehlgeschlagen: RC {rc} — {RC.describe(rc)}")
        return None
    if RC.is_warning(rc):
        logger.warning(f"TMC_DO_MEASURE Warnung: RC {rc} — {RC.describe(rc)}")

    # Warten bis Messung abgeschlossen
    time.sleep(config.measure_wait)

    # Koordinaten abfragen
    result = conn.send_command(RPC.TMC_GET_COORDINATE, 0, 1, 0)
    rc = result.get("rc", -1)

    if RC.is_fatal(rc):
        logger.error(f"TMC_GET_COORDINATE fehlgeschlagen: RC {rc} — {RC.describe(rc)}")
        return None
    if RC.is_warning(rc):
        logger.warning(f"TMC_GET_COORDINATE Warnung: RC {rc} — {RC.describe(rc)}")

    values = result.get("values", [])
    if len(values) < 3:
        logger.error(f"Unvollständige Koordinaten: {values}")
        return None

    try:
        return {
            "x": float(values[0]),
            "y": float(values[1]),
            "z": float(values[2]),
            "rc": rc,
        }
    except (ValueError, TypeError) as e:
        logger.error(f"Koordinaten-Parse-Fehler: {e} — {values}")
        return None


def run_collector(config: ConnectionConfig, db_path: str):
    """
    Startet den interaktiven Collector.

    Verbindet sich mit der Totalstation und wartet auf PID-Eingaben.
    """
    db = StagingDB(db_path)
    conn = TotalstationConnection(config)

    print(f"\n  surveylog — Interaktiver Collector")
    print(f"  Port:   {config.port}")
    print(f"  DB:     {db_path}")
    print(f"  Modus:  {'Reflektorlos' if config.reflectorless else 'Mit Reflektor'}")
    print()

    # Verbindung aufbauen
    print(f"  Verbinde mit {config.port}...")
    if not conn.connect():
        print("  ✗ Verbindung fehlgeschlagen.")
        print()
        if config._use_tcp:
            print("  Hinweis: Stelle sicher dass die BT/TCP Bridge App läuft")
            print("  und mit der Totalstation verbunden ist.")
        return

    # Ping testen
    print("  Teste Verbindung...")
    if not conn.ping():
        print("  ✗ Totalstation antwortet nicht.")
        conn.disconnect()
        return

    print("  ✓ Verbunden!\n")
    print("  Eingabe: PID (z.B. FP00010001) + Enter → misst")
    print("           Leere Eingabe + Enter           → wiederholt letzten PID+1")
    print("           'quit' + Enter                  → beendet")
    print()

    last_pid = None
    count = 0

    try:
        while True:
            try:
                pid_input = input("  PID: ").strip()
            except EOFError:
                break

            if pid_input.lower() in ("quit", "exit", "q"):
                break

            # Leere Eingabe → letzten PID wiederholen + 1
            if pid_input == "":
                if last_pid is None:
                    print("  → Noch kein PID eingegeben.")
                    continue
                pid_input = next_pid(last_pid)
                print(f"  → Verwende: {pid_input}")

            # PID validieren
            pid_input = pid_input.upper()
            parsed = parse_pid(pid_input)
            if parsed is None:
                print(f"  ✗ Ungültiger PID: '{pid_input}'")
                print("    Format: CCSSSSNNNN (z.B. FP00010001)")
                continue

            # Messen
            print(f"  Messe {pid_input}...", end="", flush=True)
            measurement = measure_point(conn, config)

            if measurement is None:
                print(" ✗ Messung fehlgeschlagen")
                continue

            # Speichern
            point = StagingPoint(
                pid=pid_input,
                x=measurement["x"],
                y=measurement["y"],
                z=measurement["z"],
                source="geocom",
            )
            db.insert(point)
            count += 1
            last_pid = pid_input

            print(f" ✓  X={measurement['x']:.3f}  Y={measurement['y']:.3f}  Z={measurement['z']:.3f}")

    except KeyboardInterrupt:
        print("\n  Abgebrochen.")
    finally:
        conn.disconnect()
        print(f"\n  {count} Punkte gespeichert in '{db_path}'")
        print(f"  Nächster Schritt: surveylog build {db_path} ausgabe.gpkg\n")
