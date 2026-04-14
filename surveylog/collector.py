"""
GSI Polling-Collector für surveylog.

Architektur (passives Polling):
  1. surveylog sendet %R1Q,2115: an die Totalstation
  2. TS07 antwortet mit letztem Messsatz im GSI-Format
  3. surveylog parst PID + E + N + H aus dem GSI-String
  4. Duplikat-Schutz: nur neue Messungen werden gespeichert
  5. Kurze Pause, dann wieder von vorne (Polling-Loop)

Das Instrument behält die volle Kontrolle:
  - PID wird am TS07 eingegeben
  - Messung wird am TS07 ausgelöst
  - surveylog lauscht nur und speichert

Verbindung über TCP-Bridge (Android):
  BT/TCP Bridge App: Bluetooth SPP → TCP localhost:4444
  surveylog: --port tcp://localhost:4444

Verbindung seriell (Linux):
  surveylog: --port /dev/rfcomm0
"""

import time
import logging
from dataclasses import dataclass

from .connection import TotalstationConnection, ConnectionConfig
from .geocom_constants import RPC
from .gsi_parser import parse_gsi_response, measurement_key
from .staging import StagingDB, StagingPoint

logger = logging.getLogger(__name__)

# GeoCOM RPC für GSI-Messsatz (TMC_GetSimpleMea → GSI-Format)
RPC_GET_LAST_MEASURE_GSI = 2115


@dataclass
class CollectorConfig:
    """Konfiguration für den Polling-Collector."""
    poll_interval: float = 0.3    # Sekunden zwischen Polls (ca. 3 Hz)
    timeout: float = 0.5          # Timeout pro Request
    reconnect_delay: float = 3.0  # Wartezeit vor Reconnect-Versuch


def run_collector(conn_config: ConnectionConfig, db_path: str,
                  poll_interval: float = 0.3):
    """
    Startet den Polling-Collector.

    Pollt kontinuierlich %R1Q,2115: und speichert neue Messungen.
    Läuft bis Ctrl+C.
    """
    db = StagingDB(db_path)
    conn = TotalstationConnection(conn_config)

    print(f"\n  surveylog — GSI Polling Collector")
    print(f"  Port:     {conn_config.port}")
    print(f"  DB:       {db_path}")
    print(f"  Polling:  {poll_interval}s ({1/poll_interval:.0f} Hz)")
    print(f"\n  Bedienung: Alles am Instrument (TS07)")
    print(f"  PID eingeben + Messung auslösen → surveylog speichert automatisch")
    print(f"  Beenden: Ctrl+C\n")

    last_key = None
    count = 0
    errors = 0

    def connect_loop():
        """Verbindet, mit stummem Retry bei Fehler."""
        while True:
            print(f"  Verbinde mit {conn_config.port}...", end="", flush=True)
            if conn.connect():
                print(" ✓")
                return
            print(f" ✗  Retry in {conn_config.reconnect_delay}s...")
            time.sleep(conn_config.reconnect_delay)

    try:
        connect_loop()
        print("  Warte auf Messungen...\n")

        while True:
            # Anfrage senden
            result = conn.send_command(RPC_GET_LAST_MEASURE_GSI)

            if result["rc"] == -1:
                # Verbindungsabbruch → stumm reconnecten
                errors += 1
                logger.warning(f"Verbindungsfehler ({errors}x), reconnecte...")
                time.sleep(conn_config.reconnect_delay)
                connect_loop()
                continue

            errors = 0  # Reset bei Erfolg

            # GSI parsen
            raw = result.get("raw", "")
            measurement = parse_gsi_response(raw)

            if measurement is None:
                # Keine gültige Messung (noch keine, oder Instrument wartet)
                time.sleep(poll_interval)
                continue

            # Duplikat-Schutz
            key = measurement_key(measurement)
            if key == last_key:
                time.sleep(poll_interval)
                continue

            # Neue Messung! Speichern.
            last_key = key
            count += 1

            point = StagingPoint(
                pid=measurement.pid,
                x=measurement.e,
                y=measurement.n,
                z=measurement.h,
                source="geocom_gsi",
            )
            db.insert(point)

            print(f"  [{count:4d}] {measurement.pid:<12}"
                  f"  E={measurement.e:12.3f}"
                  f"  N={measurement.n:12.3f}"
                  f"  H={measurement.h:8.3f}")

            time.sleep(poll_interval)

    except KeyboardInterrupt:
        print(f"\n\n  Beendet. {count} Punkte gespeichert in '{db_path}'")
        if count > 0:
            print(f"  Nächster Schritt: surveylog build {db_path} ausgabe.gpkg\n")
    finally:
        conn.disconnect()
