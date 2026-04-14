"""
GSI Stream-Collector für surveylog.

Architektur (passives Lauschen):
  Der TS07 schickt nach jeder Messung automatisch einen GSI-Datensatz
  über die Bluetooth-Verbindung. surveylog verbindet sich via TCP-Bridge
  und liest den eingehenden Stream — ohne selbst etwas zu senden.

  1. TCP-Verbindung zu BT/TCP Bridge (localhost:4444)
  2. Eingehende Bytes lesen und zu Zeilen zusammensetzen
  3. Jede Zeile auf GSI-Format prüfen
  4. Neue Messungen in die DB speichern (Duplikat-Schutz)

Das Instrument behält die volle Kontrolle:
  - PID wird am TS07 eingegeben
  - Messung wird am TS07 ausgelöst
  - surveylog lauscht nur und speichert

Verbindung über TCP-Bridge (Android):
  BT/TCP Bridge App: Bluetooth SPP → TCP localhost:4444
  surveylog: --port tcp://localhost:4444
"""

import socket
import time
import logging
from dataclasses import dataclass

from .connection import ConnectionConfig
from .gsi_parser import parse_gsi_response, measurement_key
from .staging import StagingDB, StagingPoint

logger = logging.getLogger(__name__)


@dataclass
class CollectorConfig:
    """Konfiguration für den Stream-Collector."""
    timeout: float = 10.0          # Socket-Timeout (Sekunden ohne Daten)
    reconnect_delay: float = 3.0   # Wartezeit vor Reconnect-Versuch


def _tcp_host_port(port_str: str):
    """Parst 'tcp://host:port' → (host, port)."""
    addr = port_str[len("tcp://"):]
    host, port = addr.rsplit(":", 1)
    return host, int(port)


def _connect_tcp(host: str, port: int, timeout: float) -> socket.socket:
    """Baut TCP-Verbindung auf. Gibt Socket zurück oder wirft Exception."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    s.connect((host, port))
    return s


def _read_lines(sock: socket.socket, buf: bytearray):
    """
    Liest Bytes aus dem Socket und gibt vollständige Zeilen zurück.

    Der TS07 trennt Datensätze mit \\r\\n oder \\n.
    Unvollständige Daten bleiben im buf (bytearray) für den nächsten Aufruf.
    """
    try:
        chunk = sock.recv(1024)
    except socket.timeout:
        return []
    if not chunk:
        raise ConnectionError("Verbindung vom Gegensteller getrennt")

    buf.extend(chunk)
    lines = []

    while True:
        # Suche nach Zeilenende (\n oder \r\n)
        idx = buf.find(b"\n")
        if idx == -1:
            break
        line = buf[:idx].decode("ascii", errors="replace").strip()
        del buf[:idx + 1]
        if line:
            lines.append(line)

    return lines


def run_collector(conn_config: ConnectionConfig, db_path: str,
                  poll_interval: float = 0.3):
    """
    Startet den Stream-Collector.

    Lauscht auf eingehende GSI-Datensätze vom TS07 und speichert neue Messungen.
    Läuft bis Ctrl+C.
    """
    db = StagingDB(db_path)

    print(f"\n  surveylog — GSI Stream Collector")
    print(f"  Port:  {conn_config.port}")
    print(f"  DB:    {db_path}")
    print(f"\n  Bedienung: Alles am Instrument (TS07)")
    print(f"  PID eingeben + Messung auslösen → surveylog speichert automatisch")
    print(f"  Beenden: Ctrl+C\n")

    # TCP oder seriell?
    use_tcp = conn_config.port.startswith("tcp://")
    if use_tcp:
        host, port = _tcp_host_port(conn_config.port)
    else:
        raise NotImplementedError(
            "Serieller Modus nicht implementiert — bitte TCP-Bridge verwenden"
        )

    last_key = None
    count = 0

    def connect_loop() -> socket.socket:
        """Verbindet, mit stummem Retry bei Fehler."""
        while True:
            print(f"  Verbinde mit {conn_config.port}...", end="", flush=True)
            try:
                s = _connect_tcp(host, port, conn_config.timeout)
                print(" ✓")
                return s
            except OSError as e:
                print(f" ✗  ({e})")
                print(f"  Retry in {conn_config.reconnect_delay}s...")
                time.sleep(conn_config.reconnect_delay)

    sock = connect_loop()
    buf = bytearray()
    print("  Warte auf Messungen...\n")

    try:
        while True:
            try:
                lines = _read_lines(sock, buf)
            except (ConnectionError, OSError) as e:
                logger.warning(f"Verbindungsabbruch: {e} — reconnecte...")
                try:
                    sock.close()
                except OSError:
                    pass
                buf.clear()
                time.sleep(conn_config.reconnect_delay)
                sock = connect_loop()
                print("  Warte auf Messungen...\n")
                continue

            for line in lines:
                logger.debug(f"← {line}")

                # GSI parsen
                measurement = parse_gsi_response(line)
                if measurement is None:
                    continue

                # Duplikat-Schutz
                key = measurement_key(measurement)
                if key == last_key:
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

    except KeyboardInterrupt:
        print(f"\n\n  Beendet. {count} Punkte gespeichert in '{db_path}'")
        if count > 0:
            print(f"  Nächster Schritt: surveylog build {db_path} ausgabe.gpkg\n")
    finally:
        try:
            sock.close()
        except OSError:
            pass
