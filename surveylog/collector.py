"""
GSI Collector für surveylog.

Architektur (aktives Polling mit Stream-Lesen):
  1. surveylog sendet %R1Q,2115: an die Totalstation (GeoCOM Request)
  2. TS07 antwortet mit letztem Messsatz im GSI-Format
  3. surveylog liest alle verfügbaren Bytes (mit kurzem Timeout)
  4. GSI-Zeilen werden aus dem Puffer extrahiert und geparst
  5. Duplikat-Schutz: nur neue Messungen werden gespeichert
  6. Kurze Pause, dann wieder von vorne

Das Instrument behält die volle Kontrolle:
  - PID wird am TS07 eingegeben
  - Messung wird am TS07 ausgelöst
  - surveylog pollt und speichert automatisch

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

# GeoCOM Request: letzten Messsatz als GSI abrufen
GEOCOM_GET_LAST_GSI = b"%R1Q,2115:\r\n"


@dataclass
class CollectorConfig:
    """Konfiguration für den Collector."""
    poll_interval: float = 0.5     # Sekunden zwischen Polls
    read_timeout: float = 1.0      # Timeout beim Lesen der Antwort
    reconnect_delay: float = 3.0   # Wartezeit vor Reconnect-Versuch


def _tcp_host_port(port_str: str):
    """Parst 'tcp://host:port' → (host, port)."""
    addr = port_str[len("tcp://"):]
    host, port = addr.rsplit(":", 1)
    return host, int(port)


def _connect_tcp(host: str, port: int, timeout: float = 5.0) -> socket.socket:
    """Baut TCP-Verbindung auf."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    s.connect((host, port))
    return s


def _read_available(sock: socket.socket, read_timeout: float) -> str:
    """
    Liest alle verfügbaren Bytes aus dem Socket.

    Setzt kurzen Timeout, liest solange Daten kommen,
    gibt alles als String zurück.
    """
    sock.settimeout(read_timeout)
    data = b""
    try:
        while True:
            chunk = sock.recv(1024)
            if not chunk:
                raise ConnectionError("Verbindung getrennt")
            data += chunk
            # Kurzen Timeout für weitere Chunks
            sock.settimeout(0.1)
    except socket.timeout:
        pass  # Keine weiteren Daten — normal
    return data.decode("ascii", errors="replace")


def _extract_gsi_lines(text: str) -> list:
    """
    Extrahiert alle potentiellen GSI-Zeilen aus einem Text-Block.

    Sucht nach Zeilen die mit * beginnen oder %R1P enthalten.
    """
    lines = []
    for line in text.replace("\r", "\n").split("\n"):
        line = line.strip()
        if not line:
            continue
        lines.append(line)
    return lines


def run_collector(conn_config: ConnectionConfig, db_path: str,
                  poll_interval: float = 0.5):
    """
    Startet den Polling-Collector.

    Pollt kontinuierlich und speichert neue Messungen.
    Läuft bis Ctrl+C.
    """
    db = StagingDB(db_path)

    use_tcp = conn_config.port.startswith("tcp://")
    if not use_tcp:
        raise NotImplementedError(
            "Nur TCP unterstützt — bitte BT/TCP Bridge App verwenden"
        )

    host, port = _tcp_host_port(conn_config.port)

    print(f"\n  surveylog — GSI Collector")
    print(f"  Port:  {conn_config.port}")
    print(f"  DB:    {db_path}")
    print(f"\n  Bedienung: Alles am Instrument (TS07)")
    print(f"  PID eingeben + Messung auslösen → surveylog speichert automatisch")
    print(f"  Beenden: Ctrl+C\n")

    last_key = None
    count = 0
    errors = 0

    def connect_loop() -> socket.socket:
        while True:
            print(f"  Verbinde mit {conn_config.port}...", end="", flush=True)
            try:
                s = _connect_tcp(host, port, timeout=5.0)
                print(" ✓")
                return s
            except OSError as e:
                print(f" ✗  ({e})")
                print(f"  Retry in {conn_config.reconnect_delay}s...")
                time.sleep(conn_config.reconnect_delay)

    sock = connect_loop()
    print("  Warte auf Messungen...\n")

    try:
        while True:
            # GeoCOM Request senden
            try:
                sock.settimeout(5.0)
                sock.sendall(GEOCOM_GET_LAST_GSI)
            except OSError as e:
                errors += 1
                logger.warning(f"Sendefehler ({errors}x): {e} — reconnecte...")
                try:
                    sock.close()
                except OSError:
                    pass
                time.sleep(conn_config.reconnect_delay)
                sock = connect_loop()
                print("  Warte auf Messungen...\n")
                continue

            # Antwort lesen
            try:
                raw = _read_available(sock, read_timeout=1.0)
            except ConnectionError as e:
                errors += 1
                logger.warning(f"Lesefehler ({errors}x): {e} — reconnecte...")
                try:
                    sock.close()
                except OSError:
                    pass
                time.sleep(conn_config.reconnect_delay)
                sock = connect_loop()
                print("  Warte auf Messungen...\n")
                continue

            errors = 0

            if raw:
                logger.debug(f"← {repr(raw)}")

            # Alle Zeilen prüfen
            for line in _extract_gsi_lines(raw):
                measurement = parse_gsi_response(line)
                if measurement is None:
                    continue

                # Duplikat-Schutz
                key = measurement_key(measurement)
                if key == last_key:
                    continue

                # Neue Messung!
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
        try:
            sock.close()
        except OSError:
            pass
