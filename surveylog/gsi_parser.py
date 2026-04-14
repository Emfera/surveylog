"""
GSI-Parser für surveylog.

Das TS07 sendet Messdaten im GSI-Format (Geotechnisches Serial Interface).
Der Befehl %R1Q,2115: fordert den letzten Messsatz als GSI-String an.

GSI-Wort-Format (GSI-8):
  IIIUU+VVVVVVVV
  III = Word-Index (2-stellig + 1 Stelle Unterindex, z.B. 110 → Index 11)
  UU  = Einheit (2 Zeichen, z.B. 00, 06, ..)
  +/- = Vorzeichen
  VVVVVVVV = Wert (8 Zeichen, führende Nullen)

GSI-16 hat längere Wertefelder (16 Zeichen).

Relevante Word-Indizes (erste 2 Ziffern):
  11  → PID / Punktnummer (alphanumerisch)
  81  → Easting  (E / Rechtswert) in mm → /1000 = Meter
  82  → Northing (N / Hochwert)   in mm → /1000 = Meter
  83  → Height   (H / Höhe)       in mm → /1000 = Meter

Beispiel-Response vom TS07:
  %R1P,0,0:0,110001+00000FP1 81..00+00123456 82..00+00654321 83..00+00001234
"""

import re
from dataclasses import dataclass
from typing import Optional


@dataclass
class GSIMeasurement:
    """Eine geparste GSI-Messung vom TS07."""
    pid: str      # Punktnummer / PID
    e: float      # Easting  (Rechtswert) in Meter
    n: float      # Northing (Hochwert)   in Meter
    h: float      # Height   (Höhe)       in Meter
    raw: str      # Rohstring für Debugging


def _parse_gsi_words(payload: str) -> dict:
    """
    Parst GSI-Wörter aus dem Payload-Teil einer GSI-Response.

    GSI-Wörter sind durch Leerzeichen getrennt.
    Jedes Wort beginnt mit einer 2-stelligen Index-Zahl.

    Gibt {index_int: wert_string} zurück, wobei index_int
    die ersten 2 Ziffern des Word-Tokens sind.
    """
    words = {}
    for token in payload.split():
        # Token muss mit Ziffer beginnen und ein +/- enthalten
        if not token[0].isdigit():
            continue
        pm = token.find('+')
        if pm == -1:
            pm = token.find('-')
        if pm == -1:
            continue
        prefix = token[:pm]   # z.B. '110001' oder '81..00'
        value  = token[pm:]   # z.B. '+00000FP1' oder '+00123456'
        # Index = erste 2 Ziffern
        digits = re.match(r'(\d+)', prefix)
        if not digits:
            continue
        full_index = digits.group(1)
        index = int(full_index[:2])  # immer erste 2 Ziffern
        words[index] = value
    return words


def _gsi_to_float(value: str) -> Optional[float]:
    """
    Konvertiert einen GSI-Wert zu float (Meter).
    GSI speichert Koordinaten in mm (implizit 3 Nachkommastellen).
    Beispiel: '+00123456' → 123.456 m
    """
    try:
        sign = -1 if value.startswith('-') else 1
        raw = value.lstrip('+-').lstrip('0') or '0'
        return sign * int(raw) / 1000.0
    except (ValueError, TypeError):
        return None


def _gsi_to_pid(value: str) -> Optional[str]:
    """
    Extrahiert PID aus einem GSI Word-11-Wert.
    Entfernt Vorzeichen und führende Nullen.
    Beispiel: '+00000FP00010001' → 'FP00010001'
    """
    raw = value.lstrip('+-').lstrip('0')
    return raw if raw else None


def parse_gsi_response(response: str) -> Optional[GSIMeasurement]:
    """
    Parst eine vollständige GeoCOM/GSI-Response.

    Erwartet: %R1P,0,0:RC,<GSI-Payload>
    Gibt GSIMeasurement zurück oder None wenn kein gültiger Datensatz.
    """
    if not response or '%R1P' not in response:
        return None

    # Return-Code prüfen
    rc_match = re.search(r'%R1P,\d+,\d+:(\d+)', response)
    if rc_match:
        rc = int(rc_match.group(1))
        # Nur OK (0) und bekannte Warnings sind gültig
        if rc not in (0, 1283, 1284, 1285, 1288, 1289):
            return None

    # GSI-Payload extrahieren (nach RC)
    payload_match = re.search(r'%R1P,\d+,\d+:\d+,(.*)', response)
    if not payload_match:
        return None

    payload = payload_match.group(1).strip()
    if not payload:
        return None

    words = _parse_gsi_words(payload)

    # PID (Word-Index 11)
    pid_raw = words.get(11)
    if pid_raw is None:
        return None
    pid = _gsi_to_pid(pid_raw)
    if not pid:
        return None

    # Koordinaten (81=E, 82=N, 83=H)
    e_raw = words.get(81)
    n_raw = words.get(82)
    h_raw = words.get(83)

    if None in (e_raw, n_raw, h_raw):
        return None

    e = _gsi_to_float(e_raw)
    n = _gsi_to_float(n_raw)
    h = _gsi_to_float(h_raw)

    if None in (e, n, h):
        return None

    return GSIMeasurement(pid=pid, e=e, n=n, h=h, raw=response.strip())


def measurement_key(m: GSIMeasurement) -> str:
    """
    Eindeutiger Schlüssel für Duplikat-Erkennung.
    Gleiche Messung = gleicher PID + gleiche Koordinaten.
    """
    return f"{m.pid}:{m.e:.3f}:{m.n:.3f}:{m.h:.3f}"
