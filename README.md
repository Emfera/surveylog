# surveylog

**Sensorunabhängige Vermessungs-Datenpipeline für archäologische Feldarbeit - proof of concept und Spielwiese**

Entwickelt für den Einsatz mit Leica TS07 (und anderen Totalstationen mit GeoCOM-Protokoll) sowie GNSS-CSV-Exporten. Läuft auf **Android/Termux**, Linux und Windows — ohne QGIS oder andere GUI-Abhängigkeiten.

---

## Schnellstart

### Android (Termux)

```bash
# 1. surveylog installieren
pip install surveylog

# 2. BT/TCP Bridge App starten (Bluetooth SPP → TCP)
#    Device A: TS3359283 (Totalstation) → Connected
#    Device B: TCP server 127.0.0.1:4444 → Ready

# 3. Punkte aufnehmen
surveylog collect --port tcp://localhost:4444 --db feldarbeit.db

# 4. GeoPackage erzeugen
surveylog build feldarbeit.db ausgabe.gpkg
```

### Linux/Desktop

```bash
pip install surveylog

# Mit rfcomm (Bluetooth gebunden)
surveylog collect --port /dev/rfcomm0 --db feldarbeit.db
surveylog build feldarbeit.db ausgabe.gpkg
```

---

## Installation

### Aus GitHub

```bash
pip install git+https://github.com/Emfera/surveylog.git
```

### Aus Quellcode

```bash
git clone https://github.com/Emfera/surveylog.git
cd surveylog
pip install -e .
```

---

## Bluetooth-Einrichtung (Android)

Da Android keinen direkten seriellen Bluetooth-Zugriff aus Termux erlaubt, wird eine **TCP-Bridge App** benötigt:

### Benötigte Apps (alle von F-Droid oder Play Store)

| App | Zweck |
|---|---|
| **Termux** (F-Droid) | Terminal + Python |
| **BT/TCP Bridge** (Play Store) | Bluetooth → TCP Brücke |

### BT/TCP Bridge einrichten

1. App öffnen
2. **Device A** → "Select task" → "Classic Bluetooth" → TS3359283 wählen
3. **Device B** → "Select task" → "TCP server" → Port `4444`
4. Status zeigt: **Ready**
5. App im Hintergrund lassen, zu Termux wechseln

### Totalstation einrichten (Leica TS07)

- Einstellungen → Verbind. → Schnittstelle → Port = **Bluetooth**
- Android-Handy mit TS07 koppeln (PIN: 0000)

---

## Befehle

| Befehl | Beschreibung |
|---|---|
| `surveylog collect` | Live-Aufnahme via GeoCOM (Totalstation) |
| `surveylog import` | GNSS-CSV-Datei importieren |
| `surveylog build` | GeoPackage (.gpkg) erstellen |
| `surveylog info` | Statistik der Staging-Datenbank |
| `surveylog validate` | PID-Sequenzen auf Lücken prüfen |
| `surveylog codes` | Alle verfügbaren Codes anzeigen |

### collect — Live-Aufnahme

```bash
surveylog collect --port tcp://localhost:4444 --db feldarbeit.db
```

Interaktive Eingabe:
```
  PID: FP00010001   → misst und speichert Fundpunkt 1
  PID:              → wiederholt FP00010002 (automatisch +1)
  PID: WA00010001   → misst Maueranfang
  PID: quit         → beendet
```

Optionen:
```
--port    Verbindungsport (Standard: tcp://localhost:4444)
--db      Staging-Datenbank (Standard: surveylog.db)
--timeout Verbindungs-Timeout in Sekunden (Standard: 5.0)
--wait    Wartezeit nach Messung in Sekunden (Standard: 0.8)
--prism   Mit Reflektor messen (Standard: reflektorlos)
```

### import — CSV importieren

```bash
surveylog import punkte.csv --db feldarbeit.db
```

Unterstützte Spaltenbezeichnungen (Groß-/Kleinschreibung egal):

| Koordinate | Akzeptierte Namen |
|---|---|
| X (Ost) | `x`, `east`, `easting`, `rechtswert`, `e` |
| Y (Nord) | `y`, `north`, `northing`, `nordwert`, `n` |
| Z (Höhe) | `z`, `height`, `elevation`, `hoehe`, `h`, `alt` |
| PID | `pid`, `id`, `point_id`, `punktnummer`, `name` |

### build — GeoPackage erstellen

```bash
surveylog build feldarbeit.db ausgabe.gpkg
surveylog build feldarbeit.db ausgabe.gpkg --crs 31256  # MGI Austria GK-M34
```

---

## PID-Format

```
CCSSSSNNNN
││└───┴───── NNNN: Sequenznummer (0001–9999)
│└─────────── SSSS: Feature-Nummer (0001–9999)
└──────────── CC: Code-Kürzel (2 Buchstaben)
```

Beispiele:

| PID | Bedeutung |
|---|---|
| `FP00010001` | Fundpunkt, Feature 1, Punkt 1 |
| `WA00030001` | Mauer, Feature 3, Punkt 1 |
| `GR00020001` | Grab, Feature 2, Punkt 1 |

---

## Code-Katalog

### Punkte
| Code | Deutsch | Englisch |
|---|---|---|
| `HP` | Höhenpunkt | Height Point |
| `FP` | Fundpunkt | Find Point |
| `PR` | Probe | Sample |
| `PH` | Pfostenloch | Post Hole |
| `SK` | Skelett | Skeleton |

### Linien
| Code | Deutsch | Englisch |
|---|---|---|
| `WA` | Mauer | Wall |
| `DI` | Graben | Ditch |
| `RD` | Straße | Road |

### Flächen
| Code | Deutsch | Englisch |
|---|---|---|
| `BF` | Befund | Feature Area |
| `GR` | Grab | Grave |
| `PO` | Grube | Pit |

Alle Codes: `surveylog codes`

---

## GeoCOM-Protokoll (Hintergrundinformation)

surveylog kommuniziert mit der Totalstation über das GeoCOM-ASCII-Protokoll:

```
Messung auslösen:     %R1Q,2008:11,1   → %R1P,0,0:0
Koordinaten abfragen: %R1Q,2082:0,1,0  → %R1P,0,0:0,<X>,<Y>,<Z>,...
```

Modus 11 = reflektorlos (TMC_DIST_REFL_LESS)

---

## Ausgabe: GeoPackage

Das .gpkg enthält separate Layer:

| Layer | Geometrie | Codes |
|---|---|---|
| `points` | PointZ | HP, FP, PR, PH, SK, … |
| `lines` | LineStringZ | WA, DI, RD, … (≥2 Punkte) |
| `polygons` | PolygonZ | BF, GR, PO, … (≥3 Punkte) |

Direkt in QGIS, ArcGIS und anderen GIS-Programmen öffenbar.

---

## Abhängigkeiten

| Paket | Zweck |
|---|---|
| `pyserial` | Serielle Kommunikation (Linux/Windows) |
| `click` | Kommandozeilen-Interface |

Kein GDAL, kein QGIS — funktioniert auf Android/Termux.

---

## Lizenz

MIT — Martin Fera, Universität Wien (martin.fera@univie.ac.at)
