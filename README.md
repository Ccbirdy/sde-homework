# Lieferantenperformance auf Databricks

## Projekt

Dieses Projekt analysiert die Lieferleistung für die fiktive Nordbau Handels GmbH. Das Unternehmen handelt mit Baustoffen und hat vier Niederlassungen. Der Einkauf erfasst Liefertermine und Mengenabweichungen bisher manuell in Excel.

Ziel ist eine automatische monatliche Auswertung: **Welche drei Lieferanten sind die schwächsten, und warum?** Die Ergebnisse sollen Pünktlichkeit, Vollständigkeit, Fallzahlen und konkrete Bestellpositionen zeigen. Später sollen Rechnungen und Reklamationen ergänzt werden können.

**Stand:** Projektstruktur und erste lokale Datenprüfungen sind abgeschlossen. Pipeline, Kennzahlen und Asset Bundle sind noch nicht implementiert. Es gibt noch keine geprüfte Rangliste und keinen bestätigten Lauf in Databricks.

## Daten

Die Grundlage sind sieben synthetische SAP-Exporte aus dem bereitgestellten MM-Datenpaket. Die CSV-Dateien nutzen UTF-8 und Semikolon als Trennzeichen. Die Zeilenzahlen enthalten mögliche Duplikate, aber keine Kopfzeile.

| Tabelle | Inhalt | Zeilen |
|---|---|---:|
| LFA1 | Lieferantenstammdaten | 15 |
| MARA/MAKT | Materialstammdaten und Texte | 24 |
| EKKO | Bestellköpfe | 216 |
| EKPO | Bestellpositionen | 468 |
| EKET | Einteilungen zu Bestellpositionen | 820 |
| LIKP | Lieferköpfe | 957 |
| LIPS | Lieferpositionen | 963 |

Die Quelldateien bleiben unverändert. Lokal liegen sie unter `../DIC_Arbeitsprobe_Testdaten_SAP_MM/`. Erste Prüfungen zeigen doppelte mögliche Schlüssel in EKKO, EKPO, EKET und LIPS. Ursachen und Behandlung sind noch offen.

## Meilensteine

### M1 · Daten verstehen und Qualität prüfen

- [x] Projektstruktur anlegen und alle sieben CSV-Dateien lokal lesen.
- [x] Felder, Beispiele, Zeilenzahlen und doppelte mögliche Schlüssel lokal prüfen.
- [ ] Fehlende Werte, Datumswerte, Mengen, Einheiten und Verknüpfungen prüfen.
- [ ] Regeln für Duplikate und fehlerhafte Datensätze festlegen und umsetzen.

### M2 · Datenarchitektur und Datenmodell

- [ ] Datenfluss und Schichten kurz darstellen und die Auswahl begründen.
- [ ] Die Granularität jeder Zieltabelle, ihre Schlüssel und Beziehungen festlegen.
- [ ] Bestellpositionen, Einteilungen und Teillieferungen ohne doppelte Mengen verbinden.
- [ ] Niederlassungen im Modell berücksichtigen und die spätere Ergänzung von Rechnungen und Reklamationen beschreiben.

### M3 · Pipeline-Aufbau: Medallion-Architektur in Databricks

- [ ] Bronze: Quelldaten laden und Herkunft erhalten.
- [ ] Silver: Datentypen, Qualitätsregeln und geprüfte Beziehungen umsetzen.
- [ ] Gold: monatliche Kennzahlen nach Lieferant und Niederlassung mit nachvollziehbaren Details erstellen.
- [ ] Task-Reihenfolge, Berichtszeitraum und Fehlerbehandlung festlegen; wiederholte Läufe ohne doppelte Ergebnisse prüfen.
- [ ] Die Pipeline in Databricks ausführen und die Ergebnisse prüfen.

### M4 · Kennzahlen und Geschäftsergebnis

- [ ] Pünktlichkeit, Vollständigkeit, Teillieferungen und die Zuordnung zum Berichtsmonat definieren; die Granularität begründen.
- [ ] Rangfolge und Fallzahlen festlegen; Berechnungen anhand einzelner Bestellungen prüfen.
- [ ] Die drei schwächsten Lieferanten nennen und die Ursachen mit Details belegen.
- [ ] Annahmen und Grenzen der Ergebnisse erklären.

### M5 · Deployment und Zugriff

- [ ] Code, Jobs und Konfiguration als Databricks Asset Bundle (DAB) mit `dev` und `prod` bereitstellen.
- [ ] Einen monatlichen Job mit Parametern, Abhängigkeiten und Zeitzone konfigurieren.
- [ ] Bundle-Konfiguration prüfen; Deployment und Lauf im verfügbaren Workspace mit Ziel und Nachweis dokumentieren.
- [ ] Zugriff je Niederlassung vorbereiten: Zuordnung der Nutzer und Umsetzung der Zugriffsregeln beschreiben.
- [ ] Tatsächlich geprüfte Zugriffe von noch offenen Maßnahmen trennen; Start und Deployment dokumentieren.

### M6 · Skalierbarkeit, Optimierungen und Produktion

- [ ] Engpässe bei mehr Daten benennen und passende Verbesserungen begründen, zum Beispiel bei Ladevorgängen, Joins oder Speicherung.
- [ ] Das Vorgehen für neue und geänderte Daten erklären; die Grenzen des aktuellen Ansatzes nennen.
- [ ] Weitere Schritte für den Betrieb priorisieren, etwa Überwachung, Wiederanlauf, Berechtigungen und Tests.

### M7 · Dokumentation und Vorstellung

- [ ] Rückfragen, eigene Entscheidungen und Annahmen kurz dokumentieren.
- [ ] Lösung, Ergebnisse und zentrale Entscheidungen mit passenden Nachweisen präsentieren.
- [ ] Optional: einen Power-BI-Bericht erstellen, wenn er die Analyse unterstützt.

## Projektstruktur

```text
databricks/
  src/          Laden, Aufbereiten und Kennzahlen
  resources/    Job-Konfiguration
tests/          Prüfungen der Datenlogik und Ergebnisse
outputs/        Ergebnisse und Prüfnachweise
```

Die Verzeichnisse sind bisher Platzhalter. Anleitungen zum Ausführen und Deployment folgen mit der Implementierung.
