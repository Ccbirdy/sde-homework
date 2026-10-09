# Lieferantenperformance auf Databricks

Monatliche Bewertung der Lieferanten für die fiktive Nordbau Handels GmbH mit vier Niederlassungen. Ziel: die drei schwächsten Lieferanten anhand von Pünktlichkeit und Vollständigkeit bestimmen und die Ergebnisse mit Bestell- und Lieferdaten belegen.

## Daten und Befunde

Sieben synthetische SAP-CSV-Exporte: Lieferanten, Materialien, Bestellköpfe und -positionen, Einteilungen sowie Lieferköpfe und -positionen.

Die lokale Prüfung zeigt doppelte Kandidatenschlüssel, unterschiedliche Datums- und Zahlenformate, fehlende Referenzen sowie Abweichungen bei Material und Einheit. Mehrere Einteilungen und Lieferpositionen je Bestellposition können bei direkten Joins Mengen vervielfachen. Die Kandidatenschlüssel und fachlichen Feldbedeutungen sind noch nicht bestätigt.

## Umsetzung

- [x] Lokales Notebook zur Datenqualität mit Prüfergebnissen.
- [x] Bundle-Grundkonfiguration mit `dev` und `prod`; Schema und Volume in `dev` bereitgestellt.
- [ ] Bronze-, Silver- und Gold-Verarbeitung sowie Kennzahlen und Rangliste.
- [ ] Validierung, Deployment und Lauf in Databricks.
- [ ] Zugriff nach Niederlassung.

Die Datenprüfung liegt lokal vor. Schema und Volume sind in `dev` bereitgestellt. Alle sieben CSV-Dateien liegen im Volume; Dateinamen und Größen stimmen mit den lokalen Dateien überein. Das Bronze-Notebook und ein Job mit Parametern sind in `dev` bereitgestellt, aber noch nicht ausgeführt. Silver, Gold und Lieferantenbewertung fehlen noch.

Die CSV-Dateien im Volume bilden die Landing Zone. Bronze lädt daraus Delta-Tabellen; Silver bereinigt die Daten; Gold berechnet die Kennzahlen. Entscheidungen und offene Regeln stehen in [DATA_RULES.md](DATA_RULES.md).

Ein GitHub-Actions-Workflow für die manuelle Bereitstellung nach `dev` ist vorbereitet. Die Anmeldung ist noch nicht eingerichtet; der Workflow wurde noch nicht ausgeführt.

## Aufbau

- `00_dataquality/`: Datenprüfung und lokale Ergebnisse.
- `databricks/`: Bundle-Konfiguration, Ressourcen und Bronze-Notebook.

## Offene Entscheidungen

Auswahl bei widersprüchlichen Datensätzen, Zuordnung von Teillieferungen, Einheitenumrechnung und Kennzahlendefinitionen. Die Quelldateien bleiben unverändert; fachliche Annahmen werden bei der Umsetzung dokumentiert.
