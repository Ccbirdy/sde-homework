# Lieferantenperformance auf Databricks

Monatliche Bewertung der Lieferanten für die fiktive Nordbau Handels GmbH mit vier Niederlassungen. Ziel: die drei schwächsten Lieferanten anhand von Pünktlichkeit und Vollständigkeit bestimmen und die Ergebnisse mit Bestell- und Lieferdaten belegen.

## Daten und Befunde

Sieben synthetische SAP-CSV-Exporte: Lieferanten, Materialien, Bestellköpfe und -positionen, Einteilungen sowie Lieferköpfe und -positionen.

Die lokale Prüfung zeigt doppelte Kandidatenschlüssel, unterschiedliche Datums- und Zahlenformate, fehlende Referenzen sowie Abweichungen bei Material und Einheit. Mehrere Einteilungen und Lieferpositionen je Bestellposition können bei direkten Joins Mengen vervielfachen. Die Kandidatenschlüssel und fachlichen Feldbedeutungen sind noch nicht bestätigt.

## Umsetzung

- [x] Lokales Notebook zur Datenqualität mit Prüfergebnissen.
- [x] Bundle-Grundkonfiguration mit `dev` und `prod`; Schema und Volume in `dev` bereitgestellt.
- [x] Bronze: sieben Delta-Tabellen in `dev` geladen.
- [x] Silver: Bereinigung und Quarantäne; Ausführung in dev gemeldet.
- [ ] Gold: Code für Monatskennzahlen und Rangliste vorhanden; Ausführung steht aus.
- [x] Bronze und Silver als abhängige Job-Tasks bereitgestellt.
- [ ] Zugriff nach Niederlassung.

Alle sieben CSV-Dateien wurden in `dev` als Bronze-Tabellen geladen. Die Zeilenzahlen stimmen mit den Quelldateien überein. Silver trennt verwendbare Datensätze und Quarantäne mit Fehlergründen. Die fachliche Sichtung der Quarantäne ist noch offen. Der Job ist im Code um Gold nach Silver erweitert; diese Erweiterung ist noch nicht ausgeführt.

Die CSV-Dateien im Volume bilden die Landing Zone. Bronze lädt daraus Delta-Tabellen; Silver bereinigt die Daten; Gold berechnet die Kennzahlen. Entscheidungen und offene Regeln stehen in [DATA_RULES.md](DATA_RULES.md).

GitHub Actions stellt den gewählten Branch per Bundle bereit. Das Ziel `dev` oder `prod` ist auswählbar. Beide Ziele verwenden denselben Workspace mit getrennten Schemas und Jobs. Der Workflow startet keinen Datenlauf. Der Databricks-Zeitplan ist pausiert.

Gold bewertet Bestellpositionen im Monat ihres letzten Plantermins. Pünktlichkeit misst die vollständige Lieferung bis zu diesem Termin, Vollständigkeit die Lieferung bis Monatsende. Der Score gewichtet beide Quoten gleich. Ergebnisse liegen insgesamt und je WERKS vor, mit Stichprobengröße, Ausschlüssen und Lieferbelegen. Einzelne Teiltermine werden nicht bewertet. Widgets steuern den Stichtag (Demo-Annahme: 2026-09-30) und die Mindestanzahl bewertbarer Positionen für das Ranking (zunächst 1). Die Annahmen sind noch nicht fachlich bestätigt.

## Aufbau

- `databricks/src/`: lokale Datenprüfung (`00_dataquality.ipynb`) sowie Bronze-, Silver- und Gold-Notebook.
- `databricks/resources/`: Schema, Volume und Job als Bundle-Ressourcen.

## Offene Entscheidungen

Ein separates Kosten-Notebook bietet simulierte Daten und Abfragen gegen die Billing-Systemtabellen. Ressourcen-Tags sind im Job konfiguriert; eine Serverless usage policy ist noch nicht gebunden. SQL-Abfragen für die Gold-Ergebnisse sind ebenfalls vorbereitet. Zugriffsentwurf und Betriebsgrenzen stehen in [OPERATIONS.md](OPERATIONS.md). Die neuen Notebooks sind noch nicht ausgeführt.

Bedeutung des Lieferdatums, Vollständigkeit des Exports, Bewertung einzelner Teiltermine sowie Bestätigung der Kennzahlenregeln. Konflikte und ungeklärte Einheiten werden vorerst isoliert. Ausgeschlossene Daten begrenzen die Lieferantenbewertung; fehlende gültige Lieferungen allein beweisen keine Nichtlieferung. Die Quelldateien bleiben unverändert.
