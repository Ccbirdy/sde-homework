# Lieferantenperformance auf Databricks

Monatliche Bewertung der Lieferanten für die fiktive Nordbau Handels GmbH mit vier Niederlassungen. Ziel: die drei schwächsten Lieferanten anhand von Pünktlichkeit und Vollständigkeit bestimmen und die Ergebnisse mit Bestell- und Lieferdaten belegen.

## Daten und Befunde

Sieben synthetische SAP-CSV-Exporte: Lieferanten, Materialien, Bestellköpfe und -positionen, Einteilungen sowie Lieferköpfe und -positionen.

Die lokale Prüfung zeigt doppelte Kandidatenschlüssel, unterschiedliche Datums- und Zahlenformate, fehlende Referenzen sowie Abweichungen bei Material und Einheit. Mehrere Einteilungen und Lieferpositionen je Bestellposition können bei direkten Joins Mengen vervielfachen. Die Kandidatenschlüssel und fachlichen Feldbedeutungen sind noch nicht bestätigt.

## Umsetzung

- [x] Lokales Notebook zur Datenqualität mit Prüfergebnissen.
- [x] Bundle-Grundkonfiguration mit `dev` und `prod`; Schema und Volume in `dev` bereitgestellt.
- [x] Bronze: sieben Delta-Tabellen in `dev` geladen.
- [ ] Silver, Gold, Kennzahlen und Rangliste.
- [x] Bronze-Job in Databricks bereitgestellt und ausgeführt.
- [ ] Zugriff nach Niederlassung.

Die Datenprüfung liegt lokal vor. Alle sieben CSV-Dateien wurden in `dev` als Bronze-Tabellen geladen. Die Zeilenzahlen stimmen mit den Quelldateien überein. Silver-Code und die Job-Abhängigkeit nach Bronze sind vorhanden, aber noch nicht ausgeführt. Silver trennt verwendbare Datensätze und Quarantäne mit Fehlergründen. Gold und Lieferantenbewertung fehlen noch.

Die CSV-Dateien im Volume bilden die Landing Zone. Bronze lädt daraus Delta-Tabellen; Silver bereinigt die Daten; Gold berechnet die Kennzahlen. Entscheidungen und offene Regeln stehen in [DATA_RULES.md](DATA_RULES.md).

Der GitHub-Actions-Workflow wurde einmal manuell ausgeführt. Die Auswahl zwischen `dev` und `prod` ist ergänzt, aber noch nicht ausgeführt. Beide Ziele verwenden denselben Workspace mit getrennten Schemas und Jobs.

## Aufbau

- `00_dataquality/`: Datenprüfung und lokale Ergebnisse.
- `databricks/`: Bundle-Konfiguration, Ressourcen sowie Bronze- und Silver-Notebook.

## Offene Entscheidungen

Zuordnung von Teillieferungen, fachliche Bestätigung der Annahmen und Kennzahlendefinitionen. Konflikte und ungeklärte Einheiten werden vorerst isoliert. Die Quelldateien bleiben unverändert.
