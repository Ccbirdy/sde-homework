# Zugriff und Kosten

Stand: 10.10.2026. Kosten-Notebook und SQL-Abfragen sind vorbereitet, noch nicht ausgeführt. Zugriff nach Niederlassung ist ein Entwurf und noch nicht eingerichtet.

## Datenmodell und Niederlassungen

Gold enthält aktuell eine breite Tabelle je Bestellposition, Lieferbelege und Monatsaggregate. Es gibt noch kein Sternschema mit getrennten Dimensionstabellen. Gold kann sowohl Fakten und Dimensionen als auch fertige Berichtsaggregate enthalten.

Eine zusätzliche Schicht namens Platinum ist optional. Sie kann freigegebene Sichten für die Niederlassungen enthalten, ohne die Daten je Niederlassung zu kopieren. Der Name ist eine Projektkonvention und keine Sicherheitsfunktion.

Vorgesehene Zugriffskette:

1. Kontogruppen, zum Beispiel `branch_2100`, erhalten nur die für sie vorgesehenen Leserechte.
2. WERKS bestimmt die Niederlassung einer Zeile. Eine Zeilenfilterfunktion prüft die Kontogruppen des Abfragenden. Ohne passende Zuordnung gibt sie false zurück.
3. Bei ABAC markieren governed tags die betroffenen Tabellen und die WERKS-Spalte. Eine zentrale Policy wendet die Funktion an. Ein Tag allein filtert keine Zeilen.
4. Bei einer Lösung mit dynamischen Sichten erhalten Leser Zugriff auf die Sichten, nicht auf ungeschützte Basistabellen. Alternativ schützen ABAC-Policies direkt die freigegebenen Tabellen.

Zeilen mit `scope=ALL` enthalten Werte über alle Niederlassungen und bleiben der zentralen Auswertung vorbehalten. Ein Filter auf einem solchen Gesamtergebnis berechnet keine Kennzahlen je Niederlassung neu. Für Niederlassungen werden deshalb die vorhandenen `scope=BRANCH`-Ergebnisse verwendet. Auch Detailtabellen und alternative Zugriffswege müssen geschützt werden. Ein Notebook-Parameter `branch` ist nur ein Anzeigefilter.

Offen: echte Kontogruppen, Zugriffsrechte, Verfügbarkeit von governed tags/ABAC und die Freigabe zentraler Berichte. Es wurden keine Gruppen, Grants, Policies oder Platinum-Tabellen angelegt.

Quellen: [ABAC-Grundlagen](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/core-concepts), [ABAC und Tabellenfilter](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/abac-vs-rls-cm).

## Zuordnung von Kosten

Der Job erhält die Tags `project=supplier_performance`, `environment=dev/prod` und `cost_center=supplier_analytics`. Die Kostenstelle ist eine Bundle-Variable. Diese Tags beschreiben die Ressource; sie ersetzen keine Serverless usage policy.

Für Serverless-Abrechnungstags muss ein berechtigter Administrator eine usage policy erstellen. Die Policy sollte dieselben drei Tags enthalten und für den jeweiligen Workspace und die ausführende Identität nutzbar sein. Für unterschiedliche Umgebungen sind getrennte Policies sinnvoll. Danach die kommentierte Variable `serverless_usage_policy_id` in `databricks.yml` mit der echten ID aktivieren und in der Job-YAML `budget_policy_id` aktivieren. IDs bei Bedarf je Target überschreiben. Ohne Policy-ID wurde keine Bindung eingerichtet.

Die Ausführungsidentität wird bereits in `system.billing.usage.identity_metadata.run_as` erfasst. Sie ist nicht unbedingt die Person, die den Job ausgelöst hat. Bei einem gemeinsamen Service Principal erscheint dieser für mehrere Benutzer. Für eine Zuordnung zum Auslöser wären zusätzlich Audit- oder Job-Run-Daten notwendig. Ein frei eingegebener Benutzerparameter ist dafür kein verlässlicher Nachweis.

Quelle: [Serverless usage policies](https://docs.databricks.com/aws/en/admin/usage/budget-policies).

## Kosten-Notebook

`databricks/src/04_cost_management.py` ist eine interaktive Notebook-Übersicht mit Tagesdiagramm, Budget-Simulation, Ressourcen-, Identitäts- und Job-Run-Tabellen. Es ist kein bereits veröffentlichtes AI/BI-Dashboard. Die enthaltene Abfrage `LIVE_SQL` kann als Datensatz für ein solches Dashboard verwendet werden.

- `data_mode=demo`: erfundene Daten und Preise; keine Billing-Berechtigung nötig. Die Notebook-Ausführung selbst benötigt weiterhin Compute.
- `data_mode=live`: system.billing.usage und system.billing.list_prices. Leserechte auf beide Tabellen sowie USE CATALOG/USE SCHEMA sind erforderlich.
- `start_date` / `end_date`: inklusive Grenzen, höchstens 366 Tage. `period_budget` gilt für genau diesen Zeitraum in der gewählten Währung.
- `workspace_id`: Workspace begrenzen. `project` und `job_id` sind ODER-Filter. Beide leeren, um alle verfügbaren Workspace-Kosten zu sehen. Die voreingestellte Job-ID gehört zum bestehenden dev-Job.
- `compute_reduction_pct`: hypothetische Verringerung des Compute-Kostenanteils. Kein Abschalten von Ressourcen und keine garantierte Einsparung.

Die Schätzung verwendet zeitlich passende Listenpreise und berücksichtigt negative Korrekturen. Fehlende Preise werden gezählt. Verbrauchseinheiten werden getrennt summiert. Vertragsrabatte und separate Cloud-Rechnungen sind nicht enthalten. Die Übersicht misst Kosten, keine CPU-Auslastung. Keine Daten bedeutet nicht automatisch keine Kosten. Zugriff auf Kosten- und Identitätsdaten ist getrennt von den Lieferantenberichten zu vergeben.

## Ergebnisabfragen

`databricks/src/05_result_queries.sql` enthält Basisprüfungen für eindeutige Positionsschlüssel, KPI-Werte und Summen sowie Abfragen für Rangliste, Lieferanten, Bestellpositionen, Lieferbelege und Qualität. Parameter: `catalog`, `schema_name`, `report_month`, `branch`; optional `po_number` und `po_item`. Nummern mit führenden Nullen eingeben. Die Abfragen lesen vorhandene Gold-Ergebnisse und ändern keine Tabellen. PASS/CHECK sind Abfrageergebnisse; ein grüner Job-Status allein bestätigt keine fachlich korrekten Daten.

Der separate Job `supplier_performance_review` führt das Ergebnis-Notebook manuell aus. Er startet keine Verarbeitung und hat keinen Zeitplan. Gold muss vorher erfolgreich gelaufen sein; während eines laufenden Gold-Updates können Teilstände sichtbar sein. Das Kosten-Notebook bleibt separat und wird keinem Job hinzugefügt.
