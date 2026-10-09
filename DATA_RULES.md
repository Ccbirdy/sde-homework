# Datenregeln und Annahmen

Stand: 09.10.2026. Rückfragen zur Bedeutung der Daten wurden gestellt. Eine Antwort liegt noch nicht vor. Fachliche Annahmen sind daher nicht vom Auftraggeber bestätigt.

## Schichten

| Schicht | Aufgabe | Stand |
|---|---|---|
| Landing Zone | Die sieben unveränderten CSV-Dateien im Volume `source_files` speichern. | Dateien hochgeladen. |
| Bronze | CSV-Dateien als Delta-Tabellen laden. Quellfelder als Zeichenketten behalten, Dateipfad und Ladezeit ergänzen. | In dev erfolgreich ausgeführt; Zeilenzahlen stimmen mit den CSV-Dateien überein. |
| Silver | Formate vereinheitlichen, Datentypen setzen und Datensätze mit Fehlern getrennt speichern. | Laut Rückmeldung in dev ausgeführt; fachliche Sichtung der Quarantäne noch offen. |
| Gold | Monatliche Kennzahlen und eine Lieferantenrangliste berechnen. | Code und Job-Abhängigkeit vorhanden; noch nicht ausgeführt. |

## Bisherige Entscheidungen

| Entscheidung | Grundlage | Auswirkung |
|---|---|---|
| Bronze übernimmt Quellfelder als Zeichenketten. | Nummern enthalten führende Nullen; Datums- und Zahlenformate unterscheiden sich. | Keine automatische Typumwandlung beim Laden. Leere CSV-Felder können als null gelesen werden. |
| Jeder Bronze-Lauf ersetzt den Tabelleninhalt. | Für diese Umsetzung behandeln wir die sieben Dateien als vollständigen Datenstand. Das ist eine Annahme. | Erneutes Laden hängt keine weiteren Zeilen an. Neue monatliche Exporte und historische Datenstände sind damit noch nicht geregelt. |
| Bronze behält Duplikate bei. | Doppelte Kandidatenschlüssel wurden gefunden; ihre Bedeutung ist ungeklärt. | Auswahl oder Ausschluss erfolgt erst nach einer dokumentierten Silver-Regel. |

## Silver-Regeln

Die folgenden Regeln gelten für diese Umsetzung. DATE und Nummern als Zeichenketten wurden am 09.10.2026 vom Projektbearbeiter gewählt. Die übrigen Regeln bleiben vorläufig. Keine Regel ist damit vom Auftraggeber bestätigt.

| Thema | Verarbeitung | Grenze oder Auswirkung |
|---|---|---|
| Text und Nummern | Äußere Leerzeichen entfernen, leere Texte als null speichern. Nummern bleiben Zeichenketten. MEINS, WAERS, LAND1 und LOEKZ werden großgeschrieben. | Führende Nullen bleiben erhalten. Rohwerte stehen in `_raw_record`. |
| Datum | `yyyy-MM-dd` und `dd.MM.yyyy` als DATE lesen. | Diese Formate sind in den bisherigen Ergebnissen sichtbar. Andere oder ungültige Werte führen zur Quarantäne. |
| Zahl | Punkt oder Komma als Dezimalzeichen; Speicherung als DECIMAL(38,6). | Annahme: keine Tausendertrennzeichen. Gemischte Trennzeichen, mehr als sechs Nachkommastellen und Überläufe führen zur Quarantäne. |
| Mengen und Beträge | Negative Werte isolieren; MENGE und LFIMG müssen größer als null sein. | Negative Werte werden nicht ohne Nachweis als Retouren behandelt. Fehlende optionale Preise und WEMNG bleiben null. |
| Identische Datensätze | Zeilen mit identischen ursprünglichen Quellfeldern zusammenfassen, unabhängig von Ladezeit und Dateipfad. | `_source_occurrences` erhält die Anzahl; `_source_files` erhält die Herkunft. Bronze bleibt unverändert. |
| Konflikte | Nach der Textbereinigung alle Varianten eines mehrfach belegten Kandidatenschlüssels isolieren. | Keine willkürliche Auswahl eines vermeintlich neuesten Datensatzes. Auch reine Formatunterschiede können zur Quarantäne führen. |
| Referenzen | Nur gegen eindeutige, gültige Silver-Eltern prüfen. | `UNRESOLVED_REFERENCE` bedeutet: kein nutzbarer Elternsatz. Dieser kann in Bronze fehlen oder selbst isoliert sein. |
| Einheiten | EKPO.MEINS mit MARA.MEINS vergleichen; LIPS.MEINS mit EKPO.MEINS vergleichen. Abweichungen isolieren. | Keine unbelegte Umrechnung. Abweichende Bestell- und Basiseinheiten können fachlich korrekt sein; ohne Faktor werden sie hier ausgeschlossen. |
| Material und Niederlassung | LIPS.MATNR und WERKS mit EKPO vergleichen; LIPS.WERKS zusätzlich mit LIKP vergleichen. | Konflikte isolieren. WERKS bleibt ein Code; keine erfundene Zuordnung zu Städten. |
| Lieferant | EKKO und LIKP gegen LFA1 prüfen. Bei LIPS müssen Lieferant aus Bestellung und Lieferung übereinstimmen. | Annahme: LFA1 und MARA gelten für alle MANDT-Werte dieses Datensatzes, da sie kein MANDT enthalten. |
| Löschkennzeichen | Leeres LOEKZ als nicht gelöscht, L als gelöscht markieren; andere Werte isolieren. | Annahme. Gelöschte Positionen bleiben mit `_is_deleted=true` in Silver und werden in Gold nicht bewertet. |

Pflichtfelder neben den Kandidatenschlüsseln: LFA1.NAME1; MARA.MEINS; EKKO.LIFNR/BEDAT; EKPO.MATNR/MENGE/MEINS/WERKS; EKET.EINDT/MENGE; LIKP.LIFNR/WADAT_IST/WERKS; LIPS.EBELN/EBELP/MATNR/LFIMG/MEINS/WERKS. Die Prüfung steht zentral in `specs` im Silver-Notebook. Fehlende Pflichtwerte führen zur Quarantäne. Die Pflicht für WADAT_IST ist eine Entscheidung für auswertbare Lieferungen, kein Nachweis seiner fachlichen Bedeutung.

Kandidatenschlüssel: LFA1: LIFNR; MARA: MATNR; EKKO: MANDT/EBELN; EKPO: MANDT/EBELN/EBELP; EKET: MANDT/EBELN/EBELP/ETENR; LIKP: MANDT/VBELN; LIPS: MANDT/VBELN/POSNR. Diese Schlüssel sind nicht vom Auftraggeber bestätigt.

## Silver-Ergebnisse

- Sieben `silver_*`-Tabellen enthalten Datensätze ohne die oben definierten Fehler. Sie sind noch keine fertige Bewertung und enthalten auch zukünftige Termine und gelöschte Positionen.
- `silver_quarantine` enthält Tabelle, Kandidatenschlüssel, Rohwerte, Fehlergründe, Herkunft und ursprüngliche Anzahl der isolierten Datensätze.
- `silver_load_summary` enthält je Tabelle die Bronze-Zeilen, entfernte identische Wiederholungen, Silver-Zeilen und Quarantäne-Zeilen. Es gilt: Bronze = entfernte Wiederholungen + Silver + Quarantäne. Mehrere Fehler können dieselbe Zeile betreffen.
- Jeder Lauf ersetzt die Ausgabetabellen. Die Tabellen werden einzeln geschrieben; ein Fehler kann einen teilweise aktualisierten Stand hinterlassen. Gold darf erst nach erfolgreichem Silver-Lauf starten.

Die Quarantäne kann die spätere Bewertung verändern, auch durch abhängige Datensätze. Gold muss deshalb die auswertbare Datenmenge und Ausschlüsse ausweisen. Ein fehlender auswertbarer Lieferdatensatz beweist noch keine Nichtlieferung.

## Gold-Regeln: vorläufige Annahmen

Diese Regeln wurden für einen ersten ausführbaren Entwurf gewählt. Sie sind noch nicht als fachliche Entscheidungen bestätigt. Gold startet im Job erst nach erfolgreichem Silver-Lauf.

| Thema | Regel | Grenze |
|---|---|---|
| Stichtag | Widget `as_of_date`, zunächst 2026-09-30. Nur Monate mit Monatsende bis zum Stichtag bewerten. | Demo-Annahme, kein bestätigter Extraktstichtag. Der Eingabestand muss die Lieferungen bis zu diesem Datum vollständig enthalten. |
| Granularität | Eine Bestellposition je MANDT/EBELN/EBELP. Der letzte gültige EKET.EINDT bestimmt den Berichtsmonat. | Frühere Teiltermine werden nicht separat bewertet. Das ist keine Bewertung jeder Einteilung. |
| Sollmenge | EKPO.MENGE; Summe der EKET.MENGE muss exakt übereinstimmen. | Abweichungen ausschließen, nicht automatisch korrigieren. Keine Mengentoleranz. |
| Ist-Lieferung | LIPS.LFIMG mit LIKP.WADAT_IST als Liefermenge und Lieferdatum verwenden. | WADAT_IST kann einen Warenausgang statt des Eingangs beim Kunden bezeichnen. Die fachliche Bedeutung bleibt offen. WEMNG wird nicht als zusätzliche Lieferung addiert. |
| Teil- und Überlieferungen | Gültige Lieferpositionen je Bestellposition bis zum jeweiligen Datum summieren. Überlieferungen gelten als vollständig; Erfüllungsgrad maximal 1. | Keine Zuordnung zu einzelnen EKET-Zeilen. Bestellmengen und Einteilungen werden vor dem Join zusammengefasst; dadurch kein Mehrfachzählen. |
| Pünktlichkeit | Bis einschließlich letztem Plantermin gelieferte Menge mindestens Sollmenge. | Null Tage Toleranz; auch verspätete vollständige Lieferungen bleiben unpünktlich. |
| Vollständigkeit | Bis einschließlich Ende des Berichtsmonats gelieferte Menge mindestens Sollmenge. | Spätere Lieferungen verbessern frühere Monatswerte nicht. Nachlieferungen bleiben in den Belegen sichtbar. |
| Ausschlüsse | Gelöschte oder ungültige Positionen, fehlende gültige Einteilungen, Mengenabweichungen, Termine oder Lieferungen vor BEDAT sowie zuordenbare EKPO/EKET/LIPS-Quarantäne ausschließen. | Fehlerhafte LIKP-Datensätze führen bereits in Silver zur Quarantäne abhängiger LIPS-Zeilen. Ausschlüsse erhalten keine KPI-Werte. |
| Keine Lieferzeile | Bei ansonsten auswertbarer Position und ohne zuordenbare Quarantäne wird Menge 0 verwendet. | Gilt nur unter der Annahme eines vollständigen Exports. Nicht zuordenbare Quarantäne bleibt eine zusätzliche Unsicherheit. |
| Monatliche Quoten | Anteil pünktlicher bzw. vollständiger Positionen an allen auswertbaren Positionen. Zusätzlich mittlerer, bei 1 begrenzter Mengenerfüllungsgrad je Position. | Jede Position hat gleiches Gewicht. Keine Addition von Mengen verschiedener Einheiten und keine Gewichtung nach Bestellwert. |
| Ranking | Score = (Pünktlichkeitsquote + Vollständigkeitsquote) / 2; aufsteigend sortieren. | Bei Gleichstand: niedrigere Pünktlichkeit, niedrigere Vollständigkeit, mehr bewertete Positionen, dann LIFNR. Letzteres ist nur eine stabile Sortierung, kein Leistungsunterschied. |
| Mindestmenge | Widget `min_items`, für die Demo zunächst 1 bewertbare Position pro Lieferant, Monat und Auswertungsebene. | Kleine Stichproben sind wenig belastbar. Der Wert kann erhöht werden. Weniger als drei geeignete Lieferanten ergeben weniger als drei Rangplätze. |
| Niederlassungen | `scope=ALL` über alle Werke; `scope=BRANCH` je WERKS. Jeweils neu aus den Positionen berechnen. | Keine erfundene Zuordnung zu Städten. Die Spalte allein erzwingt noch keine Zugriffsrechte. |

Quoten und Score liegen zwischen 0 und 1. `coverage_rate` umfasst nur Positionen, denen ein gültiger Lieferant und ein Berichtsmonat zugeordnet werden können. Sie ist keine Vollständigkeitsquote des gesamten Exports. Die Grundmenge aller unterschiedlichen Bronze-EKPO-Schlüssel bleibt in der Positionstabelle sichtbar, auch ohne Monatszuordnung. Bei teilweise isolierten Einteilungen ist die Monatszuordnung einer ausgeschlossenen Position nur vorläufig. Ausschlussgründe können sich überschneiden.

### Gold-Ausgaben

| Tabelle | Inhalt |
|---|---|
| `gold_order_item_performance` | Bestellposition, Lieferant, Werk, Soll-/Ist-Mengen, Termine, KPI-Werte und Ausschlussgründe. |
| `gold_delivery_evidence` | Gültige Lieferpositionen mit Belegnummer, Datum und Menge, einschließlich späterer Lieferungen; Kennzeichen zur zeitlichen Einordnung und Bewertbarkeit. |
| `gold_supplier_monthly` | Quoten, Score, Stichprobengröße und Abdeckung je Lieferant/Monat, insgesamt und je Werk. |
| `gold_worst_suppliers` | Bis zu drei schwächste geeignete Lieferanten pro Monat und Auswertungsebene. |
| `gold_quality_summary` | Auswertbare Positionen und Ausschlüsse je Grund; zusätzlich Anzahl nicht zuordenbarer Quarantänedatensätze. |
| `gold_unassigned_quarantine` | EKPO/EKET/LIPS-Quarantäne ohne passenden Bronze-Bestellpositionsschlüssel, mit Rohwerten und Gründen. |

Alle Gold-Tabellen werden je Lauf ersetzt. `_as_of_date` dokumentiert den gewählten Stichtag; `_processed_at` den Schreibzeitpunkt. Es gibt keine gemeinsame Transaktion über alle Tabellen und noch keine Historisierung mehrerer Extrakte.

## Entscheidungsstatus

Eine implementierte Regel ist noch keine fachliche Bestätigung. Nach Sichtung der Quarantäne werden Änderungen mit Datum, Grundlage und Auswirkung dokumentiert.

| Thema | Status | Nächster Schritt |
|---|---|---|
| DATE und Nummern als STRING mit führenden Nullen | Eigene Entscheidung, 09.10.2026 | Quellformate unterscheiden sich; Nummern dienen als Kennungen. Vorhandene Silver-Logik bleibt unverändert. DATE hat kein gespeichertes Anzeigeformat; für Textausgaben yyyy-MM-dd verwenden. |
| Übrige Formatumwandlung, Duplikate und Quarantäne | Vorläufig implementiert | Ergebnisse sichten und Regel je nach Befund beibehalten oder ändern. |
| LOEKZ, Einheiten und Pflicht für WADAT_IST | Vorläufige Annahmen | Fachliche Bedeutung klären; die aktuelle Verarbeitung ist keine Kundenbestätigung. |
| Lieferdatum, Stichtag, Teillieferungen und Ranking | Gold-Entwurf vom 09.10.2026, noch nicht ausgeführt | Annahmen oben prüfen; eigene Entscheidung oder tatsächliche Kundenantwort anschließend festhalten. |

Für neue Entscheidungen erfassen wir: Datum, betroffene Regel, Datenbefund, Entscheidung, Herkunft (`eigene Entscheidung`, `Rückfrage offen` oder `vom Auftraggeber bestätigt`) und Auswirkung auf Code und Auswertung. Kundenbestätigungen werden nur mit tatsächlicher Antwort dokumentiert.
