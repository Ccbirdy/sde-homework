# Datenregeln und Annahmen

Stand: 09.10.2026. Rückfragen zur Bedeutung der Daten wurden gestellt. Eine Antwort liegt noch nicht vor. Fachliche Annahmen sind daher nicht vom Auftraggeber bestätigt.

## Schichten

| Schicht | Aufgabe | Stand |
|---|---|---|
| Landing Zone | Die sieben unveränderten CSV-Dateien im Volume `source_files` speichern. | Dateien hochgeladen. |
| Bronze | CSV-Dateien als Delta-Tabellen laden. Quellfelder als Zeichenketten behalten, Dateipfad und Ladezeit ergänzen. | In dev erfolgreich ausgeführt; Zeilenzahlen stimmen mit den CSV-Dateien überein. |
| Silver | Formate vereinheitlichen, Datentypen setzen und Datensätze mit Fehlern getrennt speichern. | Code und Job-Abhängigkeit vorhanden; noch nicht ausgeführt. |
| Gold | Monatliche Kennzahlen und eine Lieferantenrangliste berechnen. | Noch nicht umgesetzt. |

## Bisherige Entscheidungen

| Entscheidung | Grundlage | Auswirkung |
|---|---|---|
| Bronze übernimmt Quellfelder als Zeichenketten. | Nummern enthalten führende Nullen; Datums- und Zahlenformate unterscheiden sich. | Keine automatische Typumwandlung beim Laden. Leere CSV-Felder können als null gelesen werden. |
| Jeder Bronze-Lauf ersetzt den Tabelleninhalt. | Für diese Umsetzung behandeln wir die sieben Dateien als vollständigen Datenstand. Das ist eine Annahme. | Erneutes Laden hängt keine weiteren Zeilen an. Neue monatliche Exporte und historische Datenstände sind damit noch nicht geregelt. |
| Bronze behält Duplikate bei. | Doppelte Kandidatenschlüssel wurden gefunden; ihre Bedeutung ist ungeklärt. | Auswahl oder Ausschluss erfolgt erst nach einer dokumentierten Silver-Regel. |

## Silver-Regeln

Die folgenden Regeln sind Entscheidungen für diese Umsetzung. Sie sind nicht vom Auftraggeber bestätigt.

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
| Löschkennzeichen | Leeres LOEKZ als nicht gelöscht, L als gelöscht markieren; andere Werte isolieren. | Annahme. Gelöschte Positionen bleiben mit `_is_deleted=true` in Silver. Ihre Verwendung in Gold ist gesondert festzulegen. |

Pflichtfelder neben den Kandidatenschlüsseln: LFA1.NAME1; MARA.MEINS; EKKO.LIFNR/BEDAT; EKPO.MATNR/MENGE/MEINS/WERKS; EKET.EINDT/MENGE; LIKP.LIFNR/WADAT_IST/WERKS; LIPS.EBELN/EBELP/MATNR/LFIMG/MEINS/WERKS. Die Prüfung steht zentral in `specs` im Silver-Notebook. Fehlende Pflichtwerte führen zur Quarantäne. Die Pflicht für WADAT_IST ist eine Entscheidung für auswertbare Lieferungen, kein Nachweis seiner fachlichen Bedeutung.

Kandidatenschlüssel: LFA1: LIFNR; MARA: MATNR; EKKO: MANDT/EBELN; EKPO: MANDT/EBELN/EBELP; EKET: MANDT/EBELN/EBELP/ETENR; LIKP: MANDT/VBELN; LIPS: MANDT/VBELN/POSNR. Diese Schlüssel sind nicht vom Auftraggeber bestätigt.

## Silver-Ergebnisse

- Sieben `silver_*`-Tabellen enthalten Datensätze ohne die oben definierten Fehler. Sie sind noch keine fertige Bewertung und enthalten auch zukünftige Termine und gelöschte Positionen.
- `silver_quarantine` enthält Tabelle, Kandidatenschlüssel, Rohwerte, Fehlergründe, Herkunft und ursprüngliche Anzahl der isolierten Datensätze.
- `silver_load_summary` enthält je Tabelle die Bronze-Zeilen, entfernte identische Wiederholungen, Silver-Zeilen und Quarantäne-Zeilen. Es gilt: Bronze = entfernte Wiederholungen + Silver + Quarantäne. Mehrere Fehler können dieselbe Zeile betreffen.
- Jeder Lauf ersetzt die Ausgabetabellen. Die Tabellen werden einzeln geschrieben; ein Fehler kann einen teilweise aktualisierten Stand hinterlassen. Gold darf erst nach erfolgreichem Silver-Lauf starten.

Die Quarantäne kann die spätere Bewertung verändern, auch durch abhängige Datensätze. Gold muss deshalb die auswertbare Datenmenge und Ausschlüsse ausweisen. Ein fehlender auswertbarer Lieferdatensatz beweist noch keine Nichtlieferung.

## Noch offene Geschäftsregeln

Bedeutung von WADAT_IST als tatsächlichem Lieferdatum, Statistikstichtag, Umgang mit gelöschten Positionen in der Bewertung, Zuordnung von Teillieferungen, Toleranzen und Ranking. Silver führt noch keine Zuordnung von Lieferungen zu EKET-Zeilen und keine Summierung über diese Beziehungen aus.
