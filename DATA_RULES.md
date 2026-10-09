# Datenregeln und Annahmen

Stand: 09.10.2026. Rückfragen zur Bedeutung der Daten wurden gestellt. Eine Antwort liegt noch nicht vor. Fachliche Annahmen sind daher nicht vom Auftraggeber bestätigt.

## Schichten

| Schicht | Aufgabe | Stand |
|---|---|---|
| Landing Zone | Die sieben unveränderten CSV-Dateien im Volume `source_files` speichern. | Dateien hochgeladen. |
| Bronze | CSV-Dateien als Delta-Tabellen laden. Quellfelder als Zeichenketten behalten, Dateipfad und Ladezeit ergänzen. | Notebook und Job in dev bereitgestellt, noch nicht ausgeführt. |
| Silver | Formate vereinheitlichen, Datentypen setzen und Regeln für Duplikate und ungültige Datensätze anwenden. | Regeln noch offen. |
| Gold | Monatliche Kennzahlen und eine Lieferantenrangliste berechnen. | Noch nicht umgesetzt. |

## Bisherige Entscheidungen

| Entscheidung | Grundlage | Auswirkung |
|---|---|---|
| Bronze übernimmt Quellfelder als Zeichenketten. | Nummern enthalten führende Nullen; Datums- und Zahlenformate unterscheiden sich. | Keine automatische Typumwandlung beim Laden. Leere CSV-Felder können als null gelesen werden. |
| Jeder Bronze-Lauf ersetzt den Tabelleninhalt. | Für diese Umsetzung behandeln wir die sieben Dateien als vollständigen Datenstand. Das ist eine Annahme. | Erneutes Laden hängt keine weiteren Zeilen an. Neue monatliche Exporte und historische Datenstände sind damit noch nicht geregelt. |
| Bronze behält Duplikate bei. | Doppelte Kandidatenschlüssel wurden gefunden; ihre Bedeutung ist ungeklärt. | Auswahl oder Ausschluss erfolgt erst nach einer dokumentierten Silver-Regel. |

## Offene Silver-Regeln

| Thema | Noch festzulegen |
|---|---|
| Datum und Zahl | Zulässige Quellformate und Behandlung nicht lesbarer Werte. |
| Duplikate | Behandlung identischer Zeilen und widersprüchlicher Datensätze mit gleichem Kandidatenschlüssel. |
| Fehlende Referenzen | Umgang mit Datensätzen ohne passenden Auftrag, Lieferanten oder Material. |
| Mengen und Einheiten | Umgang mit unterschiedlichen Einheiten; keine Umrechnung ohne begründeten Faktor. |
| Fachliche Bedeutung | Bedeutung von `WADAT_IST`, Löschkennzeichen und Zuordnung von `WERKS` zu Niederlassungen. |

Für jede festgelegte Regel ergänzen wir: Annahme, Grundlage, Verarbeitung und Auswirkung auf die Kennzahlen. Offene Regeln werden nicht als umgesetzt dargestellt.
