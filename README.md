[![Build Status](https://travis-ci.org/bitctrl/de.bsvrz.dua.aggrlve.svg?branch=master)](https://travis-ci.org/bitctrl/de.bsvrz.dua.aggrlve)
[![Build Status](https://api.bintray.com/packages/bitctrl/maven/de.bsvrz.dua.aggrlve/images/download.svg)](https://bintray.com/bitctrl/maven/de.bsvrz.dua.aggrlve)

# Segment 4 Datenübernahme und Aufbereitung (DUA), SWE 4.9 Aggregation LVE

Version: ${version}

## Übersicht

Die SWE Aggregation LVE meldet sich auf alle messwertersetzten Kurzzeitdaten an
und berechnet aus diesen Daten für alle parametrierten Fahrstreifen und
Messquerschnitte die 1-, 5-, 15-, 30-, 60- Minutenwerte sowie Tageswerte und
DTV-Werte (Durchschnittliche Tagesverkehrswerte) je Monat und je Jahr (Details
siehe [AFo] bzw. [MARZ]).

## Versionsgeschichte

### Version 2.0.3

Release-Datum: 23.11.2016

- Applikationsname für MessageSender entsprechend NERZ-Vorgabe gesetzt
- Potentielle NPE in den Logausgaben
- Korrektur der Berechnung der (D)TV-Werte

### Version 2.0.2

Release-Datum: 28.07.2016

de.bsvrz.dua.aggrlve.AggregationsDatum

- überschreibt equals und nicht hashCode. Da equals nur die Supermethode aufruft, wurde die 
  Funktion hier entfernt

de.bsvrz.dua.aggrlve.AggregationsIntervall

- hashCode-Funktion entsprechend equals ergänzt

de.bsvrz.dua.aggrlve.AggregationsAttributWert:

- unnötige equals-Funktion entfernt, da hashCode gefehlt hat und Daten der Klasse von
  außen geändert werden können

de.bsvrz.dua.aggrlve.tests.AggrLveTestBase

- der Member "_aggregationLVE" sollte nicht statisch sein, der er bei jedem Test neu initialisiert wird

- Obsolete SVN-Tags aus Kommentaren entfernt
- Obsolete inheritDoc-Kommentare entfernt

### Version 2.0.1

Release-Datum: 22.07.2016

- Umpacketierung gemäß NERZ-Konvention
  
### Version 2.0.0

Release-Datum: 31.05.2016

#### Neue Abhängigkeiten

Die SWE benötigt nun das Distributionspaket de.bsvrz.sys.funclib.bitctrl.dua
in Mindestversion 1.5.0 und de.bsvrz.sys.funclib.bitctrl in Mindestversion 1.4.0,
sowie de.bsvrz.dua.dalve in Version 2.0.0.

#### Änderungen

Folgende Änderungen gegenüber vorhergehenden Versionen wurden durchgeführt:

- Die Aggregationsberechnungen wurden gemäß den neuen Anwenderforderungen
  überarbeitet.
- Die SWE führt nun keine implizite Ausfallüberwachung mehr durch, d.h. auch
  (stark) verspätet eintreffende Datensätze werden aggregiert. Daten werden nun
  sofort aggregiert und publiziert sobald alle benötigten Quelldaten eingetroffen sind,
  es wird keine feste Zeitdauer mehr gewartet. Die Systemuhr ist damit komplett
  unabhängig von den empfangenen Datenzeitstempeln.
- Die Aggregation von Daten von virtuellen Messquerschnitten basiert jetzt auf den
  Analysewerten des virtuellen Messquerschnitts und nicht mehr auf den aggregierten
  Messquerschnitts-/Fahrstreifendaten.
- Für Fahrstreifen und virtuelle Messquerschnitte basieren die aggregierten Daten
  jetzt auf der nächstniedrigeren Aggregationsstufe, bzw. auf den Analysedaten, falls
  es keine nächstniedrigere Stufe mehr gibt. Bisher basierten die Fahrstreifendaten
  auf den messwertersetzten Werten und die Daten der virtuellen MQ auf den aggregierten
  MQ-Werten.
- Ausgefallene Datensätze werden nun besser behandelt, die Zustände Nicht ermittelbar
  und Nicht ermittelbar/fehlerhaft werden nun entsprechend den Anwenderforderungen
  gesetzt, falls Datensätze ausgefallen sind.
- Die Güte wird bei ausgefallenen Datensätzen im Intervall nun wie gefordert reduziert.
- Fahrstreifenwerte werden nun auch aggregiert, wenn der Fahrstreifen sich in keinem
  MQ befindet.
- Die SWE reagiert besser, wenn sich das Erfassungsintervall im laufenden Betrieb
  ändert.
- Die SWE erkennt jetzt auch ausgefallene Intervallwerte, die ohne leeren Datensatz
  empfangen werden. Also beispielsweise wenn zwei Datensätzen mit Erfassungsintervall
  1 Minute sich um mehr als eine Minute in der Datenzeit unterscheiden, aber
  es keinen dazwischenliegenden leeren Datensatz gibt, der einen Ausfall anzeigen
  würde.
- Die Ermittlung der anzumeldenden Fahrstreifen, virtuellen MQ und MQ ist jetzt
  robuster, wenn sich bspw. die Fahrstreifen in einem anderen Bereich befinden als
  die MQ.
- Die SWE benötigt jetzt als neue Abhängigkeit, dass das Distributionspaket der
  Datenaufbereitung LVE (in gleicher Version) installiert ist, da für die Berechnung
  der Verkehrsdichten und Verkehrsstärken sowie die Bestimmung der Erfassungsintervalle
  von VMQ auf gemeinsame Funktionalität zurückgegriffen wird.
- Das Verhalten bei der Berechnung von MQ-Werten wurde verändert, wenn ein Teil
  der Fahrstreifen ausgefallen ist. Ein Messquerschnitt wartet bei der Aggregation
  jetzt so lange auf eintreffende Fahrstreifen-Daten, bis entweder alle benötigten
  Fahrstreifen aktuelle Daten geliefert haben, oder bis der erste Fahrstreifen Daten
  für das folgende Intervall liefert. Für Intervalle, in denen Fahrstreifen-Daten fehlen,
  werden die verbleibenden Daten (bei entsprechend reduzierter Güte und gesetztem
  Interpoliert-Flag) nun normal weiter aggregiert.
- Die SWE stellt aufgrund der oben genannten Änderungen andere Anforderungen
  auf die Archivierung. Daten von (regulären) MQ müssen nicht mehr archiviert
  werden, da die MQ-Werte auf Basis der Fahrstreifen gebildet werden. Stattdessen
  sollten die Stunden-, Tages-, Monats- und Jahreswerte der Fahrstreifen und der
  virtuellen MQ archiviert werden.
- Die Berechnungszzeitpunkte für Tages, Monats und Jahreswerte wurden entsprechend
  Anwenderforderungen angepasst.
- Aggregationsdaten werden nicht mehr für Stufen gebildet, die unter dem Erfassungsintervall
  liegen. D.h. wenn FS-Daten mit Erfassungsintervall = 2 Minuten
  empfangen werden, werden keine 1-Minuten-Aggregationswerte mehr publiziert.

### Version 1.4.0

- Umstellung auf Java 8 und UTF-8

### Version 1.3.1

- Kompatibilität zu DuA-2.0 hergestellt

### Version 1.3.0

- Umstellung auf Funclib-Bitctrl-Dua

### Version 1.2.0

- Umstellung auf Maven-Build

### Version 1.1.5

  - BUGFIX: Die Attributgruppe atg.messQuerschnittVirtuellStandard wurde nicht
    korrekt ausgelesen, so das unter Umständen fehlerhafte Empfängeranmeldungen
    am Datenverteiler durchgeführt wurden.


### Version 1.1.4

  - Normalerweise findet die Berechnung von Aggregationsgroessen immer 30
    Sekunden nach der vollen Minute statt. Dieser Wert wurde in einen optionalen
    Parameter (-offset) umgewandelt. Standardwert ist 30s. Durch die Angabe
    dieses Parameters können auch (maximal 55s) verspaetete Datensaetze
    behandelt werden. 


### Version 1.1.3

  - BUGFIX #2924: Memory-Leak fuer virtuelle MQs mit mindestens einem Unter-MQ,
    der keine Daten liefert, entfernt. 

  
### Version 1.1.2

  - Potentielles Synchronisationsproblem bei der Datenerfassung für VMQ behoben.

### Version 1.1.1

  - Mögliches Memory-Leak, wenn keine Aggregation für virtuelle MQ gebildet
    werden konnte.
  - Synchronisationsproblem bei der Datenerfassung für MQ behoben

### Version 1.1.0

  - Support für virtuelle MQ

### Version 1.0.9

  - BUGFIX: SWE kann nicht mehr mit NoSuchElementException abstuerzen.

### Version 1.0.8
 
  - BUGFIX: Sämtliche Konstruktoren DataDescription(atg, asp, sim) ersetzt durch
    DataDescription(atg, asp)

### Version 1.0.7
 
  - Potentielle NPE beim Ersetzen von fehlenden Werten, wenn "leere" Datensätze
    verarbeitet werden sollen.

### Version 1.0.6
 
  - Leere Einträge in einem Aggregationsdatensatz werden ignoriert.

### Version 1.0.5

  - Die Güte von (hier via Mittelwertbildung) interpolierten Fahrstreifendaten
    wird mit einem Faktor (Parameter "gueteFaktor") gewichtet.

### Version 1.0.3

  - Der Versuch einen 60-Minuten Puffer fuer Fahrstreifen zu fuellen wird
    ignoriert (vorher Exception). Fuer Fahrstreifen gibt es keine Aggregation
    auf hoeheren Stufen als 60 Min (im Ggs. zu MQs).

### Version 1.0.2

  - Fehlerausgabe-Patch

### Version 1.0.1

  - Zwei Error-Messages zu Laufzeitfehlern umgebaut. Die Fehler sollten zum
    kontrollierten Absturz des System führen, welcher sowieso nach diesem Fehler
    stattfinden würde.

### Version 1.0.0

  - Erste vollständige Auslieferung

### Version 1.0.0b

  - Erste Auslieferung (beta, nur teilweise nach Prüfspezifikation getestet)


## Bemerkungen

Diese SWE ist eine eigenständige Datenverteiler-Applikation, welche über die
Klasse de.bsvrz.dua.aggrlve.AggregationLVE mit folgenden Parametern gestartet
werden kann (zusaetzlich zu den normalen Parametern jeder
Datenverteiler-Applikation): "-KonfigurationsBereichsPid=pid(,pid)"


## Disclaimer

Segment 4 Datenübernahme und Aufbereitung (DUA), SWE 4.9 Aggregation LVE
Copyright (C) 2007 BitCtrl Systems GmbH

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; either version 2 of the License, or (at your option) any later
version.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc., 51
Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.


## Kontakt

BitCtrl Systems GmbH
Weißenfelser Straße 67
04229 Leipzig
Phone: +49 341-490670
mailto: info@bitctrl.de
