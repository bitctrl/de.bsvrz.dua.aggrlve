******************************************************************************
*  Segment 4 Datenübernahme und Aufbereitung (DUA), SWE 4.9 Aggregation LVE  *
******************************************************************************

Version: ${version}


Übersicht
=========

Die SWE Aggregation LVE meldet sich auf alle messwertersetzten Kurzzeitdaten an
und berechnet aus diesen Daten für alle parametrierten Fahrstreifen und
Messquerschnitte die 1-, 5-, 15-, 30-, 60- Minutenwerte sowie Tageswerte und
DTV-Werte (Durchschnittliche Tagesverkehrswerte) je Monat und je Jahr (Details
siehe [AFo] bzw. [MARZ]).


Versionsgeschichte
==================

1.4.0
- Umstellung auf Java 8 und UTF-8

1.3.1
- Kompatibilität zu DuA-2.0 hergestellt

1.3.0
- Umstellung auf Funclib-Bitctrl-Dua

1.2.0
- Umstellung auf Maven-Build

Version 1.1.5

  - BUGFIX: Die Attributgruppe atg.messQuerschnittVirtuellStandard wurde nicht
    korrekt ausgelesen, so das unter Umständen fehlerhafte Empfängeranmeldungen
    am Datenverteiler durchgeführt wurden.


Version 1.1.4

  - Normalerweise findet die Berechnung von Aggregationsgroessen immer 30
    Sekunden nach der vollen Minute statt. Dieser Wert wurde in einen optionalen
    Parameter (-offset) umgewandelt. Standardwert ist 30s. Durch die Angabe
    dieses Parameters können auch (maximal 55s) verspaetete Datensaetze
    behandelt werden. 


Version 1.1.3

  - BUGFIX #2924: Memory-Leak fuer virtuelle MQs mit mindestens einem Unter-MQ,
    der keine Daten liefert, entfernt. 

  
Version 1.1.2

  - Potentielles Synchronisationsproblem bei der Datenerfassung für VMQ behoben.


Version 1.1.1

  - Mögliches Memory-Leak, wenn keine Aggregation für virtuelle MQ gebildet
    werden konnte.
  - Synchronisationsproblem bei der Datenerfassung für MQ behoben


Version 1.1.0

  - Support für virtuelle MQ


Version 1.0.9

  - BUGFIX: SWE kann nicht mehr mit NoSuchElementException abstuerzen.


Version 1.0.8
 
  - BUGFIX: Sämtliche Konstruktoren DataDescription(atg, asp, sim) ersetzt durch
    DataDescription(atg, asp)


Version 1.0.7
 
  - Potentielle NPE beim Ersetzen von fehlenden Werten, wenn "leere" Datensätze
    verarbeitet werden sollen.


Version 1.0.6
 
  - Leere Einträge in einem Aggregationsdatensatz werden ignoriert.


Version 1.0.5

  - Die Güte von (hier via Mittelwertbildung) interpolierten Fahrstreifendaten
    wird mit einem Faktor (Parameter "gueteFaktor") gewichtet.


Version 1.0.3

  - Der Versuch einen 60-Minuten Puffer fuer Fahrstreifen zu fuellen wird
    ignoriert (vorher Exception). Fuer Fahrstreifen gibt es keine Aggregation
    auf hoeheren Stufen als 60 Min (im Ggs. zu MQs).


Version 1.0.2

  - Fehlerausgabe-Patch


Version 1.0.1

  - Zwei Error-Messages zu Laufzeitfehlern umgebaut. Die Fehler sollten zum
    kontrollierten Absturz des System führen, welcher sowieso nach diesem Fehler
    stattfinden würde.


Version 1.0.0

  - Erste vollständige Auslieferung


Version 1.0.0b

  - Erste Auslieferung (beta, nur teilweise nach Prüfspezifikation getestet)


Bemerkungen
===========

Diese SWE ist eine eigenständige Datenverteiler-Applikation, welche über die
Klasse de.bsvrz.dua.aggrlve.AggregationLVE mit folgenden Parametern gestartet
werden kann (zusaetzlich zu den normalen Parametern jeder
Datenverteiler-Applikation):

	-KonfigurationsBereichsPid=pid(,pid)


Tests:
------

Die automatischen Tests, die in Zusammenhang mit der Prüfspezifikation
durchgeführt werden, sind noch nicht endgültig implementiert (bis auf Tests für
TV- und DTV-Werte). Für die Tests wird eine Verbindung zum Datenverteiler mit
einer Konfiguration mit dem Testkonfigurationsbereich
"kb.objekteTestUnterzentraleK2S_100_MessQuerschnitte" benötigt.	Die Verbindung
wird über die statische Variable CON_DATA der Klasse
de.bsvrz.dua.aggrlve.AggregationLVETest hergestellt. Die Testdaten befinden sich
im Verzeichnis extra.

	/**
	 * Verbindungsdaten
	 */
	public static final String[] CON_DATA = new String[] {
			"-datenverteiler=localhost:8083", //$NON-NLS-1$
			"-benutzer=Tester", //$NON-NLS-1$
			"-authentifizierung=c:\\passwd"}; //$NON-NLS-1$

Das Wurzelverzeichnis mit den Testdaten (csv-Dateien) muss ebenfalls innerhalb
dieser Datei verlinkt sein:

	/**
	 * Wurzelverzeichnis der Testdaten
	 */
	public static final String WURZEL = "...\\de.bsvrz.dua.aggrlve\\extra\\"; //$NON-NLS-1$


Logging-Hierarchie (Wann wird welche Art von Logging-Meldung produziert?):

ERROR:

  - DUAInitialisierungsException --> Beendigung der Applikation
  - Fehler beim An- oder Abmelden von Daten beim Datenverteiler
  - Interne unerwartete Fehler

WARNING:

  - Fehler, die die Funktionalität grundsätzlich nicht beeinträchtigen, aber zum
    Datenverlust führen können
  - Nicht identifizierbare Konfigurationsbereiche
  - Probleme beim Explorieren von Attributpfaden (von
    Plausibilisierungsbeschreibungen)
  - Wenn mehrere Objekte eines Typs vorliegen, von dem nur eine Instanz erwartet
    wird
  - Wenn Parameter nicht korrekt ausgelesen werden konnten bzw. nicht
    interpretierbar sind
  - Wenn inkompatible Parameter übergeben wurden
  - Wenn Parameter unvollständig sind
  - Wenn ein Wert bzw. Status nicht gesetzt werden konnte

INFO:

  - Wenn neue Parameter empfangen wurden

CONFIG:

  - Allgemeine Ausgaben, welche die Konfiguration betreffen
  - Benutzte Konfigurationsbereiche der Applikation bzw. einzelner Funktionen
    innerhalb der Applikation
  - Benutzte Objekte für Parametersteuerung von Applikationen (z.B. die Instanz
    der Datenflusssteuerung, die verwendet wird)
  - An- und Abmeldungen von Daten beim Datenverteiler

FINE:

  - Wenn Daten empfangen wurden, die nicht weiterverarbeitet (plausibilisiert)
    werden können (weil keine Parameter vorliegen)
  - Informationen, die nur zum Debugging interessant sind


Disclaimer
==========

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


Kontakt
=======

BitCtrl Systems GmbH
Weißenfelser Straße 67
04229 Leipzig
Phone: +49 341-490670
mailto: info@bitctrl.de
