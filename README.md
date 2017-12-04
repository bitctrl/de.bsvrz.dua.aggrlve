[![Build Status](https://travis-ci.org/bitctrl/de.bsvrz.dua.aggrlve.svg?branch=develop)](https://travis-ci.org/bitctrl/de.bsvrz.dua.aggrlve)
[![Build Status](https://api.bintray.com/packages/bitctrl/maven/de.bsvrz.dua.aggrlve/images/download.svg)](https://bintray.com/bitctrl/maven/de.bsvrz.dua.aggrlve)

# Segment 4 Datenübernahme und Aufbereitung (DUA), SWE 4.9 Aggregation LVE

Version: ${version}

## Übersicht

Die SWE Aggregation LVE meldet sich auf alle messwertersetzten Kurzzeitdaten an
und berechnet aus diesen Daten für alle parametrierten Fahrstreifen und
Messquerschnitte die 1-, 5-, 15-, 30-, 60- Minutenwerte sowie Tageswerte und
DTV-Werte (Durchschnittliche Tagesverkehrswerte) je Monat und je Jahr (Details
siehe [AFo] bzw. [MARZ]).

## Bemerkungen

Diese SWE ist eine eigenständige Datenverteiler-Applikation, welche über die
Klasse de.bsvrz.dua.aggrlve.AggregationLVE mit folgenden Parametern gestartet
werden kann (zusaetzlich zu den normalen Parametern jeder
Datenverteiler-Applikation): "-KonfigurationsBereichsPid=pid(,pid)"


## Disclaimer

Segment 4 Datenübernahme und Aufbereitung (DUA), SWE 4.9 Aggregation LVE
Copyright (C) 2017 BitCtrl Systems GmbH

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
