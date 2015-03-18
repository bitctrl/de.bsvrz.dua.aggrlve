/*
 * Segment 4 Datenübernahme und Aufbereitung (DUA), SWE 4.9 Aggregation LVE
 * Copyright (C) 2007-2015 BitCtrl Systems GmbH
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software
 * Foundation; either version 2 of the License, or (at your option) any later
 * version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc., 51
 * Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
 *
 * Contact Information:<br>
 * BitCtrl Systems GmbH<br>
 * Weißenfelser Straße 67<br>
 * 04229 Leipzig<br>
 * Phone: +49 341-490670<br>
 * mailto: info@bitctrl.de
 */
package de.bsvrz.dua.aggrlve.vmq;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import de.bsvrz.dav.daf.main.ResultData;
import de.bsvrz.dav.daf.main.config.Aspect;

/**
 * Klasse zum Speichern der Daten eines MQ, der ein Bestandteil eines VMQ ist.
 *
 * @author BitCtrl Systems GmbH, Uwe Peuker
 * @version $Id$
 */
public class VmqDataPart {

	/** der Anteil des MQ am virtuellen Messquerschnitt. */
	private final double anteil;
	/** die Liste der für den MQ empfangenen Daten nach Aspekten geordnet. */
	private final Map<Aspect, SortedMap<Long, ResultData>> dataList = new HashMap<Aspect, SortedMap<Long, ResultData>>();

	/**
	 * Konstruktor.
	 *
	 * @param anteil
	 *            der Anteil des MQ am VMQ
	 */
	public VmqDataPart(final double anteil) {
		this.anteil = anteil;
	}

	/** Standardkonstruktor mit Anteil 1 - 100 Prozent. */
	public VmqDataPart() {
		this(1.0);
	}

	/**
	 * fügt einen Ergebnisdatensatz hinzu.
	 *
	 * @param result
	 *            der Datensatz
	 */
	public void push(final ResultData result) {
		if (result.hasData()) {
			synchronized (dataList) {
				final Aspect aspect = result.getDataDescription().getAspect();
				SortedMap<Long, ResultData> map = dataList.get(aspect);
				if (map == null) {
					map = new TreeMap<Long, ResultData>();
					dataList.put(aspect, map);
				}
				if (map.size() > 3) {
					map.clear();
				}
				map.put(result.getDataTime(), result);
			}
		}
	}

	/**
	 * liefert den nächsten verfügbaren noch nicht verarbeiteten Wert ( nach Zeit geordnet).
	 *
	 * @param aspect
	 *            der Aspekt für den ein Wert bestimmt werden soll
	 * @return der Wert oder <code>null</code>
	 */
	public ResultData getNextValue(final Aspect aspect) {

		synchronized (dataList) {
			final SortedMap<Long, ResultData> map = dataList.get(aspect);
			if ((map != null) && (!map.isEmpty())) {
				return map.get(map.firstKey());
			}
		}
		return null;
	}

	/**
	 * löscht für den angegebenen Aspekte alle Werte, die nicht jünger als der angebene Zeitstempel
	 * sind.
	 *
	 * @param dataTime
	 *            der Zeitstempel
	 * @param aspect
	 *            der Aspekt
	 */
	public void clear(final long dataTime, final Aspect aspect) {

		synchronized (dataList) {
			final SortedMap<Long, ResultData> map = dataList.get(aspect);
			if (map != null) {
				while (!map.isEmpty()) {
					final Long time = map.firstKey();
					if ((time != null) && (time <= dataTime)) {
						map.remove(time);
					} else {
						break;
					}
				}
			}
		}
	}

	/**
	 * liefert den Anteil des MQ am VMQ.
	 *
	 * @return der Anteil
	 */
	public double getAnteil() {
		return anteil;
	}
}
