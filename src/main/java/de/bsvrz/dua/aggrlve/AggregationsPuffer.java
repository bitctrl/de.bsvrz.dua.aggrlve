/*
 * Segment Datenübernahme und Aufbereitung (DUA), SWE Aggregation LVE
 * Copyright (C) 2007 BitCtrl Systems GmbH
 * Copyright 2016 by Kappich Systemberatung Aachen
 * 
 * This file is part of de.bsvrz.dua.aggrlve.
 * 
 * de.bsvrz.dua.aggrlve is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * de.bsvrz.dua.aggrlve is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with de.bsvrz.dua.aggrlve.  If not, see <http://www.gnu.org/licenses/>.

 * Contact Information:
 * Kappich Systemberatung
 * Martin-Luther-Straße 14
 * 52062 Aachen, Germany
 * phone: +49 241 4090 436 
 * mail: <info@kappich.de>
 */

package de.bsvrz.dua.aggrlve;

import de.bsvrz.dav.daf.main.ClientDavInterface;
import de.bsvrz.dav.daf.main.config.SystemObject;

/**
 * Speichert alle Aggregationsdaten eines Fahrstreifens bzw. eines
 * Messquerschnitts in einem Ringpuffer die zur Errechnung des jeweils
 * nächsthoeheren Intervalls notwendig sind
 * 
 * @author BitCtrl Systems GmbH, Thierfelder
 */
public class AggregationsPuffer extends AbstraktAggregationsPuffer {

	private long _maxPufferGroesse;

	/**
	 * Standardkonstruktor.
	 * 
	 * @param dav
	 *            Verbindung zum Datenverteiler
	 * @param obj
	 *            das Objekt, dessen Daten gepuffert werden sollen
	 * @param intervall
	 *            das Aggregationsintervall, fuer das Daten in diesem Puffer
	 *            stehen (<code>null</code> deutet auf messwertersetzte
	 *            Fahstreifenwerte hin)
	 */
	public AggregationsPuffer(final ClientDavInterface dav, final SystemObject obj, final AggregationsIntervall intervall) {
		super(dav, obj);
		_maxPufferGroesse = intervall.getMaxPufferGroesse();
	}

	@Override
	protected long getMaxPufferInhalt() {
		return _maxPufferGroesse + 2;
	}

}
