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

package de.bsvrz.dua.aggrlve;

import de.bsvrz.dav.daf.main.ClientDavInterface;
import de.bsvrz.dav.daf.main.config.SystemObject;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAInitialisierungsException;

/**
 * Speichert alle Aggregationsdaten eines Fahrstreifens bzw. eines Messquerschnitts in einem
 * Ringpuffer die zur Errechnung des jeweils nächsthoeheren Intervalls notwendig sind
 *
 * @author BitCtrl Systems GmbH, Thierfelder
 */
public class AggregationsPuffer extends AbstraktAggregationsPuffer {

	/**
	 * Standardkonstruktor.
	 *
	 * @param dav
	 *            Verbindung zum Datenverteiler
	 * @param obj
	 *            das Objekt, dessen Daten gepuffert werden sollen
	 * @param intervall
	 *            das Aggregationsintervall, fuer das Daten in diesem Puffer stehen (
	 *            <code>null</code> deutet auf messwertersetzte Fahstreifenwerte hin)
	 * @throws DUAInitialisierungsException
	 *             wenn dieses Objekt nicht vollstaendig initialisiert werden konnte
	 */
	public AggregationsPuffer(final ClientDavInterface dav, final SystemObject obj,
			final AggregationsIntervall intervall) throws DUAInitialisierungsException {
		super(dav, obj, intervall);
	}

	@Override
	protected long getMaxPufferInhalt() {
		return aggregationsIntervall.getMaxPufferGroesse() + 2;
	}

}
