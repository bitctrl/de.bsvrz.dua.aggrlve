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

import de.bsvrz.dav.daf.main.Data;
import de.bsvrz.dav.daf.main.config.SystemObject;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;

/**
 * Factory zum Anlegen von Instanzen für die Verwaltung der Aggregationsdaten von virtuellen MQ.
 *
 * @author BitCtrl Systems GmbH, Uwe Peuker
 * @version $Id$
 */
public final class AggregationsVmqFactory {

	/** privater Konstruktor. */
	private AggregationsVmqFactory() {
		// es werden keine Instanzen der Factory benötigt
	}

	/**
	 * erzeugt eine Instanz zur Kombination der Aggregationsdaten eines virtuellen
	 * Messquertschnitts.
	 *
	 * @param obj
	 *            das zu Grunde liegende Systemobjekt aus der Datenverteilerkonfiguration
	 * @return das Objekt oder null, wenn der übergebene Systemobjekttyp nicht unterstützt wird
	 */
	public static AbstractAggregationsVmq create(final SystemObject obj) {

		AbstractAggregationsVmq result = null;

		Data data = obj.getConfigurationData(obj.getDataModel().getAttributeGroup(
				DUAKonstanten.ATG_MQ_VIRTUELL_STANDARD));
		if (data != null) {
			result = new StandardAggregationsVmq(obj, data);
		} else {
			data = obj.getConfigurationData(obj.getDataModel().getAttributeGroup(
					DUAKonstanten.ATG_MQ_VIRTUELL_V_LAGE));
			if (data != null) {
				result = new VLageAggregationsVmq(obj, data);
			}
		}

		return result;
	}
}
