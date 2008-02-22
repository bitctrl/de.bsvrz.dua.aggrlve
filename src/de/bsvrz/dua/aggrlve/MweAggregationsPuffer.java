/**
 * Segment 4 Datenübernahme und Aufbereitung (DUA), SWE 4.9 Aggregation LVE
 * Copyright (C) 2007 BitCtrl Systems GmbH 
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

import com.bitctrl.Constants;

import de.bsvrz.dav.daf.main.ClientDavInterface;
import de.bsvrz.dav.daf.main.Dataset;
import de.bsvrz.dav.daf.main.config.SystemObject;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAInitialisierungsException;

/**
 * Speichert alle messwerterstetzten Fahrstreifendaten eines Fahrstreifens der
 * vergangenen Stunde in einem Ringpuffer
 * 
 * @author BitCtrl Systems GmbH, Thierfelder
 *
 */
public class MweAggregationsPuffer
extends AbstraktAggregationsPuffer{

	/**
	 * aktuelle maximale Kapazitaet des Ringpuffers
	 */
	private long maxPufferAktuell = 61;


	/**
	 * Standardkonstruktor
	 * 
	 * @param dav Verbindung zum Datenverteiler
	 * @param obj das Objekt (Fahrstreifen), dessen Daten gepuffert werden sollen
	 * @throws DUAInitialisierungsException wenn dieses Objekt nicht
	 * vollstaendig initialisiert werden konnte
	 */
	public MweAggregationsPuffer(ClientDavInterface dav, SystemObject obj)
			throws DUAInitialisierungsException {
		super(dav, obj, null);
	}

	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void aktualisiere(Dataset resultat) {
		super.aktualisiere(resultat);
		if(resultat.getData() != null){
			/**
			 * hat sich das Erfassungsintervall geaendert?
			 */
			synchronized (this) {
				if(this.ringPuffer.getLast().getT() != 
				   this.ringPuffer.getFirst().getT()){
					AggregationsDatum erstesDatum = this.ringPuffer.getFirst();
					this.ringPuffer.clear();
					this.ringPuffer.add(erstesDatum);
				}
			}
			
			double T = this.ringPuffer.getFirst().getT();
			this.maxPufferAktuell =
				Math.round(Math.max(1.0, (double)Constants.MILLIS_PER_HOUR / T)) + 5;
		}
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	protected long getMaxPufferInhalt() {
		return this.maxPufferAktuell;
	}

}
