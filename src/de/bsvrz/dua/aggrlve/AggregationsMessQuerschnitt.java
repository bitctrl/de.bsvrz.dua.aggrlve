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

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import de.bsvrz.dav.daf.main.ClientDavInterface;
import de.bsvrz.dav.daf.main.DataDescription;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAInitialisierungsException;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;
import de.bsvrz.sys.funclib.bitctrl.dua.av.DAVObjektAnmeldung;
import de.bsvrz.sys.funclib.bitctrl.dua.lve.FahrStreifen;
import de.bsvrz.sys.funclib.bitctrl.dua.lve.MessQuerschnitt;

/**
 * TODO
 * 
 * @author BitCtrl Systems GmbH, Thierfelder
 *
 */
public class AggregationsMessQuerschnitt
extends AbstraktAggregationsObjekt{
	
	/**
	 * der hier betrachtete Messquerschnitt
	 */
	private MessQuerschnitt mq = null;
	
	/**
	 * Menge der Fahrstreifen, die an diesem
	 * Messquerschnitt konfiguriert sind
	 */
	private HashSet<AggregationsFahrStreifen> fsMenge = new HashSet<AggregationsFahrStreifen>();
	
	
	
	/**
	 * Standardkonstruktor
	 * 
	 * @param dav Verbindung zum Datenverteiler
	 * @param mq der Messquerschnitt dessen Aggregationsdaten ermittelt werden sollen
	 * @throws DUAInitialisierungsException wenn dieses Objekt nicht vollstaendig (mit
	 * allen Unterobjekten) initialisiert werden konnte
	 */
	public AggregationsMessQuerschnitt(final ClientDavInterface dav,
									   final MessQuerschnitt mq)
	throws DUAInitialisierungsException{
		super(dav);
		this.mq = mq;
		
		this.datenPuffer = new AggregationsPufferMenge(dav, mq.getSystemObject());
		Set<DAVObjektAnmeldung> anmeldungen = new TreeSet<DAVObjektAnmeldung>(); 
		for(AggregationsIntervall intervall:AggregationsIntervall.getInstanzen()){
			try {
				anmeldungen.add(
						new DAVObjektAnmeldung(
						mq.getSystemObject(), 
						new DataDescription(
								DAV.getDataModel().getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_MQ),
								intervall.getAspekt(),
								(short)0)));
			} catch (Exception e) {
				throw new DUAInitialisierungsException("Messquerschnitt " + mq //$NON-NLS-1$
						+ " konnte nicht initialisiert werden", e); //$NON-NLS-1$
			}
		}
		sender.modifiziereObjektAnmeldung(anmeldungen);
		
		for(FahrStreifen fs:mq.getFahrStreifen()){
			this.fsMenge.add(new AggregationsFahrStreifen(DAV, fs));
		}		
	}

	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void aggregiere(long zeitStempel,
						   AggregationsIntervall intervall) {
		/**
		 * Aggregiere alle Werte der untergeordneten Fahrstreifen
		 */
		for(AggregationsFahrStreifen fs:this.fsMenge){
			fs.aggregiere(zeitStempel, intervall);
		}
		
		// TODO
	}
	
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void finalize()
	throws Throwable {
		LOGGER.warning("Der MQ " + this.mq +  //$NON-NLS-1$
						" wird nicht mehr aggregiert"); //$NON-NLS-1$
	}
	

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return this.mq.toString();
	}
	
}
