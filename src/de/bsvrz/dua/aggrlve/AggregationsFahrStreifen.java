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

import java.util.Set;
import java.util.TreeSet;

import de.bsvrz.dav.daf.main.ClientDavInterface;
import de.bsvrz.dav.daf.main.ClientReceiverInterface;
import de.bsvrz.dav.daf.main.DataDescription;
import de.bsvrz.dav.daf.main.ReceiveOptions;
import de.bsvrz.dav.daf.main.ReceiverRole;
import de.bsvrz.dav.daf.main.ResultData;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAInitialisierungsException;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;
import de.bsvrz.sys.funclib.bitctrl.dua.av.DAVObjektAnmeldung;
import de.bsvrz.sys.funclib.bitctrl.dua.lve.FahrStreifen;

/**
 * TODO
 * 
 * @author BitCtrl Systems GmbH, Thierfelder
 *
 */
public class AggregationsFahrStreifen
extends AbstraktAggregationsObjekt
implements ClientReceiverInterface{
	
	/**
	 * der hier betrachtete Fahrstreifen
	 */
	private FahrStreifen fs = null;
	
	
	/**
	 * Standardkonstruktor
	 * 
	 * @param dav Verbindung zum Datenverteiler
	 * @param fs der Fahrstreifen dessen Aggregationsdaten ermittelt werden sollen
	 * @throws DUAInitialisierungsException wenn dieses Objekt nicht vollstaendig (mit
	 * allen Unterobjekten) initialisiert werden konnte
	 */
	public AggregationsFahrStreifen(final ClientDavInterface dav,
									final FahrStreifen fs)
	throws DUAInitialisierungsException{
		super(dav);
		this.fs = fs;
		
		Set<DAVObjektAnmeldung> anmeldungen = new TreeSet<DAVObjektAnmeldung>(); 
		for(AggregationsIntervall intervall:AggregationsIntervall.getInstanzen()){
			try {
				anmeldungen.add(
						new DAVObjektAnmeldung(
						fs.getSystemObject(), 
						new DataDescription(
								DAV.getDataModel().getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_FS),
								intervall.getAspekt(),
								(short)0)));
			} catch (Exception e) {
				throw new DUAInitialisierungsException("Fahrstreifen " + fs //$NON-NLS-1$
						+ " konnte nicht initialisiert werden", e); //$NON-NLS-1$
			}
		}
		sender.modifiziereObjektAnmeldung(anmeldungen);
		
		DAV.subscribeReceiver(this,
					fs.getSystemObject(),
					new DataDescription(
							DAV.getDataModel().getAttributeGroup(DUAKonstanten.ATG_KZD),
							DAV.getDataModel().getAspect(DUAKonstanten.ASP_MESSWERTERSETZUNG),
							(short)0),
							ReceiveOptions.normal(),
							ReceiverRole.receiver());
	}

	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void aggregiere(long zeitStempel,
						   AggregationsIntervall intervall) {
		
	}


	/**
	 * {@inheritDoc}
	 */
	public void update(ResultData[] resultate) {
		if(resultate != null){
			for(ResultData resultat:resultate){
				if(resultat != null){
					// TODO Daten sammeln
				}
			}
		}
	}
	
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void finalize()
	throws Throwable {
		LOGGER.warning("Der FS " + this.fs +  //$NON-NLS-1$
						" wird nicht mehr aggregiert"); //$NON-NLS-1$
	}
	

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return this.fs.toString();
	}
	
}
