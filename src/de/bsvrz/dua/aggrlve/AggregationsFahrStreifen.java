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

import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

import de.bsvrz.dav.daf.main.ClientDavInterface;
import de.bsvrz.dav.daf.main.ClientReceiverInterface;
import de.bsvrz.dav.daf.main.Data;
import de.bsvrz.dav.daf.main.DataDescription;
import de.bsvrz.dav.daf.main.ReceiveOptions;
import de.bsvrz.dav.daf.main.ReceiverRole;
import de.bsvrz.dav.daf.main.ResultData;
import de.bsvrz.dav.daf.main.config.AttributeGroup;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAInitialisierungsException;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;
import de.bsvrz.sys.funclib.bitctrl.dua.av.DAVObjektAnmeldung;
import de.bsvrz.sys.funclib.bitctrl.dua.lve.FahrStreifen;

/**
 * Aggregiert aus den fuer diesen Fahrstreifen gespeicherten Daten die Aggregationswerte 
 * aller Aggregationsstufen aus der jeweils darunterliegenden Stufe
 * 
 * @author BitCtrl Systems GmbH, Thierfelder
 *
 */
public class AggregationsFahrStreifen
extends AbstraktAggregationsObjekt
implements ClientReceiverInterface{
	
	/**
	 * Publikations-Attributgruppe
	 */
	private static AttributeGroup PUB_ATG = null;
	
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
		super(dav, fs.getSystemObject());
		this.fs = fs;
		if(PUB_ATG == null){
			PUB_ATG = DAV.getDataModel().getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_FS);
		}
		
		this.datenPuffer = new AggregationsPufferMenge(dav, fs.getSystemObject());
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
		if(this.objekt.getPid().equals("fs.mq.a100.0000.hfs")){ //$NON-NLS-1$
			
		}
		
		Collection<AggregationsDatum> basisDaten = 
			this.datenPuffer.getDatenFuerZeitraum(zeitStempel,
												  zeitStempel + intervall.getIntervall(), intervall);
		Data nutzDatum = null;
		
		if(!basisDaten.isEmpty()){
			nutzDatum = DAV.createData(PUB_ATG);

			if(!intervall.isDTVorTV()){
				for(AggregationsAttribut attribut:AggregationsAttribut.getInstanzen()){
					if(attribut.isGeschwindigkeitsAttribut()){
						this.aggregiereV(attribut, nutzDatum, basisDaten, zeitStempel, intervall);
					}else{
						this.aggregiereQ(attribut, nutzDatum, basisDaten, zeitStempel, intervall);
					}
				}
			}
		}
		
		ResultData resultat = new ResultData(
				this.fs.getSystemObject(),
				new DataDescription(PUB_ATG, intervall.getAspekt(), (short)0),
				zeitStempel, nutzDatum);		

		if(resultat.getData() != null){
			this.fuelleRest(resultat, intervall);
			this.datenPuffer.aktualisiere(resultat);
		}
		
		this.sende(resultat);
	}

	
	/**
	 * {@inheritDoc}
	 */
	public void update(ResultData[] resultate) {
		if(resultate != null){
			for(ResultData resultat:resultate){
				if(resultat != null){
					this.datenPuffer.aktualisiere(resultat);
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


	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean isFahrstreifen() {
		return true;
	}
	
}
