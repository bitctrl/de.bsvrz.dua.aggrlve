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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

import com.bitctrl.Constants;

import de.bsvrz.dav.daf.main.ClientDavInterface;
import de.bsvrz.dav.daf.main.ClientReceiverInterface;
import de.bsvrz.dav.daf.main.Data;
import de.bsvrz.dav.daf.main.DataDescription;
import de.bsvrz.dav.daf.main.ReceiveOptions;
import de.bsvrz.dav.daf.main.ReceiverRole;
import de.bsvrz.dav.daf.main.ResultData;
import de.bsvrz.dua.guete.GWert;
import de.bsvrz.dua.guete.GueteException;
import de.bsvrz.dua.guete.GueteVerfahren;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAInitialisierungsException;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;
import de.bsvrz.sys.funclib.bitctrl.dua.av.DAVObjektAnmeldung;
import de.bsvrz.sys.funclib.bitctrl.dua.lve.FahrStreifen;
import de.bsvrz.sys.funclib.debug.Debug;

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
		
		this.datenPuffer = new AggregationsPufferMenge(dav, fs.getSystemObject());
		Set<DAVObjektAnmeldung> anmeldungen = new TreeSet<DAVObjektAnmeldung>(); 
		for(AggregationsIntervall intervall:AggregationsIntervall.getInstanzen()){
			try {
				anmeldungen.add(
						new DAVObjektAnmeldung(
						fs.getSystemObject(), 
						intervall.getDatenBeschreibung(true)));
			} catch (Exception e) {
				throw new DUAInitialisierungsException("Fahrstreifen " + fs //$NON-NLS-1$
						+ " konnte nicht initialisiert werden", e); //$NON-NLS-1$
			}
		}
		sender.modifiziereObjektAnmeldung(anmeldungen);
		
		dav.subscribeReceiver(this,
					fs.getSystemObject(),
					new DataDescription(
							dav.getDataModel().getAttributeGroup(DUAKonstanten.ATG_KZD),
							dav.getDataModel().getAspect(DUAKonstanten.ASP_MESSWERTERSETZUNG),
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
		if(!intervall.isDTVorTV()){
			Collection<AggregationsDatum> basisDaten = 
				this.datenPuffer.getDatenFuerZeitraum(zeitStempel,
													  zeitStempel + intervall.getIntervall(), intervall);
			Data nutzDatum = null;
			
			if(!basisDaten.isEmpty()){
				nutzDatum = dav.createData(intervall.getDatenBeschreibung(true).getAttributeGroup());
	
				for(AggregationsAttribut attribut:AggregationsAttribut.getInstanzen()){
					if(attribut.isGeschwindigkeitsAttribut() || 
					   basisDaten.iterator().next().isNormiert()){
						this.aggregiereMittel(attribut, nutzDatum, basisDaten, zeitStempel, intervall);
					}else{
						this.aggregiereAlsBasisDatum(attribut, nutzDatum, basisDaten, zeitStempel, intervall);
					}
				}
			}
			
			ResultData resultat = new ResultData(
					this.fs.getSystemObject(),
					intervall.getDatenBeschreibung(true),
					zeitStempel, nutzDatum);		
	
			if(resultat.getData() != null){
				this.fuelleRest(resultat, intervall);
				this.datenPuffer.aktualisiere(resultat);
			}
			
			this.sende(resultat);
		}
	}

	
	/**
	 * Aggregiert das niedrigstmoegliche Aggregationsintervalldatum aus
	 * messwertersetzten Fahrstreifendaten (nur fuer Verkehrsstaerke-Werte)
	 * 
	 * @param attribut das Attribut, das berechnet werden soll
	 * @param nutzDatum das gesamte Aggregationsdatum (veraenderbar)
	 * @param basisDaten die der Aggregation zu Grunde liegenden Daten (muss mindestens ein Element enthalten)
	 * @param zeitStempel der Zeitstempel, mit dem die aggregierten Daten veröffentlicht werden sollen
	 * @param intervall das gewuenschte Aggregationsintervall
	 */
	protected final void aggregiereAlsBasisDatum(AggregationsAttribut attribut,
			Data nutzDatum,
			Collection<AggregationsDatum> basisDaten,
			long zeitStempel,
			AggregationsIntervall intervall){

		final double erfassungsIntervallLaenge = basisDaten.iterator().next().getT();
		boolean interpoliert = false;
		boolean nichtErfasst = false;
		double elementZaehler = 0;
		long summe = 0;
		Collection<GWert> gueteWerte = new ArrayList<GWert>();
		for(AggregationsDatum basisDatum:basisDaten){
			AggregationsAttributWert basisWert = basisDatum.getWert(attribut);
			if(basisWert.getWert() >= 0){
				elementZaehler++;
				summe += basisWert.getWert();
				gueteWerte.add(basisWert.getGuete());
				interpoliert |= basisWert.isInterpoliert();
				nichtErfasst |= basisWert.isNichtErfasst();
			}
//			else{
//				gueteWerte.add(GWert.getMinGueteWert(basisWert.getGuete().getVerfahren()));
//			}
		}

		AggregationsAttributWert exportWert = new AggregationsAttributWert(
				attribut, DUAKonstanten.NICHT_ERMITTELBAR_BZW_FEHLERHAFT, 0);
		if(elementZaehler > 0){
			exportWert.setWert(
					Math.round(	(double)summe * Constants.MILLIS_PER_HOUR /
							(erfassungsIntervallLaenge * elementZaehler)));
			exportWert.setInterpoliert(interpoliert);
			if(AggregationLVE.NICHT_ERFASST){
				exportWert.setNichtErfasst(nichtErfasst);
			}
			try {
				exportWert.setGuete(GueteVerfahren.summe(gueteWerte.toArray(new GWert[0])));
			} catch (GueteException e) {
				Debug.getLogger().warning("Guete von " + this.objekt + " fuer " + //$NON-NLS-1$ //$NON-NLS-2$
						attribut + " konnte nicht berechnet werden", e); //$NON-NLS-1$
				e.printStackTrace();
			}
		}

		exportWert.exportiere(nutzDatum, this.isFahrstreifen());
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
		Debug.getLogger().warning("Der FS " + this.fs +  //$NON-NLS-1$
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
