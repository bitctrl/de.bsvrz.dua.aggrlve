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

import de.bsvrz.dav.daf.main.ClientDavInterface;
import de.bsvrz.dav.daf.main.ClientReceiverInterface;
import de.bsvrz.dav.daf.main.Data;
import de.bsvrz.dav.daf.main.DataDescription;
import de.bsvrz.dav.daf.main.ReceiveOptions;
import de.bsvrz.dav.daf.main.ReceiverRole;
import de.bsvrz.dav.daf.main.ResultData;
import de.bsvrz.dav.daf.main.config.AttributeGroup;
import de.bsvrz.dua.guete.GWert;
import de.bsvrz.dua.guete.GueteException;
import de.bsvrz.dua.guete.GueteVerfahren;
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
		super(dav);
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
		Collection<AggregationsDatum> basisDaten = 
			this.datenPuffer.getDatenFuerZeitraum(zeitStempel,
												  zeitStempel + intervall.getIntervall(), intervall);
		Data nutzDatum = null;
		
		if(!basisDaten.isEmpty()){
			nutzDatum = DAV.createData(PUB_ATG);

			for(AggregationsAttribut attribut:AggregationsAttribut.getInstanzen()){
				if(attribut.isGeschwindigkeitsAttribut()){
					this.aggregiereV(attribut, nutzDatum, basisDaten, intervall);
				}else{
					this.aggregiereQ(attribut, nutzDatum, basisDaten, intervall);
				}
			}			
		}
		
		ResultData resultat = new ResultData(
				this.fs.getSystemObject(),
				new DataDescription(PUB_ATG, intervall.getAspekt(), (short)0),
				zeitStempel, nutzDatum);		

		if(resultat.getData() != null){
			this.fuelleRest(resultat);
			this.datenPuffer.aktualisiere(resultat);
		}
		
		this.sende(resultat);
	}

	
	/**
	 * Aggregiert ein Geschwindigkeitsdatum
	 * 
	 * @param attribut das Attribut, das berechnet werden soll
	 * @param datum das gesamte Aggregationsdatum (veraenderbar)
	 * @param basisDaten die der Aggregation zu Grunde liegenden Daten
	 * @param intervall das gewuenschte Aggregationsintervall
	 */
	private final void aggregiereV(AggregationsAttribut attribut,
			Data nutzDatum,
			Collection<AggregationsDatum> basisDaten,
			AggregationsIntervall intervall){
		/**
		 * Die Aggregation erfolgt unabhängig von der Anzahl der gültigen Kurzzeitdatenzyklen.
		 * Ausgefallene Werte werden durch den Mittelwert der vorhandenen Werte ersetzt. Um die
		 * Zuverlässigkeit der Daten nachvollziehen zu können, ist jeder aggregierte Wert mit
		 * einem Güteindex in % anzugeben. Der Güteindex wird durch arithmetische Mittelung der
		 * Güteindizes der zu aggregierenden Daten bestimmt. Der Güteindex von ausgefallenen
		 * Werten ergibt sich dabei aus dem Mittelwert der vorhandenen Werte multipliziert mit
		 * einem parametrierbaren Faktor. Des weiteren ist jeder aggregierte Wert mit einer
		 * Kennung zu versehen, ob zur Aggregation interpolierte (durch die Messwertersetzung
		 * generierte) Werte verwendet wurden. 
		 */
		Collection<AggregationsAttributWert> werte = this.ersetzteAusgefalleneWerte(attribut, basisDaten, intervall);

		boolean interpoliert = false;
		boolean nichtErfasst = false;
		long anzahl = 0;
		long summe = 0;
		Collection<GWert> gueteWerte = new ArrayList<GWert>();
		for(AggregationsAttributWert basisWert:werte){
			if(basisWert.getWert() >= 0){
				summe += basisWert.getWert();
				anzahl++;
				gueteWerte.add(basisWert.getGuete());
				interpoliert |= basisWert.isInterpoliert();
				nichtErfasst |= basisWert.isNichtErfasst();
			}
		}

		AggregationsAttributWert exportWert = new AggregationsAttributWert(
				attribut, DUAKonstanten.NICHT_ERMITTELBAR_BZW_FEHLERHAFT, 0);
		if(anzahl > 0){
			exportWert.setWert(
					Math.round(	(double)summe / (double)anzahl ));
			exportWert.setInterpoliert(interpoliert);
			exportWert.setNichtErfasst(nichtErfasst);
			try {
				exportWert.setGuete(GueteVerfahren.summe(gueteWerte.toArray(new GWert[0])));
			} catch (GueteException e) {
				LOGGER.warning("Guete von " + this.fs + " fuer " + //$NON-NLS-1$ //$NON-NLS-2$
						attribut + " konnte nicht berechnet werden", e); //$NON-NLS-1$
				e.printStackTrace();
			}
		}

		exportWert.exportiere(nutzDatum, true);
	}
	

	/**
	 * Aggregiert ein Verkehrsstärkedatum
	 * 
	 * @param attribut das Attribut, das berechnet werden soll
	 * @param nutzDatum das gesamte Aggregationsdatum (dieses muss veraenderbar sein
	 * und wird hier gefuellt)
	 * @param basisDaten die der Aggregation zu Grunde liegenden Daten
	 * @param intervall das gewuenschte Aggregationsintervall
	 */
	private final void aggregiereQ(AggregationsAttribut attribut,
								   Data nutzDatum,
								   Collection<AggregationsDatum> basisDaten,
								   AggregationsIntervall intervall){
		/**
		 * Die Aggregation erfolgt unabhängig von der Anzahl der gültigen Kurzzeitdatenzyklen.
		 * Ausgefallene Werte werden durch den Mittelwert der vorhandenen Werte ersetzt. Um die
		 * Zuverlässigkeit der Daten nachvollziehen zu können, ist jeder aggregierte Wert mit
		 * einem Güteindex in % anzugeben. Der Güteindex wird durch arithmetische Mittelung der
		 * Güteindizes der zu aggregierenden Daten bestimmt. Der Güteindex von ausgefallenen
		 * Werten ergibt sich dabei aus dem Mittelwert der vorhandenen Werte multipliziert mit
		 * einem parametrierbaren Faktor. Des weiteren ist jeder aggregierte Wert mit einer
		 * Kennung zu versehen, ob zur Aggregation interpolierte (durch die Messwertersetzung
		 * generierte) Werte verwendet wurden. 
		 */
		Collection<AggregationsAttributWert> werte = this.ersetzteAusgefalleneWerte(attribut, basisDaten, intervall);
		
		boolean interpoliert = false;
		boolean nichtErfasst = false;
		long summe = 0;
		Collection<GWert> gueteWerte = new ArrayList<GWert>();
		for(AggregationsAttributWert basisWert:werte){
			if(basisWert.getWert() >= 0){
				summe += basisWert.getWert();
				gueteWerte.add(basisWert.getGuete());
				interpoliert |= basisWert.isInterpoliert();
				nichtErfasst |= basisWert.isNichtErfasst();
			}
		}
		
		AggregationsAttributWert exportWert = new AggregationsAttributWert(
							attribut, DUAKonstanten.NICHT_ERMITTELBAR_BZW_FEHLERHAFT, 0);
		if(gueteWerte.size() > 0){
			exportWert.setWert(summe);
			exportWert.setInterpoliert(interpoliert);
			exportWert.setNichtErfasst(nichtErfasst);
			try {
				exportWert.setGuete(GueteVerfahren.summe(gueteWerte.toArray(new GWert[0])));
			} catch (GueteException e) {
				LOGGER.warning("Guete von " + this.fs + " fuer " + //$NON-NLS-1$ //$NON-NLS-2$
						attribut + " konnte nicht berechnet werden", e); //$NON-NLS-1$
				e.printStackTrace();
			}
		}
		
		exportWert.exportiere(nutzDatum, true);
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
	
}
