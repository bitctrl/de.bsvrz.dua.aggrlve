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
import java.util.Calendar;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import de.bsvrz.dav.daf.main.ClientDavInterface;
import de.bsvrz.dav.daf.main.Data;
import de.bsvrz.dav.daf.main.DataDescription;
import de.bsvrz.dav.daf.main.ResultData;
import de.bsvrz.dav.daf.main.config.AttributeGroup;
import de.bsvrz.dua.guete.GWert;
import de.bsvrz.dua.guete.GueteException;
import de.bsvrz.dua.guete.GueteVerfahren;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAInitialisierungsException;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;
import de.bsvrz.sys.funclib.bitctrl.dua.av.DAVObjektAnmeldung;
import de.bsvrz.sys.funclib.bitctrl.dua.lve.FahrStreifen;
import de.bsvrz.sys.funclib.bitctrl.dua.lve.MessQuerschnitt;

/**
 * Aggregiert aus den fuer diesen Messquerschnitt (bzw. dessen Fahrstreifen) gespeicherten
 * Daten die Aggregationswerte aller Aggregationsstufen aus der jeweils darunterliegenden Stufe
 * bzw. aus den messwertersetzten Fahrstreifendaten fuer die Basisstufe
 * 
 * @author BitCtrl Systems GmbH, Thierfelder
 *
 */
public class AggregationsMessQuerschnitt
extends AbstraktAggregationsObjekt{
	
	/**
	 * Publikations-Attributgruppe
	 */
	private static AttributeGroup PUB_ATG = null;
	
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
		super(dav, mq.getSystemObject());
		if(PUB_ATG == null){
			PUB_ATG = DAV.getDataModel().getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_MQ);
		}
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

		long begin = zeitStempel;
		long ende = zeitStempel + intervall.getIntervall();
		if(intervall.isDTVorTV()){
			GregorianCalendar cal = new GregorianCalendar();
			cal.setTimeInMillis(zeitStempel);
			
			if(intervall.equals(AggregationsIntervall.AGG_DTV_TAG)){
				cal.add(Calendar.DAY_OF_YEAR, 1);
				ende = cal.getTimeInMillis();
			}else
			if(intervall.equals(AggregationsIntervall.AGG_DTV_MONAT)){
				cal.add(Calendar.MONTH, 1);
				ende = cal.getTimeInMillis();
			}else
			if(intervall.equals(AggregationsIntervall.AGG_DTV_JAHR)){
				cal.add(Calendar.YEAR, 1);
				ende = cal.getTimeInMillis();
			}			
		}
		
		Collection<AggregationsDatum> mqDaten = this.datenPuffer.getDatenFuerZeitraum(
																	begin, ende, intervall);		
		Data nutzDatum = null;
		
		if(intervall.isDTVorTV()){
			if(!mqDaten.isEmpty()){
				/**
				 * Daten koennen aus naechstkleinerem Intervall aggregiert werden
				 */
				nutzDatum = DAV.createData(PUB_ATG);
				for(AggregationsAttribut attribut:AggregationsAttribut.getInstanzen()){
					if(!attribut.isGeschwindigkeitsAttribut()){
						this.aggregiereQ(attribut, nutzDatum, mqDaten, zeitStempel, intervall);
					}
				}
			}else{
				LOGGER.warning(intervall + " fuer " + this.objekt +  //$NON-NLS-1$
						" kann nicht berechnet werden, da keine Basisdaten (Intervall: " + //$NON-NLS-1$
						intervall.getVorgaenger() + ") zur Verfuegung stehen"); //$NON-NLS-1$
			}
		}else{
			if(mqDaten.isEmpty()){
				/**
				 * Aggregiere Basisintervall aus messwertersetzten Fahrstreifendaten
				 */
				Map<AggregationsFahrStreifen, Collection<AggregationsDatum>> fsDaten = 
					new HashMap<AggregationsFahrStreifen, Collection<AggregationsDatum>>();
				
				for(AggregationsFahrStreifen fs:this.fsMenge){
					Collection<AggregationsDatum> daten =
						fs.getPuffer().getPuffer(null).getDatenFuerZeitraum(begin, ende);
					fsDaten.put(fs, daten);
				}
				
				boolean kannBasisIntervallBerechnen = true;
				for(Collection<AggregationsDatum> daten:fsDaten.values()){
					if(daten.isEmpty()){
						kannBasisIntervallBerechnen = false;
						break;
					}
				}
				
				if(kannBasisIntervallBerechnen){
					nutzDatum = DAV.createData(PUB_ATG);
					aggregiereBasisDatum(nutzDatum, fsDaten, zeitStempel, intervall);
				}
			}else{
				/**
				 * Daten koennen aus naechstkleinerem Intervall aggregiert werden
				 */
				nutzDatum = DAV.createData(PUB_ATG);
				for(AggregationsAttribut attribut:AggregationsAttribut.getInstanzen()){
					if(attribut.isGeschwindigkeitsAttribut()){
						this.aggregiereV(attribut, nutzDatum, mqDaten, zeitStempel, intervall);
					}else{
						this.aggregiereQ(attribut, nutzDatum, mqDaten, zeitStempel, intervall);
					}
				}
			}
		}
		
		ResultData resultat = new ResultData(
				this.mq.getSystemObject(),
				new DataDescription(PUB_ATG, intervall.getAspekt(), (short)0),
				zeitStempel, nutzDatum);		

		if(resultat.getData() != null){
			this.fuelleRest(resultat, intervall);
			this.datenPuffer.aktualisiere(resultat);
		}
		
		this.sende(resultat);
	}
	
	
	/**
	 * Aggregiert das erste Aggregationsintervall fuer diesen Messquerschnitt aus
	 * Basis der uebergebenen messwertersetzten Fahrstreifendaten
	 * 
	 * @param nutzDatum ein veraenderbares Nutzdatum
	 * @param fsDaten die Datenpuffer mit den messwertersetzten Fahrstreifendaten der
	 * mit diesem Messquerschnitt assoziierten Fahrstreifen
	 * @param zeitStempel der Zeitstempel (Start)
	 * @param intervall das Aggregationsintervall
	 */
	private final void aggregiereBasisDatum(Data nutzDatum,
											Map<AggregationsFahrStreifen, Collection<AggregationsDatum>> fsDaten,
											long zeitStempel,
											AggregationsIntervall intervall){

		for(AggregationsAttribut attribut:AggregationsAttribut.getInstanzen()){
			boolean interpoliert = false;
			boolean nichtErfasst = false;
			long summe = 0;
			long anzahl = 0;
			Collection<GWert> gueteWerte = new ArrayList<GWert>();

			for(Collection<AggregationsDatum> fsQuellDatum:fsDaten.values()){
				Collection<AggregationsAttributWert> werte = 
					this.ersetzteAusgefalleneWerte(attribut, fsQuellDatum, intervall, zeitStempel);

				for(AggregationsAttributWert basisWert:werte){
					if(basisWert.getWert() >= 0){
						anzahl++;
						summe += basisWert.getWert();
						gueteWerte.add(basisWert.getGuete());
						interpoliert |= basisWert.isInterpoliert();
						nichtErfasst |= basisWert.isNichtErfasst();
					}
				}					
			}

			AggregationsAttributWert exportWert = new AggregationsAttributWert(
					attribut, DUAKonstanten.NICHT_ERMITTELBAR_BZW_FEHLERHAFT, 0);
			
			if(gueteWerte.size() > 0){
				if(attribut.isGeschwindigkeitsAttribut()){
					exportWert.setWert(Math.round((double)summe / (double)anzahl));
				}else{
					exportWert.setWert(summe);
				}
				exportWert.setInterpoliert(interpoliert);
				exportWert.setNichtErfasst(nichtErfasst);
				try {
					exportWert.setGuete(GueteVerfahren.summe(gueteWerte.toArray(new GWert[0])));
				} catch (GueteException e) {
					LOGGER.warning("Guete von " + this.objekt + " fuer " + //$NON-NLS-1$ //$NON-NLS-2$
							attribut + " konnte nicht berechnet werden", e); //$NON-NLS-1$
					e.printStackTrace();
				}
			}

			exportWert.exportiere(nutzDatum, this.isFahrstreifen());
		}
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


	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean isFahrstreifen() {
		return false;
	}
	
}
