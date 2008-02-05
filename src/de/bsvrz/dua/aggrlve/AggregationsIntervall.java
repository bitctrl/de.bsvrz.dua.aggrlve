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

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.SortedSet;
import java.util.TreeSet;

import de.bsvrz.dav.daf.main.ClientDavInterface;
import de.bsvrz.dav.daf.main.DataDescription;
import de.bsvrz.dav.daf.main.config.Aspect;
import de.bsvrz.dav.daf.main.config.AttributeGroup;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;
import de.bsvrz.sys.funclib.bitctrl.konstante.Konstante;

/**
 * Korrespondiert mit den Aspekten der Aggregationsintervalle:<br>
 * - <code>asp.agregation1Minute</code>,<br>
 * - <code>asp.agregation5Minuten</code>,<br>
 * - <code>asp.agregation15Minuten</code>,<br>
 * - <code>asp.agregation30Minuten</code>,<br>
 * - <code>asp.agregation60Minuten</code>,<br>
 * - <code>asp.agregationDtvMonat</code> und<br>
 * - <code>asp.agregationDtvJahr</code><br><br>
 * <b>Achtung:</b> Bevor auf die statischen Member dieser Klasse
 * zugegriffen werden kann, muss diese Klasse initialisiert werden 
 * 
 * @author BitCtrl Systems GmbH, Thierfelder
 *
 */
public class AggregationsIntervall
implements Comparable<AggregationsIntervall>{

	/**
	 * <code>asp.agregation1Minute</code>
	 */
	public static AggregationsIntervall AGG_1MINUTE = null; 

	/**
	 * <code>asp.agregation5Minuten</code>
	 */
	public static AggregationsIntervall AGG_5MINUTE = null; 

	/**
	 * <code>asp.agregation15Minuten</code>
	 */
	public static AggregationsIntervall AGG_15MINUTE = null; 

	/**
	 * <code>asp.agregation30Minuten</code>
	 */
	public static AggregationsIntervall AGG_30MINUTE = null; 

	/**
	 * <code>asp.agregation60Minuten</code>
	 */
	public static AggregationsIntervall AGG_60MINUTE = null; 

	/**
	 * <code>asp.agregationDtvTag</code>
	 */
	public static AggregationsIntervall AGG_DTV_TAG = null; 
	
	/**
	 * <code>asp.agregationDtvMonat</code>
	 */
	public static AggregationsIntervall AGG_DTV_MONAT = null; 
	
	/**
	 * <code>asp.agregationDtvJahr</code>
	 */
	public static AggregationsIntervall AGG_DTV_JAHR = null;

	/**
	 * der Wertebereich dieses Typs
	 */
	private static SortedSet<AggregationsIntervall> WERTE_BEREICH = new TreeSet<AggregationsIntervall>();
	
	/**
	 * die Datenbeschreibung der Publikationsdaten dieses Aggregations-
	 * Intervalls (fuer FS)
	 */
	private DataDescription datenBeschreibungFs = null;

	/**
	 * die Datenbeschreibung der Publikationsdaten dieses Aggregations-
	 * Intervalls (fuer MQ)
	 */
	private DataDescription datenBeschreibungMq = null;

	/**
	 * die Laenge des Aggregationsintervalls in ms
	 */
	private long intervallLaengeInMillis = -1;
	
	/**
	 * die maximale Anzahl der Elemente, die in einem Puffer mit Daten dieser
	 * Aggregationsstufe vorgehalten werden muessen
	 */
	private long maxPufferGroesse = 0;
	

	/**
	 * Standardkonstruktor
	 * 
	 * @param atgFs die Attributgruppe der Publikationsdaten dieses Aggregations-
	 * Intervalls (fuer FS)
	 * @param atgMq die Attributgruppe der Publikationsdaten dieses Aggregations-
	 * Intervalls (fuer MQ)
	 * @param asp der Aspekt der Publikationsdaten dieses Aggregations-
	 * Intervalls (fuer FS <b>und</b> MQ)
	 * @param intervall die Laenge des Aggregationsintervalls in ms
	 * @param maxPufferGroesse die maximale Anzahl der Elemente, die in einem Puffer mit Daten dieser
	 * Aggregationsstufe vorgehalten werden muessen
	 */
	private AggregationsIntervall(final AttributeGroup atgFs,
								  final AttributeGroup atgMq,
								  final Aspect asp,
								  final long intervall,
								  final long maxPufferGroesse) {
		this.datenBeschreibungFs = new DataDescription(atgFs, asp);
		this.datenBeschreibungMq = new DataDescription(atgMq, asp);
		this.intervallLaengeInMillis = intervall;
		this.maxPufferGroesse = maxPufferGroesse;
		WERTE_BEREICH.add(this);
	}
	
	
	/**
	 * Initialisiert die statischen Instanzen dieser Klasse
	 * 
	 * @param dav Verbindung zum Datenverteiler
	 */
	public static final void initialisiere(final ClientDavInterface dav){
		AGG_1MINUTE = new AggregationsIntervall(
				dav.getDataModel().getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_FS),
				dav.getDataModel().getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_MQ),
				dav.getDataModel().getAspect("asp.agregation1Minute"),  //$NON-NLS-1$
				Konstante.MINUTE_IN_MS, 5);
		
		AGG_5MINUTE = new AggregationsIntervall(
				dav.getDataModel().getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_FS),
				dav.getDataModel().getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_MQ),
						dav.getDataModel().getAspect("asp.agregation5Minuten"),  //$NON-NLS-1$
				5 * Konstante.MINUTE_IN_MS, 3);
		AGG_15MINUTE = new AggregationsIntervall(
				dav.getDataModel().getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_FS),
				dav.getDataModel().getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_MQ),
						dav.getDataModel().getAspect("asp.agregation15Minuten"),  //$NON-NLS-1$
				15 * Konstante.MINUTE_IN_MS, 2);
		AGG_30MINUTE = new AggregationsIntervall(
				dav.getDataModel().getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_FS),
				dav.getDataModel().getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_MQ),
						dav.getDataModel().getAspect("asp.agregation30Minuten"),  //$NON-NLS-1$
				30 * Konstante.MINUTE_IN_MS, 2);
		AGG_60MINUTE = new AggregationsIntervall(
				dav.getDataModel().getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_FS),
				dav.getDataModel().getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_MQ),
						dav.getDataModel().getAspect("asp.agregation60Minuten"),  //$NON-NLS-1$
				60 * Konstante.MINUTE_IN_MS, 40);

		AGG_DTV_TAG = new AggregationsIntervall(
				dav.getDataModel().getAttributeGroup(DUAKonstanten.ATG_DTV_FS),
				dav.getDataModel().getAttributeGroup(DUAKonstanten.ATG_DTV_MQ),
						dav.getDataModel().getAspect("asp.agregationDtvTag"),  //$NON-NLS-1$
				60 * 24 * Konstante.MINUTE_IN_MS, 50);
		AGG_DTV_MONAT = new AggregationsIntervall(
				dav.getDataModel().getAttributeGroup(DUAKonstanten.ATG_DTV_FS),
				dav.getDataModel().getAttributeGroup(DUAKonstanten.ATG_DTV_MQ),
						dav.getDataModel().getAspect("asp.agregationDtvMonat"),  //$NON-NLS-1$
				61 * 24 * Konstante.MINUTE_IN_MS, 15);
		AGG_DTV_JAHR = new AggregationsIntervall(
				dav.getDataModel().getAttributeGroup(DUAKonstanten.ATG_DTV_FS),
				dav.getDataModel().getAttributeGroup(DUAKonstanten.ATG_DTV_MQ),
						dav.getDataModel().getAspect("asp.agregationDtvJahr"),  //$NON-NLS-1$
				62 * 24 * Konstante.MINUTE_IN_MS, 0);
	}
	
	
	/**
	 * Erfragt die Menge aller statischen Instanzen dieser Klasse
	 * in sortierter Form:<br>
	 * - <code>asp.agregation1Minute</code>,<br>
	 * - <code>asp.agregation5Minuten</code>,<br>
	 * - <code>asp.agregation15Minuten</code>,<br>
	 * - <code>asp.agregation30Minuten</code>,<br>
	 * - <code>asp.agregation60Minuten</code>,<br>
	 * - <code>asp.agregationDtvMonat</code> und<br>
	 * - <code>asp.agregationDtvJahr</code><br><br>
	 * 
	 * @return die Menge aller statischen Instanzen dieser Klasse
	 */
	public static final SortedSet<AggregationsIntervall> getInstanzen(){
		return WERTE_BEREICH;
	}

	
	/**
	 * Erfragt die maximale Anzahl der Elemente, die in einem Puffer mit Daten dieser
	 * Aggregationsstufe vorgehalten werden muessen
	 * 
	 * @return die maximale Anzahl der Elemente, die in einem Puffer mit Daten dieser
	 * Aggregationsstufe vorgehalten werden muessen
	 */
	public final long getMaxPufferGroesse(){
		return this.maxPufferGroesse;
	}
	
	
	
	/**
	 * Erfragt den Publikationsaspekt der Daten fuer FS und MQ
	 *  
	 * @return der Publikationsaspekt der Daten fuer FS und MQ
	 */
	public final Aspect getAspekt(){
		return this.datenBeschreibungFs.getAspect();
	}
	

	/**
	 * Erfragt die Datenbeschreibung der Publikationsdaten dieses Aggregations-
	 * Intervalls
	 * 
	 * @param fuerFahrstreifen fuer Fahrstreifen?
	 * @return die Datenbeschreibung der Publikationsdaten dieses Aggregations-
	 * Intervalls
	 */
	public final DataDescription getDatenBeschreibung(boolean fuerFahrstreifen){
		if(fuerFahrstreifen){
			return this.datenBeschreibungFs;
		}
		return this.datenBeschreibungMq;
	}


	/**
	 * Erfragt das naechstkleinere Aggregationsintervall
	 * 
	 * @return das naechstkleinere Aggregationsintervall oder <code>null</code>,
	 * wenn vom Intervall <code>eine Minute</code> aus gesucht wird
	 */
	public final AggregationsIntervall getVorgaenger(){
		AggregationsIntervall vorgaenger = null;
		
		for(AggregationsIntervall intervall:getInstanzen()){
			if(intervall.equals(this))break;
			vorgaenger = intervall;
		}
		
		return vorgaenger;
	}
	
	
	/**
	 * Erfragt den Zeitstempel des Aggregationsdatums, das zum uebergebenen
	 * Zeitpunkt fuer dieses Aggregationsintervall berechnet werden sollte<br>
	 * <b>Achtung:</b> Die Methode geht davon aus, dass sie nur einmal in der
	 * Minute aufgerufen wird!
	 * 
	 * @param zeitpunkt ein Zeitpunkt
	 * @return der Zeitstempel des Aggregationsdatums, das zum uebergebenen
	 * Zeitpunkt fuer dieses Aggregationsintervall berechnet werden sollte oder
	 * <code>-1</code>, wenn zum uebergebenen Zeitpunkt keine Aggregation 
	 * notwendig ist
	 */
	public final long getAggregationZeitStempel(long zeitpunkt){
		long zeitStempel = -1;
		GregorianCalendar cal = new GregorianCalendar();
		cal.setTimeInMillis(zeitpunkt);
		final long minuteJetzt = cal.get(Calendar.MINUTE);
		
		if(this.equals(AGG_1MINUTE) || 
		   this.equals(AGG_5MINUTE) || 
		   this.equals(AGG_15MINUTE) ||
		   this.equals(AGG_30MINUTE) ||
		   this.equals(AGG_60MINUTE)){
			long intervallLaengeInMinuten = this.getIntervall() / Konstante.MINUTE_IN_MS; 			
			if(minuteJetzt%intervallLaengeInMinuten == 0){
				cal.add(Calendar.MINUTE, (int)(-1 * intervallLaengeInMinuten));
				cal.set(Calendar.SECOND, 0);
				cal.set(Calendar.MILLISECOND, 0);
				zeitStempel = cal.getTimeInMillis();
			}
		}else
		if(this.equals(AGG_DTV_TAG)){
			/**
			 * Versuche noch 12 Stunden im neuen Tag 
			 * DTV-Tag des Vorgaengertages zu berechnen
			 */
			final long stundeJetzt = cal.get(Calendar.HOUR_OF_DAY);
			if(stundeJetzt < 12 && minuteJetzt == 1){
				cal.add(Calendar.DAY_OF_YEAR, -1);
				cal.set(Calendar.HOUR_OF_DAY, 0);
				cal.set(Calendar.MINUTE, 0);
				cal.set(Calendar.SECOND, 0);
				cal.set(Calendar.MILLISECOND, 0);
				zeitStempel = cal.getTimeInMillis();
			}
		}else			
		if(this.equals(AGG_DTV_JAHR)){
			final long stundeJetzt = cal.get(Calendar.HOUR_OF_DAY);
			final long tagJetzt = cal.get(Calendar.DAY_OF_YEAR);
			/**
			 * Versuche noch 30 Tage im neuen Jahr 
			 * DTV-Jahr des Vorgaengerjahres zu berechnen
			 */
			if(tagJetzt < 30 && stundeJetzt == 0 && minuteJetzt == 1){
				cal.add(Calendar.YEAR, -1);
				cal.set(Calendar.DAY_OF_YEAR, 1);
				cal.set(Calendar.MINUTE, 0);
				cal.set(Calendar.SECOND, 0);
				cal.set(Calendar.MILLISECOND, 0);
				zeitStempel = cal.getTimeInMillis();
			}
		}else
		if(this.equals(AGG_DTV_MONAT)){
			/**
			 * Versuche noch 20 Tage im neuen Monat 
			 * DTV-Monat des Vorgaengermonats zu berechnen
			 */
			final long stundeJetzt = cal.get(Calendar.HOUR_OF_DAY);
			final long tagJetzt = cal.get(Calendar.DAY_OF_MONTH);
			if(tagJetzt < 20 && stundeJetzt == 0 && minuteJetzt == 1){
				cal.set(Calendar.DAY_OF_MONTH, 1);
				cal.add(Calendar.MONTH, -1);
				cal.set(Calendar.MINUTE, 0);
				cal.set(Calendar.SECOND, 0);
				cal.set(Calendar.MILLISECOND, 0);
				zeitStempel = cal.getTimeInMillis();
			}
		}
		
		return zeitStempel;
	}

	
	/**
	 * Erfragt, ob es sich bei diesem Intervall um ein DTV- bzw. TV-Intervall handelt
	 * 
	 * @return ob es sich bei diesem Intervall um ein DTV- bzw. TV-Intervall handelt
	 */
	public final boolean isDTVorTV(){
		return this.equals(AggregationsIntervall.AGG_DTV_JAHR) ||
				this.equals(AggregationsIntervall.AGG_DTV_MONAT) ||
				this.equals(AggregationsIntervall.AGG_DTV_TAG);
	}
	
	
	/**
	 * Erfragt, ob zum uebergebenen Zeitpunkt eine Aggregation 
	 * notwendig ist
	 * 
	 * @return ob zum uebergebenen Zeitpunkt eine Aggregation 
	 * notwendig ist
	 */
	public final boolean isAggregationErforderlich(long zeitpunkt){
		return getAggregationZeitStempel(zeitpunkt) != -1;
	}
	
	
	/**
	 * Erfragt die Laenge des Aggregationsintervalls in ms
	 * 
	 * @return die Laenge des Aggregationsintervalls in ms
	 */
	public final long getIntervall(){
		return this.intervallLaengeInMillis;
	}
	
	
	/**
	 * {@inheritDoc}
	 */
	public int compareTo(AggregationsIntervall that) {
		return new Long(this.getIntervall()).compareTo(that.getIntervall());
	}
	

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(Object obj) {
		boolean ergebnis = false;
		
		if(obj != null && obj instanceof AggregationsIntervall){
			AggregationsIntervall that = (AggregationsIntervall)obj;
			
			ergebnis = this.getDatenBeschreibung(true).getAspect().equals(
					that.getDatenBeschreibung(true).getAspect()) && 
					this.getDatenBeschreibung(true).getAttributeGroup().equals(
							that.getDatenBeschreibung(true).getAttributeGroup());
		}
		
		return ergebnis;
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return this.datenBeschreibungFs + "\n" + this.datenBeschreibungMq; //$NON-NLS-1$
	}

}
