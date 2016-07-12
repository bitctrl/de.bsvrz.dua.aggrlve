/*
 * Segment Datenübernahme und Aufbereitung (DUA), SWE Aggregation LVE
 * Copyright (C) 2007 BitCtrl Systems GmbH
 * Copyright 2016 by Kappich Systemberatung Aachen
 * 
 * This file is part of de.bsvrz.dua.aggrlve.
 * 
 * de.bsvrz.dua.aggrlve is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * de.bsvrz.dua.aggrlve is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with de.bsvrz.dua.aggrlve.  If not, see <http://www.gnu.org/licenses/>.

 * Contact Information:
 * Kappich Systemberatung
 * Martin-Luther-Straße 14
 * 52062 Aachen, Germany
 * phone: +49 241 4090 436 
 * mail: <info@kappich.de>
 */

package de.bsvrz.dua.aggrlve;

import de.bsvrz.dav.daf.main.ClientDavInterface;
import de.bsvrz.dav.daf.main.DataDescription;
import de.bsvrz.dav.daf.main.config.Aspect;
import de.bsvrz.dav.daf.main.config.AttributeGroup;
import de.bsvrz.dav.daf.main.config.DataModel;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Korrespondiert mit den Aspekten der Aggregationsintervalle:<br>
 * - <code>asp.agregation1Minute</code>,<br>
 * - <code>asp.agregation5Minuten</code>,<br>
 * - <code>asp.agregation15Minuten</code>,<br>
 * - <code>asp.agregation30Minuten</code>,<br>
 * - <code>asp.agregation60Minuten</code>,<br>
 * - <code>asp.agregationDtvMonat</code> und<br>
 * - <code>asp.agregationDtvJahr</code><br>
 * .<br>
 * <b>Achtung:</b> Bevor auf die statischen Member dieser Klasse zugegriffen
 * werden kann, muss diese Klasse initialisiert werden
 * 
 * @author BitCtrl Systems GmbH, Thierfelder
 * 
 * @version $Id$
 */
public final class AggregationsIntervall implements
		Comparable<AggregationsIntervall> {

	/**
	 * Millisekunden in einer Minute
	 */
	public static final long MILLIS_PER_MINUTE = (long) (60 * 1000);
	
	/**
	 * <code>asp.agregation1Minute</code>.
	 */
	public static AggregationsIntervall aGG1MINUTE;

	/**
	 * <code>asp.agregation5Minuten</code>.
	 */
	public static AggregationsIntervall aGG5MINUTE;

	/**
	 * <code>asp.agregation15Minuten</code>.
	 */
	public static AggregationsIntervall aGG15MINUTE;

	/**
	 * <code>asp.agregation30Minuten</code>.
	 */
	public static AggregationsIntervall aGG30MINUTE;

	/**
	 * <code>asp.agregation60Minuten</code>.
	 */
	public static AggregationsIntervall aGG60MINUTE;

	/**
	 * <code>asp.agregationDtvTag</code>.
	 */
	public static AggregationsIntervall aGGDTVTAG;

	/**
	 * <code>asp.agregationDtvMonat</code>.
	 */
	public static AggregationsIntervall aGGDTVMONAT;

	/**
	 * <code>asp.agregationDtvJahr</code>.
	 */
	public static AggregationsIntervall aGGDTVJAHR;

	/**
	 * der Wertebereich dieses Typs.
	 */
	private static final SortedSet<AggregationsIntervall> werteBereich = new TreeSet<>();

	/**
	 * die Datenbeschreibung der Publikationsdaten dieses Aggregations-
	 * Intervalls (fuer FS).
	 */
	private final DataDescription datenBeschreibungFs;

	/**
	 * die Datenbeschreibung der Publikationsdaten dieses Aggregations-
	 * Intervalls (fuer MQ).
	 */
	private final DataDescription datenBeschreibungMq;
	private final ZoneId _zone = ZoneId.systemDefault();

	/**
	 * die Laenge des Aggregationsintervalls in ms.
	 */
	private long intervallLaengeInMillis = -1;

	/**
	 * die maximale Anzahl der Elemente, die in einem Puffer mit Daten dieser
	 * Aggregationsstufe vorgehalten werden muessen.
	 */
	private final long maxPufferGroesse;

	/**
	 * Standardkonstruktor.
	 * 
	 * @param atgFs
	 *            die Attributgruppe der Publikationsdaten dieses Aggregations-
	 *            Intervalls (fuer FS)
	 * @param atgMq
	 *            die Attributgruppe der Publikationsdaten dieses Aggregations-
	 *            Intervalls (fuer MQ)
	 * @param asp
	 *            der Aspekt der Publikationsdaten dieses Aggregations-
	 *            Intervalls (fuer FS <b>und</b> MQ)
	 * @param intervall
	 *            die Laenge des Aggregationsintervalls in ms
	 * @param maxPufferGroesse
	 *            die maximale Anzahl der Elemente, die in einem Puffer mit
	 *            Daten dieser Aggregationsstufe vorgehalten werden muessen
	 */
	private AggregationsIntervall(final AttributeGroup atgFs,
			final AttributeGroup atgMq, final Aspect asp, final long intervall,
			final long maxPufferGroesse) {
		this.datenBeschreibungFs = new DataDescription(atgFs, asp);
		this.datenBeschreibungMq = new DataDescription(atgMq, asp);
		this.intervallLaengeInMillis = intervall;
		this.maxPufferGroesse = maxPufferGroesse;
		AggregationsIntervall.werteBereich.add(this);
	}

	/**
	 * Initialisiert die statischen Instanzen dieser Klasse.
	 * 
	 * @param dav
	 *            Verbindung zum Datenverteiler
	 */
	public static void initialisiere(final ClientDavInterface dav) {
		DataModel dataModel = dav.getDataModel();
		AggregationsIntervall.aGG1MINUTE = new AggregationsIntervall(dataModel.getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_FS), dataModel
				.getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_MQ), dataModel.getAspect("asp.agregation1Minute"), MILLIS_PER_MINUTE, 5);
		AggregationsIntervall.aGG5MINUTE = new AggregationsIntervall(dataModel.getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_FS), dataModel
				.getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_MQ), dataModel.getAspect("asp.agregation5Minuten"), 5 * MILLIS_PER_MINUTE, 3);
		AggregationsIntervall.aGG15MINUTE = new AggregationsIntervall(dataModel.getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_FS), dataModel
				.getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_MQ), dataModel.getAspect("asp.agregation15Minuten"), 15 * MILLIS_PER_MINUTE, 2);
		AggregationsIntervall.aGG30MINUTE = new AggregationsIntervall(dataModel.getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_FS), dataModel
				.getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_MQ), dataModel.getAspect("asp.agregation30Minuten"), 30 * MILLIS_PER_MINUTE, 2);
		
		// Daten, die in (D)TV-Daten eingehen benötigen den doppelten (eigentlich nötigen) Puffer
		AggregationsIntervall.aGG60MINUTE = new AggregationsIntervall(dataModel.getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_FS), dataModel
				.getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_MQ), dataModel.getAspect("asp.agregation60Minuten"), 60 * MILLIS_PER_MINUTE, 48);
		AggregationsIntervall.aGGDTVTAG = new AggregationsIntervall(dataModel.getAttributeGroup(DUAKonstanten.ATG_DTV_FS), dataModel
				.getAttributeGroup(DUAKonstanten.ATG_DTV_MQ), dataModel.getAspect("asp.agregationDtvTag"), 24 * 60 * MILLIS_PER_MINUTE, 62);
		AggregationsIntervall.aGGDTVMONAT = new AggregationsIntervall(dataModel.getAttributeGroup(DUAKonstanten.ATG_DTV_FS), dataModel
				.getAttributeGroup(DUAKonstanten.ATG_DTV_MQ), dataModel.getAspect("asp.agregationDtvMonat"), 28 * 24 * 60 * MILLIS_PER_MINUTE, 24);
		AggregationsIntervall.aGGDTVJAHR = new AggregationsIntervall(dataModel.getAttributeGroup(DUAKonstanten.ATG_DTV_FS), dataModel
				.getAttributeGroup(DUAKonstanten.ATG_DTV_MQ), dataModel.getAspect("asp.agregationDtvJahr"), 365 * 24 * 60 * MILLIS_PER_MINUTE, 3);
	}

	/**
	 * Erfragt die Menge aller statischen Instanzen dieser Klasse in sortierter Form:<br> - <code>asp.agregation1Minute</code>,<br> -
	 * <code>asp.agregation5Minuten</code>,<br> - <code>asp.agregation15Minuten</code>,<br> - <code>asp.agregation30Minuten</code>,<br> -
	 * <code>asp.agregation60Minuten</code>,<br> - <code>asp.agregationDtvMonat</code> und<br> - <code>asp.agregationDtvJahr</code>.<br> <br>
	 *
	 * @return die Menge aller statischen Instanzen dieser Klasse
	 */
	public static SortedSet<AggregationsIntervall> getInstanzen() {
		return AggregationsIntervall.werteBereich;
	}

	/**
	 * Erfragt die maximale Anzahl der Elemente, die in einem Puffer mit Daten dieser Aggregationsstufe vorgehalten werden muessen.
	 *
	 * @return die maximale Anzahl der Elemente, die in einem Puffer mit Daten dieser Aggregationsstufe vorgehalten werden muessen
	 */
	public long getMaxPufferGroesse() {
		return this.maxPufferGroesse;
	}

	/**
	 * Erfragt den Publikationsaspekt der Daten fuer FS und MQ.
	 *
	 * @return der Publikationsaspekt der Daten fuer FS und MQ
	 */
	public Aspect getAspekt() {
		return this.datenBeschreibungFs.getAspect();
	}

	/**
	 * Erfragt die Datenbeschreibung der Publikationsdaten dieses Aggregations- Intervalls.
	 *
	 * @param fuerFahrstreifen fuer Fahrstreifen?
	 * @return die Datenbeschreibung der Publikationsdaten dieses Aggregations- Intervalls
	 */
	public DataDescription getDatenBeschreibung(final boolean fuerFahrstreifen) {
		if(fuerFahrstreifen) {
			return this.datenBeschreibungFs;
		}
		return this.datenBeschreibungMq;
	}

	/**
	 * Erfragt, ob es sich bei diesem Intervall um ein DTV- bzw. TV-Intervall
	 * handelt
	 * 
	 * @return ob es sich bei diesem Intervall um ein DTV- bzw. TV-Intervall
	 *         handelt
	 */
	public boolean isDTVorTV() {
		return this.equals(AggregationsIntervall.aGGDTVJAHR)
				|| this.equals(AggregationsIntervall.aGGDTVMONAT)
				|| this.equals(AggregationsIntervall.aGGDTVTAG);
	}

	/**
	 * Erfragt, ob zum uebergebenen Zeitpunkt eine Aggregation notwendig ist.
	 *
	 * @param intervallEndeAlt Der Zeitpuntk bis zu dem bereits aggrgiert wurde
	 * @param intervallEnde Der Zeitpunkt, bis zu dem Daten vorliegen. Der zeitpunkt ist immer normiert auf ganze Erfassungsintervalle, bei einem
	 *                      Erfassungsintervall von 1 Minute liegt der übergebene Wert also immer auf einer ganzen Minute.    
	 *                         
	 * @return ob zum uebergebenen Zeitpunkt eine Aggregation notwendig ist
	 */
	public boolean isAggregationErforderlich(final long intervallEndeAlt, final long intervallEnde) {
		return getAggregationsZeitStempel(intervallEnde) > intervallEndeAlt;
	}

	/**
	 * Berechnet aus einem Zeitstempel das Ende des (ggf. davor liegenden) Aggregationsintervalls. Beispiel:
	 * 
	 * Parameter time = 12:13:44 und "this" == 5 Minuten, dann liefert die Methode als Ergebnis 12:10:00
	 * 
	 * Parameter time = 12:10:00 und "this" == 5 Minuten, dann liefert die Methode als Ergebnis das selbe 12:10:00 
	 * 
	 * @param time Zeitstempel in Millisekunden
	 * 
	 * @return Davor liegenden End-Zeitstempel des Aggregationsintervalls.
	 */
	public long getAggregationsZeitStempel(final long time) {
		LocalDateTime dateTime = Instant.ofEpochMilli(time).atZone(_zone).toLocalDateTime();
		if (this.equals(AggregationsIntervall.aGGDTVTAG)) {
			return dateTime.toLocalDate().atStartOfDay().atZone(_zone).toInstant().toEpochMilli();
		} else if (this.equals(AggregationsIntervall.aGGDTVMONAT)) {
			return dateTime.toLocalDate().withDayOfMonth(1).atStartOfDay().atZone(_zone).toInstant().toEpochMilli();
		} else if (this.equals(AggregationsIntervall.aGGDTVJAHR)) {
			return dateTime.toLocalDate().withDayOfYear(1).atStartOfDay().atZone(_zone).toInstant().toEpochMilli();
		}
		return time / intervallLaengeInMillis * intervallLaengeInMillis;
	}

	/**
	 * Gibt zu einem Intervallende den zugehörigen Intervallanfang zurück 
	 * @param intervallEnde Intervallende i.d.R. mit getAggregationsZeitStempel() erzeugt.
	 * @return Startzeitpunkt des Intervalls. Entspricht i.d.R. intervallEnde - intervallLaengeInMillis, aber da Tage, Monate und Jahre
	 * unterschiedlich lang sein können (Sommerzeit, Monatslängen, Schaltjahre) wird hier der exakte Wert berechnet.
	 */
	public long getStartZeitStempel(final long intervallEnde) {
		LocalDateTime dateTime = Instant.ofEpochMilli(intervallEnde).atZone(_zone).toLocalDateTime();
		if (this.equals(AggregationsIntervall.aGGDTVTAG)) {
			return dateTime.toLocalDate().atStartOfDay().minusDays(1).atZone(_zone).toInstant().toEpochMilli();
		} else if (this.equals(AggregationsIntervall.aGGDTVMONAT)) {
			return dateTime.toLocalDate().withDayOfMonth(1).minusMonths(1).atStartOfDay().atZone(_zone).toInstant().toEpochMilli();
		} else if (this.equals(AggregationsIntervall.aGGDTVJAHR)) {
			return dateTime.toLocalDate().withDayOfYear(1).minusYears(1).atStartOfDay().atZone(_zone).toInstant().toEpochMilli();
		}
		return intervallEnde - intervallLaengeInMillis;
	}
	
	/**
	 * Erfragt die Laenge des Aggregationsintervalls in ms.
	 * 
	 * @return die Laenge des Aggregationsintervalls in ms
	 */
	public long getIntervall() {
		return this.intervallLaengeInMillis;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int compareTo(final AggregationsIntervall that) {
		return new Long(this.getIntervall()).compareTo(that.getIntervall());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(final Object obj) {
		boolean ergebnis = false;

		if ((obj != null) && (obj instanceof AggregationsIntervall)) {
			final AggregationsIntervall that = (AggregationsIntervall) obj;

			ergebnis = this.getDatenBeschreibung(true).getAspect()
					.equals(that.getDatenBeschreibung(true).getAspect())
					&& this.getDatenBeschreibung(true)
							.getAttributeGroup()
							.equals(that.getDatenBeschreibung(true)
									.getAttributeGroup());
		}

		return ergebnis;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return this.datenBeschreibungFs + "\n" + this.datenBeschreibungMq;
	}
}
