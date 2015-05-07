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

package de.bsvrz.dua.aggrlve;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.SortedSet;
import java.util.TreeSet;

import com.bitctrl.Constants;

import de.bsvrz.dav.daf.main.ClientDavInterface;
import de.bsvrz.dav.daf.main.DataDescription;
import de.bsvrz.dav.daf.main.config.Aspect;
import de.bsvrz.dav.daf.main.config.AttributeGroup;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;

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
 * <b>Achtung:</b> Bevor auf die statischen Member dieser Klasse zugegriffen werden kann, muss diese
 * Klasse initialisiert werden
 *
 * @author BitCtrl Systems GmbH, Thierfelder
 */
public final class AggregationsIntervall implements Comparable<AggregationsIntervall> {

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
	private static SortedSet<AggregationsIntervall> werteBereich = new TreeSet<>();

	/**
	 * die Datenbeschreibung der Publikationsdaten dieses Aggregations- Intervalls (fuer FS).
	 */
	private final DataDescription datenBeschreibungFs;

	/**
	 * die Datenbeschreibung der Publikationsdaten dieses Aggregations- Intervalls (fuer MQ).
	 */
	private final DataDescription datenBeschreibungMq;

	/**
	 * die Laenge des Aggregationsintervalls in ms.
	 */
	private long intervallLaengeInMillis = -1;

	/**
	 * die maximale Anzahl der Elemente, die in einem Puffer mit Daten dieser Aggregationsstufe
	 * vorgehalten werden muessen.
	 */
	private final long maxPufferGroesse;

	/**
	 * Standardkonstruktor.
	 *
	 * @param atgFs
	 *            die Attributgruppe der Publikationsdaten dieses Aggregations- Intervalls (fuer FS)
	 * @param atgMq
	 *            die Attributgruppe der Publikationsdaten dieses Aggregations- Intervalls (fuer MQ)
	 * @param asp
	 *            der Aspekt der Publikationsdaten dieses Aggregations- Intervalls (fuer FS
	 *            <b>und</b> MQ)
	 * @param intervall
	 *            die Laenge des Aggregationsintervalls in ms
	 * @param maxPufferGroesse
	 *            die maximale Anzahl der Elemente, die in einem Puffer mit Daten dieser
	 *            Aggregationsstufe vorgehalten werden muessen
	 */
	private AggregationsIntervall(final AttributeGroup atgFs, final AttributeGroup atgMq,
			final Aspect asp, final long intervall, final long maxPufferGroesse) {
		datenBeschreibungFs = new DataDescription(atgFs, asp);
		datenBeschreibungMq = new DataDescription(atgMq, asp);
		intervallLaengeInMillis = intervall;
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
		AggregationsIntervall.aGG1MINUTE = new AggregationsIntervall(
				dav.getDataModel().getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_FS),
				dav.getDataModel().getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_MQ),
				dav.getDataModel().getAspect("asp.agregation1Minute"), Constants.MILLIS_PER_MINUTE,
				5);

		AggregationsIntervall.aGG5MINUTE = new AggregationsIntervall(
				dav.getDataModel().getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_FS),
				dav.getDataModel().getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_MQ),
				dav.getDataModel().getAspect("asp.agregation5Minuten"),
				5 * Constants.MILLIS_PER_MINUTE, 3);
		AggregationsIntervall.aGG15MINUTE = new AggregationsIntervall(
				dav.getDataModel().getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_FS),
				dav.getDataModel().getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_MQ),
				dav.getDataModel().getAspect("asp.agregation15Minuten"),
				15 * Constants.MILLIS_PER_MINUTE, 2);
		AggregationsIntervall.aGG30MINUTE = new AggregationsIntervall(
				dav.getDataModel().getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_FS),
				dav.getDataModel().getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_MQ),
				dav.getDataModel().getAspect("asp.agregation30Minuten"),
				30 * Constants.MILLIS_PER_MINUTE, 2);
		AggregationsIntervall.aGG60MINUTE = new AggregationsIntervall(
				dav.getDataModel().getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_FS),
				dav.getDataModel().getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_MQ),
				dav.getDataModel().getAspect("asp.agregation60Minuten"),
				60 * Constants.MILLIS_PER_MINUTE, 40);

		AggregationsIntervall.aGGDTVTAG = new AggregationsIntervall(
				dav.getDataModel().getAttributeGroup(DUAKonstanten.ATG_DTV_FS),
				dav.getDataModel().getAttributeGroup(DUAKonstanten.ATG_DTV_MQ),
				dav.getDataModel().getAspect("asp.agregationDtvTag"),
				60 * 24 * Constants.MILLIS_PER_MINUTE, 50);
		AggregationsIntervall.aGGDTVMONAT = new AggregationsIntervall(
				dav.getDataModel().getAttributeGroup(DUAKonstanten.ATG_DTV_FS),
				dav.getDataModel().getAttributeGroup(DUAKonstanten.ATG_DTV_MQ),
				dav.getDataModel().getAspect("asp.agregationDtvMonat"),
				61 * 24 * Constants.MILLIS_PER_MINUTE, 15);
		AggregationsIntervall.aGGDTVJAHR = new AggregationsIntervall(
				dav.getDataModel().getAttributeGroup(DUAKonstanten.ATG_DTV_FS),
				dav.getDataModel().getAttributeGroup(DUAKonstanten.ATG_DTV_MQ),
				dav.getDataModel().getAspect("asp.agregationDtvJahr"),
				62 * 24 * Constants.MILLIS_PER_MINUTE, 0);
	}

	/**
	 * Erfragt die Menge aller statischen Instanzen dieser Klasse in sortierter Form:<br>
	 * - <code>asp.agregation1Minute</code>,<br>
	 * - <code>asp.agregation5Minuten</code>,<br>
	 * - <code>asp.agregation15Minuten</code>,<br>
	 * - <code>asp.agregation30Minuten</code>,<br>
	 * - <code>asp.agregation60Minuten</code>,<br>
	 * - <code>asp.agregationDtvMonat</code> und<br>
	 * - <code>asp.agregationDtvJahr</code>.<br>
	 * <br>
	 *
	 * @return die Menge aller statischen Instanzen dieser Klasse
	 */
	public static SortedSet<AggregationsIntervall> getInstanzen() {
		return AggregationsIntervall.werteBereich;
	}

	/**
	 * Erfragt die maximale Anzahl der Elemente, die in einem Puffer mit Daten dieser
	 * Aggregationsstufe vorgehalten werden muessen.
	 *
	 * @return die maximale Anzahl der Elemente, die in einem Puffer mit Daten dieser
	 *         Aggregationsstufe vorgehalten werden muessen
	 */
	public long getMaxPufferGroesse() {
		return maxPufferGroesse;
	}

	/**
	 * Erfragt den Publikationsaspekt der Daten fuer FS und MQ.
	 *
	 * @return der Publikationsaspekt der Daten fuer FS und MQ
	 */
	public Aspect getAspekt() {
		return datenBeschreibungFs.getAspect();
	}

	/**
	 * Erfragt die Datenbeschreibung der Publikationsdaten dieses Aggregations- Intervalls.
	 *
	 * @param fuerFahrstreifen
	 *            fuer Fahrstreifen?
	 * @return die Datenbeschreibung der Publikationsdaten dieses Aggregations- Intervalls
	 */
	public DataDescription getDatenBeschreibung(final boolean fuerFahrstreifen) {
		if (fuerFahrstreifen) {
			return datenBeschreibungFs;
		}
		return datenBeschreibungMq;
	}

	/**
	 * Erfragt das naechstkleinere Aggregationsintervall.
	 *
	 * @return das naechstkleinere Aggregationsintervall oder <code>null</code>, wenn vom Intervall
	 *         <code>eine Minute</code> aus gesucht wird
	 */
	public AggregationsIntervall getVorgaenger() {
		AggregationsIntervall vorgaenger = null;

		for (final AggregationsIntervall intervall : AggregationsIntervall.getInstanzen()) {
			if (equals(intervall)) {
				break;
			}
			vorgaenger = intervall;
		}

		return vorgaenger;
	}

	/**
	 * Erfragt den Zeitstempel des Aggregationsdatums, das zum uebergebenen Zeitpunkt fuer dieses
	 * Aggregationsintervall berechnet werden sollte<br>
	 * <b>Achtung:</b> Die Methode geht davon aus, dass sie nur einmal in der Minute aufgerufen
	 * wird!
	 *
	 * @param zeitpunkt
	 *            ein Zeitpunkt
	 * @return der Zeitstempel des Aggregationsdatums, das zum uebergebenen Zeitpunkt fuer dieses
	 *         Aggregationsintervall berechnet werden sollte oder <code>-1</code>, wenn zum
	 *         uebergebenen Zeitpunkt keine Aggregation notwendig ist
	 */
	public long getAggregationZeitStempel(final long zeitpunkt) {
		long zeitStempel = -1;
		final GregorianCalendar cal = new GregorianCalendar();
		cal.setTimeInMillis(zeitpunkt);
		final long minuteJetzt = cal.get(Calendar.MINUTE);

		if (equals(AggregationsIntervall.aGG1MINUTE) || equals(AggregationsIntervall.aGG5MINUTE)
				|| equals(AggregationsIntervall.aGG15MINUTE)
				|| equals(AggregationsIntervall.aGG30MINUTE)
				|| equals(AggregationsIntervall.aGG60MINUTE)) {
			final long intervallLaengeInMinuten = getIntervall() / Constants.MILLIS_PER_MINUTE;
			if ((minuteJetzt % intervallLaengeInMinuten) == 0) {
				cal.add(Calendar.MINUTE, (int) (-1 * intervallLaengeInMinuten));
				cal.set(Calendar.SECOND, 0);
				cal.set(Calendar.MILLISECOND, 0);
				zeitStempel = cal.getTimeInMillis();
			}
		} else if (equals(AggregationsIntervall.aGGDTVTAG)) {
			/**
			 * Versuche noch 12 Stunden im neuen Tag DTV-Tag des Vorgaengertages zu berechnen
			 */
			final long stundeJetzt = cal.get(Calendar.HOUR_OF_DAY);
			if ((stundeJetzt < 12) && (minuteJetzt == 1)) {
				cal.add(Calendar.DAY_OF_YEAR, -1);
				cal.set(Calendar.HOUR_OF_DAY, 0);
				cal.set(Calendar.MINUTE, 0);
				cal.set(Calendar.SECOND, 0);
				cal.set(Calendar.MILLISECOND, 0);
				zeitStempel = cal.getTimeInMillis();
			}
		} else if (equals(AggregationsIntervall.aGGDTVJAHR)) {
			final long stundeJetzt = cal.get(Calendar.HOUR_OF_DAY);
			final long tagJetzt = cal.get(Calendar.DAY_OF_YEAR);
			/**
			 * Versuche noch 30 Tage im neuen Jahr DTV-Jahr des Vorgaengerjahres zu berechnen
			 */
			if ((tagJetzt < 30) && (stundeJetzt == 0) && (minuteJetzt == 1)) {
				cal.add(Calendar.YEAR, -1);
				cal.set(Calendar.DAY_OF_YEAR, 1);
				cal.set(Calendar.MINUTE, 0);
				cal.set(Calendar.SECOND, 0);
				cal.set(Calendar.MILLISECOND, 0);
				zeitStempel = cal.getTimeInMillis();
			}
		} else if (equals(AggregationsIntervall.aGGDTVMONAT)) {
			/**
			 * Versuche noch 20 Tage im neuen Monat DTV-Monat des Vorgaengermonats zu berechnen
			 */
			final long stundeJetzt = cal.get(Calendar.HOUR_OF_DAY);
			final long tagJetzt = cal.get(Calendar.DAY_OF_MONTH);
			if ((tagJetzt < 20) && (stundeJetzt == 0) && (minuteJetzt == 1)) {
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
	public boolean isDTVorTV() {
		return equals(AggregationsIntervall.aGGDTVJAHR) || equals(AggregationsIntervall.aGGDTVMONAT)
				|| equals(AggregationsIntervall.aGGDTVTAG);
	}

	/**
	 * Erfragt, ob zum uebergebenen Zeitpunkt eine Aggregation notwendig ist.
	 *
	 * @param zeitpunkt
	 *            der Zeitpunkt
	 * @return ob zum uebergebenen Zeitpunkt eine Aggregation notwendig ist
	 */
	public boolean isAggregationErforderlich(final long zeitpunkt) {
		return getAggregationZeitStempel(zeitpunkt) != -1;
	}

	/**
	 * Erfragt die Laenge des Aggregationsintervalls in ms.
	 *
	 * @return die Laenge des Aggregationsintervalls in ms
	 */
	public long getIntervall() {
		return intervallLaengeInMillis;
	}

	@Override
	public int compareTo(final AggregationsIntervall that) {
		return new Long(getIntervall()).compareTo(that.getIntervall());
	}

	@Override
	public boolean equals(final Object obj) {
		boolean ergebnis = false;

		if ((obj != null) && (obj instanceof AggregationsIntervall)) {
			final AggregationsIntervall that = (AggregationsIntervall) obj;

			ergebnis = getDatenBeschreibung(true).getAspect()
					.equals(that.getDatenBeschreibung(true).getAspect())
					&& getDatenBeschreibung(true).getAttributeGroup()
							.equals(that.getDatenBeschreibung(true).getAttributeGroup());
		}

		return ergebnis;
	}

	@Override
	public String toString() {
		return datenBeschreibungFs + "\n" + datenBeschreibungMq;
	}

}
