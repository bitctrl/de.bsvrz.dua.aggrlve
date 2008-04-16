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
import java.util.HashMap;
import java.util.Map;

import com.bitctrl.Constants;

import de.bsvrz.dav.daf.main.ClientDavInterface;
import de.bsvrz.dav.daf.main.Data;
import de.bsvrz.dav.daf.main.ResultData;
import de.bsvrz.dav.daf.main.SenderRole;
import de.bsvrz.dav.daf.main.config.SystemObject;
import de.bsvrz.dua.guete.GWert;
import de.bsvrz.dua.guete.GueteException;
import de.bsvrz.dua.guete.GueteVerfahren;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAUtensilien;
import de.bsvrz.sys.funclib.bitctrl.dua.av.DAVSendeAnmeldungsVerwaltung;
import de.bsvrz.sys.funclib.debug.Debug;

/**
 * Abstraktes Objekt zur Aggregation von LVE-Daten fuer Fahrstreifen und
 * Messquerschnitte.
 * 
 * @author BitCtrl Systems GmbH, Thierfelder
 * 
 * @verison $Id$
 */
public abstract class AbstraktAggregationsObjekt {

	/**
	 * die restlichen auszufuellenden Attribute der Attributgruppen
	 * <code>atg.verkehrsDatenKurzZeitFs</code> bzw
	 * <code>atg.verkehrsDatenKurzZeitMq</code>, die innerhalb der
	 * FG1-Aggregation nicht erfasst werden.
	 */
	private static final String[][] REST_ATTRIBUTE_AGGR = new String[][] {
			new String[] { null, "BMax" }, //$NON-NLS-1$
			new String[] { null, "VDelta" }, //$NON-NLS-1$
			new String[] { "vgKfz", "VgKfz" }, //$NON-NLS-1$//$NON-NLS-2$
			new String[] { "sKfz", "SKfz" }, //$NON-NLS-1$//$NON-NLS-2$
			new String[] { "b", "B" }, //$NON-NLS-1$//$NON-NLS-2$
			new String[] { "aLkw", "ALkw" }, //$NON-NLS-1$//$NON-NLS-2$
			new String[] { "kKfz", "KKfz" }, //$NON-NLS-1$//$NON-NLS-2$
			new String[] { "kLkw", "KLkw" }, //$NON-NLS-1$//$NON-NLS-2$
			new String[] { "kPkw", "KPkw" }, //$NON-NLS-1$//$NON-NLS-2$
			new String[] { "qB", "QB" }, //$NON-NLS-1$//$NON-NLS-2$
			new String[] { "kB", "KB" } //$NON-NLS-1$//$NON-NLS-2$
	};

	/**
	 * die restlichen auszufuellenden Attribute der Attributgruppen
	 * <code>atg.verkehrsDatenKurzZeitFs</code> bzw
	 * <code>atg.verkehrsDatenKurzZeitMq</code>, die innerhalb der
	 * DTV-Berechnung nicht erfasst werden.
	 */
	private static final String[][] REST_ATTRIBUTE_DTV = new String[][] {
			new String[] { null, "BMax" }, //$NON-NLS-1$
			new String[] { null, "VDelta" }, //$NON-NLS-1$
			new String[] { "vKfz", "VKfz" }, //$NON-NLS-1$//$NON-NLS-2$
			new String[] { "vPkw", "VPkw" }, //$NON-NLS-1$//$NON-NLS-2$
			new String[] { "vLkw", "VLkw" }, //$NON-NLS-1$//$NON-NLS-2$
			new String[] { "vgKfz", "VgKfz" }, //$NON-NLS-1$//$NON-NLS-2$
			new String[] { "sKfz", "SKfz" }, //$NON-NLS-1$//$NON-NLS-2$
			new String[] { "b", "B" }, //$NON-NLS-1$//$NON-NLS-2$
			new String[] { "aLkw", "ALkw" }, //$NON-NLS-1$//$NON-NLS-2$
			new String[] { "kKfz", "KKfz" }, //$NON-NLS-1$//$NON-NLS-2$
			new String[] { "kLkw", "KLkw" }, //$NON-NLS-1$//$NON-NLS-2$
			new String[] { "kPkw", "KPkw" }, //$NON-NLS-1$//$NON-NLS-2$
			new String[] { "qB", "QB" }, //$NON-NLS-1$//$NON-NLS-2$
			new String[] { "kB", "KB" } //$NON-NLS-1$//$NON-NLS-2$
	};

	/**
	 * statische Verbindung zum Datenverteiler.
	 */
	protected ClientDavInterface dav = null;

	/**
	 * Datensender.
	 */
	protected DAVSendeAnmeldungsVerwaltung sender = null;

	/**
	 * Das Systemobjekt, das hier verwaltet wird.
	 */
	protected SystemObject objekt = null;

	/**
	 * Mapt ein Systemobjekt auf sein letztes von hier aus publiziertes Datum.
	 */
	protected Map<SystemObject, ResultData> letzteDaten = new HashMap<SystemObject, ResultData>();

	/**
	 * speichert alle historischen Daten dieses Aggregationsobjektes aller
	 * Aggregationsintervalle.
	 */
	protected AggregationsPufferMenge datenPuffer = null;

	/**
	 * Standardkonstruktor.
	 * 
	 * @param dav
	 *            Verbindung zum Datenverteiler
	 * @param obj
	 *            das Systemobjekt, das hier verwaltet wird
	 */
	public AbstraktAggregationsObjekt(final ClientDavInterface dav,
			final SystemObject obj) {
		this.dav = dav;
		this.objekt = obj;
		this.sender = new DAVSendeAnmeldungsVerwaltung(dav, SenderRole.source());
	}

	/**
	 * Sendet ein Datum (Sendet nie zwei Datensaetze ohne Nutzdaten
	 * hintereinander).
	 * 
	 * @param resultat
	 *            ein Datum
	 */
	protected final void sende(final ResultData resultat) {
		if (resultat.getData() == null) {
			ResultData letztesDatum = this.letzteDaten
					.get(resultat.getObject());
			if (letztesDatum != null && letztesDatum.getData() != null) {
				this.sender.sende(resultat);
				this.letzteDaten.put(resultat.getObject(), resultat);
			}
		} else {
			this.sender.sende(resultat);
			this.letzteDaten.put(resultat.getObject(), resultat);
		}
	}

	/**
	 * Fuellt den Rest des Datensatzes (alle Werte ausser <code>qPkw</code>,
	 * <code>qLkw</code>, <code>qKfz</code>, <code>vLkw</code>,
	 * <code>vKfz</code> und <code>vPkw</code>) mit Daten<br>
	 * ggf. (bei DTV-Werten) werden auch die Attribute <code>vLkw</code>,
	 * <code>vKfz</code> und <code>vPkw</code> gefuellt
	 * 
	 * @param resultat
	 *            zu versendendes Aggregationsdatum
	 * @param intervall
	 *            der Aggregationsintervall
	 */
	protected final void fuelleRest(ResultData resultat,
			AggregationsIntervall intervall) {
		String[][] REST_ATTRIBUTE = REST_ATTRIBUTE_AGGR;

		if (intervall.isDTVorTV()) {
			REST_ATTRIBUTE = REST_ATTRIBUTE_DTV;
		}

		if (this.isFahrstreifen()) {
			resultat.getData()
					.getTimeValue("T").setMillis(intervall.getIntervall()); //$NON-NLS-1$
		}

		for (int i = 0; i < REST_ATTRIBUTE.length; i++) {
			String attributName = REST_ATTRIBUTE[i][1];
			if (this.isFahrstreifen()) {
				attributName = REST_ATTRIBUTE[i][0];
			}

			if (attributName != null) {
				resultat.getData().getItem(attributName).getUnscaledValue(
						"Wert").set(DUAKonstanten.NICHT_ERMITTELBAR); //$NON-NLS-1$
				resultat
						.getData()
						.getItem(attributName)
						.getItem("Status").getItem("Erfassung"). //$NON-NLS-1$//$NON-NLS-2$
						getUnscaledValue("NichtErfasst").set(DUAKonstanten.NEIN); //$NON-NLS-1$
				resultat.getData().getItem(attributName)
						.getItem("Status").getItem("MessWertErsetzung"). //$NON-NLS-1$//$NON-NLS-2$
						getUnscaledValue("Implausibel").set(DUAKonstanten.NEIN); //$NON-NLS-1$
				resultat
						.getData()
						.getItem(attributName)
						.getItem("Status").getItem("MessWertErsetzung"). //$NON-NLS-1$//$NON-NLS-2$
						getUnscaledValue("Interpoliert").set(DUAKonstanten.NEIN); //$NON-NLS-1$

				resultat.getData().getItem(attributName)
						.getItem("Status").getItem("PlFormal"). //$NON-NLS-1$ //$NON-NLS-2$
						getUnscaledValue("WertMax").set(DUAKonstanten.NEIN); //$NON-NLS-1$
				resultat.getData().getItem(attributName)
						.getItem("Status").getItem("PlFormal"). //$NON-NLS-1$ //$NON-NLS-2$
						getUnscaledValue("WertMin").set(DUAKonstanten.NEIN); //$NON-NLS-1$

				resultat
						.getData()
						.getItem(attributName)
						.getItem("Status").getItem("PlLogisch"). //$NON-NLS-1$ //$NON-NLS-2$
						getUnscaledValue("WertMaxLogisch").set(DUAKonstanten.NEIN); //$NON-NLS-1$
				resultat
						.getData()
						.getItem(attributName)
						.getItem("Status").getItem("PlLogisch"). //$NON-NLS-1$ //$NON-NLS-2$
						getUnscaledValue("WertMinLogisch").set(DUAKonstanten.NEIN); //$NON-NLS-1$
				resultat.getData().getItem(attributName)
						.getItem("Güte").getUnscaledValue("Index"). //$NON-NLS-1$ //$NON-NLS-2$
						set(DUAKonstanten.NICHT_ERMITTELBAR);
				resultat.getData().getItem(attributName)
						.getItem("Güte").getUnscaledValue("Verfahren"). //$NON-NLS-1$ //$NON-NLS-2$
						set(GueteVerfahren.STANDARD.getCode());
			}
		}
	}

	/**
	 * Erfragt den Datenpuffer dieses Objektes.
	 * 
	 * @return der Datenpuffer dieses Objektes
	 */
	public final AggregationsPufferMenge getPuffer() {
		return this.datenPuffer;
	}

	/**
	 * Ausgefallene Werte werden hier durch den Mittelwert der vorhandenen Werte
	 * ersetzt. Um die Zuverlässigkeit der Daten nachvollziehen zu können, ist
	 * jeder aggregierte Wert mit einem Güteindex in % anzugeben. Der Güteindex
	 * wird durch arithmetische Mittelung der Güteindizes der zu aggregierenden
	 * Daten bestimmt. Der Güteindex von ausgefallenen Werten ergibt sich dabei
	 * aus dem Mittelwert der vorhandenen Werte multipliziert mit einem
	 * parametrierbaren Faktor. Des weiteren ist jeder aggregierte Wert mit
	 * einer Kennung zu versehen, ob zur Aggregation interpolierte (durch die
	 * Messwertersetzung generierte) Werte verwendet wurden.
	 * 
	 * @param attribut
	 *            das Attribut, fuer das Daten gesucht werden
	 * @param quellDaten
	 *            die aus dem Puffer ausgelesenen Daten (darf keine leere Liste
	 *            sein)
	 * @param intervall
	 *            das Aggregationsintervall, fuer dass Daten aus den
	 *            uebergebenen Quelldaten errechnet werden sollen
	 * @param zeitStempel
	 *            der Zeitstempel, fuer den das Datum fuer das uebergebene
	 *            Intervall aggregiert werden soll (gilt eigentlich nur fuer
	 *            Tagesdaten, da die Anzahl der Stunden pro Tag zwischen 23 und
	 *            25 schwankt)
	 * @return eine Menge mit so vielen Attribut-Datensaetzen, wie fuer dieses
	 *         Intervall zur Verfuegung stehen muessen
	 */
	@Deprecated
	protected final Collection<AggregationsAttributWert> ersetzteAusgefalleneWerte(
			final AggregationsAttribut attribut,
			final Collection<AggregationsDatum> quellDaten,
			final AggregationsIntervall intervall, final long zeitStempel) {
		Collection<AggregationsAttributWert> zielDaten = new ArrayList<AggregationsAttributWert>();

		double anzahlSoll = -1;
		if (intervall.equals(AggregationsIntervall.AGG_1MINUTE)
				|| intervall.equals(AggregationsIntervall.AGG_5MINUTE)
				|| intervall.equals(AggregationsIntervall.AGG_15MINUTE)
				|| intervall.equals(AggregationsIntervall.AGG_30MINUTE)
				|| intervall.equals(AggregationsIntervall.AGG_60MINUTE)) {
			anzahlSoll = (double) intervall.getIntervall()
					/ (double) quellDaten.iterator().next().getT();
		} else if (intervall.equals(AggregationsIntervall.AGG_DTV_TAG)) {
			anzahlSoll = DUAUtensilien.getStundenVonTag(zeitStempel);
		} else if (intervall.equals(AggregationsIntervall.AGG_DTV_MONAT)) {
			anzahlSoll = 12;
		}

		if (anzahlSoll > 0) {
			long wertSumme = 0;
			long wertAnzahl = 0;
			double gueteSumme = 0;
			for (AggregationsDatum quellDatum : quellDaten) {
				AggregationsAttributWert wert = quellDatum.getWert(attribut);
				if (wert.getWert() >= 0) {
					gueteSumme += wert.getGuete().getIndexUnskaliert() >= 0 ? wert
							.getGuete().getIndex()
							: 0;
					wertSumme += wert.getWert();
					wertAnzahl++;
					zielDaten.add(wert);
				}
			}

			long mittelWert = 0;
			double mittelWertGuete = 0.0;
			if (wertAnzahl > 0) {
				mittelWert = Math.round((double) wertSumme
						/ (double) wertAnzahl);
				mittelWertGuete = (double) gueteSumme / (double) wertAnzahl;
			}

			boolean first = false;
			long zielDatenSoll = (long) anzahlSoll;
			if (anzahlSoll - (double) zielDatenSoll > 0.00005) {
				zielDatenSoll++;
				first = true;
			}
			if (zielDatenSoll - zielDaten.size() > 0) {
				for (int i = 0; i < zielDatenSoll - zielDaten.size(); i++) {
					if (wertAnzahl > 0) {
						if (attribut.isGeschwindigkeitsAttribut()) {
							zielDaten.add(new AggregationsAttributWert(
									attribut, mittelWert, AggregationLVE.GUETE
											* mittelWertGuete));
						} else {
							/**
							 * "halber" Q-Wert muss noch gewichtet werden
							 */
							if (first) {
								long teilMittelWert = Math
										.round((double) mittelWert
												* (double) (anzahlSoll - (long) anzahlSoll));
								zielDaten
										.add(new AggregationsAttributWert(
												attribut, teilMittelWert,
												AggregationLVE.GUETE
														* mittelWertGuete));
							} else {
								zielDaten
										.add(new AggregationsAttributWert(
												attribut, mittelWert,
												AggregationLVE.GUETE
														* mittelWertGuete));
							}
							first = false;
						}
					} else {
						zielDaten.add(new AggregationsAttributWert(attribut,
								DUAKonstanten.NICHT_ERMITTELBAR, 0));
					}
				}
			}
		} else {
			Debug
					.getLogger()
					.warning(
							"Die Anzahl der benoetigten Intervalle sollte nicht kleiner 0 sein"); //$NON-NLS-1$
		}

		return zielDaten;
	}

	/**
	 * Erfragt, ob fuer dieses Objekt ein Datum des uebergebenen
	 * Aggregationsintervalls fuer den uebergebenen Zeitpunkt berechnet werden
	 * muss<br>
	 * <b>Achtung:</b> Fuer die Intervalle 1 - 60Minuten ist eine Berechnung
	 * immer notwendig. Fuer alle anderen Intervalle ist eine Berechung nur dann
	 * notwendig, wenn nicht schon ein entsprechendes Datum im Archiv steht
	 * (bzw. berechnet wurde)
	 * 
	 * @param zeitStempel
	 *            ein Zeitpunkt
	 * @param intervall
	 *            der Aggregationsintervall
	 * @return ob fuer dieses Objekt ein Datum des uebergebenen
	 *         Aggregationsintervalls fuer den uebergebenen Zeitpunkt berechnet
	 *         werden muss
	 */
	protected final boolean isBerechnungNotwendig(final long zeitStempel,
			final AggregationsIntervall intervall) {
		boolean berechnen = false;

		if (intervall.equals(AggregationsIntervall.AGG_1MINUTE)
				|| intervall.equals(AggregationsIntervall.AGG_5MINUTE)
				|| intervall.equals(AggregationsIntervall.AGG_15MINUTE)
				|| intervall.equals(AggregationsIntervall.AGG_30MINUTE)
				|| intervall.equals(AggregationsIntervall.AGG_60MINUTE)) {
			berechnen = true;
		} else {
			if (!this.isFahrstreifen()) {
				AbstraktAggregationsPuffer puffer = this.datenPuffer
						.getPuffer(intervall);
				if (puffer != null) {
					berechnen = puffer.getDatenFuerZeitraum(zeitStempel,
							zeitStempel + 1000).isEmpty();
				} else {
					Debug.getLogger().warning(
							"Datenpuffer fuer " + this.objekt + " und " + //$NON-NLS-1$ //$NON-NLS-2$
									intervall
									+ " konnte nicht ermittelt werden"); //$NON-NLS-1$
				}
			}
		}

		return berechnen;
	}

	/**
	 * Aggregiert einen arithmetischen Mittelwert.
	 * 
	 * @param attribut
	 *            das Attribut, das berechnet werden soll
	 * @param nutzDatum
	 *            das gesamte Aggregationsdatum (veraenderbar)
	 * @param basisDaten
	 *            die der Aggregation zu Grunde liegenden Daten
	 * @param zeitStempel
	 *            der Zeitstempel, mit dem die aggregierten Daten veröffentlicht
	 *            werden sollen
	 * @param intervall
	 *            das gewuenschte Aggregationsintervall
	 */
	protected final void aggregiereMittel(AggregationsAttribut attribut,
			Data nutzDatum, Collection<AggregationsDatum> basisDaten,
			long zeitStempel, AggregationsIntervall intervall) {

		boolean interpoliert = false;
		boolean nichtErfasst = false;
		long anzahl = 0;
		long summe = 0;
		Collection<GWert> gueteWerte = new ArrayList<GWert>();
		for (AggregationsDatum basisDatum : basisDaten) {
			AggregationsAttributWert basisWert = basisDatum.getWert(attribut);
			if (basisWert.getWert() >= 0
					|| basisWert.getWert() == DUAKonstanten.NICHT_ERMITTELBAR) {
				summe += (basisWert.getWert() == DUAKonstanten.FEHLERHAFT ? 0
						: basisWert.getWert());
				anzahl++;
				gueteWerte.add(basisWert.getGuete());
				interpoliert |= basisWert.isInterpoliert();
				nichtErfasst |= basisWert.isNichtErfasst();
			}
		}

		AggregationsAttributWert exportWert = new AggregationsAttributWert(
				attribut, DUAKonstanten.NICHT_ERMITTELBAR_BZW_FEHLERHAFT, 0);
		if (anzahl > 0) {
			exportWert.setWert(Math.round((double) summe / (double) anzahl));
			exportWert.setInterpoliert(interpoliert);
			if (AggregationLVE.NICHT_ERFASST) {
				exportWert.setNichtErfasst(nichtErfasst);
			}
			try {
				exportWert.setGuete(GueteVerfahren.summe(gueteWerte
						.toArray(new GWert[0])));
			} catch (GueteException e) {
				Debug.getLogger().warning(
						"Guete von " + this.objekt + " fuer " + //$NON-NLS-1$ //$NON-NLS-2$
								attribut + " konnte nicht berechnet werden", e); //$NON-NLS-1$
				e.printStackTrace();
			}
		}

		exportWert.exportiere(nutzDatum, this.isFahrstreifen());
	}

	/**
	 * Berechnet eine Summe der uebergebenen Werte.
	 * 
	 * @param attribut
	 *            das Attribut, das berechnet werden soll
	 * @param nutzDatum
	 *            das gesamte Aggregationsdatum (dieses muss veraenderbar sein
	 *            und wird hier gefuellt)
	 * @param basisDaten
	 *            die der Aggregation zu Grunde liegenden Daten
	 * @param zeitStempel
	 *            der Zeitstempel, mit dem die aggregierten Daten veröffentlicht
	 *            werden sollen
	 * @param intervall
	 *            das gewuenschte Aggregationsintervall
	 */
	protected final void aggregiereSumme(AggregationsAttribut attribut,
			Data nutzDatum, Collection<AggregationsDatum> basisDaten,
			long zeitStempel, AggregationsIntervall intervall) {
		double anzahlSoll = -1;
		if (intervall.equals(AggregationsIntervall.AGG_DTV_TAG)) {
			anzahlSoll = DUAUtensilien.getStundenVonTag(zeitStempel + 2
					* Constants.MILLIS_PER_HOUR);
		}

		boolean interpoliert = false;
		boolean nichtErfasst = false;
		long summe = 0;
		double anzahlIst = 0;
		Collection<GWert> gueteWerte = new ArrayList<GWert>();
		for (AggregationsDatum basisDatum : basisDaten) {
			AggregationsAttributWert basisWert = basisDatum.getWert(attribut);
			if (basisWert.getWert() >= 0) {
				summe += basisWert.getWert();
				anzahlIst += 1.0;
				gueteWerte.add(basisWert.getGuete());
				interpoliert |= basisWert.isInterpoliert();
				nichtErfasst |= basisWert.isNichtErfasst();
			}
		}

		AggregationsAttributWert exportWert = new AggregationsAttributWert(
				attribut, DUAKonstanten.NICHT_ERMITTELBAR_BZW_FEHLERHAFT, 0);
		if (anzahlIst > 0) {
			exportWert.setWert(Math.round((double) summe
					* (AggregationLVE.APPROX_REST ? anzahlSoll / anzahlIst
							: 1.0)));
			exportWert.setInterpoliert(interpoliert);
			if (AggregationLVE.NICHT_ERFASST) {
				exportWert.setNichtErfasst(nichtErfasst);
			}
			try {
				exportWert.setGuete(GueteVerfahren.summe(gueteWerte
						.toArray(new GWert[0])));
			} catch (GueteException e) {
				Debug.getLogger().warning(
						"Guete von " + this.objekt + " fuer " + //$NON-NLS-1$ //$NON-NLS-2$
								attribut + " konnte nicht berechnet werden", e); //$NON-NLS-1$
				e.printStackTrace();
			}
		}

		exportWert.exportiere(nutzDatum, this.isFahrstreifen());
	}

	/**
	 * Erfragt das Systemobjekt.
	 * 
	 * @return das Systemobjekt
	 */
	public final SystemObject getObjekt() {
		return this.objekt;
	}

	/**
	 * Startet die Aggregation von Daten.
	 * 
	 * @param zeitStempel
	 *            der Zeitstempel, mit dem die aggregierten Daten veröffentlicht
	 *            werden sollen
	 * @param intervall
	 *            der Intervall der aggregierten Daten (auch der
	 *            Publikationsaspekt)
	 */
	public abstract void aggregiere(final long zeitStempel,
			final AggregationsIntervall intervall);

	/**
	 * Erfragt, ob es sich bei dem hier verwalteten Objekt um eine Objekt vom
	 * Typ <code>typ.fahrStreifen</code> handelt.
	 * 
	 * @return ob es sich bei dem hier verwalteten Objekt um eine Objekt vom Typ
	 *         <code>typ.fahrStreifen</code> handelt
	 */
	protected abstract boolean isFahrstreifen();

}
