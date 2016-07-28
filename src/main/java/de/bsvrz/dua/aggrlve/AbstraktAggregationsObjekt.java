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
import de.bsvrz.dav.daf.main.Data;
import de.bsvrz.dav.daf.main.ResultData;
import de.bsvrz.dav.daf.main.SenderRole;
import de.bsvrz.dav.daf.main.config.SystemObject;
import de.bsvrz.dua.dalve.analyse.lib.AnalyseAttribut;
import de.bsvrz.dua.guete.GWert;
import de.bsvrz.dua.guete.GueteException;
import de.bsvrz.dua.guete.GueteVerfahren;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;
import de.bsvrz.sys.funclib.bitctrl.dua.av.DAVSendeAnmeldungsVerwaltung;
import de.bsvrz.sys.funclib.debug.Debug;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Abstraktes Objekt zur Aggregation von LVE-Daten fuer Fahrstreifen und
 * Messquerschnitte.
 * 
 * @author BitCtrl Systems GmbH, Thierfelder
 */
public abstract class AbstraktAggregationsObjekt {

	/**
	 * die restlichen auszufuellenden Attribute der Attributgruppen
	 * <code>atg.verkehrsDatenKurzZeitFs</code> bzw
	 * <code>atg.verkehrsDatenKurzZeitMq</code>, die innerhalb der
	 * FG1-Aggregation nicht erfasst werden.
	 */
	private static final String[][] REST_ATTRIBUTE_AGGR = {
			new String[] { null, "BMax" }, new String[] { null, "VDelta" },
			new String[] { "vgKfz", "VgKfz" }, new String[] { "sKfz", "SKfz" },
			new String[] { "b", "B" }};

	/**
	 * die restlichen auszufuellenden Attribute der Attributgruppen
	 * <code>atg.verkehrsDatenKurzZeitFs</code> bzw
	 * <code>atg.verkehrsDatenKurzZeitMq</code>, die innerhalb der
	 * DTV-Berechnung nicht erfasst werden.
	 */
	private static final String[][] REST_ATTRIBUTE_DTV = {
			new String[] { null, "BMax" }, new String[] { null, "VDelta" },
			new String[] { "vKfz", "VKfz" }, new String[] { "vPkw", "VPkw" },
			new String[] { "vLkw", "VLkw" }, new String[] { "vgKfz", "VgKfz" },
			new String[] { "sKfz", "SKfz" }, new String[] { "b", "B" },
			new String[] { "aLkw", "ALkw" }, new String[] { "kKfz", "KKfz" },
			new String[] { "kLkw", "KLkw" }, new String[] { "kPkw", "KPkw" },
			new String[] { "qB", "QB" }, new String[] { "kB", "KB" } };

	/** statische Verbindung zum Datenverteiler. */
	protected ClientDavInterface dav;

	/*** Datensender. */
	protected DAVSendeAnmeldungsVerwaltung sender;

	/*** Das Systemobjekt, das hier verwaltet wird. */
	protected SystemObject objekt;

	/** Mapt ein Systemobjekt auf sein letztes von hier aus publiziertes Datum. */
	protected Map<SystemObject, ResultData> letzteDaten = new HashMap<>();

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
			final ResultData letztesDatum = this.letzteDaten.get(resultat
					.getObject());
			if ((letztesDatum != null) && (letztesDatum.getData() != null)) {
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
	protected final void fuelleRest(final ResultData resultat,
			final AggregationsIntervall intervall) {
		String[][] restAttribute = AbstraktAggregationsObjekt.REST_ATTRIBUTE_AGGR;

		if (intervall.isDTVorTV()) {
			restAttribute = AbstraktAggregationsObjekt.REST_ATTRIBUTE_DTV;
		}

		if (this.isFahrstreifen()) {
			resultat.getData().getTimeValue("T")
					.setMillis(intervall.getIntervall());
		}

		for (final String[] element : restAttribute) {
			String attributName = element[1];
			if (this.isFahrstreifen()) {
				attributName = element[0];
			}

			if (attributName != null) {
				resultat.getData().getItem(attributName)
						.getUnscaledValue("Wert")
						.set(DUAKonstanten.NICHT_ERMITTELBAR);
				resultat.getData().getItem(attributName).getItem("Status")
						.getItem("Erfassung").getUnscaledValue("NichtErfasst")
						.set(DUAKonstanten.NEIN);
				resultat.getData().getItem(attributName).getItem("Status")
						.getItem("MessWertErsetzung")
						.getUnscaledValue("Implausibel")
						.set(DUAKonstanten.NEIN);
				resultat.getData().getItem(attributName).getItem("Status")
						.getItem("MessWertErsetzung")
						.getUnscaledValue("Interpoliert")
						.set(DUAKonstanten.NEIN);

				resultat.getData().getItem(attributName).getItem("Status")
						.getItem("PlFormal").getUnscaledValue("WertMax")
						.set(DUAKonstanten.NEIN);
				resultat.getData().getItem(attributName).getItem("Status")
						.getItem("PlFormal").getUnscaledValue("WertMin")
						.set(DUAKonstanten.NEIN);

				resultat.getData().getItem(attributName).getItem("Status")
						.getItem("PlLogisch")
						.getUnscaledValue("WertMaxLogisch")
						.set(DUAKonstanten.NEIN);
				resultat.getData().getItem(attributName).getItem("Status")
						.getItem("PlLogisch")
						.getUnscaledValue("WertMinLogisch")
						.set(DUAKonstanten.NEIN);
				resultat.getData().getItem(attributName).getItem("Güte")
						.getUnscaledValue("Index")
						.set(10000);
				resultat.getData().getItem(attributName).getItem("Güte")
						.getUnscaledValue("Verfahren")
						.set(GueteVerfahren.STANDARD.getCode());
			}
		}
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
	 */
	protected final void aggregiereMittel(final AnalyseAttribut attribut,
			final Data nutzDatum,
			final Collection<AggregationsDatum> basisDaten) {
		final Collection<AggregationsAttributWert> werte = basisDaten.stream().map(d -> d.getWert(attribut)).collect(Collectors.toList());

		boolean interpoliert = false;
		long anzahl = 0;
		long summe = 0;
		
		boolean nichtErmittelbar = true;
		boolean nichtErmittelbarFehlerhaft = true;
		
		final Collection<GWert> gueteWerte = new ArrayList<>();
		for (final AggregationsAttributWert basisWert : werte) {
			if(basisWert == null) {
				interpoliert = true;
				gueteWerte.add(GWert.getMinGueteWert(GueteVerfahren.STANDARD));
			}
			else if (basisWert.getWert() >= 0) {
				summe += basisWert.getWert();
				anzahl++;
				gueteWerte.add(basisWert.getGuete());
				interpoliert |= basisWert.isInterpoliert();
				nichtErmittelbar = false;
				nichtErmittelbarFehlerhaft = false;
			}
			else {
				if(basisWert.getWert() != DUAKonstanten.NICHT_ERMITTELBAR){
					nichtErmittelbar = false;
				}
				if(basisWert.getWert() != DUAKonstanten.NICHT_ERMITTELBAR_BZW_FEHLERHAFT){
					nichtErmittelbarFehlerhaft = false;
				}
				interpoliert = true;
				gueteWerte.add(GWert.getMinGueteWert(GueteVerfahren.STANDARD));
			}
		}
		
		final AggregationsAttributWert exportWert = new AggregationsAttributWert(
				attribut, DUAKonstanten.NICHT_ERMITTELBAR, 1);
		if(!nichtErmittelbar && nichtErmittelbarFehlerhaft){
			exportWert.setWert(DUAKonstanten.NICHT_ERMITTELBAR_BZW_FEHLERHAFT);
		}
		
		if (!nichtErmittelbar && !nichtErmittelbarFehlerhaft && anzahl > 0) {
			exportWert.setWert(Math.round((double) summe / (double) anzahl));
			
			exportWert.setInterpoliert(interpoliert);
			try {
				exportWert.setGuete(GueteVerfahren.summe(gueteWerte
						.toArray(new GWert[0])));
			} catch (final GueteException e) {
				Debug.getLogger().error(
						"Guete von " + this.objekt + " fuer " + attribut
								+ " konnte nicht berechnet werden", e);
				e.printStackTrace();
			}
		}

		exportWert.exportiere(nutzDatum, this.isFahrstreifen());
	}
	
	protected final void aggregiereGeschwindigkeit(final AnalyseAttribut attrQ, final AnalyseAttribut attrV,
			final Data nutzDatum,
			final Collection<AggregationsDatum> basisDaten) {

		boolean interpoliert = false;
		long zaehler = 0;
		long nenner = 0;

		boolean nichtErmittelbar = true;
		boolean nichtErmittelbarFehlerhaft = true;
		
		final Collection<GWert> gueteZaehler = new ArrayList<>();
		final Collection<GWert> gueteNenner = new ArrayList<>();
		for (final AggregationsDatum basisWert : basisDaten) {

			final AggregationsAttributWert qwert = basisWert.getWert(attrQ);
			final AggregationsAttributWert vwert = basisWert.getWert(attrV);
			if(qwert == null || vwert == null) {
				interpoliert = true;
				gueteZaehler.add(GWert.getMinGueteWert(GueteVerfahren.STANDARD));
				gueteNenner.add(GWert.getMinGueteWert(GueteVerfahren.STANDARD));
			}
			else if ((qwert.getWert() >= 0 && vwert.getWert() >= 0)
					|| (qwert.getWert() == 0 && vwert.getWert() == DUAKonstanten.NICHT_ERMITTELBAR)) {
				
				nenner += qwert.getWert() * vwert.getWert();
				zaehler += qwert.getWert();
				try {
					gueteZaehler.add(GueteVerfahren.produkt(qwert.getGuete(), vwert.getGuete()));
				}
				catch(GueteException e) {
					Debug.getLogger().error(
							"Guete von " + this.objekt + " fuer " + attrV
									+ " konnte nicht berechnet werden", e);
				}
				gueteNenner.add(qwert.getGuete());
				interpoliert |= qwert.isInterpoliert();
				interpoliert |= vwert.isInterpoliert();

				nichtErmittelbar = false;
				nichtErmittelbarFehlerhaft = false;
			}
			else {
				if(vwert.getWert() != DUAKonstanten.NICHT_ERMITTELBAR){
					nichtErmittelbar = false;
				}
				if(vwert.getWert() != DUAKonstanten.NICHT_ERMITTELBAR_BZW_FEHLERHAFT){
					nichtErmittelbarFehlerhaft = false;
				}
				interpoliert = true;
				gueteZaehler.add(GWert.getMinGueteWert(GueteVerfahren.STANDARD));
				gueteNenner.add(GWert.getMinGueteWert(GueteVerfahren.STANDARD));
			}
		}

		final AggregationsAttributWert exportWert = new AggregationsAttributWert(
				attrV, DUAKonstanten.NICHT_ERMITTELBAR, 1);
		if(!nichtErmittelbar && nichtErmittelbarFehlerhaft){
			exportWert.setWert(DUAKonstanten.NICHT_ERMITTELBAR_BZW_FEHLERHAFT);
		}

		if (!nichtErmittelbar && !nichtErmittelbarFehlerhaft && zaehler > 0) {
			exportWert.setWert(Math.round((double) nenner / (double) zaehler));
			exportWert.setInterpoliert(interpoliert);
			try {
				GWert summez = GueteVerfahren.summe(gueteZaehler.toArray(new GWert[0]));
				GWert summen = GueteVerfahren.summe(gueteNenner.toArray(new GWert[0]));
				
				exportWert.setGuete(GueteVerfahren.quotient(summez, summen));
			} catch (final GueteException e) {
				Debug.getLogger().error(
						"Guete von " + this.objekt + " fuer " + attrV
								+ " konnte nicht berechnet werden", e);
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
	 */
	protected final void aggregiereSumme(final AnalyseAttribut attribut,
			final Data nutzDatum,
			final Collection<AggregationsDatum> basisDaten) {
		final Collection<AggregationsAttributWert> werte = basisDaten.stream().map(d -> d.getWert(attribut)).collect(Collectors.toList());

		boolean interpoliert = false;
		long summe = 0;

		boolean nichtErmittelbar = true;
		boolean nichtErmittelbarFehlerhaft = true;

		final Collection<GWert> gueteWerte = new ArrayList<>();
		for (final AggregationsAttributWert basisWert : werte) {
			if(basisWert == null) {
				interpoliert = true;
				gueteWerte.add(GWert.getMinGueteWert(GueteVerfahren.STANDARD));
			}
			else if (basisWert.getWert() >= 0) {
				summe += basisWert.getWert();
				gueteWerte.add(basisWert.getGuete());
				interpoliert |= basisWert.isInterpoliert();
				nichtErmittelbar = false;
				nichtErmittelbarFehlerhaft = false;
			}
			else {
				if(basisWert.getWert() != DUAKonstanten.NICHT_ERMITTELBAR){
					nichtErmittelbar = false;
				}
				if(basisWert.getWert() != DUAKonstanten.NICHT_ERMITTELBAR_BZW_FEHLERHAFT){
					nichtErmittelbarFehlerhaft = false;
				}
				interpoliert = true;
				gueteWerte.add(GWert.getMinGueteWert(GueteVerfahren.STANDARD));
			}
		}

		final AggregationsAttributWert exportWert = new AggregationsAttributWert(
				attribut, DUAKonstanten.NICHT_ERMITTELBAR, 1);
		if(!nichtErmittelbar && nichtErmittelbarFehlerhaft){
			exportWert.setWert(DUAKonstanten.NICHT_ERMITTELBAR_BZW_FEHLERHAFT);
		}

		if (!nichtErmittelbar && !nichtErmittelbarFehlerhaft) {
			exportWert.setWert(summe);

			exportWert.setInterpoliert(interpoliert);
			try {
				exportWert.setGuete(GueteVerfahren.summe(gueteWerte
						                                         .toArray(new GWert[0])));
			} catch (final GueteException e) {
				Debug.getLogger().error(
						"Guete von " + this.objekt + " fuer " + attribut
								+ " konnte nicht berechnet werden", e);
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
	 * Erfragt, ob es sich bei dem hier verwalteten Objekt um eine Objekt vom
	 * Typ <code>typ.fahrStreifen</code> handelt.
	 * 
	 * @return ob es sich bei dem hier verwalteten Objekt um eine Objekt vom Typ
	 *         <code>typ.fahrStreifen</code> handelt
	 */
	protected abstract boolean isFahrstreifen();

}
