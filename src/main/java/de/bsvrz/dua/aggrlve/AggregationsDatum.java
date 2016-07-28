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
import de.bsvrz.dav.daf.main.Dataset;
import de.bsvrz.dua.dalve.ErfassungsIntervallDauerMQ;
import de.bsvrz.dua.dalve.analyse.lib.AnalyseAttribut;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Enthaelt alle Informationen, die mit einem <code>ResultData</code> der
 * Attributgruppe <code>atg.verkehrsDatenKurzZeitIntervall</code> bzw.
 * <code>atg.verkehrsDatenKurzZeitFs</code> oder
 * <code>atg.verkehrsDatenKurzZeitMq</code> in den Attrbiuten <code>qPkw</code>,
 * <code>qLkw</code>, <code>qKfz</code> und <code>vLkw</code>, <code>vKfz</code>
 * , <code>vPkw</code> enthalten sind (inkl. Zeitstempel)
 * 
 * @author BitCtrl Systems GmbH, Thierfelder
 */
public class AggregationsDatum implements Comparable<AggregationsDatum>,
		Cloneable {

	/**
	 * die Datenzeit dieses Datums.
	 */
	private long datenZeit = -1;

	/**
	 * Erfassungs- bzw. Aggregationsintervall dieses Datensatzes.
	 */
	private long tT = -1;

	/**
	 * die Werte aller innerhalb der Messwertaggregation betrachteten Attribute.
	 */
	private final Map<AnalyseAttribut, AggregationsAttributWert> werte = new HashMap<>();

	/**
	 * Standardkonstruktor, erstellt ein leeres Datum mit der angegebenen Datenzeit
	 * @param datenZeit Datenzeit in Millis seit 1970
	 */
	public AggregationsDatum(long datenZeit) {
		this.datenZeit = datenZeit;
	}

	/**
	 * Standardkonstruktor, erstellt das Datum aus einem Dav-Data-Objekt.
	 *
	 * @param resultat
	 *            ein <code>ResultData</code>-Objekt eines messwertersetzten
	 *            Fahrstreifendatums bzw. eines Aggregationsdatums fuer
	 *            Fahrstreifen bzw. Messquerschnitte<br>
	 *            <b>Achtung:</b> Argument muss <code>null</code> sein und
	 *            Nutzdaten besitzen
	 * @param dav Datenverteilerverbindung
	 */
	public AggregationsDatum(final Dataset resultat, final ClientDavInterface dav) {
		this.datenZeit = resultat.getDataTime();
		
		// Erfassungsintervall ermitteln
		if (resultat.getObject().isOfType(DUAKonstanten.TYP_FAHRSTREIFEN)) {
			// Falls Fahrstreifen einfach: Aus Attribut auslesen
			if (resultat.getData() != null) {
				this.tT = resultat.getData().getTimeValue("T").getMillis();
			}
		} else {
			// Falls (Virtueller) MQ:
			if(resultat.getDataDescription().getAspect().getPid().equals(DUAKonstanten.ASP_ANALYSE)){
				// Bei Analysedaten gibt es kein T-Attribut, also Erfassungsintervall aus FS-Daten bestimmen
				ErfassungsIntervallDauerMQ instanz = ErfassungsIntervallDauerMQ.getInstanz(dav, resultat.getObject());
				if(instanz != null) {
					this.tT = instanz.getT();
				}
			}
			else {
				// Sonst das Erfassungsintervall aus dem Aspekt der aggregierten Daten nehmen.
				for(final AggregationsIntervall intervall : AggregationsIntervall.getInstanzen()) {
					if(intervall.getAspekt().equals(
							resultat.getDataDescription().getAspect())) {
						this.tT = intervall.getIntervall();
					}
				}
			}
		}
		// Alle Werte speichern
		for (final AnalyseAttribut attribut : AnalyseAttribut.getInstanzen()) {
			this.werte.put(
					attribut,
					resultat.getData() != null ? new AggregationsAttributWert(attribut, resultat) : null
			);
		}
	}

	@Override
	public AggregationsDatum clone() {
		final AggregationsDatum kopie = new AggregationsDatum(datenZeit);

		kopie.tT = this.tT;
		for (final AnalyseAttribut attribut : AnalyseAttribut
				.getInstanzen()) {
			final AggregationsAttributWert orgWert = this.getWert(attribut);
			kopie.werte.put(attribut,
					orgWert == null ? orgWert : orgWert.clone());
		}

		return kopie;
	}

	/**
	 * Erfragt den Wert eines Attributs.
	 * 
	 * @param attribut1
	 *            das Attribut
	 * @return der Wert eines Attributs
	 */
	public final AggregationsAttributWert getWert(
			final AnalyseAttribut attribut1) {
		return this.werte.get(attribut1);
	}

	@Override
	public int compareTo(final AggregationsDatum that) {
		return new Long(this.datenZeit).compareTo(that.datenZeit);
	}

	/**
	 * Erfragt die Datenzeit dieses Datums.
	 * 
	 * @return die Datenzeit dieses Datums
	 */
	public final long getDatenZeit() {
		return this.datenZeit;
	}

	/**
	 * Erfragt den Zeitpunkt bsi zu dem dieses Datum gültig ist
	 *
	 * @return die Datenzeit dieses Datums plus T
	 */
	public final long getIntervallEnde() {
		return this.datenZeit + tT;
	}

	/**
	 * Erfragt das Erfassungs- bzw. Aggregationsintervall dieses Datensatzes.
	 * 
	 * @return das Erfassungs- bzw. Aggregationsintervall dieses Datensatzes
	 */
	public final long getT() {
		return this.tT;
	}

	@Override
	public String toString() {
		String s = "Datenzeit: "
				+ DUAKonstanten.ZEIT_FORMAT_GENAU.format(new Date(
						this.datenZeit)) + " (" + this.datenZeit + ")";

		s += "\nT: " + tT;
		for (final AnalyseAttribut attribut : this.werte.keySet()) {
			s += "\n" + attribut + ":\n" + this.werte.get(attribut);
		}

		return s;
	}

}
