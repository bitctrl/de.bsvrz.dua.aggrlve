/**
 * Segment 4 Daten�bernahme und Aufbereitung (DUA), SWE 4.9 Aggregation LVE
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
 * Wei�enfelser Stra�e 67<br>
 * 04229 Leipzig<br>
 * Phone: +49 341-490670<br>
 * mailto: info@bitctrl.de
 */

package de.bsvrz.dua.aggrlve;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import de.bsvrz.dav.daf.main.Dataset;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;

/**
 * Enthaelt alle Informationen, die mit einem <code>ResultData</code> der Attributgruppe
 * <code>atg.verkehrsDatenKurzZeitIntervall</code> bzw. <code>atg.verkehrsDatenKurzZeitFs</code>
 * oder <code>atg.verkehrsDatenKurzZeitMq</code> in den Attrbiuten <code>qPkw</code>,
 * <code>qLkw</code>, <code>qKfz</code> und <code>vLkw</code>, <code>vKfz</code> , <code>vPkw</code>
 * enthalten sind (inkl. Zeitstempel)
 *
 * @author BitCtrl Systems GmbH, Thierfelder
 *
 * @version $Id$
 */
public class AggregationsDatum implements Comparable<AggregationsDatum>, Cloneable {

	/**
	 * Erfragt, ob dieses Datum auf Stunden normiert ist.
	 */
	private boolean normiert = true;

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
	private final Map<AggregationsAttribut, AggregationsAttributWert> werte = new HashMap<AggregationsAttribut, AggregationsAttributWert>();

	/**
	 * Standardkonstruktor.
	 */
	private AggregationsDatum() {
		//
	}

	/**
	 * Standardkonstruktor.
	 *
	 * @param resultat
	 *            ein <code>ResultData</code>-Objekt eines messwertersetzten Fahrstreifendatums bzw.
	 *            eines Aggregationsdatums fuer Fahrstreifen bzw. Messquerschnitte<br>
	 *            <b>Achtung:</b> Argument muss <code>null</code> sein und Nutzdaten besitzen
	 */
	public AggregationsDatum(final Dataset resultat) {
		datenZeit = resultat.getDataTime();
		if (resultat.getObject().isOfType(AggregationLVE.typFahrstreifen)) {
			if (resultat.getDataDescription().getAspect().equals(AggregationLVE.mwe)) {
				normiert = false;
			}
			if (resultat.getData() != null) {
				tT = resultat.getData().getTimeValue("T").getMillis();
			}
		} else {
			for (final AggregationsIntervall intervall : AggregationsIntervall.getInstanzen()) {
				if (intervall.getAspekt().equals(resultat.getDataDescription().getAspect())) {
					tT = intervall.getIntervall();
				}
			}
		}
		for (final AggregationsAttribut attribut : AggregationsAttribut.getInstanzen()) {
			werte.put(attribut, resultat.getData() != null ? new AggregationsAttributWert(attribut,
					resultat) : null);
		}
	}

	@Override
	public AggregationsDatum clone() {
		final AggregationsDatum kopie = new AggregationsDatum();

		kopie.normiert = normiert;
		kopie.datenZeit = datenZeit;
		kopie.tT = tT;
		for (final AggregationsAttribut attribut : AggregationsAttribut.getInstanzen()) {
			final AggregationsAttributWert orgWert = getWert(attribut);
			kopie.werte.put(attribut, orgWert == null ? orgWert : orgWert.clone());
		}

		return kopie;
	}

	/**
	 * Erfragt, ob die Werte dieses Datums auf ganze Stunden normiert sind.
	 *
	 * @return ob die Werte dieses Datums auf ganze Stunden normiert sind
	 */
	public final boolean isNormiert() {
		return normiert;
	}

	/**
	 * Erfragt den Wert eines Attributs.
	 *
	 * @param attribut1
	 *            das Attribut
	 * @return der Wert eines Attributs
	 */
	public final AggregationsAttributWert getWert(final AggregationsAttribut attribut1) {
		return werte.get(attribut1);
	}

	/**
	 * Erfragt, ob in diesem Datensatz keine Nutzdaten enthalten sind.
	 *
	 * @return ob in diesem Datensatz keine Nutzdaten enthalten sind.
	 */
	public final boolean isKeineDaten() {
		return werte.values().iterator().next() == null;
	}

	@Override
	public int compareTo(final AggregationsDatum that) {
		return new Long(datenZeit).compareTo(that.datenZeit);
	}

	/**
	 * Erfragt die Datenzeit dieses Datums.
	 *
	 * @return die Datenzeit dieses Datums
	 */
	public final long getDatenZeit() {
		return datenZeit;
	}

	/**
	 * Erfragt das Erfassungs- bzw. Aggregationsintervall dieses Datensatzes.
	 *
	 * @return das Erfassungs- bzw. Aggregationsintervall dieses Datensatzes
	 */
	public final long getT() {
		return tT;
	}

	@Override
	public boolean equals(final Object obj) {
		return super.equals(obj);
	}

	@Override
	public String toString() {
		String s = "Datenzeit: " + DUAKonstanten.ZEIT_FORMAT_GENAU.format(new Date(datenZeit))
				+ " (" + datenZeit + ")";

		s += "\nT: " + tT;
		for (final AggregationsAttribut attribut : werte.keySet()) {
			s += "\n" + attribut + ":\n" + werte.get(attribut);
		}

		return s;
	}

}