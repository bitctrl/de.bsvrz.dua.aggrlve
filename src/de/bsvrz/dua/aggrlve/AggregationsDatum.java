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

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import de.bsvrz.dav.daf.main.Dataset;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;

/**
 * Enthaelt alle Informationen, die mit einem <code>ResultData</code> der
 * Attributgruppe <code>atg.verkehrsDatenKurzZeitIntervall</code> bzw.
 * <code>atg.verkehrsDatenKurzZeitFs</code> oder
 * <code>atg.verkehrsDatenKurzZeitMq</code> in den Attrbiuten
 * <code>qPkw</code>, <code>qLkw</code>, <code>qKfz</code> und
 * <code>vLkw</code>, <code>vKfz</code>, <code>vPkw</code> enthalten
 * sind (inkl. Zeitstempel)
 * 
 * @author BitCtrl Systems GmbH, Thierfelder
 * 
 * @version $Id$
 */
public class AggregationsDatum implements Comparable<AggregationsDatum>,
		Cloneable {
	
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
	private Map<AggregationsAttribut, AggregationsAttributWert> werte = new HashMap<AggregationsAttribut, AggregationsAttributWert>();

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
	 *            ein <code>ResultData</code>-Objekt eines messwertersetzten
	 *            Fahrstreifendatums bzw. eines Aggregationsdatums fuer
	 *            Fahrstreifen bzw. Messquerschnitte<br>
	 *            <b>Achtung:</b> Argument muss <code>null</code> sein und
	 *            Nutzdaten besitzen
	 */
	public AggregationsDatum(final Dataset resultat) {
		this.datenZeit = resultat.getDataTime();
		if (resultat.getObject().isOfType(AggregationLVE.typFahrstreifen)) {
			if (resultat.getDataDescription().getAspect().equals(
					AggregationLVE.mwe)) {
				this.normiert = false;
			}
			if(resultat.getData() != null) {
				this.tT = resultat.getData().getTimeValue("T").getMillis();
			}
		} else {
			for (AggregationsIntervall intervall : AggregationsIntervall
					.getInstanzen()) {
				if (intervall.getAspekt().equals(
						resultat.getDataDescription().getAspect())) {
					this.tT = intervall.getIntervall();
				}
			}
		}
		for (AggregationsAttribut attribut : AggregationsAttribut
				.getInstanzen()) {
			this.werte.put(attribut, resultat.getData() != null?new AggregationsAttributWert(attribut,
					resultat):null);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	public AggregationsDatum clone() {
		AggregationsDatum kopie = new AggregationsDatum();

		kopie.normiert = this.normiert;
		kopie.datenZeit = this.datenZeit;
		kopie.tT = this.tT;
		for (AggregationsAttribut attribut : AggregationsAttribut
				.getInstanzen()) {
			AggregationsAttributWert orgWert = this.getWert(attribut);
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
		return this.normiert;
	}

	/**
	 * Erfragt den Wert eines Attributs.
	 * 
	 * @param attribut1 das Attribut
	 * @return der Wert eines Attributs
	 */
	public final AggregationsAttributWert getWert(
			final AggregationsAttribut attribut1) {
		return this.werte.get(attribut1);
	}
	
	/**
	 * Erfragt, ob in diesem Datensatz keine Nutzdaten enthalten sind.
	 * 
	 * @return ob in diesem Datensatz keine Nutzdaten enthalten sind.
	 */
	public final boolean isKeineDaten(){
		return this.werte.values().iterator().next() == null;
	}

	/**
	 * {@inheritDoc}
	 */
	public int compareTo(AggregationsDatum that) {
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
	 * Erfragt das Erfassungs- bzw. Aggregationsintervall dieses Datensatzes.
	 * 
	 * @return das Erfassungs- bzw. Aggregationsintervall dieses Datensatzes
	 */
	public final long getT() {
		return this.tT;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(Object obj) {
		return super.equals(obj);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		String s = "Datenzeit: " + //$NON-NLS-1$
				DUAKonstanten.ZEIT_FORMAT_GENAU
						.format(new Date(this.datenZeit))
				+ " (" + this.datenZeit + ")"; //$NON-NLS-1$ //$NON-NLS-2$

		s += "\nT: " + tT; //$NON-NLS-1$
		for (AggregationsAttribut attribut : this.werte.keySet()) {
			s += "\n" + attribut + ":\n" + this.werte.get(attribut); //$NON-NLS-1$//$NON-NLS-2$
		}

		return s;
	}

}
