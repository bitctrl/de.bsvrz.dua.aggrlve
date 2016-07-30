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

import de.bsvrz.dav.daf.main.Data;
import de.bsvrz.dav.daf.main.Dataset;
import de.bsvrz.dua.dalve.analyse.lib.AnalyseAttribut;
import de.bsvrz.dua.guete.GWert;
import de.bsvrz.dua.guete.GueteVerfahren;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAUtensilien;
import de.bsvrz.sys.funclib.bitctrl.dua.GanzZahl;
import de.bsvrz.sys.funclib.bitctrl.dua.MesswertMarkierung;

/**
 * Korrespondiert mit einem Attributwert eines messwertersetzten
 * Fahrstreifendatums bzw. eines Aggregationsdatums
 * 
 * @author BitCtrl Systems GmbH, Thierfelder
 */
public class AggregationsAttributWert extends MesswertMarkierung implements
		Comparable<AggregationsAttributWert> {

	/**
	 * das Attribut.
	 */
	private final AnalyseAttribut attr;

	/**
	 * der Wert dieses Attributs.
	 */
	private long wert = -4;

	/**
	 * die Guete.
	 */
	private GWert guete;

	/**
	 * Standardkonstruktor.
	 * 
	 * @param attr
	 *            das Attribut
	 * @param resultat
	 *            der Datensatz in dem der Attributwert steht
	 */
	public AggregationsAttributWert(final AnalyseAttribut attr,
			final Dataset resultat) {
		if (attr == null) {
			throw new NullPointerException("Attribut ist <<null>>");
		}
		final Data datenSatz = resultat.getData();
		if (datenSatz == null) {
			throw new NullPointerException("Datensatz ist <<null>>");
		}

		final String attributName = attr.getAttributName(resultat.getObject().isOfType(DUAKonstanten.TYP_FAHRSTREIFEN));

		this.attr = attr;
		this.wert = datenSatz.getItem(attributName).getUnscaledValue("Wert")
				.longValue();
		this.nichtErfasst = datenSatz.getItem(attributName).getItem("Status")
				.getItem("Erfassung").getUnscaledValue("NichtErfasst")
				.intValue() == DUAKonstanten.JA;
		this.implausibel = datenSatz.getItem(attributName).getItem("Status")
				.getItem("MessWertErsetzung").getUnscaledValue("Implausibel")
				.intValue() == DUAKonstanten.JA;
		this.interpoliert = datenSatz.getItem(attributName).getItem("Status")
				.getItem("MessWertErsetzung").getUnscaledValue("Interpoliert")
				.intValue() == DUAKonstanten.JA;

		this.formalMax = datenSatz.getItem(attributName).getItem("Status")
				.getItem("PlFormal").getUnscaledValue("WertMax").intValue() == DUAKonstanten.JA;
		this.formalMin = datenSatz.getItem(attributName).getItem("Status")
				.getItem("PlFormal").getUnscaledValue("WertMin").intValue() == DUAKonstanten.JA;

		this.logischMax = datenSatz.getItem(attributName).getItem("Status")
				.getItem("PlLogisch").getUnscaledValue("WertMaxLogisch")
				.intValue() == DUAKonstanten.JA;
		this.logischMin = datenSatz.getItem(attributName).getItem("Status")
				.getItem("PlLogisch").getUnscaledValue("WertMinLogisch")
				.intValue() == DUAKonstanten.JA;

		this.guete = new GWert(datenSatz, attributName);
	}

	/**
	 * Konstruktor fuer Zwischenergebnisse.
	 * 
	 * @param attribut
	 *            das Attribut
	 * @param wert
	 *            der Wert dieses Attributs
	 * @param guete
	 *            die Guete
	 */
	public AggregationsAttributWert(final AnalyseAttribut attribut,
			final long wert, final double guete) {
		this.attr = attribut;
		this.wert = wert;
		final GanzZahl gueteGanzZahl = GanzZahl.getGueteIndex();
		gueteGanzZahl.setSkaliertenWert(guete);
		this.guete = new GWert(gueteGanzZahl, GueteVerfahren.STANDARD, false);
	}

	@Override
	public AggregationsAttributWert clone() {
		AggregationsAttributWert kopie = null;

		try {
			kopie = (AggregationsAttributWert) super.clone();
			kopie.guete = new GWert(this.guete);
		} catch (final CloneNotSupportedException e) {
			// wird nicht geworfen
		}

		return kopie;
	}

	/**
	 * Exportiert den Inhalt dieses Objektes in ein veraenderbares Nutzdatum der
	 * Attributgruppe <code>atg.verkehrsDatenKurzZeitFs</code> oder
	 * <code>atg.verkehrsDatenKurzZeitMq</code>.
	 * 
	 * @param nutzDatum
	 *            ein veraenderbare Instanz einer Attributgruppe
	 *            <code>atg.verkehrsDatenKurzZeitFs</code> oder
	 *            <code>atg.verkehrsDatenKurzZeitMq</code>
	 * @param isFahrstreifen
	 *            ob es sich um ein Fahrstreifendatum handelt
	 */
	public final void exportiere(final Data nutzDatum,
			final boolean isFahrstreifen) {

		final String attributName = attr.getAttributName(isFahrstreifen);

		if (DUAUtensilien.isWertInWerteBereich(nutzDatum.getItem(attributName)
				.getItem("Wert"), this.wert)) {
			nutzDatum.getItem(attributName).getUnscaledValue("Wert")
					.set(this.wert);
		} else {
			nutzDatum.getItem(attributName).getUnscaledValue("Wert")
					.set(DUAKonstanten.NICHT_ERMITTELBAR_BZW_FEHLERHAFT);
		}

		nutzDatum
				.getItem(attributName)
				.getItem("Status")
				.getItem("Erfassung")
				.getUnscaledValue("NichtErfasst")
				.set(this.isNichtErfasst() ? DUAKonstanten.JA
						: DUAKonstanten.NEIN);
		nutzDatum
				.getItem(attributName)
				.getItem("Status")
				.getItem("MessWertErsetzung")
				.getUnscaledValue("Implausibel")
				.set(this.isImplausibel() ? DUAKonstanten.JA
						: DUAKonstanten.NEIN);
		nutzDatum
				.getItem(attributName)
				.getItem("Status")
				.getItem("MessWertErsetzung")
				.getUnscaledValue("Interpoliert")
				.set(this.isInterpoliert() ? DUAKonstanten.JA
						: DUAKonstanten.NEIN);

		nutzDatum
				.getItem(attributName)
				.getItem("Status")
				.getItem("PlFormal")
				.getUnscaledValue("WertMax")
				.set(this.isFormalMax() ? DUAKonstanten.JA : DUAKonstanten.NEIN);
		nutzDatum
				.getItem(attributName)
				.getItem("Status")
				.getItem("PlFormal")
				.getUnscaledValue("WertMin")
				.set(this.isFormalMin() ? DUAKonstanten.JA : DUAKonstanten.NEIN);

		nutzDatum
				.getItem(attributName)
				.getItem("Status")
				.getItem("PlLogisch")
				.getUnscaledValue("WertMaxLogisch")
				.set(this.isLogischMax() ? DUAKonstanten.JA
						: DUAKonstanten.NEIN);
		nutzDatum
				.getItem(attributName)
				.getItem("Status")
				.getItem("PlLogisch")
				.getUnscaledValue("WertMinLogisch")
				.set(this.isLogischMin() ? DUAKonstanten.JA
						: DUAKonstanten.NEIN);

		this.guete.exportiere(nutzDatum, attributName);
	}

	/**
	 * Erfragt das Attribut.
	 * 
	 * @return das Attribut
	 */
	public final AnalyseAttribut getAttribut() {
		return this.attr;
	}

	/**
	 * Setzt den Wert dieses Attributs.
	 * 
	 * @param wert
	 *            der Wert dieses Attributs
	 */
	public final void setWert(final long wert) {
		this.veraendert = true;
		this.wert = wert;
	}

	/**
	 * Erfragt den Wert dieses Attributs.
	 * 
	 * @return der Wert dieses Attributs
	 */
	public final long getWert() {
		return this.wert;
	}

	/**
	 * Erfragt die Guete dieses Attributwertes.
	 * 
	 * @return die Guete dieses Attributwertes
	 */
	public final GWert getGuete() {
		return this.guete;
	}

	/**
	 * Setzte die Guete dieses Attributwertes.
	 * 
	 * @param guete
	 *            die Guete dieses Attributwertes
	 */
	public final void setGuete(final GWert guete) {
		this.veraendert = true;
		this.guete = guete;
	}

	@Override
	public int compareTo(final AggregationsAttributWert that) {
		return new Long(this.getWert()).compareTo(that.getWert());
	}

	@Override
	public String toString() {
		return "Attribut: " + this.attr + "\nWert: " + this.wert + "\nGuete: "
				+ this.guete + "\nVeraendert: "
				+ (this.veraendert ? "Ja" : "Nein") + "\n" + super.toString();
	}

}
