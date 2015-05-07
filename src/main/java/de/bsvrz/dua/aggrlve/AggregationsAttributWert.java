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

import de.bsvrz.dav.daf.main.Data;
import de.bsvrz.dav.daf.main.Dataset;
import de.bsvrz.dua.guete.GWert;
import de.bsvrz.dua.guete.GueteVerfahren;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAUtensilien;
import de.bsvrz.sys.funclib.bitctrl.dua.GanzZahl;
import de.bsvrz.sys.funclib.bitctrl.dua.MesswertMarkierung;

/**
 * Korrespondiert mit einem Attributwert eines messwertersetzten Fahrstreifendatums bzw. eines
 * Aggregationsdatums
 *
 * @author BitCtrl Systems GmbH, Thierfelder
 */
public class AggregationsAttributWert extends MesswertMarkierung
		implements Comparable<AggregationsAttributWert>, Cloneable {

	/**
	 * das Attribut.
	 */
	private final AggregationsAttribut attr;

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
	public AggregationsAttributWert(final AggregationsAttribut attr, final Dataset resultat) {
		if (attr == null) {
			throw new NullPointerException("Attribut ist <<null>>");
		}
		final Data datenSatz = resultat.getData();
		if (datenSatz == null) {
			throw new NullPointerException("Datensatz ist <<null>>");
		}

		final String attributName = attr
				.getAttributName(resultat.getObject().isOfType(AggregationLVE.typFahrstreifen));

		this.attr = attr;
		wert = datenSatz.getItem(attributName).getUnscaledValue("Wert").longValue();
		nichtErfasst = datenSatz.getItem(attributName).getItem("Status").getItem("Erfassung")
				.getUnscaledValue("NichtErfasst").intValue() == DUAKonstanten.JA;
		implausibel = datenSatz.getItem(attributName).getItem("Status").getItem("MessWertErsetzung")
				.getUnscaledValue("Implausibel").intValue() == DUAKonstanten.JA;
		interpoliert = datenSatz.getItem(attributName).getItem("Status")
				.getItem("MessWertErsetzung").getUnscaledValue("Interpoliert")
				.intValue() == DUAKonstanten.JA;

		formalMax = datenSatz.getItem(attributName).getItem("Status").getItem("PlFormal")
				.getUnscaledValue("WertMax").intValue() == DUAKonstanten.JA;
		formalMin = datenSatz.getItem(attributName).getItem("Status").getItem("PlFormal")
				.getUnscaledValue("WertMin").intValue() == DUAKonstanten.JA;

		logischMax = datenSatz.getItem(attributName).getItem("Status").getItem("PlLogisch")
				.getUnscaledValue("WertMaxLogisch").intValue() == DUAKonstanten.JA;
		logischMin = datenSatz.getItem(attributName).getItem("Status").getItem("PlLogisch")
				.getUnscaledValue("WertMinLogisch").intValue() == DUAKonstanten.JA;

		guete = new GWert(datenSatz, attributName);
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
	public AggregationsAttributWert(final AggregationsAttribut attribut, final long wert,
			final double guete) {
		attr = attribut;
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
			kopie.guete = new GWert(guete);
		} catch (final CloneNotSupportedException e) {
			// wird nicht geworfen
		}

		return kopie;
	}

	/**
	 * Exportiert den Inhalt dieses Objektes in ein veraenderbares Nutzdatum der Attributgruppe
	 * <code>atg.verkehrsDatenKurzZeitFs</code> oder <code>atg.verkehrsDatenKurzZeitMq</code>.
	 *
	 * @param nutzDatum
	 *            ein veraenderbare Instanz einer Attributgruppe
	 *            <code>atg.verkehrsDatenKurzZeitFs</code> oder
	 *            <code>atg.verkehrsDatenKurzZeitMq</code>
	 * @param isFahrstreifen
	 *            ob es sich um ein Fahrstreifendatum handelt
	 */
	public final void exportiere(final Data nutzDatum, final boolean isFahrstreifen) {

		final String attributName = attr.getAttributName(isFahrstreifen);

		if (DUAUtensilien.isWertInWerteBereich(nutzDatum.getItem(attributName).getItem("Wert"),
				wert)) {
			nutzDatum.getItem(attributName).getUnscaledValue("Wert").set(wert);
		} else {
			nutzDatum.getItem(attributName).getUnscaledValue("Wert")
					.set(DUAKonstanten.NICHT_ERMITTELBAR_BZW_FEHLERHAFT);
		}

		nutzDatum.getItem(attributName).getItem("Status").getItem("Erfassung")
				.getUnscaledValue("NichtErfasst")
				.set(isNichtErfasst() ? DUAKonstanten.JA : DUAKonstanten.NEIN);
		nutzDatum.getItem(attributName).getItem("Status").getItem("MessWertErsetzung")
				.getUnscaledValue("Implausibel")
				.set(isImplausibel() ? DUAKonstanten.JA : DUAKonstanten.NEIN);
		nutzDatum.getItem(attributName).getItem("Status").getItem("MessWertErsetzung")
				.getUnscaledValue("Interpoliert")
				.set(isInterpoliert() ? DUAKonstanten.JA : DUAKonstanten.NEIN);

		nutzDatum.getItem(attributName).getItem("Status").getItem("PlFormal")
				.getUnscaledValue("WertMax")
				.set(isFormalMax() ? DUAKonstanten.JA : DUAKonstanten.NEIN);
		nutzDatum.getItem(attributName).getItem("Status").getItem("PlFormal")
				.getUnscaledValue("WertMin")
				.set(isFormalMin() ? DUAKonstanten.JA : DUAKonstanten.NEIN);

		nutzDatum.getItem(attributName).getItem("Status").getItem("PlLogisch")
				.getUnscaledValue("WertMaxLogisch")
				.set(isLogischMax() ? DUAKonstanten.JA : DUAKonstanten.NEIN);
		nutzDatum.getItem(attributName).getItem("Status").getItem("PlLogisch")
				.getUnscaledValue("WertMinLogisch")
				.set(isLogischMin() ? DUAKonstanten.JA : DUAKonstanten.NEIN);

		guete.exportiere(nutzDatum, attributName);
	}

	/**
	 * Erfragt das Attribut.
	 *
	 * @return das Attribut
	 */
	public final AggregationsAttribut getAttribut() {
		return attr;
	}

	/**
	 * Setzt den Wert dieses Attributs.
	 *
	 * @param wert
	 *            der Wert dieses Attributs
	 */
	public final void setWert(final long wert) {
		veraendert = true;
		this.wert = wert;
	}

	/**
	 * Erfragt den Wert dieses Attributs.
	 *
	 * @return der Wert dieses Attributs
	 */
	public final long getWert() {
		return wert;
	}

	/**
	 * Erfragt die Guete dieses Attributwertes.
	 *
	 * @return die Guete dieses Attributwertes
	 */
	public final GWert getGuete() {
		return guete;
	}

	/**
	 * Setzte die Guete dieses Attributwertes.
	 *
	 * @param guete
	 *            die Guete dieses Attributwertes
	 */
	public final void setGuete(final GWert guete) {
		veraendert = true;
		this.guete = guete;
	}

	@Override
	public int compareTo(final AggregationsAttributWert that) {
		return new Long(getWert()).compareTo(that.getWert());
	}

	@Override
	public boolean equals(final Object obj) {
		boolean ergebnis = false;

		if ((obj != null) && (obj instanceof AggregationsAttributWert)) {
			final AggregationsAttributWert that = (AggregationsAttributWert) obj;
			ergebnis = getAttribut().equals(that.getAttribut()) && (getWert() == that.getWert())
					&& (isNichtErfasst() == that.isNichtErfasst())
					&& (isImplausibel() == that.isImplausibel())
					&& (isInterpoliert() == that.isInterpoliert())
					&& ((getGuete().getIndex() - that.getGuete().getIndex()) < 0.001);
		}

		return ergebnis;
	}

	@Override
	public String toString() {
		return "Attribut: " + attr + "\nWert: " + wert + "\nGuete: " + guete + "\nVeraendert: "
				+ (veraendert ? "Ja" : "Nein") + "\n" + super.toString();
	}

}
