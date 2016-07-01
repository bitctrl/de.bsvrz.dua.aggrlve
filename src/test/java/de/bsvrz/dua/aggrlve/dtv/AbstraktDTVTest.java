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
package de.bsvrz.dua.aggrlve.dtv;

import java.util.HashMap;

import de.bsvrz.dav.daf.main.ClientDavInterface;
import de.bsvrz.dua.aggrlve.AggregationLVE;
import de.bsvrz.dua.aggrlve.AggregationUnvImporter;
import de.bsvrz.dua.aggrlve.AggregationsAttribut;
import de.bsvrz.dua.aggrlve.AggregationsAttributWert;
import de.bsvrz.dua.aggrlve.Verbindung;
import de.bsvrz.dua.guete.GWert;
import de.bsvrz.dua.guete.GueteVerfahren;
import de.bsvrz.sys.funclib.bitctrl.dua.GanzZahl;
import de.bsvrz.sys.funclib.bitctrl.dua.test.CSVImporter;
import de.bsvrz.sys.funclib.bitctrl.dua.test.DAVTest;

/**
 * Abstrakter DTV-Test Eingabe. extra/testDaten/[Version]/Messwert_Aggregation_unv.csv
 * Soll-Erwartet: extra/testDaten/[Version]/Messwert_Aggregation_TV_DTV_Soll.csv
 *
 * @author BitCtrl Systems GmbH, Thierfelder
 */
public class AbstraktDTVTest {

	/**
	 * Mappt Attribut auf relative Posisition in Tabelle.
	 */
	protected final HashMap<AggregationsAttribut, Integer> agrMap = new HashMap<>();

	/**
	 * Aggregations-Applikation.
	 */
	protected AggregationLVE aggregation = null;

	/**
	 * Input-Werte.
	 */
	protected AggregationUnvImporter inputImporter;

	/**
	 * Soll-Werte.
	 */
	protected CSVImporter outputImporter;

	/**
	 * Bereitet den Test fuer die Aggregation von TV und DTV-Daten vor.
	 *
	 * @throws Exception
	 *             wird weitergereicht
	 */
	protected final void setup() throws Exception {
		ClientDavInterface dav = DAVTest.getDav(Verbindung.getConData());
		dav.disconnect(false, "Testverbindung beendet");
		dav = DAVTest.newDav(Verbindung.getConData());

		aggregation = new AggregationLVE();
		aggregation.testStart(dav, "1.0");

		agrMap.put(AggregationsAttribut.Q_KFZ, 0);
		agrMap.put(AggregationsAttribut.Q_PKW, 1);
		agrMap.put(AggregationsAttribut.Q_LKW, 2);

		inputImporter = new AggregationUnvImporter(dav,
				Verbindung.WURZEL + "Messwert_Aggregation_unv.csv");

		outputImporter = new CSVImporter(
				Verbindung.WURZEL + "Messwert_Aggregation_TV_DTV_Soll.csv");
	}

	/**
	 * Extrahiert den Wert und den Status-String (Soll).
	 *
	 * @param attribut
	 *            das Attribut
	 * @param zeile
	 *            eine Tabellenzeile
	 * @return den Wert und den Status-String (Soll)
	 */
	protected final AggregationsAttributWert getTextDatenSatz(final AggregationsAttribut attribut,
			final String[] zeile) {

		final String wertStr = zeile[agrMap.get(attribut) * 2];
		final AggregationsAttributWert wert = new AggregationsAttributWert(attribut,
				Long.parseLong(wertStr), 0);
		final String status = zeile[(agrMap.get(attribut) * 2) + 1];
		double guete = 1.0;
		if (status.split(" ").length > 1) {
			guete = Double.parseDouble(status.split(" ")[0].replace(",", "."));
			wert.setInterpoliert(true);
		} else {
			try {
				guete = Double.parseDouble(status.replace(",", "."));
			} catch (final Exception ex) {
				ex.printStackTrace();
			}
		}
		final GanzZahl g = GanzZahl.getGueteIndex();
		g.setSkaliertenWert(guete);

		wert.setGuete(new GWert(g, GueteVerfahren.STANDARD, false));

		return wert;
	}

}
