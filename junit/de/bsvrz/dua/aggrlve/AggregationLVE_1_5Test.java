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

import java.util.Calendar;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.HashMap;

import junit.framework.Assert;

import org.junit.Test;

import com.bitctrl.Constants;

import de.bsvrz.dav.daf.main.ClientDavInterface;
import de.bsvrz.dav.daf.main.DataDescription;
import de.bsvrz.dav.daf.main.ResultData;
import de.bsvrz.dav.daf.main.config.SystemObject;
import de.bsvrz.dua.guete.GWert;
import de.bsvrz.dua.guete.GueteVerfahren;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;
import de.bsvrz.sys.funclib.bitctrl.dua.GanzZahl;
import de.bsvrz.sys.funclib.bitctrl.dua.test.CSVImporter;
import de.bsvrz.sys.funclib.bitctrl.dua.test.DAVTest;

/**
 * Allgemeine Tests fuer 1- und 5-Minuten-Intervall (entspricht den
 * Testvorschriften aus PrüfSpez Version 2.0 Abschnitt 5.1.10.2 u. 5.1.10.3
 * erster Abschnitt)
 * 
 * @author BitCtrl Systems GmbH, Thierfelder
 * 
 * @verison $Id$
 */
public class AggregationLVE_1_5Test {

	/**
	 * Sollen Assert-Statements benutzt werden?
	 */
	private static final boolean USE_ASSERT = true;

	/**
	 * Mappt Attribut auf relative Posisition in Tabelle
	 */
	private final HashMap<AggregationsAttribut, Integer> AGR_MAP = new HashMap<AggregationsAttribut, Integer>();

	/**
	 * Menge aller Fahrstreifen
	 */
	private SystemObject[] fs = new SystemObject[3];

	/**
	 * Testet, ob die 1 Minuten-Intervalle korrekt zu fünf-Minuten Intervallen
	 * zusammengerechnet und die Fahrstreifen korrekt zu Messquerschnitten
	 * zusammengerechnet werden
	 */
	@Test
	public void test1und5MinutenFSundMQ() throws Exception {
		ClientDavInterface dav = DAVTest.getDav(Verbindung.getConData());
		dav.disconnect(false, "letzte Testverbindung beendet"); //$NON-NLS-1$
		dav = DAVTest.newDav(Verbindung.getConData());

		AggregationLVE aggregation = new AggregationLVE();
		aggregation.testStart(dav);

		/**
		 * drei Fahrstreifen
		 */
		SystemObject fs1 = dav.getDataModel().getObject("fs.mq.a100.0000.hfs"); //$NON-NLS-1$
		SystemObject fs2 = dav.getDataModel().getObject("fs.mq.a100.0000.1üfs"); //$NON-NLS-1$
		SystemObject fs3 = dav.getDataModel().getObject("fs.mq.a100.0000.2üfs"); //$NON-NLS-1$
		this.fs = new SystemObject[] { fs1, fs2, fs3 };
		/**
		 * der Messquerschnitt, der sich aus den drei Fahrstreifen zusammensetzt
		 */
		SystemObject mq = dav.getDataModel().getObject("mq.a100.0000"); //$NON-NLS-1$

		AGR_MAP.put(AggregationsAttribut.Q_KFZ, 0);
		AGR_MAP.put(AggregationsAttribut.Q_PKW, 1);
		AGR_MAP.put(AggregationsAttribut.Q_LKW, 2);
		AGR_MAP.put(AggregationsAttribut.V_KFZ, 3);
		AGR_MAP.put(AggregationsAttribut.V_PKW, 4);
		AGR_MAP.put(AggregationsAttribut.V_LKW, 5);

		DataDescription dd1min = new DataDescription(dav.getDataModel()
				.getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_FS),
				AggregationsIntervall.AGG_1MINUTE.getAspekt(), (short) 0);

		TestErgebnisAnalyseImporter inputImporter = new TestErgebnisAnalyseImporter(
				dav, Verbindung.WURZEL + "Analysewerte.csv"); //$NON-NLS-1$

		CSVImporter outputImporter = new CSVImporter(Verbindung.WURZEL
				+ "Messwert_Aggregation.csv"); //$NON-NLS-1$

		GregorianCalendar cal = new GregorianCalendar();
		cal.set(Calendar.YEAR, 2000);
		cal.set(Calendar.MONTH, Calendar.JANUARY);
		cal.set(Calendar.DAY_OF_MONTH, 1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		long zeit = cal.getTimeInMillis();

		outputImporter.getNaechsteZeile();
		AggregationsMessQuerschnitt mqObj = aggregation
				.getAggregationsObjekt(mq);
		for (int a = 0; a < 5; a++) {
			long startzeit = zeit;
			for (int i = 0; i < 5; i++) {
				try {
					Thread.sleep(100L);
				} catch (InterruptedException e) {
				}

				inputImporter.importNaechsteZeile();
				for (int j = 0; j < 3; j++) {
					ResultData resultat = new ResultData(fs[j], dd1min,
							startzeit, inputImporter
									.getFSAnalyseDatensatz(j + 1));
					/**
					 * Aktualisiere das Aggregationsobjekt, das mit dem
					 * Fahrstreifen assoziiert ist
					 */
					mqObj.getAggregationsObjekt(fs[j]).getPuffer()
							.aktualisiere(resultat);
				}

				startzeit += Constants.MILLIS_PER_MINUTE;
			}
			mqObj.aggregiere(zeit, AggregationsIntervall.AGG_5MINUTE);

			/**
			 * vergleiche die für die Fahrstreifen aggregierten Daten mit den
			 * Soll-Daten aus der Tabelle Messwert-Aggr.
			 */
			int f = 0;
			String[] zeile = outputImporter.getNaechsteZeile();
			for (SystemObject fsObj : fs) {
				Collection<AggregationsDatum> daten = mqObj
						.getAggregationsObjekt(fsObj).getPuffer().getPuffer(
								AggregationsIntervall.AGG_5MINUTE)
						.getDatenFuerZeitraum(zeit, startzeit);

				if (daten != null && !daten.isEmpty()) {

					for (AggregationsAttribut attribut : AggregationsAttribut
							.getInstanzen()) {
						AggregationsAttributWert wertSoll = getTextDatenSatz(
								attribut, zeile, f);
						if (!this.equalsMitRundungsToleranz(wertSoll, daten
								.iterator().next().getWert(attribut))) {
							System.out
									.println("FS" + this.getFsNummer(fsObj) + ", Interv.: " + (a + 1) + "\nSoll:\n" + wertSoll + //$NON-NLS-1$//$NON-NLS-2$//$NON-NLS-3$
											"\nIst:\n"
											+ daten.iterator().next().getWert(
													attribut)); //$NON-NLS-1$
						}
						if (USE_ASSERT) {
							Assert
									.assertTrue(
											"FS"	+ this.getFsNummer(fsObj) + ", Interv.: " + //$NON-NLS-1$//$NON-NLS-2$
													(a + 1) + " ", this.equalsMitRundungsToleranz(wertSoll, daten.iterator().next().getWert(attribut))); //$NON-NLS-1$s
						}
					}
				}
				f++;
			}

			zeit = startzeit;
		}

	}

	/**
	 * Wurde eingefuehrt, da die "echte" Vergleichsmethode keine
	 * Rundungstoleranz gegenueber den Testdaten aufweist
	 */
	public boolean equalsMitRundungsToleranz(AggregationsAttributWert a1,
			AggregationsAttributWert a2) {
		return a1.getAttribut().equals(a2.getAttribut())
				&& Math.abs(a1.getWert() - a2.getWert()) <= 2
				&& a1.isNichtErfasst() == a2.isNichtErfasst()
				&& a1.isImplausibel() == a2.isImplausibel()
				&& a1.isInterpoliert() == a2.isInterpoliert()
				&& a1.getGuete().getIndex() - a2.getGuete().getIndex() < 0.001;
	}

	/**
	 * Erfragt die Nummer eines Fahrstreifens (in Bezug auf die
	 * PruefSpez-Tabelle der Ein- und Ausgangswerte)
	 * 
	 * @param fsObjekt
	 *            ein hier getesteter Fahrstreifen
	 * @return dessen Nummer (in Bezug auf die PruefSpez-Tabelle der Ein- und
	 *         Ausgangswerte)
	 */
	private final int getFsNummer(final SystemObject fsObjekt) {
		int i;
		for (i = 0; i < this.fs.length; i++) {
			if (fsObjekt.equals(this.fs[i]))
				break;
		}
		return i + 1;
	}

	/**
	 * Extrahiert den Wert und den Status-String (Soll)
	 * 
	 * @param attribut
	 *            das Attribut
	 * @param zeile
	 *            eine Tabellenzeile
	 * @return den Wert und den Status-String (Soll)
	 */
	private final AggregationsAttributWert getTextDatenSatz(
			final AggregationsAttribut attribut, final String[] zeile, int fs) {
		AggregationsAttributWert wert = new AggregationsAttributWert(attribut,
				Long.parseLong(zeile[26 + 11 + AGR_MAP.get(attribut) * 6 - 1
						+ fs * 2]), 0);
		String status = zeile[26 + 11 + AGR_MAP.get(attribut) * 6 + fs * 2];
		double guete = 1.0;
		if (status.split(" ").length > 1) { //$NON-NLS-1$
			guete = Double.parseDouble(status.split(" ")[0].replace(",", ".")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

			if (status != null) {
				String[] splitStatus = status.trim().split(" "); //$NON-NLS-1$

				for (int i = 0; i < splitStatus.length; i++) {
					if (splitStatus[i].equalsIgnoreCase("Impl")) { //$NON-NLS-1$
						wert.setImplausibel(true);
					}
					if (splitStatus[i].equalsIgnoreCase("Intp")) { //$NON-NLS-1$
						wert.setInterpoliert(true);
					}

					if (splitStatus[i].equalsIgnoreCase("nErf")) { //$NON-NLS-1$
						wert.setNichtErfasst(true);
					}

					if (splitStatus[i].equalsIgnoreCase("wMaL")) { //$NON-NLS-1$
						wert.setLogischMax(true);
					}

					if (splitStatus[i].equalsIgnoreCase("wMax")) { //$NON-NLS-1$
						wert.setFormalMax(true);
					}

					if (splitStatus[i].equalsIgnoreCase("wMiL")) { //$NON-NLS-1$
						wert.setLogischMin(true);
					}

					if (splitStatus[i].equalsIgnoreCase("wMin")) { //$NON-NLS-1$
						wert.setFormalMin(true);
					}

				}
			}
		} else {
			guete = Double.parseDouble(status.replace(",", ".")); //$NON-NLS-1$ //$NON-NLS-2$
		}
		GanzZahl g = GanzZahl.getGueteIndex();
		g.setSkaliertenWert(guete);

		wert.setGuete(new GWert(g, GueteVerfahren.STANDARD, false));

		return wert;
	}

}