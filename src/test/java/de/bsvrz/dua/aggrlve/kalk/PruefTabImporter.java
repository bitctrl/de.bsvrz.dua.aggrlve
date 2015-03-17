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

package de.bsvrz.dua.aggrlve.kalk;

import java.util.HashMap;
import java.util.Map;

import com.bitctrl.Constants;

import de.bsvrz.sys.funclib.bitctrl.dua.test.CSVImporter;

/**
 * Liesst komplett eine Tabelle ein, die aus (Wert, Status)-Paaren besteht. Ermoeglicht so den
 * wahrfreihen Zugriff auf alle Elemente der Tabelle
 *
 * @author BitCtrl Systems GmbH, Thierfelder
 *
 * @version $Id$
 */
public final class PruefTabImporter {

	/**
	 * Tabelleninhalt.
	 */
	private final Map<String, Element[]> tabContent = new HashMap<String, Element[]>();

	/**
	 * Standardkonstruktor.
	 *
	 * @param csvDateiName
	 *            Name der zu importierenden CSV-Datei
	 * @throws Exception
	 *             wird weitergereicht
	 */
	public PruefTabImporter(final String csvDateiName) throws Exception {
		final CSVImporter csvImporter = new CSVImporter(csvDateiName);

		csvImporter.getNaechsteZeile();
		int zeilen;
		for (zeilen = 0;; zeilen++) {
			final String[] zeile = csvImporter.getNaechsteZeile();
			if ((zeile == null) || (zeile.length == 0)) {
				break;
			}
		}

		csvImporter.reset();

		final Map<Integer, String> spalteZuName = new HashMap<Integer, String>();
		final String[] tabellenKopf = csvImporter.getNaechsteZeile();
		for (int i = 0; i < tabellenKopf.length; i += 2) {
			tabContent.put(tabellenKopf[i], new Element[zeilen]);
			spalteZuName.put(i, tabellenKopf[i]);
		}

		for (int zeilenNr = 0;; zeilenNr++) {
			final String[] tabellenZeile = csvImporter.getNaechsteZeile();
			if ((tabellenZeile == null) || (tabellenZeile.length == 0)) {
				break; // fertig
			}

			for (int i = 0; i < tabellenZeile.length; i += 2) {
				final Element[] elemente = tabContent.get(spalteZuName.get(i));
				elemente[zeilenNr] = new Element(tabellenZeile[i], tabellenZeile[i + 1]);
			}
		}
	}

	/**
	 * Erfragt ein Element einer bestimmten Spalte / Zeile.
	 *
	 * @param spaltenName
	 *            der Name der Spalte
	 * @param line
	 *            die Zeile
	 * @return ein Element einer bestimmten Spalte / Zeile
	 */
	public Element getElement(final String spaltenName, final int line) {
		return tabContent.get(spaltenName)[line];
	}

	@Override
	public String toString() {
		String ergebnis = Constants.EMPTY_STRING;

		boolean brk = false;
		for (int i = 0; !brk; i++) {
			ergebnis += "Zeile (" + (i + 2) + "):\n";
			for (final String s : tabContent.keySet()) {
				if (tabContent.get(s).length <= i) {
					brk = true;
					break;
				}
				ergebnis += s + ": " + tabContent.get(s)[i] + ", ";
			}
			ergebnis += "\n";
		}

		return ergebnis;
	}

	/**
	 * Berechnet die Summe.
	 *
	 * @param elemente
	 *            Elemente ueber denen die Summe berechnt werden soll
	 * @return die Summe
	 */
	protected static Element summe(final Element... elemente) {
		final Element summeElement = new Element("0", "1");
		double wert = 0.0;
		double guete = 0.0;
		boolean intp = false;
		double zaehler = 0;

		for (final Element e : elemente) {
			if ((e.guete > 0.0) && !e.impl && (e.wert >= 0)) {
				guete += e.guete;
				intp |= e.intp;
				wert += e.wert;
				zaehler += 1.0;
			}
		}

		if (zaehler == 0.0) {
			summeElement.impl = true;
			summeElement.wert = -3.0;
			summeElement.guete = 0.0;
		} else {
			summeElement.intp = intp;
			summeElement.wert = wert;
			summeElement.guete = guete / zaehler;
		}

		return summeElement;
	}

	/**
	 * Berechnet den Durchschnitt.
	 *
	 * @param elemente
	 *            Elemente ueber denen der Durchschnitt berechnt werden soll
	 * @return der Durchschnitt
	 */
	protected static Element durchschnitt(final Element... elemente) {
		final Element durchschnittElement = new Element("0", "1");
		double wert = 0.0;
		double guete = 0.0;
		boolean intp = false;
		double zaehler = 0;

		for (final Element e : elemente) {
			if ((e.guete > 0.0) && !e.impl && (e.wert >= 0)) {
				guete += e.guete;
				intp |= e.intp;
				wert += e.wert;
				zaehler += 1.0;
			}
		}

		if (zaehler == 0.0) {
			durchschnittElement.impl = true;
			durchschnittElement.wert = -3.0;
			durchschnittElement.guete = 0.0;
		} else {
			durchschnittElement.intp = intp;
			durchschnittElement.wert = Math.round(wert / zaehler);
			durchschnittElement.guete = guete / zaehler;
		}

		return durchschnittElement;
	}

	/**
	 * Ein Tabellenelement: (Wert, Status)-Paar.
	 *
	 * @author BitCtrl Systems GmbH, Thierfelder
	 *
	 */
	public static final class Element {

		/**
		 * der Wert.
		 */
		public double wert = Double.NaN;

		/**
		 * die Guete.
		 */
		public double guete = Double.NaN;

		/**
		 * DUA-Flag.
		 */
		public boolean nErf = false;

		/**
		 * DUA-Flag.
		 */
		public boolean wMax = false;

		/**
		 * DUA-Flag.
		 */
		public boolean wMin = false;

		/**
		 * DUA-Flag.
		 */
		public boolean wMaL = false;

		/**
		 * DUA-Flag.
		 */
		public boolean wMiL = false;

		/**
		 * DUA-Flag.
		 */
		public boolean impl = false;

		/**
		 * DUA-Flag.
		 */
		public boolean intp = false;

		/**
		 * Standardkonstruktor.
		 *
		 * @param wertStr
		 *            der wertStr
		 * @param statusStr
		 *            der statusStr
		 */
		private Element(final String wertStr, final String statusStr) {
			wert = Double.parseDouble(wertStr);
			guete = 1.0;

			int errCode = 0;

			if ((statusStr != null) && (statusStr.length() > 0)) {
				final String[] splitStatus = statusStr.trim().split(" ");

				for (final String splitStatu : splitStatus) {
					if (splitStatu.equalsIgnoreCase("Fehl")) {
						errCode = errCode - 2;
					}
					if (splitStatu.equalsIgnoreCase("nErm")) {
						errCode = errCode - 1;
					}
					if (splitStatu.equalsIgnoreCase("Impl")) {
						impl = true;
					}
					if (splitStatu.equalsIgnoreCase("Intp")) {
						intp = true;
					}
					if (splitStatu.equalsIgnoreCase("nErf")) {
						nErf = true;
					}
					if (splitStatu.equalsIgnoreCase("wMaL")) {
						wMaL = true;
					}
					if (splitStatu.equalsIgnoreCase("wMax")) {
						wMax = true;
					}
					if (splitStatu.equalsIgnoreCase("wMiL")) {
						wMiL = true;
					}
					if (splitStatu.equalsIgnoreCase("wMin")) {
						wMin = true;
					}

					try {
						guete = Double.parseDouble(splitStatu.replace(",", "."));
					} catch (final Exception e) {
						// kein float Wert
					}
				}
			}

			if (errCode < 0) {
				wert = errCode;
			}
		}

		/**
		 * Erfragt den ersten Wert.
		 *
		 * @return der erste Wert
		 */
		public String getWert() {
			return new Long(Math.round(wert)).toString();
		}

		/**
		 * Erfragt den ersten Statuswert.
		 *
		 * @return der erste Wert
		 */
		public String getStatus() {
			String s = new Double(guete).toString();
			if (impl) {
				s += " Impl";
			}
			if (intp) {
				s += " Intp";
			}
			if (nErf) {
				s += " nErf";
			}
			if (wMaL) {
				s += " wMaL";
			}
			if (wMax) {
				s += " wMax";
			}
			if (wMiL) {
				s += " wMiL";
			}
			if (wMin) {
				s += " wMin";
			}

			return s;
		}

		@Override
		public String toString() {
			String s = "undefiniert";

			if (!Double.isNaN(wert)) {
				s = Constants.EMPTY_STRING;

				s += wert + " (";
				s += "G:" + guete;
				if (impl) {
					s += " Impl";
				}
				if (intp) {
					s += " Intp";
				}
				if (nErf) {
					s += " nErf";
				}
				if (wMaL) {
					s += " wMaL";
				}
				if (wMax) {
					s += " wMax";
				}
				if (wMiL) {
					s += " wMiL";
				}
				if (wMin) {
					s += " wMin";
				}

				s += ")";
			}

			return s;
		}
	}
}
