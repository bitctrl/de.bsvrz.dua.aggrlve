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

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;

import de.bsvrz.dua.aggrlve.Verbindung;

/**
 * Dieses Programm produziert die Tabellen <code>5-6</code> und
 * <code>5-7</code> aus der Prüfspezifikation V 2.0 auf Basis der Tabelle
 * <code>Messwert_Aggregation_unv</code>
 * 
 * @author BitCtrl Systems GmbH, Thierfelder
 * 
 * @verison $Id$
 */
public class Kalkulation {

	/**
	 * Alle Attribute, die geschrieben werden sollen und die Art, wie sie
	 * Berechnet werden (nachdem der Mittelwert gebildet wurde)
	 */
	private static final RechenVorschrift[] ATTRIBUTE = new RechenVorschrift[] {
			new RechenVorschrift("qKfz(t)(1)", false, null), //$NON-NLS-1$
			new RechenVorschrift("qKfz(t)(2)", false, null), //$NON-NLS-1$
			new RechenVorschrift(
					"QKfz(Mq)", true, new String[] { "qKfz(t)(1)", "qKfz(t)(2)" }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			new RechenVorschrift("qPkw(t)(1)", false, null), //$NON-NLS-1$
			new RechenVorschrift("qPkw(t)(2)", false, null), //$NON-NLS-1$
			new RechenVorschrift(
					"QPkw(Mq)", true, new String[] { "qPkw(t)(1)", "qPkw(t)(2)" }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			new RechenVorschrift("qLkw(t)(1)", false, null), //$NON-NLS-1$
			new RechenVorschrift("qLkw(t)(2)", false, null), //$NON-NLS-1$
			new RechenVorschrift(
					"QLkw(Mq)", true, new String[] { "qLkw(t)(1)", "qLkw(t)(2)" }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			new RechenVorschrift("vKfz(t)(1)", false, null), //$NON-NLS-1$
			new RechenVorschrift("vKfz(t)(2)", false, null), //$NON-NLS-1$
			new RechenVorschrift(
					"VKfz(Mq)", false, new String[] { "vKfz(t)(1)", "vKfz(t)(2)" }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			new RechenVorschrift("vPkw(t)(1)", false, null), //$NON-NLS-1$
			new RechenVorschrift("vPkw(t)(2)", false, null), //$NON-NLS-1$
			new RechenVorschrift(
					"VPkw(Mq)", false, new String[] { "vPkw(t)(1)", "vPkw(t)(2)" }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			new RechenVorschrift("vLkw(t)(1)", false, null), //$NON-NLS-1$
			new RechenVorschrift("vLkw(t)(2)", false, null), //$NON-NLS-1$
			new RechenVorschrift(
					"VLkw(Mq)", false, new String[] { "vLkw(t)(1)", "vLkw(t)(2)" }) }; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

	/**
	 * Datenimporter
	 */
	private PruefTabImporter importer = null;

	/**
	 * Startet dieses Programm
	 * 
	 * @param args
	 *            Kommandozeilenparameter
	 */
	public static void main(String[] args) {
		final String name56 = Verbindung.WURZEL + "Tabelle56.csv"; //$NON-NLS-1$
		final String name57 = Verbindung.WURZEL + "Tabelle57.csv"; //$NON-NLS-1$
		final String quelle = Verbindung.WURZEL
				+ "Messwert_Aggregation_unv.csv"; //$NON-NLS-1$

		try {
			Kalkulation kalkulation = new Kalkulation(quelle);
			kalkulation.erzeugeTab(3, name56);
			kalkulation.erzeugeTab(2, name57);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Standardkonstruktor
	 * 
	 * @param messwertAggregationUnvDatei
	 *            Datei der Tabelle <code>Messwert_Aggregation_unv</code>
	 * @throws Exception
	 *             wenn die Datei nicht geoeffnet werden kann
	 */
	private Kalkulation(final String messwertAggregationUnvDatei)
			throws Exception {
		this.importer = new PruefTabImporter(messwertAggregationUnvDatei);
	}

	/**
	 * Berechnet aus der Eingabedatei
	 * 
	 * @param anzahlDatenSaetze
	 *            Anzahl der Datensaetze, aus denen die Aggregations daten
	 *            zusammengerechnet werden sollen
	 * @param dateiName
	 *            der Name der Ausgabedatei (CSV)
	 * @throws Exception
	 *             wenn es Probleme beim Lesen oder Schreiben gab
	 */
	private final void erzeugeTab(final int anzahlDatenSaetze,
			final String dateiName) throws Exception {
		File file = new File(dateiName);

		if (file.exists()) {
			file.delete();
		}

		if (file.createNewFile()) {
			FileOutputStream stream = new FileOutputStream(file);
			for (RechenVorschrift rv : ATTRIBUTE) {
				stream.write(rv.name.getBytes());
				stream.write(";".getBytes()); //$NON-NLS-1$
				stream.write("Status".getBytes()); //$NON-NLS-1$
				stream.write(";".getBytes()); //$NON-NLS-1$
			}
			stream.write("\n".getBytes()); //$NON-NLS-1$

			int offset = 0;

			/**
			 * Berechne 3 Werte
			 */
			for (int i = 0; i < 3; i++) {
				for (RechenVorschrift rv : ATTRIBUTE) {

					if (rv.quellen == null) {
						/**
						 * Fahrstreifendaten
						 */
						List<PruefTabImporter.Element> elemente = new ArrayList<PruefTabImporter.Element>();
						for (int j = 0 + offset; j < offset + anzahlDatenSaetze; j++) {
							try {
								elemente.add(this.importer.getElement(rv.name,
										j));
							} catch (Exception ex) {
								ex.printStackTrace();
								System.out.println(rv.name);
							}
						}
						PruefTabImporter.Element durchschnitt = PruefTabImporter
								.durchschnitt(elemente
										.toArray(new PruefTabImporter.Element[0]));

						stream.write(durchschnitt.getWert().getBytes());
						stream.write(";".getBytes()); //$NON-NLS-1$
						stream.write(durchschnitt.getStatus().getBytes());
						stream.write(";".getBytes()); //$NON-NLS-1$
					} else {
						List<PruefTabImporter.Element> elementeMQ = new ArrayList<PruefTabImporter.Element>();
						/**
						 * Fahrstreifendaten #1
						 */
						List<PruefTabImporter.Element> elementeFS1 = new ArrayList<PruefTabImporter.Element>();
						for (int j = 0 + offset; j < offset + anzahlDatenSaetze; j++) {
							elementeFS1.add(this.importer.getElement(
									rv.quellen[0], j));
						}
						elementeMQ
								.add(PruefTabImporter
										.durchschnitt(elementeFS1
												.toArray(new PruefTabImporter.Element[0])));
						/**
						 * Fahrstreifendaten #2
						 */
						List<PruefTabImporter.Element> elementeFS2 = new ArrayList<PruefTabImporter.Element>();
						for (int j = 0 + offset; j < offset + anzahlDatenSaetze; j++) {
							elementeFS2.add(this.importer.getElement(
									rv.quellen[1], j));
						}
						elementeMQ
								.add(PruefTabImporter
										.durchschnitt(elementeFS2
												.toArray(new PruefTabImporter.Element[0])));

						PruefTabImporter.Element ergebnis = null;
						if (rv.summe) {
							ergebnis = PruefTabImporter.summe(elementeMQ
									.toArray(new PruefTabImporter.Element[0]));
						} else {
							ergebnis = PruefTabImporter.durchschnitt(elementeMQ
									.toArray(new PruefTabImporter.Element[0]));
						}

						stream.write(ergebnis.getWert().getBytes());
						stream.write(";".getBytes()); //$NON-NLS-1$
						stream.write(ergebnis.getStatus().getBytes());
						stream.write(";".getBytes()); //$NON-NLS-1$
					}
				}
				stream.write("\n".getBytes()); //$NON-NLS-1$
				offset += anzahlDatenSaetze;
			}
		} else {
			throw new Exception("Konnte Datei " + //$NON-NLS-1$
					(Verbindung.WURZEL + dateiName) + " nicht anlegen"); //$NON-NLS-1$
		}
	}

	/**
	 * Hilfklasse zur Beschreibung der Ermittlung der Werte in der Zieltabelle
	 * 
	 * @author BitCtrl Systems GmbH, Thierfelder
	 * 
	 */
	private static class RechenVorschrift {

		/**
		 * Name der Ausgabespalte
		 */
		public String name = null;

		/**
		 * Ansonsten wird der Durchschnitt berechnet
		 */
		public boolean summe = true;

		/**
		 * die Daten aus denen sich die Ergebnisse ermitteln (Spaltennamen)
		 */
		public String[] quellen = null;

		/**
		 * Standardkonstruktor
		 * 
		 * @param name
		 *            Name der Ausgabespalte
		 * @param summe
		 *            ansonsten wird der Durchschnitt berechnet
		 * @param quellen
		 *            die Daten aus denen sich die Ergebnisse ermitteln
		 *            (Spaltennamen)
		 */
		protected RechenVorschrift(String name, boolean summe, String[] quellen) {
			this.name = name;
			this.summe = summe;
			this.quellen = quellen;
		}

	}
}
