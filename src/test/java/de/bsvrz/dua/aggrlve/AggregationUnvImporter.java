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

import com.bitctrl.Constants;

import de.bsvrz.dav.daf.main.ClientDavInterface;
import de.bsvrz.dav.daf.main.Data;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAUtensilien;
import de.bsvrz.sys.funclib.bitctrl.dua.test.CSVImporter;

/**
 * Liest die Ausgangsdaten für die Prüfung der Aggregation LVE (TV und DTV) ein
 *
 * @author BitCtrl Systems GmbH, Thierfelder
 */
public class AggregationUnvImporter extends CSVImporter {

	/**
	 * Verbindung zum Datenverteiler
	 */
	protected static ClientDavInterface DAV = null;

	/**
	 * Hält aktuelle Daten des FS 1-3
	 */
	protected String[] ZEILE;

	/**
	 * T
	 */
	protected static long INTERVALL = Constants.MILLIS_PER_MINUTE;

	/**
	 * Standardkonstruktor
	 *
	 * @param dav
	 *            Datenverteier-Verbindung
	 * @param csvQuelle
	 *            Quelle der Daten (CSV-Datei)
	 * @throws Exception
	 *             falls dieses Objekt nicht vollständig initialisiert werden konnte
	 */
	public AggregationUnvImporter(final ClientDavInterface dav, final String csvQuelle)
			throws Exception {
		super(csvQuelle);
		if (DAV == null) {
			DAV = dav;
		}

		/**
		 * Tabellenkopf überspringen
		 */
		getNaechsteZeile();
	}

	/**
	 * Setzt Datenintervall.
	 *
	 * @param t
	 *            Datenintervall
	 */
	public static final void setT(final long t) {
		INTERVALL = t;
	}

	/**
	 * Importiert die nächste Zeile aus der CSV-Datei.
	 *
	 */
	public final void importNaechsteZeile() {
		ZEILE = getNaechsteZeile();
	}

	/**
	 * Bildet einen Ausgabe-Datensatz der Analysewerte aus den Daten der aktuellen CSV-Zeile.
	 *
	 * @param mq
	 *            Ob es sich um einen Messquerschnitt handelt
	 * @param intervallLaenge
	 *            Intervalllaenge bei Fahrstreifendaten
	 * @param fsIndex
	 *            der Index des Fahrstreifens
	 * @return ein Datensatz der übergebenen Attributgruppe mit den Daten der nächsten Zeile oder
	 *         <code>null</code>, wenn der Dateizeiger am Ende ist
	 */
	public final Data getAnalyseDatensatz(final boolean mq, final long intervallLaenge,
			final int fsIndex) {

		Data datensatz = DAV
				.createData(DAV.getDataModel().getAttributeGroup("atg.verkehrsDatenKurzZeitMq"));
		if (!mq) {
			datensatz = DAV.createData(
					DAV.getDataModel().getAttributeGroup("atg.verkehrsDatenKurzZeitFs"));
			datensatz.getTimeValue("T").setMillis(intervallLaenge);
		}

		if (datensatz != null) {
			if (ZEILE != null) {
				try {
					int c = fsIndex * 2;
					int QKfz = parseAlsPositiveZahl(ZEILE[c + 0]);
					String QKfzStatus = ZEILE[c + 1];
					if (QKfz == -3) {
						QKfzStatus = "Fehl nErm";
						QKfz = 0;
					}

					c += 2;
					int QPkw = parseAlsPositiveZahl(ZEILE[c + 2]);
					String QPkwStatus = ZEILE[c + 3];
					if (QPkw == -3) {
						QPkwStatus = "Fehl nErm";
						QPkw = 0;
					}

					c += 2;
					int QLkw = parseAlsPositiveZahl(ZEILE[c + 4]);
					String QLkwStatus = ZEILE[c + 5];
					if (QLkw == -3) {
						QLkwStatus = "Fehl nErm";
						QLkw = 0;
					}

					c += 2;
					int VKfz = parseAlsPositiveZahl(ZEILE[c + 6]);
					String VKfzStatus = ZEILE[c + 7];
					if (VKfz == -3) {
						VKfzStatus = "Fehl nErm";
						VKfz = 0;
					}

					c += 2;
					int VPkw = parseAlsPositiveZahl(ZEILE[c + 8]);
					String VPkwStatus = ZEILE[c + 9];
					if (VPkw == -3) {
						VPkwStatus = "Fehl nErm";
						VPkw = 0;
					}

					c += 2;
					int VLkw = parseAlsPositiveZahl(ZEILE[c + 10]);
					String VLkwStatus = ZEILE[c + 11];
					if (VLkw == -3) {
						VLkwStatus = "Fehl nErm";
						VLkw = 0;
					}

					datensatz = setAttribut((mq ? "Q" : "q") + "Kfz", QKfz, QKfzStatus, datensatz);
					datensatz = setAttribut((mq ? "Q" : "q") + "Lkw", QLkw, QLkwStatus, datensatz);
					datensatz = setAttribut((mq ? "Q" : "q") + "Pkw", QPkw, QPkwStatus, datensatz);
					datensatz = setAttribut((mq ? "V" : "v") + "Kfz", VKfz, VKfzStatus, datensatz);
					datensatz = setAttribut((mq ? "V" : "v") + "Lkw", VLkw, VLkwStatus, datensatz);
					datensatz = setAttribut((mq ? "V" : "v") + "Pkw", VPkw, VPkwStatus, datensatz);
					datensatz = setAttribut((mq ? "V" : "v") + "gKfz", -1, "0", datensatz);
					datensatz = setAttribut((mq ? "B" : "b"), -1, "0", datensatz);
					if (mq) {
						datensatz = setAttribut((mq ? "B" : "b") + "Max", -1, "0", datensatz);
						datensatz = setAttribut((mq ? "V" : "v") + "Delta", -1, "0", datensatz);
					}
					datensatz = setAttribut((mq ? "S" : "s") + "Kfz", -1, "0", datensatz);
					datensatz = setAttribut((mq ? "A" : "a") + "Lkw", -1, "0", datensatz);
					datensatz = setAttribut((mq ? "K" : "k") + "Kfz", -1, "0", datensatz);
					datensatz = setAttribut((mq ? "K" : "k") + "Lkw", -1, "0", datensatz);
					datensatz = setAttribut((mq ? "K" : "k") + "Pkw", -1, "0", datensatz);
					datensatz = setAttribut((mq ? "Q" : "q") + "B", -1, "0", datensatz);
					datensatz = setAttribut((mq ? "K" : "k") + "B", -1, "0", datensatz);
				} catch (final ArrayIndexOutOfBoundsException ex) {
					datensatz = null;
				}
			} else {
				datensatz = null;
			}
		}

		return datensatz;
	}

	/**
	 * Bildet einen Ausgabe-Datensatz der Analysewerte aus den Daten der aktuellen CSV-Zeile.
	 *
	 * @param intervallLaenge
	 *            Intervalllaenge bei Fahrstreifendaten
	 * @param fsIndex
	 *            der Index des Fahrstreifens
	 * @return ein Datensatz der übergebenen Attributgruppe mit den Daten der nächsten Zeile oder
	 *         <code>null</code>, wenn der Dateizeiger am Ende ist
	 */
	public final Data getMWEDatensatz(final long intervallLaenge, final int fsIndex) {

		Data datensatz = DAV
				.createData(DAV.getDataModel().getAttributeGroup(DUAKonstanten.ATG_KZD));
		datensatz.getTimeValue("T").setMillis(intervallLaenge);
		datensatz.getUnscaledValue("ArtMittelwertbildung").set(0);

		if (datensatz != null) {
			if (ZEILE != null) {
				try {
					int c = fsIndex * 2;
					int QKfz = parseAlsPositiveZahl(ZEILE[c + 0]);
					String QKfzStatus = ZEILE[c + 1];
					if (QKfz == -3) {
						QKfzStatus = "Fehl nErm";
						QKfz = 0;
					}

					c += 2;
					int QPkw = parseAlsPositiveZahl(ZEILE[c + 2]);
					String QPkwStatus = ZEILE[c + 3];
					if (QPkw == -3) {
						QPkwStatus = "Fehl nErm";
						QPkw = 0;
					}

					c += 2;
					int QLkw = parseAlsPositiveZahl(ZEILE[c + 4]);
					String QLkwStatus = ZEILE[c + 5];
					if (QLkw == -3) {
						QLkwStatus = "Fehl nErm";
						QLkw = 0;
					}

					c += 2;
					int VKfz = parseAlsPositiveZahl(ZEILE[c + 6]);
					String VKfzStatus = ZEILE[c + 7];
					if (VKfz == -3) {
						VKfzStatus = "Fehl nErm";
						VKfz = 0;
					}

					c += 2;
					int VPkw = parseAlsPositiveZahl(ZEILE[c + 8]);
					String VPkwStatus = ZEILE[c + 9];
					if (VPkw == -3) {
						VPkwStatus = "Fehl nErm";
						VPkw = 0;
					}

					c += 2;
					int VLkw = parseAlsPositiveZahl(ZEILE[c + 10]);
					String VLkwStatus = ZEILE[c + 11];
					if (VLkw == -3) {
						VLkwStatus = "Fehl nErm";
						VLkw = 0;
					}

					datensatz = setAttribut("qKfz", QKfz, QKfzStatus, datensatz);
					datensatz = setAttribut("qLkw", QLkw, QLkwStatus, datensatz);
					datensatz = setAttribut("qPkw", QPkw, QPkwStatus, datensatz);
					datensatz = setAttribut("vKfz", VKfz, VKfzStatus, datensatz);
					datensatz = setAttribut("vLkw", VLkw, VLkwStatus, datensatz);
					datensatz = setAttribut("vPkw", VPkw, VPkwStatus, datensatz);
					datensatz = setAttribut("vgKfz", -1, "0", datensatz);
					datensatz = setAttribut("b", -1, "0", datensatz);
					datensatz = setAttribut("sKfz", -1, "0", datensatz);
					datensatz = setAttribut("tNetto", -1, "0", datensatz);
				} catch (final ArrayIndexOutOfBoundsException ex) {
					datensatz = null;
				}
			} else {
				datensatz = null;
			}
		}

		return datensatz;
	}

	/**
	 * Erfragt den Zahlenwert der geparsten Zeichenkette.
	 *
	 * @param zahl
	 *            eine Zahl als Zeichenkette
	 * @return den Zahlenwert der geparsten Zeichenkette
	 */
	private int parseAlsPositiveZahl(final String zahl) {
		int a = Integer.parseInt(zahl);
		if (a < 0) {
			a = -3;
		}
		return a;
	}

	/**
	 * Setzt Attribut in Datensatz.
	 *
	 * @param attributName
	 *            Name des Attributs
	 * @param wert
	 *            Wert des Attributs
	 * @param status
	 *            der Status
	 * @param datensatz
	 *            der Datensatz
	 * @return der veränderte Datensatz
	 */
	private Data setAttribut(final String attributName, long wert, final String status,
			final Data datensatz) {
		final Data data = datensatz;

		if ((attributName.startsWith("v") || attributName.startsWith("V")) && (wert >= 255)) {
			wert = -1;
		}

		if ((attributName.startsWith("k") || attributName.startsWith("K")) && (wert > 10000)) {
			wert = -1;
		}

		int nErf = DUAKonstanten.NEIN;
		int wMax = DUAKonstanten.NEIN;
		int wMin = DUAKonstanten.NEIN;
		int wMaL = DUAKonstanten.NEIN;
		int wMiL = DUAKonstanten.NEIN;
		int impl = DUAKonstanten.NEIN;
		int intp = DUAKonstanten.NEIN;
		double guete = 1.0;

		int errCode = 0;

		if (status != null) {
			final String[] splitStatus = status.trim().split(" ");

			for (final String splitStatu : splitStatus) {
				if (splitStatu.equalsIgnoreCase("Fehl")) {
					errCode = errCode - 2;
				}

				if (splitStatu.equalsIgnoreCase("nErm")) {
					errCode = errCode - 1;
				}

				if (splitStatu.equalsIgnoreCase("Impl")) {
					impl = DUAKonstanten.JA;
				}

				if (splitStatu.equalsIgnoreCase("Intp")) {
					intp = DUAKonstanten.JA;
				}

				if (splitStatu.equalsIgnoreCase("nErf")) {
					nErf = DUAKonstanten.JA;
				}

				if (splitStatu.equalsIgnoreCase("wMaL")) {
					wMaL = DUAKonstanten.JA;
				}

				if (splitStatu.equalsIgnoreCase("wMax")) {
					wMax = DUAKonstanten.JA;
				}

				if (splitStatu.equalsIgnoreCase("wMiL")) {
					wMiL = DUAKonstanten.JA;
				}

				if (splitStatu.equalsIgnoreCase("wMin")) {
					wMin = DUAKonstanten.JA;
				}

				try {
					guete = Float.parseFloat(splitStatu.replace(",", "."));
				} catch (final Exception e) {
					// kein float Wert
				}
			}
		}

		if (errCode < 0) {
			wert = errCode;
		}

		DUAUtensilien.getAttributDatum(attributName + ".Wert", data).asUnscaledValue().set(wert);
		DUAUtensilien.getAttributDatum(attributName + ".Status.Erfassung.NichtErfasst", data)
				.asUnscaledValue().set(nErf);
		DUAUtensilien.getAttributDatum(attributName + ".Status.PlFormal.WertMax", data)
				.asUnscaledValue().set(wMax);
		DUAUtensilien.getAttributDatum(attributName + ".Status.PlFormal.WertMin", data)
				.asUnscaledValue().set(wMin);
		DUAUtensilien.getAttributDatum(attributName + ".Status.PlLogisch.WertMaxLogisch", data)
				.asUnscaledValue().set(wMaL);
		DUAUtensilien.getAttributDatum(attributName + ".Status.PlLogisch.WertMinLogisch", data)
				.asUnscaledValue().set(wMiL);
		DUAUtensilien.getAttributDatum(attributName + ".Status.MessWertErsetzung.Implausibel", data)
				.asUnscaledValue().set(impl);
		DUAUtensilien
		.getAttributDatum(attributName + ".Status.MessWertErsetzung.Interpoliert", data)
				.asUnscaledValue().set(intp);
		DUAUtensilien.getAttributDatum(attributName + ".Güte.Index", data).asScaledValue()
				.set(guete);
		DUAUtensilien.getAttributDatum(attributName + ".Güte.Verfahren", data).asUnscaledValue()
				.set(0);

		return datensatz;
	}
}
