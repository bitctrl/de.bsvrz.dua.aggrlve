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
 * Liest die Ausgangsdaten für die Prüfung der Datenaufbereitung LVE ein
 *
 * @author BitCtrl Systems GmbH, Görlitz
 *
 * @verison $Id$
 */
public class AnalysewerteImporter extends CSVImporter {

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
	public AnalysewerteImporter(final ClientDavInterface dav, final String csvQuelle)
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
	 * Setzt Datenintervall
	 *
	 * @param t
	 *            Datenintervall
	 */
	public static final void setT(final long t) {
		INTERVALL = t;
	}

	/**
	 * Importiert die nächste Zeile aus der CSV-Datei
	 *
	 */
	public final void importNaechsteZeile() {
		ZEILE = getNaechsteZeile();
	}

	/**
	 * Bildet einen Ausgabe-Datensatz der MQ-Analysewerte aus den Daten der aktuellen CSV-Zeile
	 *
	 * @return ein Datensatz der übergebenen Attributgruppe mit den Daten der nächsten Zeile oder
	 *         <code>null</code>, wenn der Dateizeiger am Ende ist
	 */
	public final Data getMQAnalyseDatensatz() {
		Data datensatz = DAV.createData(DAV.getDataModel().getAttributeGroup(
				"atg.verkehrsDatenKurzZeitMq"));

		if (datensatz != null) {
			if (ZEILE != null) {
				try {
					final int QKfz = Integer.parseInt(ZEILE[84]);
					final String QKfzStatus = ZEILE[85];
					final int QLkw = Integer.parseInt(ZEILE[86]);
					final String QLkwStatus = ZEILE[87];
					final int QPkw = Integer.parseInt(ZEILE[88]);
					final String QPkwStatus = ZEILE[89];
					final int VKfz = Integer.parseInt(ZEILE[90]);
					final String VKfzStatus = ZEILE[91];
					final int VLkw = Integer.parseInt(ZEILE[92]);
					final String VLkwStatus = ZEILE[93];
					final int VPkw = Integer.parseInt(ZEILE[94]);
					final String VPkwStatus = ZEILE[95];
					final int VgKfz = Integer.parseInt(ZEILE[96]);
					final String VgKfzStatus = ZEILE[97];
					final int B = Integer.parseInt(ZEILE[98]);
					final String BStatus = ZEILE[99];
					final int BMax = Integer.parseInt(ZEILE[100]);
					final String BMaxStatus = ZEILE[101];
					final int SKfz = Integer.parseInt(ZEILE[100]);
					final String SKfzStatus = ZEILE[101];
					final int ALkw = Integer.parseInt(ZEILE[102]);
					final String ALkwStatus = ZEILE[103];
					final int KKfz = Integer.parseInt(ZEILE[104]);
					final String KKfzStatus = ZEILE[105];
					final int KLkw = Integer.parseInt(ZEILE[106]);
					final String KLkwStatus = ZEILE[107];
					final int KPkw = Integer.parseInt(ZEILE[108]);
					final String KPkwStatus = ZEILE[109];
					final int QB = Integer.parseInt(ZEILE[110]);
					final String QBStatus = ZEILE[111];
					final int KB = Integer.parseInt(ZEILE[112]);
					final String KBStatus = ZEILE[113];
					final int VDelta = Integer.parseInt(ZEILE[114]);
					final String VDeltaStatus = ZEILE[115];

					datensatz = setAttribut("QKfz", QKfz, QKfzStatus, datensatz);
					datensatz = setAttribut("QLkw", QLkw, QLkwStatus, datensatz);
					datensatz = setAttribut("QPkw", QPkw, QPkwStatus, datensatz);
					datensatz = setAttribut("VKfz", VKfz, VKfzStatus, datensatz);
					datensatz = setAttribut("VLkw", VLkw, VLkwStatus, datensatz);
					datensatz = setAttribut("VPkw", VPkw, VPkwStatus, datensatz);
					datensatz = setAttribut("VgKfz", VgKfz, VgKfzStatus, datensatz);
					datensatz = setAttribut("B", B, BStatus, datensatz);
					datensatz = setAttribut("BMax", BMax, BMaxStatus, datensatz);
					datensatz = setAttribut("SKfz", SKfz, SKfzStatus, datensatz);
					datensatz = setAttribut("ALkw", ALkw, ALkwStatus, datensatz);
					datensatz = setAttribut("KKfz", KKfz, KKfzStatus, datensatz);
					datensatz = setAttribut("KLkw", KLkw, KLkwStatus, datensatz);
					datensatz = setAttribut("KPkw", KPkw, KPkwStatus, datensatz);
					datensatz = setAttribut("QB", QB, QBStatus, datensatz);
					datensatz = setAttribut("KB", KB, KBStatus, datensatz);
					datensatz = setAttribut("VDelta", VDelta, VDeltaStatus, datensatz);

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
	 * Bildet einen Ausgabe-Datensatz der FS-Analysewerte aus den Daten der aktuellen CSV-Zeile
	 *
	 * @param FS
	 *            Fahrstreifen (1-3)
	 * @return ein Datensatz der übergebenen Attributgruppe mit den Daten der nächsten Zeile oder
	 *         <code>null</code>, wenn der Dateizeiger am Ende ist
	 */
	public final Data getFSAnalyseDatensatz(final int FS) {
		Data datensatz = DAV.createData(DAV.getDataModel().getAttributeGroup(
				"atg.verkehrsDatenKurzZeitIntervall"));

		final int fsMulti = FS - 1;

		if (datensatz != null) {

			if (ZEILE != null) {
				try {
					final int qKfz = Integer.parseInt(ZEILE[0 + (fsMulti * 2)]);
					final String qKfzStatus = ZEILE[1 + (fsMulti * 2)];
					final int qPkw = Integer.parseInt(ZEILE[6 + (fsMulti * 2)]);
					final String qPkwStatus = ZEILE[7 + (fsMulti * 2)];
					final int qLkw = Integer.parseInt(ZEILE[12 + (fsMulti * 2)]);
					final String qLkwStatus = ZEILE[13 + (fsMulti * 2)];
					final int vKfz = Integer.parseInt(ZEILE[18 + (fsMulti * 2)]);
					final String vKfzStatus = ZEILE[19 + (fsMulti * 2)];
					final int vPkw = Integer.parseInt(ZEILE[24 + (fsMulti * 2)]);
					final String vPkwStatus = ZEILE[25 + (fsMulti * 2)];
					final int vLkw = Integer.parseInt(ZEILE[30 + (fsMulti * 2)]);
					final String vLkwStatus = ZEILE[31 + (fsMulti * 2)];
					final int vgKfz = Integer.parseInt(ZEILE[36 + (fsMulti * 2)]);
					final String vgKfzStatus = ZEILE[37 + (fsMulti * 2)];
					final int b = Integer.parseInt(ZEILE[42 + (fsMulti * 2)]);
					final String bStatus = ZEILE[43 + (fsMulti * 2)];
					final int aLkw = Integer.parseInt(ZEILE[48 + (fsMulti * 2)]);
					final String aLkwStatus = ZEILE[49 + (fsMulti * 2)];
					final int sKfz = 1;

					datensatz.getTimeValue("T").setMillis(INTERVALL);
					datensatz.getUnscaledValue("ArtMittelwertbildung").set(0);
					datensatz = setAttribut("qKfz", qKfz, qKfzStatus, datensatz);
					datensatz = setAttribut("qPkw", qPkw, qPkwStatus, datensatz);
					datensatz = setAttribut("qLkw", qLkw, qLkwStatus, datensatz);
					datensatz = setAttribut("vKfz", vKfz, vKfzStatus, datensatz);
					datensatz = setAttribut("vPkw", vPkw, vPkwStatus, datensatz);
					datensatz = setAttribut("vLkw", vLkw, vLkwStatus, datensatz);
					datensatz = setAttribut("vgKfz", vgKfz, vgKfzStatus, datensatz);
					datensatz = setAttribut("b", b, bStatus, datensatz);
					datensatz = setAttribut("tNetto", aLkw, aLkwStatus, datensatz);
					datensatz = setAttribut("sKfz", sKfz, null, datensatz);

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
	 * Setzt Attribut in Datensatz
	 *
	 * @param attributName
	 *            Name des Attributs
	 * @param wert
	 *            Wert des Attributs
	 * @param datensatz
	 *            der Datensatz
	 * @return der veränderte Datensatz
	 */
	private final Data setAttribut(final String attributName, long wert, final String status,
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
		DUAUtensilien
				.getAttributDatum(attributName + ".Status.MessWertErsetzung.Implausibel", data)
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
