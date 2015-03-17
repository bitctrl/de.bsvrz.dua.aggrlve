/**
 * Segment 4 Daten�bernahme und Aufbereitung (DUA)
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

import com.bitctrl.Constants;

import de.bsvrz.dav.daf.main.ClientDavInterface;
import de.bsvrz.dav.daf.main.Data;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAUtensilien;
import de.bsvrz.sys.funclib.bitctrl.dua.test.CSVImporter;

/**
 * Liest die Ausgangsdaten f�r die Pr�fung der Datenaufbereitung LVE ein.
 *
 * @author BitCtrl Systems GmbH, G�rlitz
 *
 * @version $Id$
 */
public final class TestErgebnisAnalyseImporter extends CSVImporter {

	/**
	 * Verbindung zum Datenverteiler.
	 */
	protected static ClientDavInterface sDav = null;

	/**
	 * H�lt aktuelle Daten des FS 1-3.
	 */
	protected String[] zeile;

	/**
	 * T.
	 */
	protected static long iNTERVALL = Constants.MILLIS_PER_MINUTE;

	/**
	 * Standardkonstruktor.
	 *
	 * @param dav
	 *            Datenverteier-Verbindung
	 * @param csvQuelle
	 *            Quelle der Daten (CSV-Datei)
	 * @throws Exception
	 *             falls dieses Objekt nicht vollst�ndig initialisiert werden konnte
	 */
	public TestErgebnisAnalyseImporter(final ClientDavInterface dav, final String csvQuelle)
			throws Exception {
		super(csvQuelle);
		if (sDav == null) {
			sDav = dav;
		}

		/**
		 * Tabellenkopf �berspringen
		 */
		getNaechsteZeile();
	}

	/**
	 * Setzt Datenintervall.
	 *
	 * @param t
	 *            Datenintervall
	 */
	public static void setT(final long t) {
		iNTERVALL = t;
	}

	/**
	 * Importiert die n�chste Zeile aus der CSV-Datei.
	 *
	 */
	public void importNaechsteZeile() {
		zeile = getNaechsteZeile();
	}

	/**
	 * Bildet einen Ausgabe-Datensatz der MQ-Analysewerte aus den Daten der aktuellen CSV-Zeile.
	 *
	 * @return ein Datensatz der �bergebenen Attributgruppe mit den Daten der n�chsten Zeile oder
	 *         <code>null</code>, wenn der Dateizeiger am Ende ist
	 */
	public Data getMQAnalyseDatensatz() {
		Data datensatz = sDav.createData(sDav.getDataModel().getAttributeGroup(
				"atg.verkehrsDatenKurzZeitMq"));

		if (datensatz != null) {
			if (zeile != null) {
				try {
					final int QKfz = Integer.parseInt(zeile[84]);
					final String QKfzStatus = zeile[85];
					final int QLkw = Integer.parseInt(zeile[86]);
					final String QLkwStatus = zeile[87];
					final int QPkw = Integer.parseInt(zeile[88]);
					final String QPkwStatus = zeile[89];
					final int VKfz = Integer.parseInt(zeile[90]);
					final String VKfzStatus = zeile[91];
					final int VLkw = Integer.parseInt(zeile[92]);
					final String VLkwStatus = zeile[93];
					final int VPkw = Integer.parseInt(zeile[94]);
					final String VPkwStatus = zeile[95];
					final int VgKfz = Integer.parseInt(zeile[96]);
					final String VgKfzStatus = zeile[97];
					final int B = Integer.parseInt(zeile[98]);
					final String BStatus = zeile[99];
					final int BMax = Integer.parseInt(zeile[100]);
					final String BMaxStatus = zeile[101];
					final int SKfz = Integer.parseInt(zeile[100]);
					final String SKfzStatus = zeile[101];
					final int ALkw = Integer.parseInt(zeile[102]);
					final String ALkwStatus = zeile[103];
					final int KKfz = Integer.parseInt(zeile[104]);
					final String KKfzStatus = zeile[105];
					final int KLkw = Integer.parseInt(zeile[106]);
					final String KLkwStatus = zeile[107];
					final int KPkw = Integer.parseInt(zeile[108]);
					final String KPkwStatus = zeile[109];
					final int QB = Integer.parseInt(zeile[110]);
					final String QBStatus = zeile[111];
					final int KB = Integer.parseInt(zeile[112]);
					final String KBStatus = zeile[113];
					final int VDelta = Integer.parseInt(zeile[114]);
					final String VDeltaStatus = zeile[115];

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
	 * Bildet einen Ausgabe-Datensatz der FS-Analysewerte aus den Daten der aktuellen CSV-Zeile.
	 *
	 * @param fs
	 *            Fahrstreifen (1-3)
	 * @return ein Datensatz der �bergebenen Attributgruppe mit den Daten der n�chsten Zeile oder
	 *         <code>null</code>, wenn der Dateizeiger am Ende ist
	 */
	public Data getFSAnalyseDatensatz(final int fs) {
		Data datensatz = sDav.createData(sDav.getDataModel().getAttributeGroup(
				"atg.verkehrsDatenKurzZeitFs"));

		final int fsMulti = fs - 1;

		if (datensatz != null) {

			if (zeile != null) {
				try {
					final int qKfz = Integer.parseInt(zeile[0 + (fsMulti * 2)]);
					final String qKfzStatus = zeile[1 + (fsMulti * 2)];
					final int qPkw = Integer.parseInt(zeile[6 + (fsMulti * 2)]);
					final String qPkwStatus = zeile[7 + (fsMulti * 2)];
					final int qLkw = Integer.parseInt(zeile[12 + (fsMulti * 2)]);
					final String qLkwStatus = zeile[13 + (fsMulti * 2)];
					final int vKfz = Integer.parseInt(zeile[18 + (fsMulti * 2)]);
					final String vKfzStatus = zeile[19 + (fsMulti * 2)];
					final int vPkw = Integer.parseInt(zeile[24 + (fsMulti * 2)]);
					final String vPkwStatus = zeile[25 + (fsMulti * 2)];
					final int vLkw = Integer.parseInt(zeile[30 + (fsMulti * 2)]);
					final String vLkwStatus = zeile[31 + (fsMulti * 2)];
					final int vgKfz = Integer.parseInt(zeile[36 + (fsMulti * 2)]);
					final String vgKfzStatus = zeile[37 + (fsMulti * 2)];
					final int b = Integer.parseInt(zeile[42 + (fsMulti * 2)]);
					final String bStatus = zeile[43 + (fsMulti * 2)];
					final int aLkw = Integer.parseInt(zeile[48 + (fsMulti * 2)]);
					final String aLkwStatus = zeile[49 + (fsMulti * 2)];
					final int kKfz = Integer.parseInt(zeile[54 + (fsMulti * 2)]);
					final String kKfzStatus = zeile[55 + (fsMulti * 2)];
					final int kLkw = Integer.parseInt(zeile[60 + (fsMulti * 2)]);
					final String kLkwStatus = zeile[61 + (fsMulti * 2)];
					final int kPkw = Integer.parseInt(zeile[66 + (fsMulti * 2)]);
					final String kPkwStatus = zeile[67 + (fsMulti * 2)];
					final int qB = Integer.parseInt(zeile[72 + (fsMulti * 2)]);
					final String qBStatus = zeile[73 + (fsMulti * 2)];
					final int kB = Integer.parseInt(zeile[78 + (fsMulti * 2)]);
					final String kBStatus = zeile[79 + (fsMulti * 2)];
					final int sKfz = 1;

					if ((vLkw == -1) && vLkwStatus.equals("0 nErm ")) {
						datensatz = setAttribut("vLkw", 0, "0", datensatz);
					} else {
						datensatz = setAttribut("vLkw", vLkw, vLkwStatus, datensatz);
					}
					datensatz.getTimeValue("T").setMillis(iNTERVALL);
					datensatz = setAttribut("qKfz", qKfz, qKfzStatus, datensatz);
					datensatz = setAttribut("qPkw", qPkw, qPkwStatus, datensatz);
					datensatz = setAttribut("qLkw", qLkw, qLkwStatus, datensatz);
					datensatz = setAttribut("vKfz", vKfz, vKfzStatus, datensatz);
					datensatz = setAttribut("vPkw", vPkw, vPkwStatus, datensatz);

					datensatz = setAttribut("vgKfz", vgKfz, vgKfzStatus, datensatz);
					datensatz = setAttribut("b", b, bStatus, datensatz);
					datensatz = setAttribut("aLkw", aLkw, aLkwStatus, datensatz);
					datensatz = setAttribut("kKfz", kKfz, kKfzStatus, datensatz);
					datensatz = setAttribut("kLkw", kLkw, kLkwStatus, datensatz);
					datensatz = setAttribut("kPkw", kPkw, kPkwStatus, datensatz);
					datensatz = setAttribut("qB", qB, qBStatus, datensatz);
					datensatz = setAttribut("kB", kB, kBStatus, datensatz);
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
	 * @return der ver�nderte Datensatz
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
		DUAUtensilien
				.getAttributDatum(attributName + ".Status.MessWertErsetzung.Implausibel", data)
		.asUnscaledValue().set(impl);
		DUAUtensilien
				.getAttributDatum(attributName + ".Status.MessWertErsetzung.Interpoliert", data)
		.asUnscaledValue().set(intp);
		DUAUtensilien.getAttributDatum(attributName + ".G�te.Index", data).asScaledValue()
		.set(guete);
		DUAUtensilien.getAttributDatum(attributName + ".G�te.Verfahren", data).asUnscaledValue()
		.set(0);

		return datensatz;
	}
}