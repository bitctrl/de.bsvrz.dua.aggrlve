/** 
 * Segment 4 Datenübernahme und Aufbereitung (DUA)
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

import com.bitctrl.Constants;

import de.bsvrz.dav.daf.main.ClientDavInterface;
import de.bsvrz.dav.daf.main.Data;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAUtensilien;
import de.bsvrz.sys.funclib.bitctrl.dua.test.CSVImporter;

/**
 * Liest die Ausgangsdaten für die Prüfung der Datenaufbereitung LVE ein.
 * 
 * @author BitCtrl Systems GmbH, Görlitz
 * 
 * @version $Id$
 */
public final class TestErgebnisAnalyseImporter extends CSVImporter {

	/**
	 * Verbindung zum Datenverteiler.
	 */
	protected static ClientDavInterface sDav = null;

	/**
	 * Hält aktuelle Daten des FS 1-3.
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
	 *             falls dieses Objekt nicht vollständig initialisiert werden
	 *             konnte
	 */
	public TestErgebnisAnalyseImporter(final ClientDavInterface dav,
			final String csvQuelle) throws Exception {
		super(csvQuelle);
		if (sDav == null) {
			sDav = dav;
		}

		/**
		 * Tabellenkopf überspringen
		 */
		this.getNaechsteZeile();
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
	 * Importiert die nächste Zeile aus der CSV-Datei.
	 * 
	 */
	public void importNaechsteZeile() {
		zeile = this.getNaechsteZeile();
	}

	/**
	 * Bildet einen Ausgabe-Datensatz der MQ-Analysewerte aus den Daten der
	 * aktuellen CSV-Zeile.
	 * 
	 * @return ein Datensatz der übergebenen Attributgruppe mit den Daten der
	 *         nächsten Zeile oder <code>null</code>, wenn der Dateizeiger am
	 *         Ende ist
	 */
	public Data getMQAnalyseDatensatz() {
		Data datensatz = sDav.createData(sDav.getDataModel().getAttributeGroup(
				"atg.verkehrsDatenKurzZeitMq")); //$NON-NLS-1$

		if (datensatz != null) {
			if (zeile != null) {
				try {
					int QKfz = Integer.parseInt(zeile[84]);
					String QKfzStatus = zeile[85];
					int QLkw = Integer.parseInt(zeile[86]);
					String QLkwStatus = zeile[87];
					int QPkw = Integer.parseInt(zeile[88]);
					String QPkwStatus = zeile[89];
					int VKfz = Integer.parseInt(zeile[90]);
					String VKfzStatus = zeile[91];
					int VLkw = Integer.parseInt(zeile[92]);
					String VLkwStatus = zeile[93];
					int VPkw = Integer.parseInt(zeile[94]);
					String VPkwStatus = zeile[95];
					int VgKfz = Integer.parseInt(zeile[96]);
					String VgKfzStatus = zeile[97];
					int B = Integer.parseInt(zeile[98]);
					String BStatus = zeile[99];
					int BMax = Integer.parseInt(zeile[100]);
					String BMaxStatus = zeile[101];
					int SKfz = Integer.parseInt(zeile[100]);
					String SKfzStatus = zeile[101];
					int ALkw = Integer.parseInt(zeile[102]);
					String ALkwStatus = zeile[103];
					int KKfz = Integer.parseInt(zeile[104]);
					String KKfzStatus = zeile[105];
					int KLkw = Integer.parseInt(zeile[106]);
					String KLkwStatus = zeile[107];
					int KPkw = Integer.parseInt(zeile[108]);
					String KPkwStatus = zeile[109];
					int QB = Integer.parseInt(zeile[110]);
					String QBStatus = zeile[111];
					int KB = Integer.parseInt(zeile[112]);
					String KBStatus = zeile[113];
					int VDelta = Integer.parseInt(zeile[114]);
					String VDeltaStatus = zeile[115];

					datensatz = setAttribut("QKfz", QKfz, QKfzStatus, datensatz); //$NON-NLS-1$
					datensatz = setAttribut("QLkw", QLkw, QLkwStatus, datensatz); //$NON-NLS-1$
					datensatz = setAttribut("QPkw", QPkw, QPkwStatus, datensatz); //$NON-NLS-1$
					datensatz = setAttribut("VKfz", VKfz, VKfzStatus, datensatz); //$NON-NLS-1$
					datensatz = setAttribut("VLkw", VLkw, VLkwStatus, datensatz); //$NON-NLS-1$
					datensatz = setAttribut("VPkw", VPkw, VPkwStatus, datensatz); //$NON-NLS-1$
					datensatz = setAttribut(
							"VgKfz", VgKfz, VgKfzStatus, datensatz); //$NON-NLS-1$
					datensatz = setAttribut("B", B, BStatus, datensatz); //$NON-NLS-1$
					datensatz = setAttribut("BMax", BMax, BMaxStatus, datensatz); //$NON-NLS-1$
					datensatz = setAttribut("SKfz", SKfz, SKfzStatus, datensatz); //$NON-NLS-1$
					datensatz = setAttribut("ALkw", ALkw, ALkwStatus, datensatz); //$NON-NLS-1$
					datensatz = setAttribut("KKfz", KKfz, KKfzStatus, datensatz); //$NON-NLS-1$
					datensatz = setAttribut("KLkw", KLkw, KLkwStatus, datensatz); //$NON-NLS-1$
					datensatz = setAttribut("KPkw", KPkw, KPkwStatus, datensatz); //$NON-NLS-1$
					datensatz = setAttribut("QB", QB, QBStatus, datensatz); //$NON-NLS-1$
					datensatz = setAttribut("KB", KB, KBStatus, datensatz); //$NON-NLS-1$
					datensatz = setAttribut(
							"VDelta", VDelta, VDeltaStatus, datensatz); //$NON-NLS-1$

				} catch (ArrayIndexOutOfBoundsException ex) {
					datensatz = null;
				}
			} else {
				datensatz = null;
			}
		}

		return datensatz;
	}

	/**
	 * Bildet einen Ausgabe-Datensatz der FS-Analysewerte aus den Daten der
	 * aktuellen CSV-Zeile.
	 * 
	 * @param fs
	 *            Fahrstreifen (1-3)
	 * @return ein Datensatz der übergebenen Attributgruppe mit den Daten der
	 *         nächsten Zeile oder <code>null</code>, wenn der Dateizeiger am
	 *         Ende ist
	 */
	public Data getFSAnalyseDatensatz(final int fs) {
		Data datensatz = sDav.createData(sDav.getDataModel().getAttributeGroup(
				"atg.verkehrsDatenKurzZeitFs")); //$NON-NLS-1$

		int fsMulti = fs - 1;

		if (datensatz != null) {

			if (zeile != null) {
				try {
					int qKfz = Integer.parseInt(zeile[0 + (fsMulti * 2)]);
					String qKfzStatus = zeile[1 + (fsMulti * 2)];
					int qPkw = Integer.parseInt(zeile[6 + (fsMulti * 2)]);
					String qPkwStatus = zeile[7 + (fsMulti * 2)];
					int qLkw = Integer.parseInt(zeile[12 + (fsMulti * 2)]);
					String qLkwStatus = zeile[13 + (fsMulti * 2)];
					int vKfz = Integer.parseInt(zeile[18 + (fsMulti * 2)]);
					String vKfzStatus = zeile[19 + (fsMulti * 2)];
					int vPkw = Integer.parseInt(zeile[24 + (fsMulti * 2)]);
					String vPkwStatus = zeile[25 + (fsMulti * 2)];
					int vLkw = Integer.parseInt(zeile[30 + (fsMulti * 2)]);
					String vLkwStatus = zeile[31 + (fsMulti * 2)];
					int vgKfz = Integer.parseInt(zeile[36 + (fsMulti * 2)]);
					String vgKfzStatus = zeile[37 + (fsMulti * 2)];
					int b = Integer.parseInt(zeile[42 + (fsMulti * 2)]);
					String bStatus = zeile[43 + (fsMulti * 2)];
					int aLkw = Integer.parseInt(zeile[48 + (fsMulti * 2)]);
					String aLkwStatus = zeile[49 + (fsMulti * 2)];
					int kKfz = Integer.parseInt(zeile[54 + (fsMulti * 2)]);
					String kKfzStatus = zeile[55 + (fsMulti * 2)];
					int kLkw = Integer.parseInt(zeile[60 + (fsMulti * 2)]);
					String kLkwStatus = zeile[61 + (fsMulti * 2)];
					int kPkw = Integer.parseInt(zeile[66 + (fsMulti * 2)]);
					String kPkwStatus = zeile[67 + (fsMulti * 2)];
					int qB = Integer.parseInt(zeile[72 + (fsMulti * 2)]);
					String qBStatus = zeile[73 + (fsMulti * 2)];
					int kB = Integer.parseInt(zeile[78 + (fsMulti * 2)]);
					String kBStatus = zeile[79 + (fsMulti * 2)];
					int sKfz = 1;

					datensatz.getTimeValue("T").setMillis(iNTERVALL); //$NON-NLS-1$
					datensatz = setAttribut("qKfz", qKfz, qKfzStatus, datensatz); //$NON-NLS-1$
					datensatz = setAttribut("qPkw", qPkw, qPkwStatus, datensatz); //$NON-NLS-1$
					datensatz = setAttribut("qLkw", qLkw, qLkwStatus, datensatz); //$NON-NLS-1$
					datensatz = setAttribut("vKfz", vKfz, vKfzStatus, datensatz); //$NON-NLS-1$
					datensatz = setAttribut("vPkw", vPkw, vPkwStatus, datensatz); //$NON-NLS-1$
					datensatz = setAttribut("vLkw", vLkw, vLkwStatus, datensatz); //$NON-NLS-1$
					datensatz = setAttribut(
							"vgKfz", vgKfz, vgKfzStatus, datensatz); //$NON-NLS-1$
					datensatz = setAttribut("b", b, bStatus, datensatz); //$NON-NLS-1$
					datensatz = setAttribut("aLkw", aLkw, aLkwStatus, datensatz); //$NON-NLS-1$
					datensatz = setAttribut("kKfz", kKfz, kKfzStatus, datensatz); //$NON-NLS-1$
					datensatz = setAttribut("kLkw", kLkw, kLkwStatus, datensatz); //$NON-NLS-1$
					datensatz = setAttribut("kPkw", kPkw, kPkwStatus, datensatz); //$NON-NLS-1$
					datensatz = setAttribut("qB", qB, qBStatus, datensatz); //$NON-NLS-1$
					datensatz = setAttribut("kB", kB, kBStatus, datensatz); //$NON-NLS-1$
					datensatz = setAttribut("sKfz", sKfz, null, datensatz); //$NON-NLS-1$

				} catch (ArrayIndexOutOfBoundsException ex) {
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
	 * @return der veränderte Datensatz
	 */
	private Data setAttribut(final String attributName, long wert,
			String status, Data datensatz) {
		Data data = datensatz;

		if ((attributName.startsWith("v") || attributName.startsWith("V")) //$NON-NLS-1$ //$NON-NLS-2$
				&& wert >= 255) {
			wert = -1;
		}

		if ((attributName.startsWith("k") || attributName.startsWith("K")) //$NON-NLS-1$ //$NON-NLS-2$
				&& wert > 10000) {
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
			String[] splitStatus = status.trim().split(" "); //$NON-NLS-1$

			for (int i = 0; i < splitStatus.length; i++) {
				if (splitStatus[i].equalsIgnoreCase("Fehl")) {
					errCode = errCode - 2;
				}

				if (splitStatus[i].equalsIgnoreCase("nErm")) {
					errCode = errCode - 1;
				}

				if (splitStatus[i].equalsIgnoreCase("Impl")) {
					impl = DUAKonstanten.JA;
				}

				if (splitStatus[i].equalsIgnoreCase("Intp")) {
					intp = DUAKonstanten.JA;
				}

				if (splitStatus[i].equalsIgnoreCase("nErf")) {
					nErf = DUAKonstanten.JA;
				}

				if (splitStatus[i].equalsIgnoreCase("wMaL")) {
					wMaL = DUAKonstanten.JA;
				}

				if (splitStatus[i].equalsIgnoreCase("wMax")) {
					wMax = DUAKonstanten.JA;
				}
					
				if (splitStatus[i].equalsIgnoreCase("wMiL")) {
					wMiL = DUAKonstanten.JA;
				}
					
				if (splitStatus[i].equalsIgnoreCase("wMin")) {
					wMin = DUAKonstanten.JA;
				}

				try {
					guete = Float.parseFloat(splitStatus[i].replace(",", ".")); //$NON-NLS-1$ //$NON-NLS-2$
				} catch (Exception e) {
					// kein float Wert
				}
			}
		}

		if (errCode < 0) {
			wert = errCode;
		}

		DUAUtensilien
				.getAttributDatum(attributName + ".Wert", data).asUnscaledValue().set(wert); //$NON-NLS-1$
		DUAUtensilien
				.getAttributDatum(
						attributName + ".Status.Erfassung.NichtErfasst", data).asUnscaledValue().set(nErf); //$NON-NLS-1$
		DUAUtensilien
				.getAttributDatum(
						attributName + ".Status.PlFormal.WertMax", data).asUnscaledValue().set(wMax); //$NON-NLS-1$
		DUAUtensilien
				.getAttributDatum(
						attributName + ".Status.PlFormal.WertMin", data).asUnscaledValue().set(wMin); //$NON-NLS-1$
		DUAUtensilien
				.getAttributDatum(
						attributName + ".Status.PlLogisch.WertMaxLogisch", data).asUnscaledValue().set(wMaL); //$NON-NLS-1$
		DUAUtensilien
				.getAttributDatum(
						attributName + ".Status.PlLogisch.WertMinLogisch", data).asUnscaledValue().set(wMiL); //$NON-NLS-1$
		DUAUtensilien
				.getAttributDatum(
						attributName + ".Status.MessWertErsetzung.Implausibel", data).asUnscaledValue().set(impl); //$NON-NLS-1$
		DUAUtensilien
				.getAttributDatum(
						attributName + ".Status.MessWertErsetzung.Interpoliert", data).asUnscaledValue().set(intp); //$NON-NLS-1$
		DUAUtensilien
				.getAttributDatum(attributName + ".Güte.Index", data).asScaledValue().set(guete); //$NON-NLS-1$
		DUAUtensilien
				.getAttributDatum(attributName + ".Güte.Verfahren", data).asUnscaledValue().set(0); //$NON-NLS-1$

		return datensatz;
	}
}
