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

import de.bsvrz.dav.daf.main.ClientDavInterface;
import de.bsvrz.dav.daf.main.Data;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAUtensilien;
import de.bsvrz.sys.funclib.bitctrl.dua.test.CSVImporter;
import de.bsvrz.sys.funclib.bitctrl.konstante.Konstante;

/**
 * Liest die Ausgangsdaten für die Prüfung der Aggregation LVE (TV und DTV) ein
 * 
 * @author BitCtrl Systems GmbH, Thierfelder
 *
 */
public class AggregationUnvImporter
extends CSVImporter{
	
	/**
	 * Verbindung zum Datenverteiler
	 */
	protected static ClientDavInterface DAV = null;
	
	/**
	 * Hält aktuelle Daten des FS 1-3
	 */
	protected String ZEILE[];
	
	/**
	 * T
	 */
	protected static long INTERVALL = Konstante.MINUTE_IN_MS;
	

	/**
	 * Standardkonstruktor
	 * 
	 * @param dav Datenverteier-Verbindung
	 * @param csvQuelle Quelle der Daten (CSV-Datei)
	 * @throws Exception falls dieses Objekt nicht vollständig initialisiert werden konnte
	 */
	public AggregationUnvImporter(final ClientDavInterface dav, 
								    final String csvQuelle)
	throws Exception{
		super(csvQuelle);
		if(DAV == null){
			DAV = dav;
		}
		
		/**
		 * Tabellenkopf überspringen
		 */
		this.getNaechsteZeile();
	}
	
	
	/**
	 * Setzt Datenintervall
	 * 
	 * @param t Datenintervall
	 */
	public static final void setT(final long t){
		INTERVALL = t;
	}
	
	/**
	 * Importiert die nächste Zeile aus der CSV-Datei
	 *
	 */
	public final void importNaechsteZeile() {
		ZEILE = this.getNaechsteZeile();
	}
	
	/**
	 * Bildet einen Ausgabe-Datensatz der MQ-Analysewerte aus den Daten der aktuellen CSV-Zeile
	 * @return ein Datensatz der übergebenen Attributgruppe mit den Daten der nächsten Zeile
	 * oder <code>null</code>, wenn der Dateizeiger am Ende ist
	 */
	public final Data getMQAnalyseDatensatz() {
		Data datensatz = DAV.createData(DAV.getDataModel().getAttributeGroup("atg.verkehrsDatenKurzZeitMq")); //$NON-NLS-1$
		
		if(datensatz != null){
			if(ZEILE != null){
				try {
					int c = 0;
					int QKfz = this.parseAlsPositiveZahl(ZEILE[c + 0]);
					String QKfzStatus = ZEILE[c + 1];
					c+=2;
					int QPkw = this.parseAlsPositiveZahl(ZEILE[c + 2]);
					String QPkwStatus = ZEILE[c + 3];
					c+=2;
					int QLkw = this.parseAlsPositiveZahl(ZEILE[c + 4]);
					String QLkwStatus = ZEILE[c + 5];
					c+=2;
					int VKfz = this.parseAlsPositiveZahl(ZEILE[c + 6]);
					String VKfzStatus = ZEILE[c + 7];
					c+=2;
					int VLkw = this.parseAlsPositiveZahl(ZEILE[c + 8]);
					String VLkwStatus = ZEILE[c + 9];
					c+=2;
					int VPkw = this.parseAlsPositiveZahl(ZEILE[c + 10]);
					String VPkwStatus = ZEILE[c + 11];
									
					datensatz = setAttribut("QKfz", QKfz, QKfzStatus, datensatz); //$NON-NLS-1$
					datensatz = setAttribut("QLkw", QLkw, QLkwStatus, datensatz); //$NON-NLS-1$
					datensatz = setAttribut("QPkw", QPkw, QPkwStatus, datensatz); //$NON-NLS-1$
					datensatz = setAttribut("VKfz", VKfz, VKfzStatus, datensatz); //$NON-NLS-1$
					datensatz = setAttribut("VLkw", VLkw, VLkwStatus, datensatz); //$NON-NLS-1$
					datensatz = setAttribut("VPkw", VPkw, VPkwStatus, datensatz); //$NON-NLS-1$
					datensatz = setAttribut("VgKfz", -1, "0", datensatz); //$NON-NLS-1$ //$NON-NLS-2$
					datensatz = setAttribut("B", -1,  "0", datensatz); //$NON-NLS-1$ //$NON-NLS-2$
					datensatz = setAttribut("BMax",  -1,  "0", datensatz); //$NON-NLS-1$ //$NON-NLS-2$
					datensatz = setAttribut("SKfz",  -1,  "0", datensatz); //$NON-NLS-1$ //$NON-NLS-2$
					datensatz = setAttribut("ALkw",  -1,  "0", datensatz); //$NON-NLS-1$ //$NON-NLS-2$
					datensatz = setAttribut("KKfz",  -1,  "0", datensatz); //$NON-NLS-1$ //$NON-NLS-2$
					datensatz = setAttribut("KLkw",  -1,  "0", datensatz); //$NON-NLS-1$ //$NON-NLS-2$
					datensatz = setAttribut("KPkw",  -1,  "0", datensatz); //$NON-NLS-1$ //$NON-NLS-2$
					datensatz = setAttribut("QB",  -1,  "0", datensatz); //$NON-NLS-1$ //$NON-NLS-2$
					datensatz = setAttribut("KB",  -1,  "0", datensatz); //$NON-NLS-1$ //$NON-NLS-2$
					datensatz = setAttribut("VDelta",  -1,  "0", datensatz); //$NON-NLS-1$ //$NON-NLS-2$
				}catch(ArrayIndexOutOfBoundsException ex){
					datensatz = null;
				}
			}else{
				datensatz = null;
			}
		}
	
		return datensatz;
	}
	
	
	/**
	 * Erfragt den Zahlenwert der geparsten Zeichenkette
	 * 
	 * @param zahl eine Zahl als Zeichenkette
	 * @return den Zahlenwert der geparsten Zeichenkette
	 */
	private final int parseAlsPositiveZahl(String zahl){
		int a = Integer.parseInt(zahl);
		if(a < 0){
			a = 0;
		}
		return a;
	}
	
	
	/**
	 * Setzt Attribut in Datensatz
	 * 
	 * @param attributName Name des Attributs
	 * @param wert Wert des Attributs
	 * @param datensatz der Datensatz
	 * @return der veränderte Datensatz
	 */
	private final Data setAttribut(final String attributName, long wert, String status, Data datensatz){
		Data data = datensatz;
	
		if((attributName.startsWith("v") || attributName.startsWith("V")) //$NON-NLS-1$ //$NON-NLS-2$
				&& wert >= 255) {
			wert = -1;
		}
		
		if((attributName.startsWith("k") || attributName.startsWith("K")) //$NON-NLS-1$ //$NON-NLS-2$
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
		
		if(status != null) {
			String[] splitStatus = status.trim().split(" "); //$NON-NLS-1$
			
			for(int i = 0; i<splitStatus.length;i++) {
				if(splitStatus[i].equalsIgnoreCase("Fehl")) //$NON-NLS-1$
					errCode = errCode-2;
				
				if(splitStatus[i].equalsIgnoreCase("nErm")) //$NON-NLS-1$
					errCode = errCode-1;
				
				if(splitStatus[i].equalsIgnoreCase("Impl")) //$NON-NLS-1$
					 impl = DUAKonstanten.JA;
				
				if(splitStatus[i].equalsIgnoreCase("Intp")) //$NON-NLS-1$
					intp = DUAKonstanten.JA;				

				if(splitStatus[i].equalsIgnoreCase("nErf")) //$NON-NLS-1$
					nErf = DUAKonstanten.JA;

				if(splitStatus[i].equalsIgnoreCase("wMaL")) //$NON-NLS-1$
					wMaL = DUAKonstanten.JA;
				
				if(splitStatus[i].equalsIgnoreCase("wMax")) //$NON-NLS-1$
					wMax = DUAKonstanten.JA;

				if(splitStatus[i].equalsIgnoreCase("wMiL")) //$NON-NLS-1$
					wMiL = DUAKonstanten.JA;

				if(splitStatus[i].equalsIgnoreCase("wMin")) //$NON-NLS-1$
					wMin = DUAKonstanten.JA;
				
				try {
					guete = Float.parseFloat(splitStatus[i].replace(",", ".")); //$NON-NLS-1$ //$NON-NLS-2$
				} catch (Exception e) {
					//kein float Wert
				}
			}
		}
			
		if(errCode < 0)
			wert = errCode;

		DUAUtensilien.getAttributDatum(attributName + ".Wert", data).asUnscaledValue().set(wert); //$NON-NLS-1$
		DUAUtensilien.getAttributDatum(attributName + ".Status.Erfassung.NichtErfasst", data).asUnscaledValue().set(nErf); //$NON-NLS-1$
		DUAUtensilien.getAttributDatum(attributName + ".Status.PlFormal.WertMax", data).asUnscaledValue().set(wMax); //$NON-NLS-1$
		DUAUtensilien.getAttributDatum(attributName + ".Status.PlFormal.WertMin", data).asUnscaledValue().set(wMin); //$NON-NLS-1$
		DUAUtensilien.getAttributDatum(attributName + ".Status.PlLogisch.WertMaxLogisch", data).asUnscaledValue().set(wMaL); //$NON-NLS-1$
		DUAUtensilien.getAttributDatum(attributName + ".Status.PlLogisch.WertMinLogisch", data).asUnscaledValue().set(wMiL); //$NON-NLS-1$
		DUAUtensilien.getAttributDatum(attributName + ".Status.MessWertErsetzung.Implausibel", data).asUnscaledValue().set(impl); //$NON-NLS-1$
		DUAUtensilien.getAttributDatum(attributName + ".Status.MessWertErsetzung.Interpoliert", data).asUnscaledValue().set(intp); //$NON-NLS-1$
		DUAUtensilien.getAttributDatum(attributName + ".Güte.Index", data).asScaledValue().set(guete); //$NON-NLS-1$
		DUAUtensilien.getAttributDatum(attributName + ".Güte.Verfahren", data).asUnscaledValue().set(0); //$NON-NLS-1$
				
		return datensatz;
	}
}
