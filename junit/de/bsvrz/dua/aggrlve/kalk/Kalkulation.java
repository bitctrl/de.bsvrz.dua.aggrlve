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

import de.bsvrz.dua.aggrlve.Verbindung;
import de.bsvrz.sys.funclib.bitctrl.dua.test.CSVImporter;

/**
 * Dieses Programm produziert die Tabellen <code>5-6</code> und 
 * <code>5-7</code> aus der Prüfspezifikation V 2.0 auf Basis der
 * Tabelle <code>Messwert_Aggregation_unv</code>
 * 
 * @author BitCtrl Systems GmbH, Thierfelder
 *
 */
public class Kalkulation {

	/**
	 * Alle Attribute, die geschrieben werden sollen
	 */
	private static final String[] ATTRIBUTE = new String[]{
		"qKfz(t)(1)", //$NON-NLS-1$
		"qKfz(t)(2)", //$NON-NLS-1$
		"QKfz(Mq)", //$NON-NLS-1$
		"qPkw(t)(1)", //$NON-NLS-1$
		"qPkw(t)(2)", //$NON-NLS-1$
		"QPkw(Mq)", //$NON-NLS-1$
		"qLkw(t)(1)", //$NON-NLS-1$
		"qLkw(t)(2)", //$NON-NLS-1$
		"QLkw(Mq)", //$NON-NLS-1$
		"vKfz(t)(1)", //$NON-NLS-1$
		"vKfz(t)(2)", //$NON-NLS-1$
		"VKfz(Mq)",  //$NON-NLS-1$
		"vPkw(t)(1)", //$NON-NLS-1$
		"vPkw(t)(2)", //$NON-NLS-1$
		"VPkw(Mq)", //$NON-NLS-1$
		"vLkw(t)(1)", //$NON-NLS-1$
		"vLkw(t)(2)", //$NON-NLS-1$
		"VLkw(Mq)"}; //$NON-NLS-1$
	
	/**
	 * Importiert die Tabelle <code>Messwert_Aggregation_unv</code>
	 */
	CSVImporter importer = null;

	
	
	/**
	 * Standardkonstruktor
	 * 
	 * @param messwertAggregationUnvDatei Datei der Tabelle
	 * <code>Messwert_Aggregation_unv</code>
	 * @throws Exception wenn die Datei nicht geoeffnet werden kann
	 */
	public Kalkulation(final File messwertAggregationUnvDatei)
	throws Exception{
		this.importer = new CSVImporter(messwertAggregationUnvDatei);
		this.importer.getNaechsteZeile();
	}
	

	/**
	 * Erzeugt aus der Eingangstabelle das Analogon von Tabelle 
	 * <code>5-6</code> aus der Prüfspezifikation V 2.0 
	 * 
	 * @param dateiName der Name der Ausgabedatei (CSV)
	 * @throws Exception wenn es Probleme beim Lesen oder Schreiben gab
	 */
	public final void ergeugeTab56(final String dateiName)
	throws Exception{
		this.erzeugeTab(3, dateiName);
	}

	
	/**
	 * Erzeugt aus der Eingangstabelle das Analogon von Tabelle 
	 * <code>5-7</code> aus der Prüfspezifikation V 2.0 
	 * 
	 * @param dateiName der Name der Ausgabedatei (CSV)
	 * @throws Exception wenn es Probleme beim Lesen oder Schreiben gab
	 */
	public final void ergeugeTab57(final String dateiName)
	throws Exception{
		this.erzeugeTab(2, dateiName);
	}

	
	/**
	 * Berechnet aus der Eingabedatei
	 * 
	 * @param anzahlDatenSaetze Anzahl der Datensaetze, aus denen die Aggregations
	 * daten zusammengerechnet werden sollen
	 * @param dateiName der Name der Ausgabedatei (CSV)
	 * @throws Exception wenn es Probleme beim Lesen oder Schreiben gab
	 */
	private final void erzeugeTab(final int anzahlDatenSaetze, final String dateiName)
	throws Exception{
		File file = new File(Verbindung.WURZEL + dateiName);
		if(file.createNewFile()){
			for(int i = 0; i<3; i++){
				for(int j = 0; j < anzahlDatenSaetze; j++){
					
				}
				
			}
		}else{
			throw new Exception("Konnte Datei " +  //$NON-NLS-1$
					(Verbindung.WURZEL + dateiName) + " nicht anlegen"); //$NON-NLS-1$
		}		
	}
}
