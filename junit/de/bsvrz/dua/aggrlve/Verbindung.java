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

import java.io.File;

/**
 * Speichert die Verbindungsdaten.
 * 
 * @author BitCtrl Systems GmbH, Thierfelder
 * 
 * @version $Id$
 */
public final class Verbindung {

	/**
	 * Standardkonstruktor.
	 */
	private Verbindung() {
		//
	}
	
	/**
	 * Verbindungsdaten.
	 */
	private static final String[] CON_DATA = new String[] {
			"-datenverteiler=localhost:8083", //$NON-NLS-1$
			"-benutzer=Tester", //$NON-NLS-1$
			"-authentifizierung=passwd", //$NON-NLS-1$
			"-debugLevelStdErrText=WARNING", //$NON-NLS-1$
			"-debugLevelFileText=WARNING" }; //$NON-NLS-1$

	// /**
	// * Verbindungsdaten fuer Eclipse
	// */
	// private static final String[] CON_DATA = new String[] {
	// "-datenverteiler=localhost:8083", //$NON-NLS-1$
	// "-benutzer=Tester", //$NON-NLS-1$
	// "-authentifizierung=c:\\passwd", //$NON-NLS-1$
	// "-debugLevelStdErrText=WARNING", //$NON-NLS-1$
	// "-debugLevelFileText=WARNING"}; //$NON-NLS-1$

	/**
	 * Wurzelverzeichnis der Testdaten.
	 */
	// public static final String WURZEL = "extra\\testDaten\\V_2.7.9(05.04.08)"
	// + File.separator; //$NON-NLS-1$
	public static final String WURZEL = ".." + File.separator + "testDaten" + File.separator + "V_2.7.9(05.04.08)" + File.separator; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	// public static final String WURZEL = ".." + File.separator + "testDaten" +
	// File.separator + "V_2.7.6(01.04.08)" + File.separator; //$NON-NLS-1$
	// //$NON-NLS-2$ //$NON-NLS-3$
	// public static final String WURZEL = ".." + File.separator + "testDaten" +
	// File.separator + "V_2.7.3(20.03.08)" + File.separator; //$NON-NLS-1$
	// //$NON-NLS-2$ //$NON-NLS-3$
	// public static final String WURZEL = ".." + File.separator + "testDaten" +
	// File.separator + "V_2.7.2(12.03.08)" + File.separator; //$NON-NLS-1$
	// //$NON-NLS-2$ //$NON-NLS-3$
	// public static final String WURZEL = ".." + File.separator + "testDaten" +
	// File.separator + "V_2.7.1(05.03.08)" + File.separator; //$NON-NLS-1$
	// //$NON-NLS-2$ //$NON-NLS-3$

	/**
	 * Erfragt eine Kopie der Verbindungsdaten.
	 * 
	 * @return eine Kopie der Verbindungsdaten
	 */
	public static String[] getConData() {
		String[] conDataKopie = new String[CON_DATA.length];

		for (int i = 0; i < conDataKopie.length; i++) {
			conDataKopie[i] = CON_DATA[i];
		}

		return conDataKopie;
	}

}
