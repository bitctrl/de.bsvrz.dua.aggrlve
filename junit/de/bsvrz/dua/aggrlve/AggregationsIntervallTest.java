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

import java.sql.Date;
import java.util.Calendar;
import java.util.GregorianCalendar;

import junit.framework.Assert;

import org.junit.Test;

import com.bitctrl.Constants;

import de.bsvrz.dav.daf.main.ClientDavInterface;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;
import de.bsvrz.sys.funclib.bitctrl.dua.test.DAVTest;

/**
 * Allgemeine Tests<br>
 * Testet insbesondere, ob die Berechnungszeitpunkte fuer die unterschiedlichen
 * Aggregationsintervalle richtig bestimmt werden (dies muss stichprobenartig
 * anhand der Konsolenausgabe nachgeprueft werden).
 * 
 * @author BitCtrl Systems GmbH, Thierfelder
 * 
 * @version $Id$
 */
public class AggregationsIntervallTest {

	/**
	 * Testet:<br> - ob die Berechnungsintervalle in der richtigen Reihenfolge
	 * angelegt werden<br> - ob die Berechnungszeitpunkte fuer die
	 * unterschiedlichen Aggregationsintervalle richtig bestimmt werden (dies
	 * muss stichprobenartig anhand der Konsolenausgabe nachgeprueft werden).
	 * 
	 * @throws Exception wird weitergereicht
	 */
	@Test
	public void testGetInstanzen() throws Exception {
		ClientDavInterface dav = DAVTest.getDav(Verbindung.getConData());
		AggregationsIntervall.initialisiere(dav);
		AggregationsIntervall[] instanzen = new AggregationsIntervall[AggregationsIntervall
				.getInstanzen().size()];
		instanzen[0] = AggregationsIntervall.aGG1MINUTE;
		instanzen[1] = AggregationsIntervall.aGG5MINUTE;
		instanzen[2] = AggregationsIntervall.aGG15MINUTE;
		instanzen[3] = AggregationsIntervall.aGG30MINUTE;
		instanzen[4] = AggregationsIntervall.aGG60MINUTE;
		instanzen[5] = AggregationsIntervall.aGGDTVTAG;
		instanzen[6] = AggregationsIntervall.aGGDTVMONAT;
		instanzen[7] = AggregationsIntervall.aGGDTVJAHR;

		int i = 0;
		for (AggregationsIntervall intervall : AggregationsIntervall
				.getInstanzen()) {
			Assert.assertEquals(instanzen[i++], intervall);
		}

		GregorianCalendar cal = new GregorianCalendar();
		long jetzt = System.currentTimeMillis();
		for (AggregationsIntervall intervall : AggregationsIntervall
				.getInstanzen()) {
			if (intervall.equals(AggregationsIntervall.aGGDTVTAG)) {
				break;
			}
			cal.setTimeInMillis(jetzt);
			// cal.set(Calendar.MONTH, -1);
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);

			for (long zeitpunkt = cal.getTimeInMillis() + 30
					* Constants.MILLIS_PER_SECOND; zeitpunkt < cal
					.getTimeInMillis()
					+ Constants.MILLIS_PER_HOUR * 2; zeitpunkt += Constants.MILLIS_PER_MINUTE) {
				if (intervall.isAggregationErforderlich(zeitpunkt)) {
//					System.out
//							.println(DUAKonstanten.ZEIT_FORMAT_GENAU
//									.format(new Date(zeitpunkt))
//									+ ", " + intervall + ": " + //$NON-NLS-1$ //$NON-NLS-2$
//									DUAKonstanten.ZEIT_FORMAT_GENAU
//											.format(new Date(
//													intervall
//															.getAggregationZeitStempel(zeitpunkt))));
				} else {
//					System.out.println(DUAKonstanten.ZEIT_FORMAT_GENAU
//							.format(new Date(zeitpunkt))
//							+ ", " + intervall + ": nicht erforderlich"); //$NON-NLS-1$ //$NON-NLS-2$
				}
			}
		}

		cal.setTimeInMillis(jetzt);
		cal.set(Calendar.MONTH, -1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);

		AggregationsIntervall intervall = AggregationsIntervall.aGGDTVTAG;
		for (long zeitpunkt = cal.getTimeInMillis() + 30
				* Constants.MILLIS_PER_SECOND; zeitpunkt < cal
				.getTimeInMillis()
				+ Constants.MILLIS_PER_HOUR * 60; zeitpunkt += Constants.MILLIS_PER_MINUTE) {
			if (intervall.isAggregationErforderlich(zeitpunkt)) {
//				System.out
//						.println(DUAKonstanten.ZEIT_FORMAT_GENAU
//								.format(new Date(zeitpunkt))
//								+ ", " + intervall + ": " + //$NON-NLS-1$ //$NON-NLS-2$
//								DUAKonstanten.ZEIT_FORMAT_GENAU
//										.format(new Date(
//												intervall
//														.getAggregationZeitStempel(zeitpunkt))));
			} else {
//				System.out.println(DUAKonstanten.ZEIT_FORMAT_GENAU
//						.format(new Date(zeitpunkt))
//						+ ", " + intervall + ": nicht erforderlich"); //$NON-NLS-1$ //$NON-NLS-2$
			}
		}

		dav.disconnect(false, Constants.EMPTY_STRING);
	}

}
