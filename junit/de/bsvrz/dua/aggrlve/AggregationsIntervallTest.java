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

import de.bsvrz.dav.daf.main.ClientDavInterface;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;
import de.bsvrz.sys.funclib.bitctrl.dua.test.DAVTest;
import de.bsvrz.sys.funclib.bitctrl.konstante.Konstante;

/**
 * <b>Vorlaeufige Version, bis zur Klaerung der Probleme mit den Testdaten)</b>
 * 
 * Allgemeine Tests
 * 
 * @author BitCtrl Systems GmbH, Thierfelder
 *
 */
public class AggregationsIntervallTest {
		
	/**
	 * Datenverteilerverbindung herstellen
	 */
	@Test
	public void testGetInstanzen()
	throws Exception{
		ClientDavInterface dav = DAVTest.getDav(AggregationLVETest.CON_DATA);
		AggregationsIntervall.initialisiere(dav);
		AggregationsIntervall instanzen[] = new AggregationsIntervall[AggregationsIntervall.getInstanzen().size()];
		instanzen[0] = AggregationsIntervall.AGG_1MINUTE;
		instanzen[1] = AggregationsIntervall.AGG_5MINUTE;
		instanzen[2] = AggregationsIntervall.AGG_15MINUTE;
		instanzen[3] = AggregationsIntervall.AGG_30MINUTE;
		instanzen[4] = AggregationsIntervall.AGG_60MINUTE;
		instanzen[5] = AggregationsIntervall.AGG_DTV_TAG;
		instanzen[6] = AggregationsIntervall.AGG_DTV_MONAT;
		instanzen[7] = AggregationsIntervall.AGG_DTV_JAHR;

		int i = 0;
		for(AggregationsIntervall intervall:AggregationsIntervall.getInstanzen()){
			Assert.assertEquals(instanzen[i++], intervall);
		}
		
		
		GregorianCalendar cal = new GregorianCalendar();
		long jetzt = System.currentTimeMillis();
		for(AggregationsIntervall intervall:AggregationsIntervall.getInstanzen()){
			if(intervall.equals(AggregationsIntervall.AGG_DTV_TAG))break;
			cal.setTimeInMillis(jetzt);
			//cal.set(Calendar.MONTH, -1);
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);

			for(long zeitpunkt = cal.getTimeInMillis() + 30 * Konstante.SEKUNDE_IN_MS; 
			zeitpunkt < cal.getTimeInMillis() + Konstante.STUNDE_IN_MS * 2; zeitpunkt += Konstante.MINUTE_IN_MS){
				if(intervall.isAggregationErforderlich(zeitpunkt)){
					System.out.println(DUAKonstanten.ZEIT_FORMAT_GENAU.format(new Date(zeitpunkt)) + ", " + intervall + ": " + //$NON-NLS-1$ //$NON-NLS-2$
						DUAKonstanten.ZEIT_FORMAT_GENAU.format(new Date(intervall.getAggregationZeitStempel(zeitpunkt))));
				}else{
					System.out.println(DUAKonstanten.ZEIT_FORMAT_GENAU.format(new Date(zeitpunkt)) + ", " + intervall + ": nicht erforderlich"); //$NON-NLS-1$ //$NON-NLS-2$
				}
			}		
		}

		cal.setTimeInMillis(jetzt);
		cal.set(Calendar.MONTH, -1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);

		AggregationsIntervall intervall = AggregationsIntervall.AGG_DTV_TAG;
		for(long zeitpunkt = cal.getTimeInMillis() + 30 * Konstante.SEKUNDE_IN_MS; 
		zeitpunkt < cal.getTimeInMillis() + Konstante.STUNDE_IN_MS * 60; zeitpunkt += Konstante.MINUTE_IN_MS){
			if(intervall.isAggregationErforderlich(zeitpunkt)){
				System.out.println(DUAKonstanten.ZEIT_FORMAT_GENAU.format(new Date(zeitpunkt)) + ", " + intervall + ": " + //$NON-NLS-1$ //$NON-NLS-2$
					DUAKonstanten.ZEIT_FORMAT_GENAU.format(new Date(intervall.getAggregationZeitStempel(zeitpunkt))));
			}else{
				System.out.println(DUAKonstanten.ZEIT_FORMAT_GENAU.format(new Date(zeitpunkt)) + ", " + intervall + ": nicht erforderlich"); //$NON-NLS-1$ //$NON-NLS-2$
			}
		}		

		
		dav.disconnect(false, Konstante.LEERSTRING);
	}
	
}
