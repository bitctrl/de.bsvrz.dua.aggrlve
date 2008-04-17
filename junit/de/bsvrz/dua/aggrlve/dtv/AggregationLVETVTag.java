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
package de.bsvrz.dua.aggrlve.dtv;

import java.util.Calendar;
import java.util.Collection;
import java.util.GregorianCalendar;

import junit.framework.Assert;

import org.junit.Test;

import com.bitctrl.Constants;

import de.bsvrz.dav.daf.main.ClientDavInterface;
import de.bsvrz.dav.daf.main.DataDescription;
import de.bsvrz.dav.daf.main.ResultData;
import de.bsvrz.dav.daf.main.config.SystemObject;
import de.bsvrz.dua.aggrlve.AggregationsAttribut;
import de.bsvrz.dua.aggrlve.AggregationsAttributWert;
import de.bsvrz.dua.aggrlve.AggregationsDatum;
import de.bsvrz.dua.aggrlve.AggregationsIntervall;
import de.bsvrz.dua.aggrlve.AggregationsMessQuerschnitt;
import de.bsvrz.dua.aggrlve.Verbindung;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;
import de.bsvrz.sys.funclib.bitctrl.dua.test.DAVTest;

/**
 * Testet die Aggregation von TV-Tagesdaten.
 * 
 * @author BitCtrl Systems GmbH, Thierfelder
 * 
 * @version $Id$
 */
public class AggregationLVETVTag extends AbstraktDTVTest {

	/**
	 * Testet die Aggregation von TV-Tagesdaten.
	 * 
	 * 
	 * @throws Exception wird weitergereicht
	 */
	@Test
	public void testTVTag() throws Exception {
		this.setup();
		ClientDavInterface dav = DAVTest.getDav(Verbindung.getConData());

		SystemObject mq = dav.getDataModel().getObject("mq.a100.0000"); //$NON-NLS-1$

		DataDescription dd = new DataDescription(dav.getDataModel()
				.getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_MQ),
				AggregationsIntervall.aGG60MINUTE.getAspekt(), (short) 0);

		GregorianCalendar cal = new GregorianCalendar();
		cal.set(Calendar.YEAR, 2000);
		cal.set(Calendar.MONTH, Calendar.JANUARY);
		cal.set(Calendar.DAY_OF_MONTH, 1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		long zeit = cal.getTimeInMillis();

		outputImporter.getNaechsteZeile();
		AggregationsMessQuerschnitt mqObj = aggregation
				.getAggregationsObjekt(mq);
		long startzeit = zeit;
		for (int a = 0; a < 24; a++) {

			try {
				Thread.sleep(50L);
			} catch (InterruptedException e) {
				//
			}

			inputImporter.importNaechsteZeile();
			ResultData resultat = new ResultData(mq, dd, startzeit,
					inputImporter.getAnalyseDatensatz(true, 0, 0));
			mqObj.getPuffer().aktualisiere(resultat);
			startzeit += Constants.MILLIS_PER_HOUR;

		}
		mqObj.aggregiere(zeit, AggregationsIntervall.aGGDTVTAG);

		Collection<AggregationsDatum> daten = mqObj.getPuffer().getPuffer(
				AggregationsIntervall.aGGDTVTAG).getDatenFuerZeitraum(zeit,
				startzeit);

		if (daten != null && !daten.isEmpty()) {
			String[] zeile = outputImporter.getNaechsteZeile();
			int i = 1;
			for (AggregationsAttribut attribut : agrMap.keySet()) {
				i++;
				AggregationsAttributWert wertSoll = getTextDatenSatz(attribut,
						zeile);
				Assert
						.assertEquals(
								"Zeile " + i + ": ", wertSoll, daten.iterator().next().getWert(attribut)); //$NON-NLS-1$ //$NON-NLS-2$
			}
		}
	}
}
