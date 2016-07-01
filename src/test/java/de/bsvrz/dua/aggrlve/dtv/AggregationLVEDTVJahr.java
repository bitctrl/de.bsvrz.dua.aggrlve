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
package de.bsvrz.dua.aggrlve.dtv;

import java.util.Calendar;
import java.util.Collection;
import java.util.GregorianCalendar;

import org.junit.Assert;
import org.junit.Test;

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
 * Testet die Aggregation von DTV-Jahreswerten.
 *
 * @author BitCtrl Systems GmbH, Thierfelder
 */
public class AggregationLVEDTVJahr extends AbstraktDTVTest {

	/**
	 * Testet die Aggregation von DTV-Jahredaten.
	 *
	 * @throws Exception
	 *             wird weitergereicht
	 */
	@Test
	public void testDTVJahr() throws Exception {
		setup();
		final ClientDavInterface dav = DAVTest.getDav(Verbindung.getConData());

		final SystemObject mq = dav.getDataModel().getObject("mq.a100.0000");

		final DataDescription dd = new DataDescription(
				dav.getDataModel().getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_MQ),
				AggregationsIntervall.aGGDTVMONAT.getAspekt());

		final GregorianCalendar cal = new GregorianCalendar();
		cal.set(Calendar.YEAR, 2000);
		cal.set(Calendar.MONTH, Calendar.JANUARY);
		cal.set(Calendar.DAY_OF_MONTH, 1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		final long zeit = cal.getTimeInMillis();

		outputImporter.getNaechsteZeile();
		final AggregationsMessQuerschnitt mqObj = aggregation.getAggregationsObjekt(mq);
		long startzeit = zeit;
		for (int a = 0; a < 12; a++) {
			inputImporter.importNaechsteZeile();
			final ResultData resultat = new ResultData(mq, dd, startzeit,
					inputImporter.getAnalyseDatensatz(true, 0, 0));
			mqObj.getPuffer().aktualisiere(resultat);

			cal.add(Calendar.MONTH, 1);
			startzeit = cal.getTimeInMillis();
		}
		mqObj.aggregiere(zeit, AggregationsIntervall.aGGDTVJAHR);

		final Collection<AggregationsDatum> daten = mqObj.getPuffer()
				.getPuffer(AggregationsIntervall.aGGDTVJAHR).getDatenFuerZeitraum(zeit, startzeit);

		if ((daten != null) && !daten.isEmpty()) {
			outputImporter.getNaechsteZeile();
			outputImporter.getNaechsteZeile();
			final String[] zeile = outputImporter.getNaechsteZeile();
			int i = 1;
			for (final AggregationsAttribut attribut : agrMap.keySet()) {
				i++;
				final AggregationsAttributWert wertSoll = getTextDatenSatz(attribut, zeile);
				Assert.assertEquals("Zeile " + i + ": ", wertSoll,
						daten.iterator().next().getWert(attribut));
			}
		}
	}
}
