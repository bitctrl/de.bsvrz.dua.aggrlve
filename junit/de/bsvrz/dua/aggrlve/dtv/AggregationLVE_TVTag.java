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

import de.bsvrz.dav.daf.main.ClientDavInterface;
import de.bsvrz.dav.daf.main.DataDescription;
import de.bsvrz.dav.daf.main.ResultData;
import de.bsvrz.dav.daf.main.config.SystemObject;
import de.bsvrz.dua.aggrlve.AggregationLVETest;
import de.bsvrz.dua.aggrlve.AggregationsAttribut;
import de.bsvrz.dua.aggrlve.AggregationsAttributWert;
import de.bsvrz.dua.aggrlve.AggregationsDatum;
import de.bsvrz.dua.aggrlve.AggregationsIntervall;
import de.bsvrz.dua.aggrlve.AggregationsMessQuerschnitt;
import de.bsvrz.sys.funclib.bitctrl.app.Pause;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;
import de.bsvrz.sys.funclib.bitctrl.dua.test.DAVTest;
import de.bsvrz.sys.funclib.bitctrl.konstante.Konstante;

/**
 * Testet die Aggregation von TV-Tagesdaten
 *  
 * @author BitCtrl Systems GmbH, Thierfelder
 *
 */
public class AggregationLVE_TVTag
extends AbstraktDTVTest{	
	
	/**
	 * Testet die Aggregation von TV-Tagesdaten
	 */
	@Test
	public void testTVTag()
	throws Exception{
		this.setup();
		ClientDavInterface dav = DAVTest.getDav(AggregationLVETest.CON_DATA);

		SystemObject mq = dav.getDataModel().getObject("mq.a100.0000"); //$NON-NLS-1$

		DataDescription dd = new DataDescription(
				dav.getDataModel().getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_MQ),
				AggregationsIntervall.AGG_60MINUTE.getAspekt(),
				(short)0);

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
		AggregationsMessQuerschnitt mqObj = aggregation.getAggregationsObjekt(mq);
		long startzeit = zeit;
		for(int a = 0; a<24; a++){
			
			Pause.warte(50);

			inputImporter.importNaechsteZeile();
			ResultData resultat = new ResultData(mq, dd, startzeit, inputImporter.getMQAnalyseDatensatz());
			mqObj.getPuffer().aktualisiere(resultat);
			startzeit += Konstante.STUNDE_IN_MS;
				
		}
		mqObj.aggregiere(zeit, AggregationsIntervall.AGG_DTV_TAG);
		
		Collection<AggregationsDatum> daten = mqObj.getPuffer().
				getPuffer(AggregationsIntervall.AGG_DTV_TAG).getDatenFuerZeitraum(zeit, startzeit);

		if(daten !=null && !daten.isEmpty()){
			String[] zeile = outputImporter.getNaechsteZeile();
			for(AggregationsAttribut attribut:AGR_MAP.keySet()){
				AggregationsAttributWert wertSoll = getTextDatenSatz(attribut, zeile);
					Assert.assertEquals(wertSoll, daten.iterator().next().getWert(attribut));
			}
		}
	}
}
