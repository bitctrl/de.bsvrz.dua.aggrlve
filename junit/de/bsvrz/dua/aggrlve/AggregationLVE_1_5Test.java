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

import java.util.Calendar;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.HashMap;

import junit.framework.Assert;

import org.junit.Test;

import de.bsvrz.dav.daf.main.ClientDavInterface;
import de.bsvrz.dav.daf.main.DataDescription;
import de.bsvrz.dav.daf.main.ResultData;
import de.bsvrz.dav.daf.main.config.SystemObject;
import de.bsvrz.dua.guete.GWert;
import de.bsvrz.dua.guete.GueteVerfahren;
import de.bsvrz.sys.funclib.bitctrl.app.Pause;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;
import de.bsvrz.sys.funclib.bitctrl.dua.GanzZahl;
import de.bsvrz.sys.funclib.bitctrl.dua.test.CSVImporter;
import de.bsvrz.sys.funclib.bitctrl.dua.test.DAVTest;
import de.bsvrz.sys.funclib.bitctrl.konstante.Konstante;

/**
 * <b>Vorlaeufige Version, bis zur Klaerung der Probleme mit den Testdaten)</b>
 * 
 * Allgemeine Tests fuer 1- und 5-Minuten-Intervall
 * 
 * @author BitCtrl Systems GmbH, Thierfelder
 *
 */
public class AggregationLVE_1_5Test{
		
	/**
	 * Mappt Attribut auf relative Posisition in Tabelle
	 */
	private final HashMap<AggregationsAttribut, Integer> AGR_MAP = new HashMap<AggregationsAttribut, Integer>(); 
	
	
	/**
	 * Testet, ob die 1 Minuten intervalle korrekt zu fünf-Minuten
	 * Intervallen zusammengerechnet und die Fahrstreifen korrekt zu
	 * Messquerschnitten zusammengerechnet werden
	 */
	@Test
	public void test1und5MinutenFSundMQ()
	throws Exception{
		ClientDavInterface dav = DAVTest.getDav(Verbindung.getConData());

		AggregationLVE aggregation = new AggregationLVE();
		aggregation.testStart(dav);
		
		AGR_MAP.put(AggregationsAttribut.Q_KFZ, 0);
		AGR_MAP.put(AggregationsAttribut.Q_PKW, 1);
		AGR_MAP.put(AggregationsAttribut.Q_LKW, 2);
		AGR_MAP.put(AggregationsAttribut.V_KFZ, 3);
		AGR_MAP.put(AggregationsAttribut.V_PKW, 4);
		AGR_MAP.put(AggregationsAttribut.V_LKW, 5);
		
		SystemObject mq = dav.getDataModel().getObject("mq.a100.0000"); //$NON-NLS-1$

		SystemObject fs1 = dav.getDataModel().getObject("fs.mq.a100.0000.hfs"); //$NON-NLS-1$
		SystemObject fs2 = dav.getDataModel().getObject("fs.mq.a100.0000.1üfs"); //$NON-NLS-1$
		SystemObject fs3 = dav.getDataModel().getObject("fs.mq.a100.0000.2üfs"); //$NON-NLS-1$

		DataDescription dd = new DataDescription(
				dav.getDataModel().getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_FS),
				AggregationsIntervall.AGG_1MINUTE.getAspekt(),
				(short)0);
		SystemObject[] fs = new SystemObject[]{fs1,fs2,fs3};

		TestErgebnisAnalyseImporter inputImporter = new TestErgebnisAnalyseImporter(dav, 
				"C:\\Dokumente und Einste" + //$NON-NLS-1$
				"llungen\\Thierfelder\\workspace3.3\\de.bsvrz.dua.aggrlve\\extra\\Analysewerte.csv"); //$NON-NLS-1$

		CSVImporter outputImporter = new CSVImporter( 
				"C:\\Dokumente und Einste" + //$NON-NLS-1$
				"llungen\\Thierfelder\\workspace3.3\\de.bsvrz.dua.aggrlve\\extra\\Messwert_Aggregation.csv"); //$NON-NLS-1$

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
		for(int a = 0; a<5; a++){
			long startzeit = zeit;
			for(int i = 0; i<5; i++){
				Pause.warte(50);
				
				inputImporter.importNaechsteZeile();
				for(int j = 0; j<3; j++){
					ResultData resultat = new ResultData(fs[j], dd, startzeit, inputImporter.getFSAnalyseDatensatz(j + 1));
					resultat.getData().getTimeValue("T").setMillis(Konstante.MINUTE_IN_MS); //$NON-NLS-1$
					mqObj.getAggregationsObjekt(fs[j]).getPuffer().aktualisiere(resultat);	
				}
				
				startzeit += Konstante.MINUTE_IN_MS;
			}
			mqObj.aggregiere(zeit, AggregationsIntervall.AGG_5MINUTE);
			
			/**
			 * vergleiche
			 */
			int f = 0;
			String[] zeile = outputImporter.getNaechsteZeile();
			for(SystemObject fsObj:fs){
				Collection<AggregationsDatum> daten = mqObj.getAggregationsObjekt(fsObj).
						getPuffer().getPuffer(AggregationsIntervall.AGG_5MINUTE).
						getDatenFuerZeitraum(zeit, startzeit);

				int j = 0;
				if(daten !=null && !daten.isEmpty()){
					for(AggregationsAttribut attribut:AggregationsAttribut.getInstanzen()){
						if(attribut.equals(AggregationsAttribut.V_LKW)){
							AggregationsAttributWert wertSoll = getTextDatenSatz(attribut, zeile, f);
							//System.out.println(fsObj + ", I:" + a + " Soll: " + wertSoll + ", Ist:" + daten.iterator().next().getWert(attribut));  //$NON-NLS-1$//$NON-NLS-2$
//							if(j++ > 0){
							Assert.assertEquals(fsObj + ", I:" + a + " ", wertSoll, daten.iterator().next().getWert(attribut));  //$NON-NLS-1$//$NON-NLS-2$
							//System.out.println(fsObj + ", I:" + a + " " + wertSoll.equals(daten.iterator().next().getWert(attribut)));  //$NON-NLS-1$//$NON-NLS-2$
	//						}
						}
					}
				}
				f++;
			}
			
			zeit = startzeit;
		}
		
	}
	
	
	/**
	 * Extrahiert den Wert und den Status-String (Soll)
	 * 
	 * @param attribut das Attribut
	 * @param zeile eine Tabellenzeile
	 * @return den Wert und den Status-String (Soll)
	 */
	private final AggregationsAttributWert getTextDatenSatz(final AggregationsAttribut attribut,
						        		  	final String[] zeile, int fs){
		AggregationsAttributWert wert = new AggregationsAttributWert(attribut, Long.parseLong(zeile[26+11+AGR_MAP.get(attribut) * 6 - 1 + fs * 2]), 0);
		String status = zeile[26+11+AGR_MAP.get(attribut) * 6 + fs * 2];
		double guete = 1.0;
		if(status.split(" ").length > 1){ //$NON-NLS-1$
			guete = Double.parseDouble(status.split(" ")[0].replace(",", ".")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			wert.setInterpoliert(true);
		}else{
			guete = Double.parseDouble(status.replace(",", ".")); //$NON-NLS-1$ //$NON-NLS-2$
		}
		GanzZahl g = GanzZahl.getGueteIndex();
		g.setSkaliertenWert(guete);
		
		wert.setGuete(new GWert(g, GueteVerfahren.STANDARD, false));
		
		return wert;
	}
}