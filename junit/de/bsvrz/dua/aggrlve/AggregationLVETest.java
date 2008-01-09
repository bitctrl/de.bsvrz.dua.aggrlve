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

import org.junit.Test;

import de.bsvrz.dav.daf.main.ClientDavInterface;
import de.bsvrz.dav.daf.main.ClientSenderInterface;
import de.bsvrz.dav.daf.main.DataDescription;
import de.bsvrz.dav.daf.main.ResultData;
import de.bsvrz.dav.daf.main.SenderRole;
import de.bsvrz.dav.daf.main.config.SystemObject;
import de.bsvrz.sys.funclib.bitctrl.app.Pause;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;
import de.bsvrz.sys.funclib.bitctrl.dua.test.DAVTest;
import de.bsvrz.sys.funclib.bitctrl.konstante.Konstante;

/**
 * Allgemeine Tests 		!!!!!!!Kann wieder raus!!!!!!!!
 * 
 * @author BitCtrl Systems GmbH, Thierfelder
 *
 */
public class AggregationLVETest 
implements ClientSenderInterface{

	private int i;
	
	/**
	 * Sendet Testdaten
	 */
//	@Test
	public void sendeFahrstreifen()
	throws Exception{
		ClientDavInterface dav = DAVTest.getDav(Verbindung.getConData());
		dav.disconnect(false, "Testverbindung beendet"); //$NON-NLS-1$
		dav = DAVTest.newDav(Verbindung.getConData());

		AggregationLVE aggregation = new AggregationLVE();
		aggregation.testStart(dav);

		SystemObject fs1 = dav.getDataModel().getObject("fs.mq.a100.0000.hfs"); //$NON-NLS-1$
		SystemObject fs2 = dav.getDataModel().getObject("fs.mq.a100.0000.1üfs"); //$NON-NLS-1$
		SystemObject fs3 = dav.getDataModel().getObject("fs.mq.a100.0000.2üfs"); //$NON-NLS-1$

		SystemObject fs11 = dav.getDataModel().getObject("fs.mq.a100.0001.hfs"); //$NON-NLS-1$
		SystemObject fs21 = dav.getDataModel().getObject("fs.mq.a100.0001.1üfs"); //$NON-NLS-1$
		SystemObject fs31 = dav.getDataModel().getObject("fs.mq.a100.0001.2üfs"); //$NON-NLS-1$

		
		DataDescription dd = new DataDescription(
				dav.getDataModel().getAttributeGroup(DUAKonstanten.ATG_KZD),
				dav.getDataModel().getAspect(DUAKonstanten.ASP_MESSWERTERSETZUNG),
				(short)0);
		SystemObject[] fs = new SystemObject[]{fs1,fs2,fs3};
		SystemObject[] fs_1 = new SystemObject[]{fs11,fs21,fs31};
		try{
			dav.subscribeSender(this, fs, dd, SenderRole.source());
		}catch(Exception ex){
			ex.printStackTrace();
		}

		try{
			dav.subscribeSender(this, fs_1, dd, SenderRole.source());
		}catch(Exception ex){
			ex.printStackTrace();
		}

		Pause.warte(1000L);
		
		AnalysewerteImporter importer = new AnalysewerteImporter(dav, Verbindung.WURZEL + "Analysewerte.csv"); //$NON-NLS-1$
		
		long itvl = 30L;
		
		AnalysewerteImporter.setT(60 * 1000L);
//		GregorianCalendar cal = new GregorianCalendar();
//		cal.setTimeInMillis(System.currentTimeMillis());		
//		cal.set(Calendar.SECOND, (int)(((cal.get(Calendar.SECOND) / itvl) * itvl) + itvl));
//		cal.set(Calendar.MILLISECOND, 0);
		
		long time = Konstante.STUNDE_IN_MS;
		
		/**
		 * Fs1 aller 30 s
		 * Fs2 und Fs3 aller 60s
		 */
//		importer.importNaechsteZeile();
		for(int i = 0; i<100; i++){
//			while(System.currentTimeMillis() < cal.getTimeInMillis()){
				Pause.warte(1000);
	//		}
			
			importer.importNaechsteZeile();
			for(int j = 0; j<3; j++){
				ResultData resultat = new ResultData(fs[j], dd, time, importer.getFSAnalyseDatensatz(j + 1));
				resultat.getData().getTimeValue("T").setMillis(60 * 1000L); //$NON-NLS-1$
				dav.sendData(resultat);
			}
			
			time += 60L * Konstante.SEKUNDE_IN_MS;
		}
		
		
		
//		/**
//		 * Fs1 aller 30 s
//		 * Fs2 und Fs3 aller 60s
//		 * 
//		 * Fs11 aller 60 s
//		 * Fs21 und Fs31 aller 30s
//		 */
//		importer.importNaechsteZeile();
//		for(int i = 0; i<100000; i++){
//			while(System.currentTimeMillis() < cal.getTimeInMillis()){
//				Pause.warte(1000);
//			}
//
//			for(int j = 0; j<3; j++){
//				if(j == 0){
//					ResultData resultat = new ResultData(fs[j], dd, cal.getTimeInMillis() - itvl * 1000L, importer.getFSAnalyseDatensatz(j + 1));
//					resultat.getData().getTimeValue("T").setMillis(30 * 1000L);
//					dav.sendData(resultat);
//				}else{
//					if(i%2 == 0){
//						ResultData resultat = new ResultData(fs[j], dd, cal.getTimeInMillis() - 60 * 1000L, importer.getFSAnalyseDatensatz(j + 1));
//						resultat.getData().getTimeValue("T").setMillis(60 * 1000L);
//						dav.sendData(resultat);
//					}
//				}				
//			}
//			
//			for(int j = 0; j<3; j++){
//				if(j == 1 || j == 2){
//					ResultData resultat = new ResultData(fs_1[j], dd, cal.getTimeInMillis() - itvl * 1000L, importer.getFSAnalyseDatensatz(j + 1));
//					resultat.getData().getTimeValue("T").setMillis(30 * 1000L);
//					dav.sendData(resultat);
//				}else{
//					if(i%2 == 0){
//						ResultData resultat = new ResultData(fs_1[j], dd, cal.getTimeInMillis() - 60 * 1000L, importer.getFSAnalyseDatensatz(j + 1));
//						dav.sendData(resultat);
//					}
//				}				
//			}
//			
//
//			cal.add(Calendar.SECOND, 30);
//			//cal.add(Calendar.SECOND, (int)itvl);
//		}
	}


	/**
	 * {@inheritDoc}
	 */
	public void dataRequest(SystemObject object, DataDescription dataDescription, byte state) {
		
	}


	/**
	 * {@inheritDoc}
	 */
	public boolean isRequestSupported(SystemObject object,
			DataDescription dataDescription) {
		return false;
	}

}
