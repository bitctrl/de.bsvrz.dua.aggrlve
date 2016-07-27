/*
 * Copyright 2016 by Kappich Systemberatung Aachen
 * 
 * This file is part of de.bsvrz.dua.aggrlve.tests.
 * 
 * de.bsvrz.dua.aggrlve.tests is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * de.bsvrz.dua.aggrlve.tests is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with de.bsvrz.dua.aggrlve.tests.  If not, see <http://www.gnu.org/licenses/>.

 * Contact Information:
 * Kappich Systemberatung
 * Martin-Luther-Straße 14
 * 52062 Aachen, Germany
 * phone: +49 241 4090 436 
 * mail: <info@kappich.de>
 */

package de.bsvrz.dua.aggrlve.tests;

import de.bsvrz.dav.daf.main.ClientSenderInterface;
import de.bsvrz.dav.daf.main.Data;
import de.bsvrz.dav.daf.main.DataDescription;
import de.bsvrz.dav.daf.main.ResultData;
import de.bsvrz.dav.daf.main.config.SystemObject;
import de.bsvrz.dua.tests.ColumnLayout;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;
import org.junit.Test;

import java.util.List;

/**
 * TBD Dokumentation
 *
 * @author Kappich Systemberatung
 */
public class TestDuAAggrLve extends AggrLveTestBase {

	private SystemObject[] _testFs;
	private SystemObject[] _testFs3;
	private SystemObject[] _testMq3;
	private SystemObject[] _testVmq;

	@Override
	public void setUp() throws Exception {
		super.setUp();

		_testFs = new SystemObject[]{
				_dataModel.getObject("fs.mq.1.hfs")
		};

		_testFs3 = new SystemObject[]{
				_dataModel.getObject("fs.mq.3.hfs"),
				_dataModel.getObject("fs.mq.3.1üfs"),
				_dataModel.getObject("fs.mq.3.2üfs")
		};

		for(SystemObject obj : _testFs) {
			fakeParamApp.publishParam(obj.getPid(), "atg.verkehrsDatenKurzZeitAnalyseFs",
			                          "{" +
					                          "kKfz:{Grenz:'48',Max:'68'}," +
					                          "kLkw:{Grenz:'28',Max:'38'}," +
					                          "kPkw:{Grenz:'48',Max:'68'}," +
					                          "kB:{Grenz:'58',Max:'77'}," +
					                          "fl:{k1:'2,2',k2:'0,02'}" +
					                          "}"
			);
		}

		for(SystemObject obj : _testFs3) {
			fakeParamApp.publishParam(obj.getPid(), "atg.verkehrsDatenKurzZeitAnalyseFs",
			                          "{" +
					                          "kKfz:{Grenz:'48',Max:'68'}," +
					                          "kLkw:{Grenz:'28',Max:'38'}," +
					                          "kPkw:{Grenz:'48',Max:'68'}," +
					                          "kB:{Grenz:'58',Max:'77'}," +
					                          "fl:{k1:'2,2',k2:'0,02'}" +
					                          "}"
			);
		}


		_testMq3 = new SystemObject[]{
				_dataModel.getObject("mq.3")
		};

		for(SystemObject obj : _testMq3) {
			fakeParamApp.publishParam(obj.getPid(), "atg.verkehrsDatenKurzZeitAnalyseMq",
			                          "{" +
					                          "KKfz:{Grenz:'48',Max:'68'}," +
					                          "KLkw:{Grenz:'28',Max:'38'}," +
					                          "KPkw:{Grenz:'48',Max:'68'}," +
					                          "KB:{Grenz:'58',Max:'77'}," +
					                          "fl:{k1:'2,2',k2:'0,02'}," +
					                          "wichtung:['40','60']" +
					                          "}"
			);

		}

		_testVmq = new SystemObject[]{
				_dataModel.getObject("vmq.vor1")
		};

		for(SystemObject obj : _testVmq) {
			fakeParamApp.publishParam(obj.getPid(), "atg.verkehrsDatenKurzZeitAnalyseMq",
			                          "{" +
					                          "KKfz:{Grenz:'48',Max:'68'}," +
					                          "KLkw:{Grenz:'28',Max:'38'}," +
					                          "KPkw:{Grenz:'48',Max:'68'}," +
					                          "KB:{Grenz:'58',Max:'77'}," +
					                          "fl:{k1:'2,2',k2:'0,02'}," +
					                          "wichtung:['40','60']" +
					                          "}"
			);

		}
	}

	@Test
	public void testFs15() throws Exception {
		DataDescription dataDescription1 = new DataDescription(_dataModel.getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_FS), _dataModel.getAspect(DUAKonstanten.ASP_ANALYSE));
		DataDescription dataDescription2 = new DataDescription(_dataModel.getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_FS), _dataModel.getAspect("asp.agregation1Minute"));
		startTestCase("DUA28-FS15.csv", _testFs, _testFs, dataDescription1, dataDescription2, new AggrLveColumnLayout());
	}
	
	@Test
	public void testFs60() throws Exception {
		DataDescription dataDescription1 = new DataDescription(_dataModel.getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_FS), _dataModel.getAspect(DUAKonstanten.ASP_ANALYSE));
		DataDescription dataDescription2 = new DataDescription(_dataModel.getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_FS), _dataModel.getAspect("asp.agregation5Minuten"));
		startTestCase("DUA28-FS60.csv", _testFs, _testFs, dataDescription1, dataDescription2, new AggrLveColumnLayout()
		{
			@Override
			public int getOutOffset() {
				return 4;
			}

			@Override
			public long getIntervallLength() {
				return 60000;
			}
		});
	}	
	
	@Test
	public void testFs300() throws Exception {
		DataDescription dataDescription1 = new DataDescription(_dataModel.getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_FS), _dataModel.getAspect(DUAKonstanten.ASP_ANALYSE));
		DataDescription dataDescription2 = new DataDescription(_dataModel.getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_FS), _dataModel.getAspect("asp.agregation15Minuten"));
		startTestCase("DUA28-FS300.csv", _testFs, _testFs, dataDescription1, dataDescription2, new AggrLveColumnLayout()
		{
			@Override
			public int getOutOffset() {
				return 2;
			}

			@Override
			public long getIntervallLength() {
				return 300000;
			}
		});
	}

	@Test
	public void testMq60() throws Exception {
		DataDescription dataDescription1 = new DataDescription(_dataModel.getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_FS), _dataModel.getAspect(DUAKonstanten.ASP_ANALYSE));
		DataDescription dataDescription2 = new DataDescription(_dataModel.getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_MQ), _dataModel.getAspect("asp.agregation1Minute"));
		startTestCase("DUA28-MQ60.csv", _testFs3, _testMq3, dataDescription1, dataDescription2, new AggrLveColumnLayout()
		{
			@Override
			public int getOutOffset() {
				return 0;
			}

			@Override
			public long getIntervallLength() {
				return 60000;
			}

			@Override
			public boolean groupingEnabled() {
				return true;
			}
		});	
	}


	@Test
	public void testVmq300() throws Exception {
		DataDescription dataDescription1 = new DataDescription(_dataModel.getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_MQ), _dataModel.getAspect(DUAKonstanten.ASP_ANALYSE));
		DataDescription dataDescription2 = new DataDescription(_dataModel.getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_MQ), _dataModel.getAspect("asp.agregation15Minuten"));

		DataDescription dd = new DataDescription(_connection.getDataModel()
				                                         .getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_FS), _connection
				                                         .getDataModel().getAspect(DUAKonstanten.ASP_ANALYSE));
		
		// Erfassungsintervall simulieren
		final Data data = _connection.createData(dd.getAttributeGroup());
		
		resetData(data);
		
		data.getTimeValue("T").setMillis(300000);
		
		_connection.subscribeSource(new ClientSenderInterface() {
			@Override
			public void dataRequest(final SystemObject object, final DataDescription dataDescription, final byte state) {
			}

			@Override
			public boolean isRequestSupported(final SystemObject object, final DataDescription dataDescription) {
				return false;
			}
		},new ResultData(
				_testFs[0], dd , 0, data
		));
		
		
		startTestCase("DUA28-VMQ300.csv", _testVmq, _testVmq, dataDescription1, dataDescription2, new AggrLveColumnLayout()
		{
			@Override
			public int getOutOffset() {
				return 2;
			}

			@Override
			public long getIntervallLength() {
				return 300000;
			}
		});
	}

	private class AggrLveColumnLayout extends ColumnLayout{
		@Override
		public int getColumnCount(final boolean in) {
			return 3;
		}

		@Override
		public void setValues(final SystemObject testObject, final Data item, final List<String> row, final int realCol, final String type, final boolean in) {
			item.getTextValue("Wert").setText(row.get(realCol));
			Data mwItem = item.getItem("Status").getItem("MessWertErsetzung");
			Data guete = item.getItem("Güte");
			mwItem.getTextValue("Interpoliert").setText(row.get(realCol + 1));
			String percent = row.get(realCol + 2);
			if(percent.endsWith("%")) {
				percent = percent.substring(0, percent.length() - 1);
			}
			percent = percent.replace(',', '.');
			guete.getUnscaledValue("Index").set(Double.parseDouble(percent) * 100);
		}

		@Override
		public int getOutOffset() {
			return 3;
		}

		@Override
		public long getIntervallLength() {
			return 15000;
		}

		@Override
		public boolean groupingEnabled() {
			return false;
		}
	}
}
