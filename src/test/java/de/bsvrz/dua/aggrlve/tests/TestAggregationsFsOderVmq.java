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
import de.bsvrz.dav.daf.main.config.Aspect;
import de.bsvrz.dav.daf.main.config.AttributeGroup;
import de.bsvrz.dav.daf.main.config.SystemObject;
import de.bsvrz.dua.aggrlve.AggregationsFsOderVmq;
import de.bsvrz.dua.aggrlve.AggregationsIntervall;
import de.bsvrz.dua.dalve.ErfassungsIntervallDauerMQ;
import de.bsvrz.dua.tests.DuATestBase;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;
import de.bsvrz.sys.funclib.bitctrl.dua.lve.DuaVerkehrsNetz;
import de.bsvrz.sys.funclib.kappich.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ArrayBlockingQueue;

/**
 * TBD Dokumentation
 *
 * @author Kappich Systemberatung
 */
public class TestAggregationsFsOderVmq extends DuATestBase{

	private AggregationsFsOderVmq _fs;
	private AggregationsFsOderVmq _vmq;
	private ArrayBlockingQueue<String> _receivedFs = new ArrayBlockingQueue<String>(32);
	private ArrayBlockingQueue<String> _receivedVmq = new ArrayBlockingQueue<String>(32);

	@Override
	protected @NotNull String[] getConfigurationAreas() {
		return new String[]{"kb.duaTestFs"};
	}

	@Override
	public void setUp() throws Exception {
		super.setUp();
		DuaVerkehrsNetz.initialisiere(_connection);
		AggregationsIntervall.initialisiere(_connection);
		_fs = new AggregationsFsOderVmq(_connection, _dataModel.getObject("fs.mq.1.hfs")){
			@Override
			protected void send(final ResultData resultat) {
				String str;
				if(!resultat.hasData()) {
					str = resultat.getDataTime() / 1000 + " NODATA";
				}
				else {
					str = resultat.getDataTime() / 1000 + " "
							+ resultat.getData().getTimeValue("T").getSeconds() + " "
							+ resultat.getData().getItem("qKfz").getUnscaledValue("Wert").longValue() + " "
							+ resultat.getData().getItem("vKfz").getUnscaledValue("Wert").longValue();
				}
				_receivedFs.add(str);
				System.out.println("resultat = " + resultat);
			}
		};
		_vmq = new AggregationsFsOderVmq(_connection, _dataModel.getObject("vmq.vor1")){
			@Override
			protected void send(final ResultData resultat) {
				String str;
				if(!resultat.hasData()) {
					str = resultat.getDataTime() / 1000 + " NODATA";
				}
				else {
					str = resultat.getDataTime() / 1000 + " "
							+ resultat.getData().getItem("QKfz").getUnscaledValue("Wert").longValue() + " "
							+ resultat.getData().getItem("VKfz").getUnscaledValue("Wert").longValue();
				}
				_receivedVmq.add(str);
				System.out.println("resultat = " + resultat);
			}
		};
	}

	/**
	 * Normaler Test, bei dem alle Daten da sind. Es sollten korrekt 1, 5 und 15 Min Aggregationen generiert werden
	 * @throws Exception
	 */
	@Test
	public void testErfassungsIntervallRegular() throws Exception {
		_fs.update(makeDataFs(0, 60, 1600, 100));
		assertReceivedFs(0, 60, 1600, 100);
		assertReceivedFs(-90000, 86400, -1, -1);  // Tageswert vom Vortag
		assertReceivedFs(-2682000, 2419200, -1, -1);  // Monatswert vom Vormonat
		assertReceivedFs(-31539600, 31536000, -1, -1); // Jahreswert vom Vorjahr
		
		_fs.update(makeDataFs(60, 60, 1600, 100));
		assertReceivedFs(60, 60, 1600, 100);
		
		_fs.update(makeDataFs(120, 60, 1600, 100));
		assertReceivedFs(120, 60, 1600, 100);
		
		_fs.update(makeDataFs(180, 60, 1600, 100));
		assertReceivedFs(180, 60, 1600, 100);
		
		_fs.update(makeDataFs(240, 60, 1600, 100));
		assertReceivedFs(240, 60, 1600, 100);
		assertReceivedFs(0, 300, 1600, 100);
		
		
		_fs.update(makeDataFs(300, 60, 1600, 100));
		assertReceivedFs(300, 60, 1600, 100);
		
		_fs.update(makeDataFs(360, 60, 1600, 100));
		assertReceivedFs(360, 60, 1600, 100);
		
		_fs.update(makeDataFs(420, 60, 1600, 100));
		assertReceivedFs(420, 60, 1600, 100);
		
		_fs.update(makeDataFs(480, 60, 1600, 100));
		assertReceivedFs(480, 60, 1600, 100);
		
		_fs.update(makeDataFs(540, 60, 1600, 100));
		assertReceivedFs(540, 60, 1600, 100);
		assertReceivedFs(300, 300, 1600, 100);
		
		
		_fs.update(makeDataFs(600, 60, 1600, 100));
		assertReceivedFs(600, 60, 1600, 100);
		
		_fs.update(makeDataFs(660, 60, 1600, 100));
		assertReceivedFs(660, 60, 1600, 100);
		
		_fs.update(makeDataFs(720, 60, 1600, 100));
		assertReceivedFs(720, 60, 1600, 100);
		
		_fs.update(makeDataFs(780, 60, 1600, 100));
		assertReceivedFs(780, 60, 1600, 100);
		
		_fs.update(makeDataFs(840, 60, 1600, 100));
		assertReceivedFs(840, 60, 1600, 100);
		assertReceivedFs(600, 300, 1600, 100);
		assertReceivedFs(0, 900, 1600, 100);
	}
	
	
	/**
	 * Test, bei dem die Daten (kurz) mittendrin ausfallen
	 * @throws Exception
	 */
	@Test
	public void testErfassungsIntervallGap1() throws Exception {
		_fs.update(makeDataFs(0, 60, 1600, 100));
		assertReceivedFs(0, 60, 1600, 100);
		assertReceivedFs(-90000, 86400, -1, -1);  // Tageswert vom Vortag
		assertReceivedFs(-2682000, 2419200, -1, -1);  // Monatswert vom Vormonat
		assertReceivedFs(-31539600, 31536000, -1, -1); // Jahreswert vom Vorjahr
		
		_fs.update(makeDataFs(60, 60, 1600, 100));
		assertReceivedFs(60, 60, 1600, 100);
		
		_fs.update(makeDataGapFs(177));

		_fs.update(makeDataFs(180, 60, 1600, 100));
		assertReceivedFs(120, 60, -1, -1);
		assertReceivedFs(180, 60, 1600, 100);
		
		_fs.update(makeDataFs(240, 60, 1600, 100));
		assertReceivedFs(240, 60, 1600, 100);
		assertReceivedFs(0, 300, 1600, 100);
		
		
		_fs.update(makeDataFs(300, 60, 1600, 100));
		assertReceivedFs(300, 60, 1600, 100);
		
		_fs.update(makeDataFs(360, 60, 1600, 100));
		assertReceivedFs(360, 60, 1600, 100);
		
		//_fs.update(makeData(420, 60, 1600, 100));
		// Datum "fällt aus"

		_fs.update(makeDataFs(480, 60, 1600, 100));
		assertReceivedFs(420, 60, -1, -1);
		assertReceivedFs(480, 60, 1600, 100);
		
		_fs.update(makeDataFs(540, 60, 1600, 100));
		assertReceivedFs(540, 60, 1600, 100);
		assertReceivedFs(300, 300, 1600, 100);
		
		
		_fs.update(makeDataFs(600, 60, 1600, 100));
		assertReceivedFs(600, 60, 1600, 100);
		
		_fs.update(makeDataFs(660, 60, 1600, 100));
		assertReceivedFs(660, 60, 1600, 100);

		// Überflüssiger leerer Datensatz, der nicht stören darf
		_fs.update(makeDataGapFs(736));

		_fs.update(makeDataFs(720, 60, 1600, 100));
		assertReceivedFs(720, 60, 1600, 100);
		
		_fs.update(makeDataFs(780, 60, 1600, 100));
		assertReceivedFs(780, 60, 1600, 100);
		
		_fs.update(makeDataFs(840, 60, 1600, 100));
		assertReceivedFs(840, 60, 1600, 100);
		assertReceivedFs(600, 300, 1600, 100);
		assertReceivedFs(0, 900, 1600, 100);
	}
	
	/**
	 * Test, bei dem die Daten (lang) mittendrin ausfallen
	 * @throws Exception
	 */
	@Test
	public void testErfassungsIntervallGap2() throws Exception {
		_fs.update(makeDataFs(0, 60, 1600, 100));
		assertReceivedFs(0, 60, 1600, 100);
		assertReceivedFs(-90000, 86400, -1, -1);  // Tageswert vom Vortag
		assertReceivedFs(-2682000, 2419200, -1, -1);  // Monatswert vom Vormonat
		assertReceivedFs(-31539600, 31536000, -1, -1); // Jahreswert vom Vorjahr
		
		_fs.update(makeDataFs(60, 60, 1600, 100));
		assertReceivedFs(60, 60, 1600, 100);
		
		_fs.update(makeDataGapFs(177));

		// Lücke

		_fs.update(makeDataFs(660, 60, 1600, 100));

		// Lücke füllen
		assertReceivedFs(120, 60, -1, -1);
		assertReceivedFs(180, 60, -1, -1);
		assertReceivedFs(240, 60, -1, -1);
		assertReceivedFs(0, 300, 1600, 100);
		assertReceivedFs(300, 60, -1, -1);
		assertReceivedFs(360, 60, -1, -1);
		assertReceivedFs(420, 60, -1, -1);
		assertReceivedFs(480, 60, -1, -1);
		assertReceivedFs(540, 60, -1, -1);
		assertReceivedFs(300, 300, -1, -1);
		assertReceivedFs(600, 60, -1, -1);
		
		assertReceivedFs(660, 60, 1600, 100);

		// Überflüssiger leerer Datensatz, der nicht stören darf
		_fs.update(makeDataGapFs(736));

		_fs.update(makeDataFs(720, 60, 1600, 100));
		assertReceivedFs(720, 60, 1600, 100);
		
		_fs.update(makeDataFs(780, 60, 1600, 100));
		assertReceivedFs(780, 60, 1600, 100);
		
		_fs.update(makeDataFs(840, 60, 1600, 100));
		assertReceivedFs(840, 60, 1600, 100);
		assertReceivedFs(600, 300, 1600, 100);
		assertReceivedFs(0, 900, 1600, 100);
	}
	
	
	/**
	 * Test, für 2 Minuten Erfassungsintervall
	 * @throws Exception
	 */
	@Test
	public void testErfassungsIntervallNonMatching() throws Exception {
		_fs.update(makeDataFs(0, 120, 1600, 100));
		
		assertReceivedFs(-90000, 86400, -1, -1);  // Tageswert vom Vortag
		assertReceivedFs(-2682000, 2419200, -1, -1);  // Monatswert vom Vormonat
		assertReceivedFs(-31539600, 31536000, -1, -1); // Jahreswert vom Vorjahr
		
		_fs.update(makeDataFs(120, 120, 1600, 50));

		_fs.update(makeDataFs(240, 120, 1600, 150));
		assertReceivedFs(0, 300, 1600, 100);
		
		_fs.update(makeDataFs(360, 120, 1600, 75));

		_fs.update(makeDataFs(480, 120, 1600, 25));
		assertReceivedFs(300, 300, 1600, 50);
	}	
	
	/**
	 * Test, für 2 Minuten Erfassungsintervall
	 * @throws Exception
	 */
	@Test
	public void testVmqDtv() throws Exception {
		ClientSenderInterface sender = new ClientSenderInterface() {
			@Override
			public void dataRequest(final SystemObject object, final DataDescription dataDescription, final byte state) {

			}

			@Override
			public boolean isRequestSupported(final SystemObject object, final DataDescription dataDescription) {
				return false;
			}
		};
		_connection.subscribeSource(sender, makeDataFs(0, 120, 1600, 100));

		ErfassungsIntervallDauerMQ instanz = ErfassungsIntervallDauerMQ.getInstanz(_connection, _vmq.getObjekt());

		Thread.sleep(1000);

		Assert.assertEquals(120000, instanz.getT());
		
		_vmq.update(makeDataVmq(0, 1600, 100));

		assertReceivedVmq(-90000, -1, -1);  // Tageswert vom Vortag
		assertReceivedVmq(-2682000, -1, -1); // Monatswert vom Vormonat
		assertReceivedVmq(-31539600, -1, -1); // Jahreswert vom Vorjahr

	}

	private void assertReceivedFs(final long timeSeconds, final int tSeconds, final int q, final int v) {
		String expected = timeSeconds + " " + tSeconds + " " + q + " " + v;
		String actual = _receivedFs.poll();
		Assert.assertEquals(expected, actual);
	}
	
	private void assertReceivedVmq(final long timeSeconds, final int q, final int v) {
		String expected = timeSeconds + " " + q + " " + v;
		String actual = _receivedVmq.poll();
		Assert.assertEquals(expected, actual);
	}


	private ResultData makeDataFs(final long timeSeconds, final int tSeconds, final int q, final int v) {
		AttributeGroup atg = _dataModel.getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_FS);
		Aspect asp = _dataModel.getAspect(DUAKonstanten.ASP_ANALYSE);
		Data data = _connection.createData(atg);
		resetData(data);
		data.getTimeValue("T").setMillis(tSeconds * 1000);
		data.getItem("qKfz").getItem("Wert").asUnscaledValue().set(q);
		data.getItem("vKfz").getItem("Wert").asUnscaledValue().set(v);	
		data.getItem("qKfz").getItem("Güte").getUnscaledValue("Index").set(10000);
		data.getItem("vKfz").getItem("Güte").getUnscaledValue("Index").set(10000);
		return new ResultData(_fs.getObjekt(), new DataDescription(atg, asp), timeSeconds * 1000L, data);
	}

	private ResultData makeDataGapFs(final int timeSeconds) {
		AttributeGroup atg = _dataModel.getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_FS);
		Aspect asp = _dataModel.getAspect(DUAKonstanten.ASP_ANALYSE);
		return new ResultData(_fs.getObjekt(), new DataDescription(atg, asp), timeSeconds * 1000L, null);
	}
	
	private ResultData makeDataVmq(final long timeSeconds, final int q, final int v) {
		AttributeGroup atg = _dataModel.getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_MQ);
		Aspect asp = _dataModel.getAspect(DUAKonstanten.ASP_ANALYSE);
		Data data = _connection.createData(atg);
		resetData(data);
		data.getItem("QKfz").getItem("Wert").asUnscaledValue().set(q);
		data.getItem("VKfz").getItem("Wert").asUnscaledValue().set(v);	
		data.getItem("QKfz").getItem("Güte").getUnscaledValue("Index").set(10000);
		data.getItem("VKfz").getItem("Güte").getUnscaledValue("Index").set(10000);
		return new ResultData(_vmq.getObjekt(), new DataDescription(atg, asp), timeSeconds * 1000L, data);
	}

	private ResultData makeDataGapVmq(final int timeSeconds) {
		AttributeGroup atg = _dataModel.getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_MQ);
		Aspect asp = _dataModel.getAspect(DUAKonstanten.ASP_ANALYSE);
		return new ResultData(_vmq.getObjekt(), new DataDescription(atg, asp), timeSeconds * 1000L, null);
	}
}
