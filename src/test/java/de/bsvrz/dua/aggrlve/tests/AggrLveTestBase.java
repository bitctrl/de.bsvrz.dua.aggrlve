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

import de.bsvrz.dav.daf.main.ResultData;
import de.bsvrz.dav.daf.main.SendSubscriptionNotConfirmed;
import de.bsvrz.dua.aggrlve.AggregationLVE;
import de.bsvrz.dua.tests.DuATestBase;
import de.bsvrz.sys.funclib.commandLineArgs.ArgumentList;
import org.junit.After;
import org.junit.Before;

/**
 * TBD Dokumentation
 *
 * @author Kappich Systemberatung
 */
public class AggrLveTestBase extends DuATestBase {
	protected AggregationLVE _aggregationLVE;

	protected static String[] getLveArgs() {
		return new String[]{"-KonfigurationsBereichsPid=kb.duaTestFs"};
	}


	@Before
	public void setUp() throws Exception {
		super.setUp();
		_aggregationLVE = new AggregationLVE();
		_aggregationLVE.parseArguments(new ArgumentList(AggrLveTestBase.getLveArgs()));
		_aggregationLVE.initialize(_connection);
	}

	@Override
	protected String[] getConfigurationAreas() {
		return new String[]{"kb.duaTestFs"};
	}

	@After
	public void tearDown() throws Exception {
		_aggregationLVE.getVerbindung().disconnect(false, "");
		super.tearDown();
	}

	@Override
	public void sendData(final ResultData... resultDatas) throws SendSubscriptionNotConfirmed {
		_aggregationLVE.update(resultDatas);
	}
}
