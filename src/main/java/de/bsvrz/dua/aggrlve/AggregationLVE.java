/*
 * Segment Datenübernahme und Aufbereitung (DUA), SWE Aggregation LVE
 * Copyright (C) 2007 BitCtrl Systems GmbH
 * Copyright 2016 by Kappich Systemberatung Aachen
 * 
 * This file is part of de.bsvrz.dua.aggrlve.
 * 
 * de.bsvrz.dua.aggrlve is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * de.bsvrz.dua.aggrlve is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with de.bsvrz.dua.aggrlve.  If not, see <http://www.gnu.org/licenses/>.

 * Contact Information:
 * Kappich Systemberatung
 * Martin-Luther-Straße 14
 * 52062 Aachen, Germany
 * phone: +49 241 4090 436 
 * mail: <info@kappich.de>
 */

package de.bsvrz.dua.aggrlve;

import de.bsvrz.dav.daf.main.ResultData;
import de.bsvrz.dav.daf.main.config.SystemObject;
import de.bsvrz.dua.dalve.ErfassungsIntervallDauerMQ;
import de.bsvrz.sys.funclib.application.StandardApplicationRunner;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAInitialisierungsException;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAUtensilien;
import de.bsvrz.sys.funclib.bitctrl.dua.adapter.AbstraktVerwaltungsAdapter;
import de.bsvrz.sys.funclib.bitctrl.dua.dfs.typen.SWETyp;
import de.bsvrz.sys.funclib.bitctrl.dua.lve.DuaVerkehrsNetz;
import de.bsvrz.sys.funclib.bitctrl.dua.lve.FahrStreifen;
import de.bsvrz.sys.funclib.bitctrl.dua.lve.MessQuerschnitt;
import de.bsvrz.sys.funclib.bitctrl.modell.SystemObjekt;
import de.bsvrz.sys.funclib.debug.Debug;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Die SWE Aggregation LVE meldet sich auf alle messwertersetzten Kurzzeitdaten
 * an und berechnet aus diesen Daten für alle parametrierten Fahrstreifen und
 * Messquerschnitte die 1-, 5-, 15-, 30-, 60- Minutenwerte sowie Tageswerte und
 * DTV-Werte (Durchschnittliche Tagesverkehrswerte) je Monat und je Jahr
 * (Details siehe [AFo] bzw. [MARZ]).<br>
 * Diese Applikation initialisiert nur alle in den uebergebenen
 * Konfigurationsbereichen konfigurierten Messquerschnitte. Von diesen Objekten
 * aus werden dann auch die assoziierten Fahrstreifen initialisiert
 * 
 * @author BitCtrl Systems GmbH, Thierfelder
 */
public final class AggregationLVE extends AbstraktVerwaltungsAdapter {


	/**
	 * alle Messquerschnitte, fuer die Daten aggregiert werden sollen.
	 */
	private final Map<SystemObject, AggregationsMessQuerschnitt> messQuerschnitte = new HashMap<>();


	/**
	 * alle Fahrstreifen und VMQ, fuer die Daten aggregiert werden sollen.
	 */
	private final Map<SystemObject, AggregationsFsOderVmq> fsUndVmq = new HashMap<>();

	private static final Debug _debug = Debug.getLogger();
	
	@Override
	protected void initialisiere() throws DUAInitialisierungsException {

		/**
		 * DUA-Verkehrs-Netz initialisieren
		 */
		DuaVerkehrsNetz.initialisiere(this.verbindung);

		/**
		 * Aggregationsintervalle initialisieren
		 */
		AggregationsIntervall.initialisiere(this.verbindung);


		final Collection<SystemObject> mqObjects = DUAUtensilien
				.getBasisInstanzen(
						this.verbindung.getDataModel().getType(
								DUAKonstanten.TYP_MQ), this.verbindung,
						this.getKonfigurationsBereiche());	
		
		final Collection<SystemObject> vmqObjects = DUAUtensilien
				.getBasisInstanzen(
						this.verbindung.getDataModel().getType(
								DUAKonstanten.TYP_MQ_VIRTUELL), this.verbindung,
						this.getKonfigurationsBereiche());

		final Collection<SystemObject> fsObjects = new HashSet<>(DUAUtensilien
	            .getBasisInstanzen(
	                    this.verbindung.getDataModel().getType(
	                            DUAKonstanten.TYP_FAHRSTREIFEN), this.verbindung,
	                    this.getKonfigurationsBereiche()));
		
//		// Erfassungsintervalldauern von VMQ möglichst früh initialisieren
		for(SystemObject vmqObject : vmqObjects) {
			ErfassungsIntervallDauerMQ.getInstanz(getVerbindung(), vmqObject);
		}

		for(SystemObject mqObject : mqObjects) {
			// Alle Fahrstreifen hinzufügen, die sich nicht in den angegebenenen Bereichen befinden, die aber von einem MQ gebraucht werden
			final MessQuerschnitt mq = MessQuerschnitt.getInstanz(mqObject);
			if(mq == null) continue;
			mq.getFahrStreifen().stream().map(SystemObjekt::getSystemObject).forEach((e) -> {
				if(fsObjects.add(e)){
					_debug.warning("Fahrstreifen " + e + " wird zusätzlich berechnet, da er vom Messquerschnitt " + mqObject + " referenziert wird.");
				}
			});
		}

		for(SystemObject fsObject : fsObjects) {
			fsUndVmq.put(fsObject, new AggregationsFsOderVmq(this.verbindung, fsObject));
		}

		for (final SystemObject mqObjekt : mqObjects) {
			final MessQuerschnitt mq = MessQuerschnitt.getInstanz(mqObjekt);
			if (mq == null) {
				throw new DUAInitialisierungsException(
						"Konfiguration von Messquerschnitt "
								+ mqObjekt
								+ " konnte nicht vollstaendig ausgelesen werden");
			} else {
				HashMap<SystemObject, AggregationsFsOderVmq> fsObjectsForMq = new HashMap<>();
				for(FahrStreifen fs : mq.getFahrStreifen()) {
					SystemObject systemObject = fs.getSystemObject();
					AggregationsFsOderVmq aggregationsFsOderVmq = fsUndVmq.get(systemObject);
					assert aggregationsFsOderVmq != null;
					fsObjectsForMq.put(systemObject, aggregationsFsOderVmq);
				}
				messQuerschnitte.put(mqObjekt, new AggregationsMessQuerschnitt(this.verbindung, fsObjectsForMq, mq.getSystemObject()));
			}
		}
		
		for(SystemObject vmqObject : vmqObjects) {
			fsUndVmq.put(vmqObject, new AggregationsFsOderVmq(this.verbindung, vmqObject));
		}
	}

	/**
	 * Startet diese Applikation.
	 * 
	 * @param argumente
	 *            Argumente der Kommandozeile
	 */
	public static void main(final String[] argumente) {
		StandardApplicationRunner.run(new AggregationLVE(), argumente);
	}

	@Override
	public SWETyp getSWETyp() {
		return SWETyp.SWE_AGGREGATION_LVE;
	}

	@Override
	public void update(final ResultData[] results) {
		// Die Daten werden von den einzelnen Objekten selbst angemeldet, dies ist nur für Testfälle
		for(ResultData result : results) {
			AggregationsFsOderVmq aggregationsFsOderVmq = fsUndVmq.get(result.getObject());
			if(aggregationsFsOderVmq != null){
				aggregationsFsOderVmq.update(result);
			}	
		}
	}
}
