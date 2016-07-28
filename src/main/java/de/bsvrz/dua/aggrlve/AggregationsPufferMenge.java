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

import de.bsvrz.dav.daf.main.ClientDavInterface;
import de.bsvrz.dav.daf.main.config.SystemObject;
import de.bsvrz.sys.funclib.debug.Debug;

import java.util.*;

/**
 * Speichert alle historischen Daten eines Aggregationsobjektes aller
 * Aggregationsintervalle.
 * 
 * @author BitCtrl Systems GmbH, Thierfelder
 */
public class AggregationsPufferMenge {
	/**
	 * Menge aller Puffer mit Aggregationsdaten (indiziert nach Erfassungsintervall (Millisekunden)).
	 */
	private final NavigableMap<Long, AbstraktAggregationsPuffer> pufferMenge = new TreeMap<>();

	private final ClientDavInterface _dav;
	private final SystemObject _obj;

	private long _erfassungsIntervall = 0;
	
	private static final Debug _debug = Debug.getLogger();

	/**
	 * Standardkonstruktor.
	 * 
	 * @param dav Verbindung zum Datenverteiler
	 * @param obj Systemobjekt, das gepuffert wird
	 */
	public AggregationsPufferMenge(final ClientDavInterface dav,
			final SystemObject obj) {
		_dav = dav;
		_obj = obj;
	}

	/**
	 * Aktualisiert die Puffer zur Speicherung der aggregierten Daten wenn sich das Erfassungsintervall ändert. 
	 * @param erfassungsIntervall Neues Erfassungsintervall
	 */
	protected void updatePuffer(final long erfassungsIntervall){
		// Wenn sich nichts ändert, nichts tun
		if(erfassungsIntervall <= 0 || erfassungsIntervall == _erfassungsIntervall) return;

		// Aktuelles Analyse-Intervall löschen
		this.pufferMenge.remove(_erfassungsIntervall);
		
		// Neues Erfassungsintervall setzen
		_erfassungsIntervall = erfassungsIntervall;
		
		// Alle Puffer löschen, die vor dem neuen Erfassungsintervall liegen (z.B. Erfassungsintervall 2 Minuten: Puffer für 1 Minute löschen)
		for(Iterator<Long> iterator = this.pufferMenge.keySet().iterator(); iterator.hasNext(); ) {
			final Long aggregationsIntervall = iterator.next();
			if(aggregationsIntervall <= erfassungsIntervall) {
				iterator.remove();
			}
		}
		
		// Neuen Puffer für neues Erfassungsintervall einfügen
		this.pufferMenge.put(erfassungsIntervall, new AnalyseAggregationsPuffer(_dav, _obj));
		
		// Falls notwendig weitere fehlende Puffer ergänzen. Puffer bleiben nach Möglichkeit bestehen, sofern sich nichts geändert hat
		// D.h. falls sich das Erfassungsintervall bspw. von 15 Sek. auf 1 Min ändert bleibt der 5-Minuten-Puffer und alle weiteren längeren Puffer stehen.
		for(final AggregationsIntervall intervall : AggregationsIntervall.getInstanzen()) {
			if(intervall.getIntervall() > erfassungsIntervall) {
				if(this.pufferMenge.containsKey(intervall.getIntervall())) break;
				
				if(intervall.isDTVorTV() || intervall.equals(AggregationsIntervall.aGG60MINUTE)) {
					this.pufferMenge.put(intervall.getIntervall(), new ArchivAggregationsPuffer(_dav, _obj, intervall));
				}
				else {
					this.pufferMenge.put(intervall.getIntervall(), new AggregationsPuffer(_dav, _obj, intervall));
				}
			}
		}
	}

	/**
	 * Aktualisiert diese Menge von Aggregationspuffern mit neuen Daten. Alte
	 * Daten werden dabei ggf. aus dem betroffenen Puffer gelöscht
	 *
	 * @param datum Zu aggregierendes Datum
	 * @param isAnalyse Handelt es sich um einen Analysewert (Eingangsdatum), falls ja wird ggf. das Erfassungsintervall und
	 *                  die zu berechnenden Aggregationsstufen aktualisiert.   
	 */
	public void aktualisiere(final AggregationsDatum datum, boolean isAnalyse) {
		long erfassungsIntervall = datum.getT();
		if(erfassungsIntervall <= 0) {
			erfassungsIntervall = _erfassungsIntervall;
		}
		if(isAnalyse) {
			updatePuffer(erfassungsIntervall);
		}
		final AbstraktAggregationsPuffer puffer = this.pufferMenge.get(erfassungsIntervall);
		if (puffer != null) {
			if(!isAnalyse && puffer instanceof AnalyseAggregationsPuffer){
				// Verhindert, dass z.B. bei einem Erfassungsintervall von 1 Minute die 
				// (eigentlich relativ redundanten) Daten der Aggregationsstufe 1 Minute
				// nochmal zusätzlich in den Analysepuffer geschrieben werden  
				return;
			}
			puffer.aktualisiere(datum);
		} else {
			Debug.getLogger().fine("Puffer fuer Erfassungsintervall " + erfassungsIntervall + " existiert nicht");
		}
	}

	/**
	 * Erfragt alle in dieser Puffermenge gespeicherten Datensaetze <b>eines</b>
	 * Unterpuffers, deren Zeitstempel im Intervall [begin, ende[ liegen und
	 * deren Erfassungs- bzw. Aggregationsintervall kleiner dem uebergebenen
	 * Aggregationsintervall ist<br>
	 * 
	 * @param begin
	 *            Begin des Intervalls
	 * @param ende
	 *            Ende des Intervalls
	 * @param aggregationsIntervall
	 *            das Intervall, fuer dessen Aggregation Daten gesucht werden
	 * @return alle in diesem Puffer gespeicherten Datensaetze deren Zeitstempel
	 *         im Intervall [begin, ende[ liegen (bzw. eine leere Liste)
	 */
	public final Collection<AggregationsDatum> getDatenFuerZeitraum(
			final long begin, final long ende,
			final AggregationsIntervall aggregationsIntervall) {
		Collection<AggregationsDatum> daten;

		AbstraktAggregationsPuffer puffer = null;
		
		Map.Entry<Long, AbstraktAggregationsPuffer> pufferEntry = pufferMenge.lowerEntry(aggregationsIntervall.getIntervall());
		
		if(pufferEntry == null){
			// Notfalls das selbe Intervall nehmen
			pufferEntry = pufferMenge.floorEntry(aggregationsIntervall.getIntervall());
		}
		
		if(pufferEntry != null){
			puffer = pufferEntry.getValue();
		}

		if(puffer == null){
			_debug.warning("Keine Daten gepuffert für " + _obj + " Intervall " + aggregationsIntervall);
			return Collections.emptyList();
		}
		
		daten = puffer.getDatenFuerZeitraum(begin, ende);

		return daten;
	}

	/**
	 * Erfragt den Datenpuffer fuer Daten des uebergebenen
	 * Aggregationsintervalls.
	 * 
	 * @param intervall
	 *            ein Aggregationsintervall (<code>null</code> erfragt den
	 *            Datenpuffer fuer messwertersetzte Fahrstreifendaten)
	 * @return den Datenpuffer fuer Daten des uebergebenen
	 *         Aggregationsintervalls
	 */
	public final AbstraktAggregationsPuffer getPuffer(
			final AggregationsIntervall intervall) {
		AbstraktAggregationsPuffer puffer;
		if(intervall == null){
			puffer = this.pufferMenge.firstEntry().getValue();
		}
		else {
			puffer = this.pufferMenge.get(intervall.getIntervall());
		}
		
		return puffer;
	}
}
