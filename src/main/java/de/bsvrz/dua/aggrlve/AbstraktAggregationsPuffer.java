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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;

/**
 * Abstrakte Blaupause fuer einen Ringpuffer, der alle Daten eines bestimmten
 * Aggregationsintervalls speichert, die zur Berechnung des naechstgroesseren
 * Intervalls notwendig sind.
 * 
 * @author BitCtrl Systems GmbH, Thierfelder
 * 
 */
public abstract class AbstraktAggregationsPuffer {

	/**
	 * Verbindung zum Datenverteiler.
	 */
	protected ClientDavInterface dav;

	/**
	 * Ringpuffer mit den zeitlich aktuellsten Daten.
	 */
	protected final LinkedList<AggregationsDatum> ringPuffer = new LinkedList<>();

	/**
	 * das Systemobjekt, dessen Daten hier gespeichert werden.
	 */
	protected SystemObject objekt;

	/**
	 * Standardkonstruktor.
	 * 
	 * @param dav
	 *            Verbindung zum Datenverteiler
	 * @param obj
	 *            das Objekt, dessen Daten gepuffert werden sollen
	 */
	public AbstraktAggregationsPuffer(final ClientDavInterface dav,
			final SystemObject obj) {
		this.dav = dav;
		this.objekt = obj;
	}

	/**
	 * Aktualisiert diesen Puffer mit neuen Daten. Alte Daten werden dabei aus
	 * dem Puffer gelöscht
	 *
	 * @param datum Neues Datum
	 */
	public void aktualisiere(final AggregationsDatum datum) {
		synchronized (ringPuffer) {
			this.ringPuffer.addFirst(datum);
			while (this.ringPuffer.size() > this.getMaxPufferInhalt()) {
				if (!this.ringPuffer.isEmpty()) {
					this.ringPuffer.removeLast();
				}
			}
		}
	}

	/**
	 * Erfragt alle in diesem Puffer gespeicherten Datensaetze deren Zeitstempel
	 * im Intervall [begin, ende[ liegen.
	 * 
	 * @param begin
	 *            Begin des Intervalls
	 * @param ende
	 *            Ende des Intervalls
	 * @return alle in diesem Puffer gespeicherten Datensaetze deren Zeitstempel
	 *         im Intervall [begin, ende[ liegen (bzw. eine leere Liste)
	 */
	public final Collection<AggregationsDatum> getDatenFuerZeitraum(
			final long begin, final long ende) {
		final Collection<AggregationsDatum> daten = new ArrayList<>();

		synchronized (ringPuffer) {
			for (final AggregationsDatum einzelDatum : this.ringPuffer) {
				if ((einzelDatum.getDatenZeit() >= begin)
						&& (einzelDatum.getDatenZeit() < ende)) {
					daten.add(einzelDatum.clone());
				}
			}
		}

		return daten;
	}

	/**
	 * Erfragt die maximale Anzahl der Elemente, die fuer diesen Puffer
	 * zugelassen sind.
	 * 
	 * @return die maximale Anzahl der Elemente, die fuer diesen Puffer
	 *         zugelassen sind
	 */
	protected abstract long getMaxPufferInhalt();

	@Override
	public String toString() {
		return objekt + " " +  ringPuffer;
	}

	/**
	 * ermittelt, ob der Ringpuffer der Aggregationsdaten leer ist.
	 * 
	 * @return den Zustand
	 */
	protected boolean ringPufferisEmpty() {
		boolean result = false;
		synchronized (ringPuffer) {
			result = ringPuffer.isEmpty();
		}
		return result;
	}

	/**
	 * Gibt das zeitlich letze gespeicherte Datum zurück
	 * @return Das letzte (aktuellste) Datum
	 */
	public AggregationsDatum getLast() {
		return ringPuffer.peekFirst();
	}
}
