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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import de.bsvrz.dav.daf.main.ClientDavInterface;
import de.bsvrz.dav.daf.main.ResultData;
import de.bsvrz.dav.daf.main.config.Aspect;
import de.bsvrz.dav.daf.main.config.SystemObject;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAInitialisierungsException;
import de.bsvrz.sys.funclib.debug.Debug;

/**
 * Speichert alle historischen Daten eines Aggregationsobjektes aller
 * Aggregationsintervalle.
 * 
 * @author BitCtrl Systems GmbH, Thierfelder
 * 
 * @version $Id$
 */
public class AggregationsPufferMenge {

	/**
	 * Alle Aspekte, deren Daten in diesem Objekt gespeichert werden in
	 * aufsteigender Reihenfolge.
	 */
	private static Aspect[] aspekteSortiert = null;

	/**
	 * Menge aller Puffer mit Aggregationsdaten (vom Aspekt aus betrachtet).
	 */
	private Map<Aspect, AbstraktAggregationsPuffer> pufferMenge = new HashMap<Aspect, AbstraktAggregationsPuffer>();

	/**
	 * Standardkonstruktor.
	 * 
	 * @param dav
	 *            Verbindung zum Datenverteiler
	 * @param obj
	 *            das Objekt, dessen Daten gepuffert werden sollen
	 * @throws DUAInitialisierungsException
	 *             wenn dieses Objekt nicht vollstaendig initialisiert werden
	 *             konnte
	 */
	public AggregationsPufferMenge(final ClientDavInterface dav,
			final SystemObject obj) throws DUAInitialisierungsException {
		if (aspekteSortiert == null) {
			aspekteSortiert = new Aspect[AggregationsIntervall.getInstanzen()
					.size() + 1];
			aspekteSortiert[0] = AggregationLVE.mwe;
			int i = 1;
			for (AggregationsIntervall intervall : AggregationsIntervall
					.getInstanzen()) {
				aspekteSortiert[i++] = intervall.getAspekt();
			}
		}

		this.pufferMenge.put(AggregationLVE.mwe, new MweAggregationsPuffer(dav,
				obj));
		for (AggregationsIntervall intervall : AggregationsIntervall
				.getInstanzen()) {
			if (intervall.equals(AggregationsIntervall.aGG60MINUTE)
					|| intervall.equals(AggregationsIntervall.aGGDTVTAG)
					|| intervall.equals(AggregationsIntervall.aGGDTVMONAT)
					|| intervall.equals(AggregationsIntervall.aGGDTVJAHR)) {
				if (!obj.getType().equals(AggregationLVE.typFahrstreifen)) {
					this.pufferMenge.put(intervall.getAspekt(),
							new DTVAggregationsPuffer(dav, obj, intervall));
				}
			} else {
				this.pufferMenge.put(intervall.getAspekt(),
						new AggregationsPuffer(dav, obj, intervall));
			}
		}
	}

	/**
	 * Aktualisiert diese Menge von Aggregationspuffern mit neuen Daten. Alte
	 * Daten werden dabei ggf. aus dem betroffenen Puffer gelöscht
	 * 
	 * @param resultat
	 *            ein aktuelles Datum mit Aggregations- oder messwertersetzten
	 *            Fahrstreifendaten
	 */
	public void aktualisiere(ResultData resultat) {
		AbstraktAggregationsPuffer puffer = this.pufferMenge.get(resultat
				.getDataDescription().getAspect());
		if (puffer != null) {
			puffer.aktualisiere(resultat);
		} else {
			Debug
					.getLogger()
					.error(
							"Puffer fuer Objekt " + resultat.getObject() + " und Aspekt " + //$NON-NLS-1$ //$NON-NLS-2$
									resultat.getDataDescription().getAspect()
									+ " existiert nicht"); //$NON-NLS-1$
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
		Collection<AggregationsDatum> daten = new ArrayList<AggregationsDatum>();

		AggregationsIntervall ausgangsIntervall = aggregationsIntervall
				.getVorgaenger();
		if (ausgangsIntervall == null) {
			daten = this.pufferMenge.get(AggregationLVE.mwe)
					.getDatenFuerZeitraum(begin, ende);
		} else {
			int start = 0;
			for (int i = 0; i < aspekteSortiert.length; i++) {
				if (aspekteSortiert[i].equals(ausgangsIntervall
						.getDatenBeschreibung(true).getAspect())) {
					start = i;
				}
			}

			for (int i = start; i >= 0; i--) {
				AbstraktAggregationsPuffer puffer = this.pufferMenge
						.get(aspekteSortiert[i]);
				if (puffer != null) {
					daten = puffer.getDatenFuerZeitraum(begin, ende);
					if (aggregationsIntervall.isDTVorTV() || !daten.isEmpty()) {
						break;
					}
				}
			}
		}

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
		if (intervall == null) {
			return this.pufferMenge.get(AggregationLVE.mwe);
		}
		return this.pufferMenge.get(intervall.getDatenBeschreibung(true)
				.getAspect());
	}
}
