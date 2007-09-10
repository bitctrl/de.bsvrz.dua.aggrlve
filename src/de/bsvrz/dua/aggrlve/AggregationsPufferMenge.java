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
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;
import de.bsvrz.sys.funclib.debug.Debug;

/**
 * Speichert alle historischen Daten eines Aggregationsobjektes aller
 * Aggregationsintervalle
 * 
 * @author BitCtrl Systems GmbH, Thierfelder
 *
 */
public class AggregationsPufferMenge {
	
	/**
	 * Debug-Logger
	 */
	private static final Debug LOGGER = Debug.getLogger();
	
	/**
	 * Aspekt der messwertersetzten Fahrstreifendaten
	 */
	private static Aspect MWE = null;
	
	/**
	 * Alle Aspekte, deren Daten in diesem Objekt gespeichert werden in aufsteigender
	 * Reihenfolge
	 */
	private static Aspect[] ASPEKTE_SORTIERT = null;
	
	/**
	 * Menge aller Puffer mit Aggregationsdaten (vom Aspekt aus betrachtet)
	 */
	private Map<Aspect, AbstraktAggregationsPuffer> pufferMenge = new HashMap<Aspect, AbstraktAggregationsPuffer>(); 
	
	
	/**
	 * Standardkonstruktor
	 * 
	 * @param dav Verbindung zum Datenverteiler
	 * @param obj das Objekt, dessen Daten gepuffert werden sollen
	 * @throws DUAInitialisierungsException wenn dieses Objekt nicht
	 * vollstaendig initialisiert werden konnte
	 */
	public AggregationsPufferMenge(final ClientDavInterface dav,
								   final SystemObject obj)
	throws DUAInitialisierungsException{
		if(MWE == null){
			MWE = dav.getDataModel().getAspect(DUAKonstanten.ASP_MESSWERTERSETZUNG);
			ASPEKTE_SORTIERT = new Aspect[AggregationsIntervall.getInstanzen().size() + 1];
			ASPEKTE_SORTIERT[0] = MWE;
			int i = 1;
			for(AggregationsIntervall intervall:AggregationsIntervall.getInstanzen()){
				ASPEKTE_SORTIERT[i++] = intervall.getAspekt();
			}
		}
		
		if(obj.getType().getPid().equals(DUAKonstanten.TYP_FAHRSTREIFEN)){
			this.pufferMenge.put(
					MWE,
					new MweAggregationsPuffer(dav, obj));			
		}
		for(AggregationsIntervall intervall:AggregationsIntervall.getInstanzen()){
			this.pufferMenge.put(intervall.getAspekt(), new AggregationsPuffer(dav, obj, intervall));
		}
	}
	
	
	/**
	 * Aktualisiert diese Menge von Aggregationspuffern mit neuen Daten. Alte
	 * Daten werden dabei ggf. aus dem betroffenen Puffer gelöscht
	 * 
	 * @param resultat ein aktuelles Datum mit Aggregations- oder messwertersetzten
	 * Fahrstreifendaten
	 */
	public void aktualisiere(ResultData resultat){
		if(resultat.getData() != null){
			AbstraktAggregationsPuffer puffer = this.pufferMenge.get(resultat.getDataDescription().getAspect());
			if(puffer != null){
				puffer.aktualisiere(resultat);
			}else{
				LOGGER.error("Puffer fuer Objekt " + resultat.getObject() + " und Aspekt " + //$NON-NLS-1$ //$NON-NLS-2$
						resultat.getDataDescription().getAspect() + " existiert nicht"); //$NON-NLS-1$
			}
		}
	}
	
	
	/**
	 * Erfragt alle in dieser Puffermenge gespeicherten Datensaetze <b>eines Unterpuffers</b>,
	 * deren Zeitstempel im Intervall [begin, ende[ liegen und deren Erfassungs- bzw.
	 * Aggregationsintervall kleiner gleich dem uebergebenen Ausgangsintervall ist
	 * 
	 * @param begin Begin des Intervalls
	 * @param ende Ende des Intervalls
	 * @param ausgangsIntervall das Intervall, von dem aus nach Daten gesucht wird
	 * (<code>null</code> steht fuer messwertersetzte Fahrstreifendaten)
	 * @return alle in diesem Puffer gespeicherten Datensaetze deren Zeitstempel
	 * im Intervall [begin, ende[ liegen (bzw. eine leere Liste)
	 */
	public final Collection<AggregationsDatum> getDatenFuerZeitraum(final long begin, 
																	final long ende,
																	final AggregationsIntervall ausgangsIntervall){
		Collection<AggregationsDatum> daten = new ArrayList<AggregationsDatum>();
		
		if(ausgangsIntervall == null){
			/**
			 * suche messwertersetzte Daten
			 */
			daten = this.pufferMenge.get(MWE).getDatenFuerZeitraum(begin, ende);
		}else{
			int start = 0; 
			for(int i = 0; i<ASPEKTE_SORTIERT.length; i++){
				if(ASPEKTE_SORTIERT[i].equals(ausgangsIntervall.getAspekt())){
					start = i;
				}
			}
			
			for(int i = start; i>=0; i--){
				AbstraktAggregationsPuffer puffer = this.pufferMenge.get(ASPEKTE_SORTIERT[i]);
				if(puffer != null){
					daten = puffer.getDatenFuerZeitraum(begin, ende);
					if(!daten.isEmpty()){
						break;
					}
				}
			}
		}
		
		return daten;
	}
	
}
