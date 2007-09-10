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
import java.util.LinkedList;

import de.bsvrz.dav.daf.main.ClientDavInterface;
import de.bsvrz.dav.daf.main.ResultData;
import de.bsvrz.dav.daf.main.config.SystemObject;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAInitialisierungsException;

/**
 * Abstrakte Blaupause fuer einen Ringpuffer, der alle Daten
 * eines bestimmten Aggregationsintervalls speichert, die zur
 * Berechnung des naechstgroesseren Intervalls notwendig sind
 * 
 * @author BitCtrl Systems GmbH, Thierfelder
 *
 */
public abstract class AbstraktAggregationsPuffer{
	
	/**
	 * Verbindung zum Datenverteiler
	 */
	private static ClientDavInterface DAV = null;
	
	/**
	 * das Aggregationsintervall, fuer das Daten in diesem Puffer
	 * stehen (<code>null</code> deutet auf messwertersetzte Fahstreifenwerte hin)
	 */
	protected AggregationsIntervall aggregationsIntervall = null;
	
	/**
	 * Ringpuffer mit den zeitlich aktuellsten Daten
	 */
	protected LinkedList<AggregationsDatum> ringPuffer = new LinkedList<AggregationsDatum>(); 
		
	
	
	/**
	 * Standardkonstruktor
	 * 
	 * @param dav Verbindung zum Datenverteiler
	 * @param obj das Objekt, dessen Daten gepuffert werden sollen
	 * @param intervall das Aggregationsintervall, fuer das Daten in diesem Puffer
	 * stehen (<code>null</code> deutet auf messwertersetzte Fahstreifenwerte hin)
	 * @throws DUAInitialisierungsException wenn dieses Objekt nicht
	 * vollstaendig initialisiert werden konnte
	 */
	public AbstraktAggregationsPuffer(final ClientDavInterface dav,
									  final SystemObject obj,
									  final AggregationsIntervall intervall)
	throws DUAInitialisierungsException{
		if(DAV == null){
			DAV = dav;
		}
		this.aggregationsIntervall = intervall; 
	}
	
	
	/**
	 * Aktualisiert diesen Puffer mit neuen Daten. Alte Daten werden dabei aus
	 * dem Puffer gelöscht
	 * 
	 * @param resultat ein aktuelles Datum dieses Aggregationsintervalls
	 */
	public void aktualisiere(ResultData resultat){
		if(resultat.getData() != null){
			AggregationsDatum neuesDatum = new AggregationsDatum(resultat);
			synchronized (this) {
				this.ringPuffer.addFirst(neuesDatum);
				while(this.ringPuffer.size() > this.getMaxPufferInhalt())
					this.ringPuffer.removeLast();
			}
		}
	}
	
	
	/**
	 * Erfragt alle in diesem Puffer gespeicherten Datensaetze deren Zeitstempel
	 * im Intervall [begin, ende[ liegen
	 * 
	 * @param begin Begin des Intervalls
	 * @param ende Ende des Intervalls
	 * @return alle in diesem Puffer gespeicherten Datensaetze deren Zeitstempel
	 * im Intervall [begin, ende[ liegen (bzw. eine leere Liste)
	 */
	public final Collection<AggregationsDatum> getDatenFuerZeitraum(final long begin, 
																	final long ende){
		Collection<AggregationsDatum> daten = new ArrayList<AggregationsDatum>();
		
		synchronized (this) {
			for(AggregationsDatum einzelDatum:this.ringPuffer){
				if(einzelDatum.getDatenZeit() >= begin && 
				   einzelDatum.getDatenZeit() < ende){
					daten.add( (AggregationsDatum)einzelDatum.clone() );
				}
			}			
		}
		
		return daten;
	}
	
	
	/**
	 * Erfragt die maximale Anzahl der Elemente, die fuer diesen Puffer zugelassen sind
	 *  
	 * @return die maximale Anzahl der Elemente, die fuer diesen Puffer zugelassen sind
	 */
	protected abstract long getMaxPufferInhalt();


	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		String s = "Datenart: " + (this.aggregationsIntervall == null?//$NON-NLS-1$
				"FS-MWE":this.aggregationsIntervall);//$NON-NLS-1$
		
		s += "\nMAX: " + this.getMaxPufferInhalt() + "\nInhalt: " + //$NON-NLS-1$//$NON-NLS-2$
					(this.ringPuffer.isEmpty()?"leer\n":"\n");//$NON-NLS-1$//$NON-NLS-2$
		for(AggregationsDatum datum:this.ringPuffer){
			s += datum + "\n"; //$NON-NLS-1$
		}
		
		return s;
	}

}
