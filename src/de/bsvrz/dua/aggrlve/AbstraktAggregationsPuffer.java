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
import de.bsvrz.dav.daf.main.DataDescription;
import de.bsvrz.dav.daf.main.Dataset;
import de.bsvrz.dav.daf.main.archive.ArchiveData;
import de.bsvrz.dav.daf.main.archive.ArchiveDataKind;
import de.bsvrz.dav.daf.main.archive.ArchiveDataKindCombination;
import de.bsvrz.dav.daf.main.archive.ArchiveDataQueryResult;
import de.bsvrz.dav.daf.main.archive.ArchiveDataSpecification;
import de.bsvrz.dav.daf.main.archive.ArchiveDataStream;
import de.bsvrz.dav.daf.main.archive.ArchiveOrder;
import de.bsvrz.dav.daf.main.archive.ArchiveQueryPriority;
import de.bsvrz.dav.daf.main.archive.ArchiveRequestManager;
import de.bsvrz.dav.daf.main.archive.ArchiveRequestOption;
import de.bsvrz.dav.daf.main.archive.ArchiveTimeSpecification;
import de.bsvrz.dav.daf.main.archive.TimingType;
import de.bsvrz.dav.daf.main.config.SystemObject;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAInitialisierungsException;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;
import de.bsvrz.sys.funclib.bitctrl.konstante.Konstante;
import de.bsvrz.sys.funclib.debug.Debug;

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
	 * Debug-Logger
	 */
	private static final Debug LOGGER = Debug.getLogger();
	
	/**
	 * Verbindung zum Datenverteiler
	 */
	private static ClientDavInterface DAV = null;
	
	/**
	 * Archiv-Manager
	 */
	private static ArchiveRequestManager ARCHIV = null;
	
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
			ARCHIV = DAV.getArchive();
		}
		this.aggregationsIntervall = intervall;
		
		if(ARCHIV != null && ARCHIV.isArchiveAvailable()){
			final long jetzt = System.currentTimeMillis();
			long beginArchivAnfrage = -1;
			long endeArchivAnfrage = jetzt;
						
			if(intervall.equals(AggregationsIntervall.AGG_60MINUTE)){
				/**
				 * Zum Start der Applikation sollen moeglichst die Datensaetze
				 * der letzten 24 Stunden bereitstehen
				 */
				beginArchivAnfrage = jetzt - Konstante.STUNDE_IN_MS * intervall.getMaxPufferGroesse();
			}else
			if(intervall.equals(AggregationsIntervall.AGG_DTV_TAG)){
				/**
				 * Zum Start der Applikation sollen moeglichst die Datensaetze
				 * der letzten 50 Tage bereitstehen
				 */
				beginArchivAnfrage = jetzt - Konstante.STUNDE_IN_MS * 24 * intervall.getMaxPufferGroesse();
			}else
			if(intervall.equals(AggregationsIntervall.AGG_DTV_MONAT)){
				/**
				 * Zum Start der Applikation sollen moeglichst die Datensaetze
				 * der letzten 15 Monate bereitstehen
				 */
				beginArchivAnfrage = jetzt - Konstante.STUNDE_IN_MS * 24 * 30 * intervall.getMaxPufferGroesse();
			}

			if(beginArchivAnfrage > 0){
				DataDescription datenBeschreibung = new DataDescription(
						DAV.getDataModel().getAttributeGroup(DUAKonstanten.ATG_KURZZEIT_FS),
						intervall.getAspekt(),
						(short)0);
				
				ArchiveTimeSpecification zeit = new ArchiveTimeSpecification(
						TimingType.DATA_TIME, false, beginArchivAnfrage, endeArchivAnfrage);
	
				ArchiveDataSpecification archivDatenBeschreibung = 
					new ArchiveDataSpecification(zeit, 
							new ArchiveDataKindCombination(ArchiveDataKind.ONLINE),
							ArchiveOrder.BY_DATA_TIME, 
							ArchiveRequestOption.NORMAL,
							datenBeschreibung, 
							obj);
							
				try{
					ArchiveDataQueryResult result = ARCHIV.request(ArchiveQueryPriority.MEDIUM, archivDatenBeschreibung);
					ArchiveDataStream[] streams = result.getStreams();
					for(ArchiveDataStream stream:streams){
						ArchiveData archiveDatum = stream.take();
						if(archiveDatum != null && archiveDatum.getData() != null){
							this.aktualisiere(archiveDatum);	
						} 
					}
				} catch (Exception e) {
					LOGGER.error(Konstante.LEERSTRING, e);
					e.printStackTrace();
				}
			}
		}
	}
	
	
	/**
	 * Aktualisiert diesen Puffer mit neuen Daten. Alte Daten werden dabei aus
	 * dem Puffer gelöscht
	 * 
	 * @param resultat ein aktuelles Datum dieses Aggregationsintervalls
	 */
	public void aktualisiere(Dataset resultat){
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
