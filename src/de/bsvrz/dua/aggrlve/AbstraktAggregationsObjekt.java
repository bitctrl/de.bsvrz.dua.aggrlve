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

import java.util.HashMap;
import java.util.Map;

import de.bsvrz.dav.daf.main.ClientDavInterface;
import de.bsvrz.dav.daf.main.ResultData;
import de.bsvrz.dav.daf.main.SenderRole;
import de.bsvrz.dav.daf.main.config.SystemObject;
import de.bsvrz.sys.funclib.bitctrl.dua.av.DAVSendeAnmeldungsVerwaltung;
import de.bsvrz.sys.funclib.debug.Debug;

/**
 * Abstraktes Objekt zur Aggregation von LVE-Daten fuer Fahrstreifen 
 * und Messquerschnitte
 * 
 * @author BitCtrl Systems GmbH, Thierfelder
 *
 */
public abstract class AbstraktAggregationsObjekt {

	/**
	 * Debug-Logger
	 */
	protected static final Debug LOGGER = Debug.getLogger();
	
	/**
	 * statische Verbindung zum Datenverteiler
	 */
	protected static ClientDavInterface DAV = null;
	
	/**
	 * Datensender
	 */
	protected DAVSendeAnmeldungsVerwaltung sender = null;
	
	/**
	 * Mapt ein Systemobjekt auf sein letztes von hier aus publiziertes Datum
	 */
	protected Map<SystemObject, ResultData> letzteDaten = new HashMap<SystemObject, ResultData>();
	
	
	/**
	 * Standardkonstruktor
	 * 
	 * @param dav Verbindung zum Datenverteiler
	 */
	public AbstraktAggregationsObjekt(final ClientDavInterface dav){
		if(DAV == null){
			DAV = dav;
		}
		this.sender = new DAVSendeAnmeldungsVerwaltung(DAV, SenderRole.source());		
	}


	/**
	 * Sendet ein Datum (Sendet nie zwei Datensaetze ohne Nutzdaten hintereinander)
	 *  
	 * @param resultat ein Datum
	 */
	protected final void sende(final ResultData resultat){
		if(resultat.getData() == null){
			ResultData letztesDatum = this.letzteDaten.get(resultat.getObject());
			if(letztesDatum != null && letztesDatum.getData() != null){
				this.sender.sende(resultat);
				this.letzteDaten.put(resultat.getObject(), resultat);
			}
		}else{
			this.sender.sende(resultat);
			this.letzteDaten.put(resultat.getObject(), resultat);
		}	
	}
	
	
	/**
	 * Startet die Aggregation von Daten
	 * 
	 * @param zeitStempel der Zeitstempel, mit dem die aggregierten Daten veröffentlicht
	 * werden sollen
	 * @param intervall der Intervall der aggregierten Daten (auch der Publikationsaspekt)
	 */
	public abstract void aggregiere(final long zeitStempel,
									final AggregationsIntervall intervall);
	
}
