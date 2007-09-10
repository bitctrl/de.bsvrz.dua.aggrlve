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
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;
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
	 * die restlichen auszufuellenden Attribute der Attributgruppen
	 * <code>atg.verkehrsDatenKurzZeitFs</code> bzw <code>atg.verkehrsDatenKurzZeitMq</code>,
	 * die innerhalb der FG1-Aggregation nicht erfasst werden
	 */
	private static final String[][] REST_ATTRIBUTE = new String[][]{
		 	 new String[]{null, "BMax"},  //$NON-NLS-1$
			 new String[]{"vgKfz", "VgKfz"},  //$NON-NLS-1$//$NON-NLS-2$
			 new String[]{"sKfz", "SKfz"},  //$NON-NLS-1$//$NON-NLS-2$
			 new String[]{"b", "B"},  //$NON-NLS-1$//$NON-NLS-2$
			 new String[]{"aLkw", "ALkw"},  //$NON-NLS-1$//$NON-NLS-2$
			 new String[]{"kKfz", "KKfz"},  //$NON-NLS-1$//$NON-NLS-2$
			 new String[]{"kLkw", "KLkw"},  //$NON-NLS-1$//$NON-NLS-2$
			 new String[]{"kPkw", "KPkw"},  //$NON-NLS-1$//$NON-NLS-2$
			 new String[]{"qB", "QB"},  //$NON-NLS-1$//$NON-NLS-2$
			 new String[]{"kB", "KB"} //$NON-NLS-1$//$NON-NLS-2$
	};
	
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
	 * speichert alle historischen Daten dieses Aggregationsobjektes aller Aggregationsintervalle 
	 */
	protected AggregationsPufferMenge datenPuffer = null;
	
	
	
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
	 * Fuellt den Rest des Datensatzes (alle Werte ausser <code>qPkw</code>, <code>qLkw</code>,
	 *  <code>qKfz</code>, <code>vLkw</code>, <code>vKfz</code> und <code>vPkw</code>) mit Daten
	 *  
	 * @param ein zu versendendes Aggregationsdatum
	 */
	protected final void fuelleRest(ResultData resultat){
		boolean isFahrstreifen = resultat.getObject().isOfType(AggregationLVE.TYP_FAHRSTREIFEN);
		
		for(int i = 0; i<REST_ATTRIBUTE.length; i++){
			String attributName = REST_ATTRIBUTE[i][1];
			if(isFahrstreifen){
				attributName = REST_ATTRIBUTE[i][0];
			}
			
			if(attributName != null){
				resultat.getData().getItem(attributName).getUnscaledValue("Wert").set(DUAKonstanten.NICHT_ERMITTELBAR);  //$NON-NLS-1$
				resultat.getData().getItem(attributName).getItem("Status").getItem("Erfassung").  //$NON-NLS-1$//$NON-NLS-2$
												getUnscaledValue("NichtErfasst").set(DUAKonstanten.NEIN); //$NON-NLS-1$
				resultat.getData().getItem(attributName).getItem("Status").getItem("MessWertErsetzung").  //$NON-NLS-1$//$NON-NLS-2$
												getUnscaledValue("Implausibel").set(DUAKonstanten.NEIN); //$NON-NLS-1$
				resultat.getData().getItem(attributName).getItem("Status").getItem("MessWertErsetzung").  //$NON-NLS-1$//$NON-NLS-2$
												getUnscaledValue("Interpoliert").set(DUAKonstanten.NEIN); //$NON-NLS-1$

				resultat.getData().getItem(attributName).getItem("Status").getItem("PlFormal"). //$NON-NLS-1$ //$NON-NLS-2$
												getUnscaledValue("WertMax").set(DUAKonstanten.NEIN); //$NON-NLS-1$
				resultat.getData().getItem(attributName).getItem("Status").getItem("PlFormal"). //$NON-NLS-1$ //$NON-NLS-2$
												getUnscaledValue("WertMin").set(DUAKonstanten.NEIN); //$NON-NLS-1$

				resultat.getData().getItem(attributName).getItem("Status").getItem("PlLogisch"). //$NON-NLS-1$ //$NON-NLS-2$
												getUnscaledValue("WertMaxLogisch").set(DUAKonstanten.NEIN); //$NON-NLS-1$
				resultat.getData().getItem(attributName).getItem("Status").getItem("PlLogisch"). //$NON-NLS-1$ //$NON-NLS-2$
												getUnscaledValue("WertMinLogisch").set(DUAKonstanten.NEIN); //$NON-NLS-1$							
			}
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
