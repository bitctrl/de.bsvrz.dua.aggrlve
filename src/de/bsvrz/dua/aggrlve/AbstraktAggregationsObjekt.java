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
	 * Ausgefallene Werte werden hier durch den Mittelwert der vorhandenen Werte ersetzt. Um die
	 * Zuverlässigkeit der Daten nachvollziehen zu können, ist jeder aggregierte Wert mit
	 * einem Güteindex in % anzugeben. Der Güteindex wird durch arithmetische Mittelung der
	 * Güteindizes der zu aggregierenden Daten bestimmt. Der Güteindex von ausgefallenen
	 * Werten ergibt sich dabei aus dem Mittelwert der vorhandenen Werte multipliziert mit
	 * einem parametrierbaren Faktor. Des weiteren ist jeder aggregierte Wert mit einer
	 * Kennung zu versehen, ob zur Aggregation interpolierte (durch die Messwertersetzung
	 * generierte) Werte verwendet wurden. 
	 * 
	 * @param attribut das Attribut, fuer das Daten gesucht werden
	 * @param quellDaten die aus dem Puffer ausgelesenen Daten
	 * @param intervall das Aggregationsintervall, fuer dass Daten aus den uebergebenen
	 * Quelldaten errechnet werden sollen 
	 * @return so viele Datensaetze, wie fuer dieses Intervall zur Verfuegung stehen muessen
	 */
	protected final Collection<AggregationsAttributWert> ersetzteAusgefalleneWerte(
			AggregationsAttribut attribut,
			Collection<AggregationsDatum> quellDaten,
			AggregationsIntervall intervall){
		Collection<AggregationsAttributWert> zielDaten = new ArrayList<AggregationsAttributWert>();
		
		long anzahlSoll = -1;
		if(intervall.equals(AggregationsIntervall.AGG_1MINUTE) || 
		   intervall.equals(AggregationsIntervall.AGG_5MINUTE) ||
		   intervall.equals(AggregationsIntervall.AGG_15MINUTE) ||
		   intervall.equals(AggregationsIntervall.AGG_30MINUTE) ||
		   intervall.equals(AggregationsIntervall.AGG_60MINUTE)){
			anzahlSoll = intervall.getIntervall() / 
							quellDaten.iterator().next().getT();
		}else
		if(intervall.equals(AggregationsIntervall.AGG_DTV_TAG)){
			// TODO 
		}else
		if(intervall.equals(AggregationsIntervall.AGG_DTV_MONAT)){
			// TODO 
		}else
		if(intervall.equals(AggregationsIntervall.AGG_DTV_JAHR)){
			// TODO
		}
		
		long wertSumme = 0;
		long wertAnzahl = 0;
		double gueteSumme = 0;
		for(AggregationsDatum quellDatum:quellDaten){
			AggregationsAttributWert wert = quellDatum.getWert(attribut);
			if(wert.getWert() >= 0){
				gueteSumme += wert.getGuete().getIndexUnskaliert() >= 0?wert.getGuete().getIndex():0;
				wertSumme += wert.getWert();
				wertAnzahl++;
				zielDaten.add(wert);
			}
		}

		long mittelWert = 0;
		double mittelWertGuete = 0.0;
		if(wertAnzahl > 0){
			mittelWert = Math.round((double)wertSumme / (double)wertAnzahl);
			mittelWertGuete = (double)gueteSumme / (double)wertAnzahl;
		}

		if(anzahlSoll - zielDaten.size() > 0){
			for(int i = 0; i<anzahlSoll - zielDaten.size(); i++){
				if(wertAnzahl > 0){
					zielDaten.add(new AggregationsAttributWert(attribut, mittelWert, AggregationLVE.GUETE * mittelWertGuete));
				}else{
					zielDaten.add(new AggregationsAttributWert(attribut, -1, 0));
				}
			}
		}
		
		return zielDaten;
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
