package de.bsvrz.dua.aggrlve;

import java.util.HashMap;
import java.util.Map;

import de.bsvrz.dav.daf.main.Dataset;

/**
 * Enthaelt alle Informationen, die mit einem <code>ResultData</code>
 * der Attributgruppe <code>atg.verkehrsDatenKurzZeitIntervall</code> bzw. 
 * <code>atg.verkehrsDatenKurzZeitFs</code> oder <code>atg.verkehrsDatenKurzZeitMq</code>
 * in den Attrbiuten <code>qPkw</code>, <code>qLkw</code>, <code>qKfz</code> und
 *  <code>vLkw</code>, <code>vKfz</code>, <code>vPkw</code> enthalten sind
 * (inkl. Zeitstempel) 
 * 
 * @author BitCtrl Systems GmbH, Thierfelder
 *
 */
public class AggregationsDatum 
implements Comparable<AggregationsDatum>, Cloneable{

	/**
	 * die Datenzeit dieses Datums
	 */
	private long datenZeit = -1;
	
	/**
	 * Erfassungs- bzw. Aggregationsintervall dieses Datensatzes
	 */
	private long T = -1;

	/**
	 * die Werte aller innerhalb der Messwertaggregation betrachteten Attribute
	 */
	private Map<AggregationsAttribut, AggregationsAttributWert> werte = 
						new HashMap<AggregationsAttribut, AggregationsAttributWert>();
	
	
	/**
	 * Standardkonstruktor
	 */
	private AggregationsDatum(){
		//
	}
	
	
	/**
	 * Standardkonstruktor
	 * 
	 * @param resultat ein <code>ResultData</code>-Objekt eines messwertersetzten
	 * Fahrstreifendatums bzw. eines Aggregationsdatums fuer Fahrstreifen bzw. Messquerschnitte<br>
	 * <b>Achtung:</b> Argument muss <code>null</code> sein und Nutzdaten besitzen
	 */
	public AggregationsDatum(final Dataset resultat){
		this.datenZeit = resultat.getDataTime();
		if(resultat.getObject().isOfType(AggregationLVE.TYP_FAHRSTREIFEN)){
			this.T = resultat.getData().getTimeValue("T").getMillis(); //$NON-NLS-1$
		}else{
			for(AggregationsIntervall intervall:AggregationsIntervall.getInstanzen()){
				if(intervall.getAspekt().equals(resultat.getDataDescription().getAspect())){
					this.T = intervall.getIntervall();
				}
			}
		}
		for(AggregationsAttribut attribut:AggregationsAttribut.getInstanzen()){
			this.werte.put(attribut, new AggregationsAttributWert(attribut, resultat));
		}
	}
	
	
	/**
	 * {@inheritDoc}
	 */
	public AggregationsDatum clone(){
		AggregationsDatum kopie = new AggregationsDatum();
		
		kopie.datenZeit = this.datenZeit;
		kopie.T = this.T;
		for(AggregationsAttribut attribut:AggregationsAttribut.getInstanzen()){
			kopie.werte.put(attribut, this.getWert(attribut).clone());
		}
				
		return kopie;
	}

	
	/**
	 * Erfragt den Wert eines Attributs
	 * 
	 * @return der Wert eines Attributs
	 */
	public final AggregationsAttributWert getWert(final AggregationsAttribut attribut){
		return this.werte.get(attribut);
	}
	

	/**
	 * {@inheritDoc}
	 */
	public int compareTo(AggregationsDatum that) {
		return new Long(this.datenZeit).compareTo(that.datenZeit);
	}

	
	/**
	 * Erfragt die Datenzeit dieses Datums
	 * 
	 * @return die Datenzeit dieses Datums
	 */
	public final long getDatenZeit(){
		return this.datenZeit;
	}
	
	
	/**
	 * Erfragt das Erfassungs- bzw. Aggregationsintervall dieses Datensatzes
	 * 
	 * @return das Erfassungs- bzw. Aggregationsintervall dieses Datensatzes
	 */
	public final long getT(){
		return this.T;
	}
	
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(Object obj) {
		return super.equals(obj);
	}

	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return super.toString();
	}
	
}
