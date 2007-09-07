package de.bsvrz.dua.aggrlve;

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

	private long datenZeit = -1;
	
	private long T = -1;

	
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Object clone(){
		Object a = null;
		try {
			a = super.clone();
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}
		return a;
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
