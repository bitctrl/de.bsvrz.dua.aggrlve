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

import de.bsvrz.dav.daf.main.Data;
import de.bsvrz.dav.daf.main.Dataset;
import de.bsvrz.dua.guete.GWert;
import de.bsvrz.dua.guete.GueteVerfahren;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAUtensilien;
import de.bsvrz.sys.funclib.bitctrl.dua.GanzZahl;
import de.bsvrz.sys.funclib.bitctrl.dua.MesswertMarkierung;

/**
 * Korrespondiert mit einem Attributwert eines messwertersetzten Fahrstreifendatums
 * bzw. eines Aggregationsdatums
 * 
 * @author BitCtrl Systems GmbH, Thierfelder
 *
 */
public class AggregationsAttributWert
extends MesswertMarkierung
implements Comparable<AggregationsAttributWert>, Cloneable{

	/**
	 * das Attribut
	 */
	private AggregationsAttribut attr = null;
	
	/**
	 * der Wert dieses Attributs
	 */
	private long wert = -4;
	
	/**
	 * die Guete
	 */
	private GWert guete = null;
	
	
	/**
	 * Standardkonstruktor
	 * 
	 * @param attr das Attribut
	 * @param resultat der Datensatz in dem der Attributwert steht
	 */
	public AggregationsAttributWert(final AggregationsAttribut attr,
									final Dataset resultat){
		if(attr == null){
			throw new NullPointerException("Attribut ist <<null>>"); //$NON-NLS-1$
		}
		Data datenSatz = resultat.getData();
		if(datenSatz == null){
			throw new NullPointerException("Datensatz ist <<null>>"); //$NON-NLS-1$
		}
	
		String attributName = attr.getAttributName(
						resultat.getObject().isOfType(AggregationLVE.TYP_FAHRSTREIFEN));

		this.attr = attr;
		this.wert = datenSatz.getItem(attributName).getUnscaledValue("Wert").longValue();  //$NON-NLS-1$
		this.nichtErfasst = datenSatz.getItem(attributName).getItem("Status").getItem("Erfassung").  //$NON-NLS-1$//$NON-NLS-2$
										getUnscaledValue("NichtErfasst").intValue() == DUAKonstanten.JA; //$NON-NLS-1$
		this.implausibel = datenSatz.getItem(attributName).getItem("Status").getItem("MessWertErsetzung").  //$NON-NLS-1$//$NON-NLS-2$
										getUnscaledValue("Implausibel").intValue() == DUAKonstanten.JA; //$NON-NLS-1$
		this.interpoliert = datenSatz.getItem(attributName).getItem("Status").getItem("MessWertErsetzung").  //$NON-NLS-1$//$NON-NLS-2$
										getUnscaledValue("Interpoliert").intValue() == DUAKonstanten.JA; //$NON-NLS-1$

		this.formalMax = datenSatz.getItem(attributName).getItem("Status").getItem("PlFormal"). //$NON-NLS-1$ //$NON-NLS-2$
										getUnscaledValue("WertMax").intValue() == DUAKonstanten.JA; //$NON-NLS-1$
		this.formalMin = datenSatz.getItem(attributName).getItem("Status").getItem("PlFormal"). //$NON-NLS-1$ //$NON-NLS-2$
										getUnscaledValue("WertMin").intValue() == DUAKonstanten.JA; //$NON-NLS-1$

		this.logischMax = datenSatz.getItem(attributName).getItem("Status").getItem("PlLogisch"). //$NON-NLS-1$ //$NON-NLS-2$
										getUnscaledValue("WertMaxLogisch").intValue() == DUAKonstanten.JA; //$NON-NLS-1$
		this.logischMin = datenSatz.getItem(attributName).getItem("Status").getItem("PlLogisch"). //$NON-NLS-1$ //$NON-NLS-2$
										getUnscaledValue("WertMinLogisch").intValue() == DUAKonstanten.JA; //$NON-NLS-1$

		this.guete = new GWert(datenSatz, attributName);
	}
	
	
	/**
	 * Konstruktor fuer Zwischenergebnisse
	 * 
	 * @param attribut das Attribut
	 * @param wert der Wert dieses Attributs
	 * @param guete die Guete
	 */
	public AggregationsAttributWert(AggregationsAttribut attribut, long wert, double guete){
		this.attr = attribut;
		this.wert = wert;
		GanzZahl gueteGanzZahl = GanzZahl.getGueteIndex();
		gueteGanzZahl.setSkaliertenWert(guete);
		this.guete = new GWert(gueteGanzZahl, GueteVerfahren.STANDARD, false);
	}
	
	
	/**
	 * {@inheritDoc}
	 */
	public AggregationsAttributWert clone(){
		AggregationsAttributWert kopie = null;
		
		try {
			kopie = (AggregationsAttributWert)super.clone();
			kopie.guete = new GWert(this.guete);		
		} catch (CloneNotSupportedException e) {
			// wird nicht geworfen
		}
		
		return kopie;
	}
	
	
	/**
	 * Exportiert den Inhalt dieses Objektes in ein veraenderbares Nutzdatum
	 * der Attributgruppe <code>atg.verkehrsDatenKurzZeitFs</code> oder
	 * <code>atg.verkehrsDatenKurzZeitMq</code>
	 * 
	 * @param nutzDatum ein veraenderbare Instanz einer Attributgruppe
	 * <code>atg.verkehrsDatenKurzZeitFs</code> oder
	 * <code>atg.verkehrsDatenKurzZeitMq</code>
	 * @param isFahrstreifen ob es sich um ein Fahrstreifendatum handelt
	 */
	public final void exportiere(Data nutzDatum, boolean isFahrstreifen){
		
		String attributName = attr.getAttributName(isFahrstreifen);

		if(DUAUtensilien.isWertInWerteBereich(nutzDatum.getItem(attributName).getItem("Wert"), this.wert)){ //$NON-NLS-1$
			nutzDatum.getItem(attributName).getUnscaledValue("Wert").set(this.wert);  //$NON-NLS-1$			
		}else{
			nutzDatum.getItem(attributName).getUnscaledValue("Wert").set(DUAKonstanten.NICHT_ERMITTELBAR_BZW_FEHLERHAFT);  //$NON-NLS-1$
		}		

		nutzDatum.getItem(attributName).getItem("Status").getItem("Erfassung").  //$NON-NLS-1$//$NON-NLS-2$
										getUnscaledValue("NichtErfasst").set(this.isNichtErfasst()?DUAKonstanten.JA:DUAKonstanten.NEIN); //$NON-NLS-1$
		nutzDatum.getItem(attributName).getItem("Status").getItem("MessWertErsetzung").  //$NON-NLS-1$//$NON-NLS-2$
										getUnscaledValue("Implausibel").set(this.isImplausibel()?DUAKonstanten.JA:DUAKonstanten.NEIN); //$NON-NLS-1$
		nutzDatum.getItem(attributName).getItem("Status").getItem("MessWertErsetzung").  //$NON-NLS-1$//$NON-NLS-2$
										getUnscaledValue("Interpoliert").set(this.isInterpoliert()?DUAKonstanten.JA:DUAKonstanten.NEIN); //$NON-NLS-1$

		nutzDatum.getItem(attributName).getItem("Status").getItem("PlFormal"). //$NON-NLS-1$ //$NON-NLS-2$
										getUnscaledValue("WertMax").set(this.isFormalMax()?DUAKonstanten.JA:DUAKonstanten.NEIN); //$NON-NLS-1$
		nutzDatum.getItem(attributName).getItem("Status").getItem("PlFormal"). //$NON-NLS-1$ //$NON-NLS-2$
										getUnscaledValue("WertMin").set(this.isFormalMin()?DUAKonstanten.JA:DUAKonstanten.NEIN); //$NON-NLS-1$

		nutzDatum.getItem(attributName).getItem("Status").getItem("PlLogisch"). //$NON-NLS-1$ //$NON-NLS-2$
										getUnscaledValue("WertMaxLogisch").set(this.isLogischMax()?DUAKonstanten.JA:DUAKonstanten.NEIN); //$NON-NLS-1$
		nutzDatum.getItem(attributName).getItem("Status").getItem("PlLogisch"). //$NON-NLS-1$ //$NON-NLS-2$
										getUnscaledValue("WertMinLogisch").set(this.isLogischMin()?DUAKonstanten.JA:DUAKonstanten.NEIN); //$NON-NLS-1$
		
		this.guete.exportiere(nutzDatum, attributName);
	}
	
	
	/**
	 * Erfragt das Attribut
	 * 
	 * @return das Attribut
	 */
	public final AggregationsAttribut getAttribut(){
		return this.attr;
	}


	/**
	 * Setzt den Wert dieses Attributs
	 * 
	 * @param wert der Wert dieses Attributs
	 */
	public final void setWert(final long wert){
		this.veraendert = true;
		this.wert = wert;
	}

	
	/**
	 * Erfragt den Wert dieses Attributs
	 * 
	 * @return der Wert dieses Attributs
	 */
	public final long getWert(){
		return this.wert;
	}

	
	/**
	 * Erfragt die Guete dieses Attributwertes
	 * 
	 * @return die Guete dieses Attributwertes
	 */
	public final GWert getGuete(){
		return this.guete;
	}
	
	
	/**
	 * Setzte die Guete dieses Attributwertes
	 * 
	 * @param guete die Guete dieses Attributwertes
	 */
	public final void setGuete(final GWert guete){
		this.veraendert = true;
		this.guete = guete;
	}

	
	/**
	 * {@inheritDoc}
	 */
	public int compareTo(AggregationsAttributWert that) {
		return new Long(this.getWert()).compareTo(that.getWert());
	}
	

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(Object obj) {
		boolean ergebnis = false;
		
		if(obj != null && obj instanceof AggregationsAttributWert){
			AggregationsAttributWert that = (AggregationsAttributWert)obj;
			ergebnis = this.getAttribut().equals(that.getAttribut()) &&
					   this.getWert() == that.getWert() &&
					   this.isNichtErfasst() == that.isNichtErfasst() &&
					   this.isImplausibel() == that.isImplausibel() &&
					   this.isInterpoliert() == that.isInterpoliert() &&
					   this.getGuete().getIndex() - that.getGuete().getIndex() < 0.001;
		}
		
		return ergebnis;
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return "Attribut: " + this.attr + "\nWert: " + this.wert +   //$NON-NLS-1$ //$NON-NLS-2$
			   "\nGuete: " + this.guete +   //$NON-NLS-1$
			   "\nVeraendert: " + (this.veraendert?"Ja":"Nein") +  //$NON-NLS-1$ //$NON-NLS-2$//$NON-NLS-3$
			   "\n" + super.toString(); //$NON-NLS-1$
	}
	
}

