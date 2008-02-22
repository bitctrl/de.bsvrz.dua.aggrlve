/** 
 * Segment 4 Datenübernahme und Aufbereitung (DUA)
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

package de.bsvrz.dua.aggrlve.kalk;

import java.util.HashMap;
import java.util.Map;

import com.bitctrl.Constants;

import de.bsvrz.sys.funclib.bitctrl.dua.test.CSVImporter;

/**
 * Liesst komplett eine Tabelle ein, die aus (Wert, Status)-Paaren besteht. Ermoeglicht
 * so den wahrfreihen Zugriff auf alle Elemente der Tabelle
 *  
 * @author BitCtrl Systems GmbH, Thierfelder
 *
 */
public class PruefTabImporter {

	/**
	 * Tabelleninhalt
	 */
	private Map<String, Element[]> tabContent = new HashMap<String, Element[]>();
	
	
	/**
	 * Standardkonstruktor
	 * 
	 * @param csvDateiName Name der zu importierenden CSV-Datei
	 * @throws Exception wird weitergereicht
	 */
	public PruefTabImporter(final String csvDateiName)
	throws Exception{
		CSVImporter csvImporter = new CSVImporter(csvDateiName);
		
		csvImporter.getNaechsteZeile();
		int zeilen;
		for(zeilen = 0; ; zeilen++){
			String[] zeile = csvImporter.getNaechsteZeile();
			if(zeile == null || zeile.length == 0){
				break;
			}
		}
		
		csvImporter.reset();
		
		Map<Integer, String> spalteZuName = new HashMap<Integer, String>(); 
		String[] tabellenKopf = csvImporter.getNaechsteZeile();
		for(int i = 0; i < tabellenKopf.length; i += 2){
			this.tabContent.put(tabellenKopf[i], new Element[zeilen]);
			spalteZuName.put(i, tabellenKopf[i]);
		}
		
		for(int zeilenNr = 0; ; zeilenNr++){
			String[] tabellenZeile = csvImporter.getNaechsteZeile();
			if(tabellenZeile == null || tabellenZeile.length == 0){
				break; // fertig
			}
			
			for(int i = 0; i < tabellenZeile.length; i += 2){
				Element[] elemente = this.tabContent.get(spalteZuName.get(i));
				elemente[zeilenNr] = new Element(tabellenZeile[i], tabellenZeile[i + 1]);
			}
		}
	}
	
	
	/**
	 * Erfragt ein Element einer bestimmten Spalte / Zeile 
	 * 
	 * @param spaltenName der Name der Spalte
	 * @param line die Zeile
	 * @return ein Element einer bestimmten Spalte / Zeile
	 */
	public final Element getElement(String spaltenName, int line){
		return this.tabContent.get(spaltenName)[line];
	}
	

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		String ergebnis = Constants.EMPTY_STRING;
		
		boolean brk = false;
		for(int i = 0; !brk; i++){
			ergebnis += "Zeile (" + (i+2) + "):\n"; //$NON-NLS-1$ //$NON-NLS-2$
			for(String s:this.tabContent.keySet()){
				if(this.tabContent.get(s).length <= i){
					brk = true;
					break;
				}
				ergebnis += s + ": " + this.tabContent.get(s)[i] + ", ";   //$NON-NLS-1$//$NON-NLS-2$
			}
			ergebnis += "\n"; //$NON-NLS-1$
		}
		
		return ergebnis;
	}

	
	/**
	 * Berechnet die Summe
	 * 
	 * @param elemente Elemente ueber denen die Summe berechnt werden soll
	 */
	protected static final Element summe(Element... elemente){
		Element summeElement = new Element("0", "1");  //$NON-NLS-1$//$NON-NLS-2$
		double wert = 0.0;
		double guete = 0.0;
		boolean intp = false;
		double zaehler = 0;
		
		for(Element e:elemente){
			if(e.guete > 0.0 && !e.impl && e.wert >= 0){
				guete += e.guete;
				intp |= e.intp;
				wert += e.wert;
				zaehler += 1.0; 
			}
		}
		
		if(zaehler == 0.0){
			summeElement.impl = true;
			summeElement.wert = -3.0;
			summeElement.guete = 0.0;
		}else{
			summeElement.intp = intp;
			summeElement.wert = wert;
			summeElement.guete = guete / zaehler;			
		}
		
		return summeElement;
	}

	
	/**
	 * Berechnet den Durchschnitt
	 * 
	 * @param elemente Elemente ueber denen der Durchschnitt berechnt werden soll
	 */
	protected static final Element durchschnitt(Element... elemente){
		Element durchschnittElement = new Element("0", "1");  //$NON-NLS-1$//$NON-NLS-2$
		double wert = 0.0;
		double guete = 0.0;
		boolean intp = false;
		double zaehler = 0;
		
		for(Element e:elemente){
			if(e.guete > 0.0 && !e.impl && e.wert >= 0){
				guete += e.guete;
				intp |= e.intp;
				wert += e.wert;
				zaehler += 1.0; 
			}
		}
		
		if(zaehler == 0.0){
			durchschnittElement.impl = true;
			durchschnittElement.wert = -3.0;
			durchschnittElement.guete = 0.0;
		}else{
			durchschnittElement.intp = intp;
			durchschnittElement.wert = Math.round(wert / zaehler);
			durchschnittElement.guete = guete / zaehler;			
		}
		
		return durchschnittElement;
	}
	
	
	/**
	 * Ein Tabellenelement: (Wert, Status)-Paar
	 * 
	 * @author BitCtrl Systems GmbH, Thierfelder
	 *
	 */
	public static class Element{
		
		public double wert = Double.NaN; 
		
		public double guete = Double.NaN;
		
		public boolean nErf = false;
		public boolean wMax = false;
		public boolean wMin = false;
		public boolean wMaL = false;
		public boolean wMiL = false;
		public boolean impl = false;
		public boolean intp = false;
		
		
		/**
		 * Standardkonstruktor
		 * 
		 * @param wertStr der wertStr
		 * @param statusStr der statusStr
		 */
		private Element(final String wertStr, final String statusStr){
			this.wert = Double.parseDouble(wertStr);
			this.guete = 1.0;
			
			int errCode = 0;
			
			if(statusStr != null && statusStr.length() > 0) {
				String[] splitStatus = statusStr.trim().split(" "); //$NON-NLS-1$
				
				for(int i = 0; i<splitStatus.length; i++) {
					if(splitStatus[i].equalsIgnoreCase("Fehl")) //$NON-NLS-1$
						errCode = errCode-2;
					if(splitStatus[i].equalsIgnoreCase("nErm")) //$NON-NLS-1$
						errCode = errCode-1;
					if(splitStatus[i].equalsIgnoreCase("Impl")) //$NON-NLS-1$
						impl = true;
					if(splitStatus[i].equalsIgnoreCase("Intp")) //$NON-NLS-1$
						intp = true;				
					if(splitStatus[i].equalsIgnoreCase("nErf")) //$NON-NLS-1$
						nErf = true;
					if(splitStatus[i].equalsIgnoreCase("wMaL")) //$NON-NLS-1$
						wMaL = true;
					if(splitStatus[i].equalsIgnoreCase("wMax")) //$NON-NLS-1$
						wMax = true;
					if(splitStatus[i].equalsIgnoreCase("wMiL")) //$NON-NLS-1$
						wMiL = true;
					if(splitStatus[i].equalsIgnoreCase("wMin")) //$NON-NLS-1$
						wMin = true;
					
					try {
						guete = Double.parseDouble(splitStatus[i].replace(",", ".")); //$NON-NLS-1$ //$NON-NLS-2$
					} catch (Exception e) {
						//	kein float Wert
					}
				}
			}
				
			if(errCode < 0)
				wert = errCode;
		}
		
		
		/**
		 * Erfragt den ersten Wert
		 * 
		 * @return der erste Wert
		 */
		public final String getWert(){
			return new Long(Math.round(this.wert)).toString();
		}
		
		
		/**
		 * Erfragt den ersten Statuswert
		 * 
		 * @return der erste Wert
		 */
		public final String getStatus(){
			String s = new Double(guete).toString();
			if(impl){
				s += " Impl"; //$NON-NLS-1$
			}
			if(intp){
				s += " Intp"; //$NON-NLS-1$
			}
			if(nErf){
				s += " nErf"; //$NON-NLS-1$
			}
			if(wMaL){
				s += " wMaL"; //$NON-NLS-1$
			}
			if(wMax){
				s += " wMax"; //$NON-NLS-1$
			}
			if(wMiL){
				s += " wMiL"; //$NON-NLS-1$
			}
			if(wMin){
				s += " wMin"; //$NON-NLS-1$
			}

			return s;
		}

		
		/**
		 * {@inheritDoc}
		 */
		@Override
		public String toString() {
			String s = "undefiniert"; //$NON-NLS-1$
			
			if( !Double.isNaN(wert) ){
				s = Constants.EMPTY_STRING;
				
				s += wert + " ("; //$NON-NLS-1$
				s += "G:" + guete; //$NON-NLS-1$
				if(impl){
					s += " Impl"; //$NON-NLS-1$
				}
				if(intp){
					s += " Intp"; //$NON-NLS-1$
				}
				if(nErf){
					s += " nErf"; //$NON-NLS-1$
				}
				if(wMaL){
					s += " wMaL"; //$NON-NLS-1$
				}
				if(wMax){
					s += " wMax"; //$NON-NLS-1$
				}
				if(wMiL){
					s += " wMiL"; //$NON-NLS-1$
				}
				if(wMin){
					s += " wMin"; //$NON-NLS-1$
				}
				
				s += ")"; //$NON-NLS-1$
			}
			
			return s;
		}
	}	
}
