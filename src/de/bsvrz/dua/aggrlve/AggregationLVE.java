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
import java.util.Calendar;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;

import de.bsvrz.dav.daf.main.ClientDavInterface;
import de.bsvrz.dav.daf.main.ResultData;
import de.bsvrz.dav.daf.main.config.Aspect;
import de.bsvrz.dav.daf.main.config.SystemObject;
import de.bsvrz.dav.daf.main.config.SystemObjectType;
import de.bsvrz.sys.funclib.application.StandardApplicationRunner;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAInitialisierungsException;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAUtensilien;
import de.bsvrz.sys.funclib.bitctrl.dua.ObjektWecker;
import de.bsvrz.sys.funclib.bitctrl.dua.adapter.AbstraktVerwaltungsAdapterMitGuete;
import de.bsvrz.sys.funclib.bitctrl.dua.dfs.typen.SWETyp;
import de.bsvrz.sys.funclib.bitctrl.dua.lve.DuaVerkehrsNetz;
import de.bsvrz.sys.funclib.bitctrl.dua.lve.MessQuerschnitt;
import de.bsvrz.sys.funclib.bitctrl.dua.schnittstellen.IObjektWeckerListener;

/**
 * Die SWE Aggregation LVE meldet sich auf alle messwertersetzten Kurzzeitdaten
 * an und berechnet aus diesen Daten für alle parametrierten Fahrstreifen und
 * Messquerschnitte die 1-, 5-, 15-, 30-, 60- Minutenwerte sowie Tageswerte und
 * DTV-Werte (Durchschnittliche Tagesverkehrswerte) je Monat und je Jahr (Details
 * siehe [AFo] bzw. [MARZ]).<br>
 * Diese Applikation initialisiert nur alle in den uebergebenen Konfigurationsbereichen
 * konfigurierten Messquerschnitte. Von diesen Objekten aus werden dann auch die
 * assoziierten Fahrstreifen initialisiert
 * 
 * @author BitCtrl Systems GmbH, Thierfelder
 *
 */
public class AggregationLVE
extends AbstraktVerwaltungsAdapterMitGuete
implements IObjektWeckerListener{

	/**
	 * indiziert, ob diese das Flag <code>nicht erfasst</code> uebernommen werden
	 * soll
	 */
	public static final boolean NICHT_ERFASST = false;

	/**
	 * indiziert, bei der TV-Tag-Berechnung nicht vorhandene Wert durch das
	 * Mittel der restlichen Werte ersetzte werden sollen. 
	 */
	public static final boolean APPROX_REST = true;
	
	/**
	 * indiziert, ob diese Applikation im Test-Modus läuft,
	 * in welchem sie nicht von der Systemzeit getriggert wird
	 */
	public static boolean TEST = false;
	
	/**
	 * der Guetefaktor dieser SWE
	 */
	public static double GUETE;
	
	/**
	 * der Systemobjekttyp Fahrstreifen
	 */
	public static SystemObjectType TYP_FAHRSTREIFEN = null;
	
	/**
	 * Aspekt der messwertersetzten Fahrstreifendaten
	 */
	public static Aspect MWE = null;
	
	/**
	 * der interne Kontrollprozess dient der zeitlichen Steuerung der Aggregationsberechnungen
	 * (1min, …, 60min). Nach dem Starten führt dieser Prozess immer 30s nach jeder vollen Minute
	 * eine Ueberprüfung für alle Fahrstreifen bzw. Messquerschnitte
	 */
	private ObjektWecker wecker = new ObjektWecker();
	
	/**
	 * alle Messquerschnitte, fuer die Daten aggregiert werden sollen
	 */
	private Map<SystemObject, AggregationsMessQuerschnitt> messQuerschnitte = 
											new HashMap<SystemObject, AggregationsMessQuerschnitt>();

	
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void initialisiere()
	throws DUAInitialisierungsException {
		super.initialisiere();
		
		/**
		 * DUA-Verkehrs-Netz initialisieren
		 */
		DuaVerkehrsNetz.initialisiere(this.verbindung);
		
		/**
		 * Aggregationsintervalle initialisieren 
		 */
		AggregationsIntervall.initialisiere(this.verbindung);
		
		GUETE = this.getGueteFaktor();
		TYP_FAHRSTREIFEN = this.verbindung.getDataModel().getType(DUAKonstanten.TYP_FAHRSTREIFEN);
		MWE = this.verbindung.getDataModel().getAspect(DUAKonstanten.ASP_MESSWERTERSETZUNG);

		Collection<SystemObject> alleMqObjImKB = DUAUtensilien.getBasisInstanzen(
				this.verbindung.getDataModel().getType(DUAKonstanten.TYP_MQ),
				this.verbindung,
				this.getKonfigurationsBereiche());

		for(SystemObject mqObjekt:alleMqObjImKB){
			MessQuerschnitt mq = MessQuerschnitt.getInstanz(mqObjekt);
			if(mq == null){
				throw new DUAInitialisierungsException("Konfiguration von Messquerschnitt " + //$NON-NLS-1$ 
						mq + " konnte nicht vollstaendig ausgelesen werden"); //$NON-NLS-1$
			}else{
				messQuerschnitte.put(mqObjekt, new AggregationsMessQuerschnitt(this.verbindung, mq));
			}
		}
			
		if(!TEST){
			wecker.setWecker(this, getNaechstenWeckZeitPunkt());
		}
	}
	
	
	/**
	 * Startet diese Applikation nur fuer Testzwecke
	 * 
	 * @param dav Verbindung zum Datenverteiler
	 * @throws Exception wenn die Initialisierung fehlschlaegt 
	 */
	public final void testStart(final ClientDavInterface dav)
	throws Exception{
		TEST = true;
		this.komArgumente = new ArrayList<String>();
		this.komArgumente.add("-KonfigurationsBereichsPid=" + //$NON-NLS-1$
				"kb.objekteTestUnterzentraleK2S_100_MessQuerschnitte"); //$NON-NLS-1$
		this.initialize(dav);
	}
	
	
	/**
	 * {@inheritDoc}
	 */
	public void alarm() {
		final long jetzt = System.currentTimeMillis();
		
		for(AggregationsMessQuerschnitt mq:this.messQuerschnitte.values()){
			for(AggregationsIntervall intervall:AggregationsIntervall.getInstanzen()){
				if(intervall.isAggregationErforderlich(jetzt)){
					mq.aggregiere(intervall.getAggregationZeitStempel(jetzt),
								  intervall);
				}				
			}
		}
		
		wecker.setWecker(this, getNaechstenWeckZeitPunkt());
	}
	
	
	/**
	 * Erfragt ein Aggregationsobjekt (nur fuer Testzwecke)
	 * 
	 * @param obj das assoziierte Systemobjekt
	 */
	public AggregationsMessQuerschnitt getAggregationsObjekt(final SystemObject obj){
		return this.messQuerschnitte.get(obj);
	}
	
	
	/**
	 * Erfragt den Zeitpunkt, der exakt 30s nach der Minute liegt, in der 
	 * diese Methode aufgerufen wird (Absolute Zeit ohne Sommer- und Winterzeit)
	 * 
	 * @return der Zeitpunkt, der exakt 30s nach der Minute liegt, in der 
	 * diese Methode aufgerufen wird
	 */
	private final long getNaechstenWeckZeitPunkt(){
		GregorianCalendar cal = new GregorianCalendar();
		cal.setTimeInMillis(System.currentTimeMillis());
		
		if(cal.get(Calendar.SECOND) >= 25){
			cal.add(Calendar.MINUTE, 1);	
		}
		cal.set(Calendar.MILLISECOND, 0);
		cal.set(Calendar.SECOND, 30);
				
		return cal.getTimeInMillis();
	}
	
	
	/**
	 * Startet diese Applikation
	 * 
	 * @param argumente Argumente der Kommandozeile
	 */
	public static void main(String argumente[]){
        Thread.setDefaultUncaughtExceptionHandler(new Thread.
        				UncaughtExceptionHandler(){
            public void uncaughtException(@SuppressWarnings("unused")
			Thread t, Throwable e) {
                LOGGER.error("Applikation wird wegen" +  //$NON-NLS-1$
                		" unerwartetem Fehler beendet", e);  //$NON-NLS-1$
            	e.printStackTrace();
                Runtime.getRuntime().exit(0);
            }
        });
		StandardApplicationRunner.run(new AggregationLVE(), argumente);
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public double getStandardGueteFaktor() {
		return 0.9;
	}
	
	
	/**
	 * {@inheritDoc}
	 */
	public SWETyp getSWETyp() {
		return SWETyp.SWE_AGGREGATION_LVE;
	}	
	
	
	/**
	 * {@inheritDoc}
	 */
	public void update(ResultData[] resultate) {
		// Daten werden von den Untermodulen selbst entgegen genommen
	}

}
