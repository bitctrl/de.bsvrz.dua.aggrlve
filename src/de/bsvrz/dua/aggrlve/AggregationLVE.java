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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import de.bsvrz.dav.daf.main.ClientDavConnection;
import de.bsvrz.dav.daf.main.ClientDavInterface;
import de.bsvrz.dav.daf.main.DataDescription;
import de.bsvrz.dav.daf.main.ReceiveOptions;
import de.bsvrz.dav.daf.main.ReceiverRole;
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
import de.bsvrz.sys.funclib.bitctrl.dua.lve.FahrStreifen;
import de.bsvrz.sys.funclib.bitctrl.dua.lve.MessQuerschnitt;
import de.bsvrz.sys.funclib.bitctrl.dua.schnittstellen.IObjektWeckerListener;
import de.bsvrz.sys.funclib.debug.Debug;

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
	
	
	/***********************
	 * Nur fuer Testzwecke *
	 ***********************/
	
	/**
	 * Schaltet saemtliche Funktionalitaeten ab, die sich an der
	 * lokalen Systemzeit orientieren. Dadurch wird der Zeitrafferbetrieb
	 * dieser SWE ermoeglicht.
	 */	
	private static boolean ZEIT_RAFFER = false;

	/**
	 * alle Fahrstreifen, mit den Messquerschnitten, zu denen sie gehören
	 */
	private Map<SystemObject, SystemObject> fsMq = 
											new HashMap<SystemObject, SystemObject>();
	/**
	 * alle Messquerschnitte, mit den Fahrstreifen, zu denen sie gehören
	 */
	private Map<SystemObject, Set<SystemObject>> mqFs = 
											new HashMap<SystemObject, Set<SystemObject>>();
	
	/**
	 * Letztes Fahrstreifendatum pro Fahrstreifen
	 */
	private Map<SystemObject, ResultData> fsDataHist = 
		new HashMap<SystemObject, ResultData>();

	/**
	 * Zweite Datenverteiler-Verbindung fuer Testzwecke
	 */
	private ClientDavInterface dav2 = null;


	/*********************
	 * Normale Variablen * 
	 *********************/

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
				
				Set<SystemObject> fsList = new HashSet<SystemObject>();
				for(FahrStreifen fs:mq.getFahrStreifen()){
					this.fsMq.put(fs.getSystemObject(), mq.getSystemObject());
					fsList.add(fs.getSystemObject());
				}
				this.mqFs.put(mq.getSystemObject(), fsList);
			}
		}
			
		if(!ZEIT_RAFFER){
			wecker.setWecker(this, getNaechstenWeckZeitPunkt());
		}else{
			/**
			 * Anmeldung auf alle Rohdaten, die hier verarbeitet werden sollen
			 * unter der Vorraussetzung, dass diese Daten im 1min-Intervall gesendet
			 * werden
			 */
			try {
				this.dav2 = new ClientDavConnection(this.getVerbindung().getClientDavParameters());
				this.dav2.connect();
				this.dav2.login();
				
				for(SystemObject fs:fsMq.keySet()){
					this.dav2.subscribeReceiver(this,
							fs,
							new DataDescription(
									this.dav2.getDataModel().getAttributeGroup(DUAKonstanten.ATG_KZD),
									this.dav2.getDataModel().getAspect(DUAKonstanten.ASP_MESSWERTERSETZUNG),
									(short)0),
									ReceiveOptions.normal(),
									ReceiverRole.receiver());
				}
			} catch (Exception e) {
				throw new DUAInitialisierungsException("Testapplikation konnte nicht gestartet werden", e); //$NON-NLS-1$
			}
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
		ZEIT_RAFFER = true;
		Debug.getLogger().config("Applikation fuer Testzwecke gestartet"); //$NON-NLS-1$
		this.komArgumente = new ArrayList<String>();
		this.komArgumente.add("-KonfigurationsBereichsPid=" + //$NON-NLS-1$
				"kb.duaTestObjekte"); //$NON-NLS-1$
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
            	Debug.getLogger().error("Applikation wird wegen" +  //$NON-NLS-1$
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
		if(resultate != null){
			for(ResultData resultat:resultate){
				if(resultat != null){
					synchronized (dav2) {
						this.fsDataHist.put(resultat.getObject(), resultat);

						SystemObject mq = this.fsMq.get(resultat.getObject());
						int fsZaehler = 0;
						for(SystemObject fs:this.mqFs.get(mq)){
							if(this.fsDataHist.get(fs) != null){
								fsZaehler++;
							}
						}

						if(fsZaehler == this.mqFs.get(mq).size()){
							/**
							 * fuer alle Fs des Mq sind Daten im Puffer
							 */
							this.loeseBerechnungAus(mq, resultat.getDataTime());
							this.loescheMqPuffer(mq);
						}
					}
				}
			}
		}
	}
	
	
	/**
	 * Leitet eine Berechnung mit allen bis zum uebergebenen Zeitpunkt
	 * eingetroffenen Daten fuer den uebergebenen Zeitpunkt aus
	 * 
	 * @param mqObj der Messquerschnitt, fuer den die Berechnung (Aggregation)
	 * stattfinden soll
	 * @param jetzt der Zeitpunkt der Berechnung
	 */
	private final void loeseBerechnungAus(final SystemObject mqObj, final long jetzt){
		synchronized (dav2) {
			AggregationsMessQuerschnitt mqZiel = null;
			for(AggregationsMessQuerschnitt mq:this.messQuerschnitte.values()){
				if(mq.getObjekt().equals(mqObj)){
					mqZiel = mq;
					break;
				}					
			}			

			if(mqZiel != null){
				for(AggregationsIntervall intervall:AggregationsIntervall.getInstanzen()){
					if(intervall.isAggregationErforderlich(jetzt)){
						mqZiel.aggregiere(intervall.getAggregationZeitStempel(jetzt),
										  intervall);
					}
				}				
			}else{
				throw new RuntimeException("TEST: Kein MQ gefunden"); //$NON-NLS-1$
			}
		}
	}


	/**
	 * Löscht den aktuellen Fahrstreifen-Datenpuffer fuer einen bestimmten Messquerschnitt
	 * 
	 * @param mq ein Messquerschnitt
	 */
	private final void loescheMqPuffer(final SystemObject mq){
		if(mq != null && this.mqFs.get(mq) != null){
			for(SystemObject fs:this.mqFs.get(mq)){
				this.fsDataHist.put(fs, null);
			}			
		}
	}
	
}
