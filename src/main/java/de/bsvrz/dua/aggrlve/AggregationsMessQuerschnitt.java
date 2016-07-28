/*
 * Segment Datenübernahme und Aufbereitung (DUA), SWE Aggregation LVE
 * Copyright (C) 2007 BitCtrl Systems GmbH
 * Copyright 2016 by Kappich Systemberatung Aachen
 * 
 * This file is part of de.bsvrz.dua.aggrlve.
 * 
 * de.bsvrz.dua.aggrlve is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * de.bsvrz.dua.aggrlve is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with de.bsvrz.dua.aggrlve.  If not, see <http://www.gnu.org/licenses/>.

 * Contact Information:
 * Kappich Systemberatung
 * Martin-Luther-Straße 14
 * 52062 Aachen, Germany
 * phone: +49 241 4090 436 
 * mail: <info@kappich.de>
 */

package de.bsvrz.dua.aggrlve;

import de.bsvrz.dav.daf.main.ClientDavInterface;
import de.bsvrz.dav.daf.main.Data;
import de.bsvrz.dav.daf.main.ResultData;
import de.bsvrz.dav.daf.main.config.SystemObject;
import de.bsvrz.dua.dalve.analyse.lib.AnalyseAttribut;
import de.bsvrz.dua.dalve.analyse.lib.CommonFunctions;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAInitialisierungsException;
import de.bsvrz.sys.funclib.bitctrl.dua.av.DAVObjektAnmeldung;
import de.bsvrz.sys.funclib.debug.Debug;

import java.util.*;
import java.util.function.BiConsumer;

/**
 * Aggregiert aus den fuer diesen Messquerschnitt (bzw. dessen Fahrstreifen)
 * gespeicherten Daten die Aggregationswerte aller Aggregationsstufen aus der
 * jeweils darunterliegenden Stufe bzw. aus den messwertersetzten
 * Fahrstreifendaten fuer die Basisstufe
 * 
 * @author BitCtrl Systems GmbH, Thierfelder
 */
public final class AggregationsMessQuerschnitt extends
		AbstraktAggregationsObjekt implements BiConsumer<Long, AggregationsIntervall> {

	private static final Debug _debug = Debug.getLogger();
	
	/** der hier betrachtete Messquerschnitt. */
	private final SystemObject mq;

	/**
	 * Menge der Fahrstreifen, die an diesem Messquerschnitt konfiguriert sind.
	 */
	private final Map<SystemObject, AggregationsFsOderVmq> fsMenge;
	
	/**
	 * DaLve-Funktionen zur Berechnung der abgeleiteten Werte (Verkehrsdichten usw.)
	 */
	private final CommonFunctions _commonfunctions;

	/**
	 * Zuletzt berechnete Daten je Intervall
	 */
	private final Map<AggregationsIntervall, AggregationsDatum> lastValues = new HashMap<>();

	/**
	 * Standardkonstruktor.
	 * 
	 * @param dav
	 *            Verbindung zum Datenverteiler
	 * @param fsMenge    Menge der Fahrstreifen dieses MQ
	 * 
	 * @param systemObject MQ-Objekt
	 *                        
	 * @throws DUAInitialisierungsException
	 *             wenn dieses Objekt nicht vollstaendig (mit allen
	 *             Unterobjekten) initialisiert werden konnte
	 */
	public AggregationsMessQuerschnitt(
			final ClientDavInterface dav,
			final HashMap<SystemObject, AggregationsFsOderVmq> fsMenge,
			final SystemObject systemObject)
			throws DUAInitialisierungsException {
		super(dav, systemObject);
		this.mq = systemObject;

		final Set<DAVObjektAnmeldung> anmeldungen = new TreeSet<>();
		for (final AggregationsIntervall intervall : AggregationsIntervall
				.getInstanzen()) {
			try {
				anmeldungen.add(new DAVObjektAnmeldung(systemObject,
						intervall.getDatenBeschreibung(false)));
			} catch (final Exception e) {
				throw new DUAInitialisierungsException("Messquerschnitt " + systemObject
						+ " konnte nicht initialisiert werden", e);
			}
		}
		sender.modifiziereObjektAnmeldung(anmeldungen);

		this.fsMenge = fsMenge;

		_commonfunctions = new CommonFunctions(dav, systemObject);

		for(AggregationsFsOderVmq aggregationsFsOderVmq : fsMenge.values()) {
			aggregationsFsOderVmq.addListener(this);
		}
	}
	
	@Override
	public String toString() {
		return this.mq.toString();
	}

	@Override
	protected boolean isFahrstreifen() {
		return false;
	}

	@Override
	/**
	 * Berechnet die MQ-Daten für ein Intervall (falls alle FS-Daten da sind), bzw. berechnet das Vorgänger-Intervall falls es noch nicht berechnet wurde.
	 * 
	 * Diese Methode wird jedes mal aufgerufen wenn für einen fahrstreifen ein aggregiertes Datum gebildet wurde.
	 */
	public void accept(Long startZeitStempel, AggregationsIntervall intervall) {
		
		final Map<AggregationsFsOderVmq, AggregationsDatum> fsMap = new HashMap<>();
		
		AggregationsDatum letzterWert = lastValues.get(intervall);

		if(letzterWert != null && startZeitStempel <= letzterWert.getDatenZeit()){
			// Zeitrücksprung ignorieren
			return;
		}
		
		if(!getFsDaten(startZeitStempel, intervall, fsMap)){
			// ggf. Vorgänger-Intervall berechnen
			fsMap.clear();

			long zeitStempelVor = intervall.getStartZeitStempel(startZeitStempel);
			if(letzterWert == null || letzterWert.getDatenZeit() < zeitStempelVor) {
				// Das letzte Intervall wurde noch nicht berechnet, das also nachholen
				getFsDaten(zeitStempelVor, intervall, fsMap);
								
				aggregiere(zeitStempelVor, intervall, fsMap.values());
			}
			// Es sind nicht alle Daten da, also warten bis entweder alle Daten da sind 
			// oder das nächste Intervall berechnet werden soll
			return;
		}

		// Alle Daten da, also Aggregation direkt berechnen
		aggregiere(startZeitStempel, intervall, fsMap.values());
	}

	/**
	 * Gibt alle vorhandenen Fahrstreifendaten im angegebenen Intervall zurück
	 * @param zeitStempel Startzeit des Intervalls
	 * @param intervall Intervall
	 * @param fsMap Ziel-Map für Daten
	 * @return true wenn alle Daten da waren, sonst false.
	 */
	private boolean getFsDaten(final Long zeitStempel, final AggregationsIntervall intervall, final Map<AggregationsFsOderVmq, AggregationsDatum> fsMap) {
		boolean complete = true;
		
		for(AggregationsFsOderVmq fahrStreifen : fsMenge.values()) {
			AbstraktAggregationsPuffer puffer = fahrStreifen.getPuffer().getPuffer(intervall);
			if(puffer == null) {
				// FS hatte noch nie Daten oder Erfassungsintervall ist zu groß
				complete = false;
				fsMap.put(fahrStreifen, new AggregationsDatum(zeitStempel)); // Leeren Datensatz einfügen
				continue;
			}
			Collection<AggregationsDatum> datenFuerZeitraum = puffer.getDatenFuerZeitraum(zeitStempel, zeitStempel + 1);
			if(datenFuerZeitraum.isEmpty()){
				// Noch nicht alle Daten da
				complete = false;
				fsMap.put(fahrStreifen, new AggregationsDatum(zeitStempel)); // Leeren Datensatz einfügen
				continue;
			}
			if(datenFuerZeitraum.size() > 1){
				// Das sollte nicht passieren, da die FS/VMQ genau einen aggregierten Wert pro Intervall haben sollten
				_debug.warning("Mehrere Aggregationswerte für Zeitbereich " + zeitStempel + " " + intervall, objekt);
			}
			
			fsMap.put(fahrStreifen, datenFuerZeitraum.iterator().next());
		}
		return complete;
	}

	/**
	 * Eigentliche Aggregationsfunktion 
	 * @param zeitStempel Startzeitstempel
	 * @param intervall Intervall
	 * @param basisDaten Eingangsdaten (ein Objekt je FS), ggf. mit Markierung "keine Daten"
	 */
	private void aggregiere(final long zeitStempel, final AggregationsIntervall intervall, final Collection<AggregationsDatum> basisDaten) {
		Data nutzDatum = dav.createData(intervall.getDatenBeschreibung(false).getAttributeGroup());

		this.aggregiereGeschwindigkeit(AnalyseAttribut.Q_KFZ, AnalyseAttribut.V_KFZ, nutzDatum, basisDaten);
		this.aggregiereGeschwindigkeit(AnalyseAttribut.Q_LKW, AnalyseAttribut.V_LKW, nutzDatum, basisDaten);
		this.aggregiereGeschwindigkeit(AnalyseAttribut.Q_PKW, AnalyseAttribut.V_PKW, nutzDatum, basisDaten);

		this.aggregiereSumme(AnalyseAttribut.Q_KFZ, nutzDatum, basisDaten);
		this.aggregiereSumme(AnalyseAttribut.Q_LKW, nutzDatum, basisDaten);
		this.aggregiereSumme(AnalyseAttribut.Q_PKW, nutzDatum, basisDaten);

		_commonfunctions.berechneLkwAnteil(nutzDatum);

		AggregationsDatum last = lastValues.get(intervall);

		_commonfunctions.berechneDichte(nutzDatum, "Kfz", () -> last == null ? 0 : last.getWert(AnalyseAttribut.K_KFZ).getWert());
		_commonfunctions.berechneDichte(nutzDatum, "Lkw", () -> last == null ? 0 : last.getWert(AnalyseAttribut.K_LKW).getWert());
		_commonfunctions.berechneDichte(nutzDatum, "Pkw", () -> last == null ? 0 : last.getWert(AnalyseAttribut.K_PKW).getWert());

		_commonfunctions.berechneBemessungsVerkehrsStaerke(nutzDatum);

		_commonfunctions.berechneBemessungsDichte(nutzDatum, () -> last == null ? 0 : last.getWert(AnalyseAttribut.K_B).getWert());

		final ResultData resultat = new ResultData(
				this.mq,
				intervall.getDatenBeschreibung(false), 
				zeitStempel,
				nutzDatum);

		if (resultat.getData() != null) {
			this.fuelleRest(resultat, intervall);
		}

		this.sende(resultat);

		lastValues.put(intervall, new AggregationsDatum(resultat, dav));
	}
}
