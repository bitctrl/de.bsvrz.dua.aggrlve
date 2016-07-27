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

import de.bsvrz.dav.daf.main.*;
import de.bsvrz.dav.daf.main.config.SystemObject;
import de.bsvrz.dua.dalve.analyse.lib.AnalyseAttribut;
import de.bsvrz.dua.dalve.analyse.lib.CommonFunctions;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAInitialisierungsException;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;
import de.bsvrz.sys.funclib.bitctrl.dua.av.DAVObjektAnmeldung;
import de.bsvrz.sys.funclib.debug.Debug;

import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;

/**
 * Empfängt Analysewerte und aggregiert die Daten für diesen Fahrstreifen oder VMQ.
 * Leitet ggf. die Daten an MQ weiter, die diesen Fahrstreifen enthalten.
 * 
 * @author BitCtrl Systems GmbH, Thierfelder
 * 
 * @version $Id$
 */
public class AggregationsFsOderVmq extends AbstraktAggregationsObjekt
		implements ClientReceiverInterface {

	private static final Debug _debug = Debug.getLogger();

	/**
	 * Anzahl Millisekunden in einer Stunde
	 */
	public static final long MILLIS_PER_HOUR = 60 * 60 * 1000L;
	
	/**
	 * der hier betrachtete Fahrstreifen.
	 */
	private final SystemObject _systemObject;

	/**
	 * DaLve-Funktionen zur Berechnung der abgeleiteten Werte (Verkehrsdichten usw.)
	 */
	private final CommonFunctions _commonFunctions;

	/**
	 * speichert alle historischen Daten dieses Aggregationsobjektes aller
	 * Aggregationsintervalle.
	 */
	protected AggregationsPufferMenge datenPuffer;

	/**
	 * Angemeldete Listener (MQ), die diesen FS benutzen (falls es ein FS ist).
	 * 
	 * Die Listener erhalten als Ersten Parameter den Intervallstartzeitpunkt und als zweiten Parameter das Intervall.
	 */
	private final Collection<BiConsumer<Long, AggregationsIntervall>> listeners = new CopyOnWriteArrayList<>();

	/**
	 * Zuletzt berechnetes Intervallende, entspricht in der Regel dem Zeitstempel des zuletzt empfangenen Datensatzes + T
	 */
	private long intervallEndeNormiertAlt = Long.MIN_VALUE;

	/**
	 * Standardkonstruktor.
	 * 
	 * @param dav
	 *            Verbindung zum Datenverteiler
	 * @param systemObject Systemobjekt des Fahrstreifens oder virtuellen Messquerschnitts
	 * @throws DUAInitialisierungsException
	 *             wenn dieses Objekt nicht vollstaendig (mit allen
	 *             Unterobjekten) initialisiert werden konnte
	 */
	public AggregationsFsOderVmq(final ClientDavInterface dav, final SystemObject systemObject) throws DUAInitialisierungsException {
		super(dav, systemObject);
		this._systemObject = systemObject;

		this.datenPuffer = new AggregationsPufferMenge(dav, _systemObject);
		
		final Set<DAVObjektAnmeldung> anmeldungen = new TreeSet<>();
		for (final AggregationsIntervall intervall : AggregationsIntervall
				.getInstanzen()) {
			try {
				anmeldungen.add(new DAVObjektAnmeldung(
						_systemObject,
						intervall.getDatenBeschreibung(isFahrstreifen())));
			} catch (final Exception e) {
				throw new DUAInitialisierungsException("Fahrstreifen " + _systemObject
						+ " konnte nicht initialisiert werden", e);
			}
		}
		sender.modifiziereObjektAnmeldung(anmeldungen);

		DataDescription dataDescriptionReceive = new DataDescription(
				dav.getDataModel().getAttributeGroup(isFahrstreifen() ? DUAKonstanten.ATG_KURZZEIT_FS : DUAKonstanten.ATG_KURZZEIT_MQ),
				dav.getDataModel().getAspect(DUAKonstanten.ASP_ANALYSE)
		);
		dav.subscribeReceiver(
				this,
				_systemObject,
				dataDescriptionReceive,
				ReceiveOptions.normal(),
				ReceiverRole.receiver()
		);
		
		this._commonFunctions = new CommonFunctions(dav, _systemObject);
	}

	/**
	 * Fügt einen Listener hinzu, der Informiert wird wenn ein neuer Aggregationsdatensatz gebildet wurde.
	 * Die Listener erhalten als Ersten Parameter den Intervallstartzeitpunkt und als zweiten Parameter das Intervall.
	 * 
	 * @param consumer Listener
	 */
	public void addListener(BiConsumer<Long, AggregationsIntervall> consumer){
		listeners.add(consumer);
	}

	/**
	 * Entfernt einen Listener.
	 * @param consumer Listener
	 */
	public void removeListener(BiConsumer<Long, AggregationsIntervall> consumer){
		listeners.remove(consumer);
	}

	/**
	 * Aggregiert die Daten eines Fahrstreifens oder VMQ von einem bestimmten Zeitbereich und veröffentlicht das Aggregierte Ergebnis. Außerdem werden alle MQ
	 * informiert, die die Daten dieses Fahrstreifens verwenden, sodass diese ggf. einen neuen MQ-Wert berechnen können (sobald alle FS-Daten da sind).
	 *
	 * @param zeitStempelVon Startzeitstempel (inklusiv) in Millisekunden seit 1970
	 * @param zeitStempelBis Endzeitstempel (exkusiv) in Millisekunden seit 1970
	 * @param intervall Verwendetes Aggregationsintervall. Z.B. 5 Minuten. zeitStempelBis - zeitStempelVon sollte dem Intervall entsprechen.
	 *                     Da Jahre und Monate nicht alle gleich lang sind (Schaltjahre usw.) sind auf jeden Fall die ersten beiden Parameter zu beachten,
	 *                     die die exakten Zeitstempel enthalten.    
	 */
	public void aggregiere(final long zeitStempelVon, final long zeitStempelBis, final AggregationsIntervall intervall) {
		final Collection<AggregationsDatum> basisDaten = this.datenPuffer
				.getDatenFuerZeitraum(zeitStempelVon, zeitStempelBis, intervall);
		Data nutzDatum = null;

		nutzDatum = dav.createData(intervall.getDatenBeschreibung(isFahrstreifen()).getAttributeGroup());

		this.aggregiereGeschwindigkeit(AnalyseAttribut.Q_KFZ, AnalyseAttribut.V_KFZ, nutzDatum, basisDaten);
		this.aggregiereGeschwindigkeit(AnalyseAttribut.Q_LKW, AnalyseAttribut.V_LKW, nutzDatum, basisDaten);
		this.aggregiereGeschwindigkeit(AnalyseAttribut.Q_PKW, AnalyseAttribut.V_PKW, nutzDatum, basisDaten);

		this.aggregiereMittel(AnalyseAttribut.Q_KFZ, nutzDatum, basisDaten);
		this.aggregiereMittel(AnalyseAttribut.Q_LKW, nutzDatum, basisDaten);
		this.aggregiereMittel(AnalyseAttribut.Q_PKW, nutzDatum, basisDaten);


		AbstraktAggregationsPuffer puffer = this.datenPuffer.getPuffer(intervall);

		AggregationsDatum last = puffer == null ? null : puffer.getLast();

		_commonFunctions.berechneLkwAnteil(nutzDatum);

		_commonFunctions.berechneDichte(nutzDatum, "Kfz", () -> last == null ? 0 : last.getWert(AnalyseAttribut.K_KFZ).getWert());
		_commonFunctions.berechneDichte(nutzDatum, "Lkw", () -> last == null ? 0 : last.getWert(AnalyseAttribut.K_LKW).getWert());
		_commonFunctions.berechneDichte(nutzDatum, "Pkw", () -> last == null ? 0 : last.getWert(AnalyseAttribut.K_PKW).getWert());

		_commonFunctions.berechneBemessungsVerkehrsStaerke(nutzDatum);

		_commonFunctions.berechneBemessungsDichte(nutzDatum, () -> last == null ? 0 : last.getWert(AnalyseAttribut.K_B).getWert());

		final ResultData resultat = new ResultData(
				this._systemObject,
				intervall.getDatenBeschreibung(isFahrstreifen()), 
				zeitStempelVon,
				nutzDatum);

		if (resultat.getData() != null) {
			this.fuelleRest(resultat, intervall);
			this.datenPuffer.aktualisiere(new AggregationsDatum(resultat, dav), false);
		}

		send(resultat);

		for(BiConsumer<Long, AggregationsIntervall> listener : listeners) {
			listener.accept(zeitStempelVon, intervall);
		}
	}

	/**
	 * Sendet einen Ergebnisdatensatz. Kann für Testfälle überschrieben werden
	 * @param resultat Ergebnis
	 */
	protected void send(final ResultData resultat) {
		this.sende(resultat);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void update(final ResultData... resultate) {
		if (resultate != null) {
			for (final ResultData resultat : resultate) {
				if (resultat != null && resultat.hasData()) {
					receivedData(resultat);
				}
			}
		}
	}

	/**
	 * Methode wird für jeden empfangenen Analyse-Eingangsdatensatz aufgerufen und triggert die Aggregation
	 * @param resultat Datensatz
	 */
	protected void receivedData(final ResultData resultat) {
		AggregationsDatum datum = new AggregationsDatum(resultat, dav);
		this.datenPuffer.aktualisiere(datum, true);

		long t = datum.getT();

		if(t <= 0){
			_debug.warning("Erfassungsintervall für " + _systemObject.getPidOrId() + " kann nicht bestimmt werden.");
			return;
		}

		long intervallEndeNormiertNeu = datum.getIntervallEnde() / t * t;

		boolean firstDataSet = false;
		
		if(intervallEndeNormiertAlt == Long.MIN_VALUE){
			// Erster Datensatz
			intervallEndeNormiertAlt = intervallEndeNormiertNeu - t;
			firstDataSet = true;
		}

		if(intervallEndeNormiertNeu - intervallEndeNormiertAlt > MILLIS_PER_HOUR) {
			// Maximal Datenlücken von einer Stunde auffüllen.
			if (resultat.getData() == null) {
				for (final AggregationsIntervall intervall : AggregationsIntervall
						.getInstanzen()) {
					send(new ResultData(
							this._systemObject, intervall
							.getDatenBeschreibung(isFahrstreifen()),
							resultat.getDataTime(), null));
				}
			}
		}
		else {
			// Fehlende Datenzeiten zwischen vorheriger Berechnungszeit und aktueller Berechnungszeit auffüllen.
			// hierzu werden die fehlenden Intervalle eingefügt.
			for(long intervallEndeNormiert = intervallEndeNormiertAlt + t; intervallEndeNormiert <= intervallEndeNormiertNeu; intervallEndeNormiert += t) {

				if(intervallEndeNormiert < intervallEndeNormiertNeu) {
					// Leeren Datensatz einfügen, das wird erst hier gemacht, damit auch Lücken erkannt werden, die nicht durch leere Datensätze
					// verursacht werden, wenn z.B. bei einem Erfassungsintervall von 1 Minute erst nach 5 Minuten der nächste Datensatz kommt
					// und 4 Datensätze einfach fehlen.
					this.datenPuffer.aktualisiere(new AggregationsDatum(intervallEndeNormiert - t), true);
				}
				
				for(AggregationsIntervall intervall : AggregationsIntervall.getInstanzen()) {
					long intervallMillis = intervall.getIntervall();
					if(intervallMillis >= t) {
						if(intervall.isAggregationErforderlich(intervallEndeNormiertAlt, intervallEndeNormiert)) {
							long aggregationsZeitStempel = intervall.getAggregationsZeitStempel(intervallEndeNormiert);
							this.aggregiere(intervall.getStartZeitStempel(aggregationsZeitStempel), aggregationsZeitStempel, intervall);
						}
						else if(firstDataSet && intervall.isDTVorTV()) {
							// Vorherigen Datensatz berechnen aus den Archivdaten
							long aggregationsZeitStempel = intervall.getAggregationsZeitStempel(intervallEndeNormiertAlt);
							aggregiere(intervall.getStartZeitStempel(aggregationsZeitStempel), aggregationsZeitStempel, intervall);
						}
					}
				}
				intervallEndeNormiertAlt = intervallEndeNormiert;
			}

		}
		intervallEndeNormiertAlt = intervallEndeNormiertNeu;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return this._systemObject.toString();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean isFahrstreifen() {
		return this._systemObject.isOfType(DUAKonstanten.TYP_FAHRSTREIFEN);
	}

	/**
	 * Erfragt den Datenpuffer dieses Objektes.
	 * 
	 * @return der Datenpuffer dieses Objektes
	 */
	public final AggregationsPufferMenge getPuffer() {
		return this.datenPuffer;
	}
}
