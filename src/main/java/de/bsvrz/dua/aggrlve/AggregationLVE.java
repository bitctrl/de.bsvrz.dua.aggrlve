/*
 * Segment 4 Datenübernahme und Aufbereitung (DUA), SWE 4.9 Aggregation LVE
 * Copyright (C) 2007-2015 BitCtrl Systems GmbH
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
import de.bsvrz.dua.aggrlve.vmq.VMqAggregator;
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
import de.bsvrz.sys.funclib.commandLineArgs.ArgumentList;
import de.bsvrz.sys.funclib.debug.Debug;

/**
 * Die SWE Aggregation LVE meldet sich auf alle messwertersetzten Kurzzeitdaten an und berechnet aus
 * diesen Daten für alle parametrierten Fahrstreifen und Messquerschnitte die 1-, 5-, 15-, 30-, 60-
 * Minutenwerte sowie Tageswerte und DTV-Werte (Durchschnittliche Tagesverkehrswerte) je Monat und
 * je Jahr (Details siehe [AFo] bzw. [MARZ]).<br>
 * Diese Applikation initialisiert nur alle in den uebergebenen Konfigurationsbereichen
 * konfigurierten Messquerschnitte. Von diesen Objekten aus werden dann auch die assoziierten
 * Fahrstreifen initialisiert
 *
 * @author BitCtrl Systems GmbH, Thierfelder
 */
public final class AggregationLVE extends AbstraktVerwaltungsAdapterMitGuete
		implements IObjektWeckerListener {

	private static final Debug LOGGER = Debug.getLogger();

	private static final int OFFSET_MIN = 5;

	private static final int OFFSET_MAX = 55;

	/***************************************************************************
	 * Nur fuer Testzwecke *
	 **************************************************************************/

	/**
	 * Schaltet saemtliche Funktionalitaeten ab, die sich an der lokalen Systemzeit orientieren.
	 * Dadurch wird der Zeitrafferbetrieb dieser SWE ermoeglicht.
	 */
	private static boolean zeitRaffer;

	/**
	 * alle Fahrstreifen, mit den Messquerschnitten, zu denen sie gehören.
	 */
	private final Map<SystemObject, SystemObject> fsMq = new HashMap<>();
	/**
	 * alle Messquerschnitte, mit den Fahrstreifen, zu denen sie gehören.
	 */
	private final Map<SystemObject, Set<SystemObject>> mqFs = new HashMap<>();

	/**
	 * Letztes Fahrstreifendatum pro Fahrstreifen.
	 */
	private final Map<SystemObject, ResultData> fsDataHist = new HashMap<>();

	/**
	 * Zweite Datenverteiler-Verbindung fuer Testzwecke.
	 */
	private ClientDavInterface dav2;

	/***************************************************************************
	 * Normale Variablen *
	 **************************************************************************/

	/**
	 * indiziert, ob diese das Flag <code>nicht erfasst</code> uebernommen werden soll.
	 */
	public static final boolean NICHT_ERFASST = false;

	/**
	 * indiziert, bei der TV-Tag-Berechnung nicht vorhandene Wert durch das Mittel der restlichen
	 * Werte ersetzte werden sollen.
	 */
	public static final boolean APPROX_REST = true;

	/**
	 * der Guetefaktor dieser SWE.
	 */
	public static double guete;

	/**
	 * der Systemobjekttyp Fahrstreifen.
	 */
	public static SystemObjectType typFahrstreifen;

	/**
	 * Aspekt der messwertersetzten Fahrstreifendaten.
	 */
	public static Aspect mwe;

	/**
	 * der interne Kontrollprozess dient der zeitlichen Steuerung der Aggregationsberechnungen
	 * (1min, …, 60min). Nach dem Starten führt dieser Prozess immer 30s nach jeder vollen Minute
	 * eine Ueberprüfung für alle Fahrstreifen bzw. Messquerschnitte.
	 */
	private final ObjektWecker wecker = new ObjektWecker();

	/**
	 * alle Messquerschnitte, fuer die Daten aggregiert werden sollen.
	 */
	private final Map<SystemObject, AggregationsMessQuerschnitt> messQuerschnitte = new HashMap<>();

	private int berechnungsOffset = 30;

	@Override
	protected void initialisiere() throws DUAInitialisierungsException {
		super.initialisiere();

		final String timeoutString = DUAUtensilien.getArgument("offset", getKomArgumente());
		if (timeoutString != null) {
			try {
				berechnungsOffset = Integer.parseInt(timeoutString);
			} catch (final Exception ex) {
				;
			}
		}
		if (berechnungsOffset < OFFSET_MIN) {
			berechnungsOffset = OFFSET_MIN;
			LOGGER.warning("Berechnungsoffset muss im Bereich [" + OFFSET_MIN + ", " + OFFSET_MAX
					+ "] liegen! Korrigiere auf " + OFFSET_MIN + "s.");
		} else if (berechnungsOffset > OFFSET_MAX) {
			berechnungsOffset = OFFSET_MAX;
			LOGGER.warning("Berechnungsoffset muss im Bereich [" + OFFSET_MIN + ", " + OFFSET_MAX
					+ "] liegen! Korrigiere auf " + OFFSET_MAX + "s.");
		}

		final ArgumentList argumentList = new ArgumentList(
				getKomArgumente().toArray(new String[getKomArgumente().size()]));
		final boolean ignoreVmq = argumentList.fetchArgument("-ignoreVMQ=false").booleanValue();

		/**
		 * DUA-Verkehrs-Netz initialisieren
		 */
		DuaVerkehrsNetz.initialisiere(getVerbindung());

		/**
		 * Aggregationsintervalle initialisieren
		 */
		AggregationsIntervall.initialisiere(getVerbindung());

		/** Aggregation für virtuelle MQ initialisieren. */
		if (!ignoreVmq) {
			VMqAggregator.getInstance().init(getVerbindung());
		}

		AggregationLVE.guete = getGueteFaktor();
		AggregationLVE.typFahrstreifen = getVerbindung().getDataModel()
				.getType(DUAKonstanten.TYP_FAHRSTREIFEN);
		AggregationLVE.mwe = getVerbindung().getDataModel()
				.getAspect(DUAKonstanten.ASP_MESSWERTERSETZUNG);

		final Collection<SystemObject> alleMqObjImKB = DUAUtensilien.getBasisInstanzen(
				getVerbindung().getDataModel().getType(DUAKonstanten.TYP_MQ), getVerbindung(),
				getKonfigurationsBereiche());

		for (final SystemObject mqObjekt : alleMqObjImKB) {
			final MessQuerschnitt mq = MessQuerschnitt.getInstanz(mqObjekt);
			if (mq == null) {
				throw new DUAInitialisierungsException("Konfiguration von Messquerschnitt " + mq
						+ " konnte nicht vollstaendig ausgelesen werden");
			} else {
				messQuerschnitte.put(mqObjekt,
						new AggregationsMessQuerschnitt(getVerbindung(), mq));

				final Set<SystemObject> fsList = new HashSet<>();
				for (final FahrStreifen fs : mq.getFahrStreifen()) {
					fsMq.put(fs.getSystemObject(), mq.getSystemObject());
					fsList.add(fs.getSystemObject());
				}
				mqFs.put(mq.getSystemObject(), fsList);
			}
		}

		if (!AggregationLVE.zeitRaffer) {
			wecker.setWecker(this, getNaechstenWeckZeitPunkt());
		} else {
			/**
			 * Anmeldung auf alle Rohdaten, die hier verarbeitet werden sollen unter der
			 * Vorraussetzung, dass diese Daten im 1min-Intervall gesendet werden
			 */
			try {
				dav2 = new ClientDavConnection(getVerbindung().getClientDavParameters());
				dav2.connect();
				dav2.login();

				for (final SystemObject fs : fsMq.keySet()) {
					dav2.subscribeReceiver(this, fs,
							new DataDescription(
									dav2.getDataModel().getAttributeGroup(DUAKonstanten.ATG_KZD),
									dav2.getDataModel()
											.getAspect(DUAKonstanten.ASP_MESSWERTERSETZUNG)),
							ReceiveOptions.normal(), ReceiverRole.receiver());
				}
			} catch (final Exception e) {
				throw new DUAInitialisierungsException(
						"Testapplikation konnte nicht gestartet werden", e);
			}
		}
	}

	/**
	 * Startet diese Applikation nur fuer Testzwecke.
	 *
	 * @param dav
	 *            Verbindung zum Datenverteiler
	 * @param gueteFaktor
	 *            der Guetefaktor als Zeichenkette.
	 * @throws Exception
	 *             wenn die Initialisierung fehlschlaegt
	 */
	public void testStart(final ClientDavInterface dav, final String gueteFaktor) throws Exception {
		AggregationLVE.zeitRaffer = true;
		LOGGER.config("Applikation fuer Testzwecke gestartet");
		clearKomArguments();
		addKomArgument("-KonfigurationsBereichsPid=" + "kb.duaTestObjekte");
		addKomArgument("-gueteFaktor=" + gueteFaktor);

		initialize(dav);
	}

	@Override
	public void alarm() {
		final long jetzt = System.currentTimeMillis();

		for (final AggregationsMessQuerschnitt mq : messQuerschnitte.values()) {
			for (final AggregationsIntervall intervall : AggregationsIntervall.getInstanzen()) {
				if (intervall.isAggregationErforderlich(jetzt)) {
					mq.aggregiere(intervall.getAggregationZeitStempel(jetzt), intervall);
				}
			}
		}

		wecker.setWecker(this, getNaechstenWeckZeitPunkt());
	}

	/**
	 * Erfragt ein Aggregationsobjekt (nur fuer Testzwecke).
	 *
	 * @param obj
	 *            das assoziierte Systemobjekt
	 * @return ein Aggregationsobjekt (nur fuer Testzwecke).
	 */
	public AggregationsMessQuerschnitt getAggregationsObjekt(final SystemObject obj) {
		return messQuerschnitte.get(obj);
	}

	/**
	 * Erfragt den Zeitpunkt, der exakt 30s nach der Minute liegt, in der diese Methode aufgerufen
	 * wird (Absolute Zeit ohne Sommer- und Winterzeit).
	 *
	 * @return der Zeitpunkt, der exakt 30s nach der Minute liegt, in der diese Methode aufgerufen
	 *         wird
	 */
	private long getNaechstenWeckZeitPunkt() {
		final GregorianCalendar cal = new GregorianCalendar();
		cal.setTimeInMillis(System.currentTimeMillis());

		if (cal.get(Calendar.SECOND) >= (berechnungsOffset - 2)) {
			cal.add(Calendar.MINUTE, 1);
		}
		cal.set(Calendar.MILLISECOND, 0);
		cal.set(Calendar.SECOND, berechnungsOffset);

		return cal.getTimeInMillis();
	}

	/**
	 * Startet diese Applikation.
	 *
	 * @param argumente
	 *            Argumente der Kommandozeile
	 */
	public static void main(final String[] argumente) {
		StandardApplicationRunner.run(new AggregationLVE(), argumente);
	}

	@Override
	public double getStandardGueteFaktor() {
		return 0.9;
	}

	@Override
	public SWETyp getSWETyp() {
		return SWETyp.SWE_AGGREGATION_LVE;
	}

	@Override
	public void update(final ResultData[] resultate) {
		if (resultate != null) {
			for (final ResultData resultat : resultate) {
				if (resultat != null) {
					synchronized (dav2) {
						fsDataHist.put(resultat.getObject(), resultat);

						final SystemObject mq = fsMq.get(resultat.getObject());
						int fsZaehler = 0;
						for (final SystemObject fs : mqFs.get(mq)) {
							if (fsDataHist.get(fs) != null) {
								fsZaehler++;
							}
						}

						if (fsZaehler == mqFs.get(mq).size()) {
							/**
							 * fuer alle Fs des Mq sind Daten im Puffer
							 */
							loeseBerechnungAus(mq, resultat.getDataTime());
							loescheMqPuffer(mq);
						}
					}
				}
			}
		}
	}

	/**
	 * Leitet eine Berechnung mit allen bis zum uebergebenen Zeitpunkt eingetroffenen Daten fuer den
	 * uebergebenen Zeitpunkt aus.
	 *
	 * @param mqObj
	 *            der Messquerschnitt, fuer den die Berechnung (Aggregation) stattfinden soll
	 * @param jetzt
	 *            der Zeitpunkt der Berechnung
	 */
	private void loeseBerechnungAus(final SystemObject mqObj, final long jetzt) {
		synchronized (dav2) {
			AggregationsMessQuerschnitt mqZiel = null;
			for (final AggregationsMessQuerschnitt mq : messQuerschnitte.values()) {
				if (mq.getObjekt().equals(mqObj)) {
					mqZiel = mq;
					break;
				}
			}

			if (mqZiel != null) {
				for (final AggregationsIntervall intervall : AggregationsIntervall.getInstanzen()) {
					if (intervall.isAggregationErforderlich(jetzt)) {
						mqZiel.aggregiere(intervall.getAggregationZeitStempel(jetzt), intervall);
					}
				}
			} else {
				throw new RuntimeException("TEST: Kein MQ gefunden");
			}
		}
	}

	/**
	 * Löscht den aktuellen Fahrstreifen-Datenpuffer fuer einen bestimmten Messquerschnitt.
	 *
	 * @param mq
	 *            ein Messquerschnitt
	 */
	private void loescheMqPuffer(final SystemObject mq) {
		if ((mq != null) && (mqFs.get(mq) != null)) {
			for (final SystemObject fs : mqFs.get(mq)) {
				fsDataHist.put(fs, null);
			}
		}
	}

}
