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

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.bitctrl.Constants;

import de.bsvrz.dav.daf.main.ClientDavInterface;
import de.bsvrz.dav.daf.main.Data;
import de.bsvrz.dav.daf.main.ResultData;
import de.bsvrz.dav.daf.main.config.SystemObject;
import de.bsvrz.dua.guete.GWert;
import de.bsvrz.dua.guete.GueteException;
import de.bsvrz.dua.guete.GueteVerfahren;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAInitialisierungsException;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;
import de.bsvrz.sys.funclib.bitctrl.dua.av.DAVObjektAnmeldung;
import de.bsvrz.sys.funclib.bitctrl.dua.lve.FahrStreifen;
import de.bsvrz.sys.funclib.bitctrl.dua.lve.MessQuerschnitt;
import de.bsvrz.sys.funclib.debug.Debug;

/**
 * Aggregiert aus den fuer diesen Messquerschnitt (bzw. dessen Fahrstreifen) gespeicherten Daten die
 * Aggregationswerte aller Aggregationsstufen aus der jeweils darunterliegenden Stufe bzw. aus den
 * messwertersetzten Fahrstreifendaten fuer die Basisstufe
 *
 * @author BitCtrl Systems GmbH, Thierfelder
 */
public final class AggregationsMessQuerschnitt extends AbstraktAggregationsObjekt {

	private static final Debug LOGGER = Debug.getLogger();

	/** der hier betrachtete Messquerschnitt. */
	private final MessQuerschnitt mq;

	/**
	 * Menge der Fahrstreifen, die an diesem Messquerschnitt konfiguriert sind.
	 */
	private final Map<SystemObject, AggregationsFahrStreifen> fsMenge = new HashMap<>();

	/**
	 * Standardkonstruktor.
	 *
	 * @param dav
	 *            Verbindung zum Datenverteiler
	 * @param mq
	 *            der Messquerschnitt dessen Aggregationsdaten ermittelt werden sollen
	 * @throws DUAInitialisierungsException
	 *             wenn dieses Objekt nicht vollstaendig (mit allen Unterobjekten) initialisiert
	 *             werden konnte
	 */
	public AggregationsMessQuerschnitt(final ClientDavInterface dav, final MessQuerschnitt mq)
			throws DUAInitialisierungsException {
		super(dav, mq.getSystemObject());
		this.mq = mq;

		datenPuffer = new AggregationsPufferMenge(dav, mq.getSystemObject());
		final Set<DAVObjektAnmeldung> anmeldungen = new TreeSet<>();
		for (final AggregationsIntervall intervall : AggregationsIntervall.getInstanzen()) {
			try {
				anmeldungen.add(new DAVObjektAnmeldung(mq.getSystemObject(),
						intervall.getDatenBeschreibung(false)));
			} catch (final Exception e) {
				throw new DUAInitialisierungsException(
						"Messquerschnitt " + mq + " konnte nicht initialisiert werden", e);
			}
		}
		sender.modifiziereObjektAnmeldung(anmeldungen);

		for (final FahrStreifen fs : mq.getFahrStreifen()) {
			fsMenge.put(fs.getSystemObject(), new AggregationsFahrStreifen(dav, fs));
		}
	}

	@Override
	public void aggregiere(final long zeitStempel, final AggregationsIntervall intervall) {
		/**
		 * Aggregiere alle Werte der untergeordneten Fahrstreifen
		 */
		for (final AggregationsFahrStreifen fs : fsMenge.values()) {
			fs.aggregiere(zeitStempel, intervall);
		}

		if (isBerechnungNotwendig(zeitStempel, intervall)) {
			final long begin = zeitStempel;
			long ende = zeitStempel + intervall.getIntervall();
			if (intervall.isDTVorTV()) {
				final GregorianCalendar cal = new GregorianCalendar();
				cal.setTimeInMillis(zeitStempel);

				if (intervall.equals(AggregationsIntervall.aGGDTVTAG)) {
					cal.add(Calendar.DAY_OF_YEAR, 1);
					ende = cal.getTimeInMillis();
				} else if (intervall.equals(AggregationsIntervall.aGGDTVMONAT)) {
					cal.add(Calendar.MONTH, 1);
					ende = cal.getTimeInMillis();
				} else if (intervall.equals(AggregationsIntervall.aGGDTVJAHR)) {
					cal.add(Calendar.YEAR, 1);
					ende = cal.getTimeInMillis();
				}
			}

			final Collection<AggregationsDatum> mqDaten = datenPuffer.getDatenFuerZeitraum(begin,
					ende, intervall);

			Data nutzDatum = null;

			if (intervall.equals(AggregationsIntervall.aGGDTVTAG)) {
				if (!mqDaten.isEmpty()) {
					/**
					 * Daten koennen aus naechstkleinerem Intervall aggregiert werden
					 */
					nutzDatum = dav
							.createData(intervall.getDatenBeschreibung(false).getAttributeGroup());
					for (final AggregationsAttribut attribut : AggregationsAttribut
							.getInstanzen()) {
						if (!attribut.isGeschwindigkeitsAttribut()) {
							aggregiereSumme(attribut, nutzDatum, mqDaten, zeitStempel, intervall);
						}
					}
				} else {
					LOGGER.warning(intervall + " fuer " + objekt
							+ " kann nicht berechnet werden, da keine Basisdaten (Intervall: "
							+ intervall.getVorgaenger() + ") zur Verfuegung stehen");
				}
			} else {
				if (mqDaten.isEmpty()) {
					if (!intervall.isDTVorTV()) {
						/**
						 * Aggregiere Basisintervall aus messwertersetzten Fahrstreifendaten
						 */
						final Map<AggregationsFahrStreifen, Collection<AggregationsDatum>> fsDaten = new HashMap<>();

						for (final AggregationsFahrStreifen fs : fsMenge.values()) {
							final Collection<AggregationsDatum> daten = fs.getPuffer()
									.getPuffer(null).getDatenFuerZeitraum(begin, ende);
							fsDaten.put(fs, daten);
						}

						boolean kannBasisIntervallBerechnen = false;
						for (final Collection<AggregationsDatum> daten : fsDaten.values()) {
							if (!daten.isEmpty()) {
								kannBasisIntervallBerechnen = true;
								break;
							}
						}

						if (kannBasisIntervallBerechnen) {
							nutzDatum = dav.createData(
									intervall.getDatenBeschreibung(false).getAttributeGroup());
							aggregiereBasisDatum(nutzDatum, fsDaten, zeitStempel, intervall);
						}

					}
				} else {
					/**
					 * Daten koennen aus naechstkleinerem Intervall aggregiert werden
					 */
					nutzDatum = dav
							.createData(intervall.getDatenBeschreibung(false).getAttributeGroup());
					for (final AggregationsAttribut attribut : AggregationsAttribut
							.getInstanzen()) {
						aggregiereMittel(attribut, nutzDatum, mqDaten, zeitStempel, intervall);
					}
				}
			}

			final ResultData resultat = new ResultData(mq.getSystemObject(),
					intervall.getDatenBeschreibung(false), zeitStempel, nutzDatum);

			if (resultat.getData() != null) {
				fuelleRest(resultat, intervall);
				datenPuffer.aktualisiere(resultat);
			}

			sende(resultat);
		}
	}

	/**
	 * Erfragt ein Aggregationsobjekt unterhalb dieses Objektes (nur fuer Testzwecke).
	 *
	 * @param obj
	 *            das assoziierte Systemobjekt
	 * @return ein Aggregationsobjekt unterhalb dieses Objektes (nur fuer Testzwecke).
	 */
	public AggregationsFahrStreifen getAggregationsObjekt(final SystemObject obj) {
		return fsMenge.get(obj);
	}

	/**
	 * Aggregiert das erste Aggregationsintervall fuer diesen Messquerschnitt aus Basis der
	 * uebergebenen messwertersetzten Fahrstreifendaten.
	 *
	 * @param nutzDatum
	 *            ein veraenderbares Nutzdatum
	 * @param fsDaten
	 *            die Datenpuffer mit den messwertersetzten Fahrstreifendaten der mit diesem
	 *            Messquerschnitt assoziierten Fahrstreifen
	 * @param zeitStempel
	 *            der Zeitstempel (Start)
	 * @param intervall
	 *            das Aggregationsintervall
	 */
	private void aggregiereBasisDatum(final Data nutzDatum,
			final Map<AggregationsFahrStreifen, Collection<AggregationsDatum>> fsDaten,
			final long zeitStempel, final AggregationsIntervall intervall) {

		for (final AggregationsAttribut attribut : AggregationsAttribut.getInstanzen()) {
			boolean interpoliert = false;
			boolean nichtErfasst = false;
			final AggregationsAttributWert exportWert = new AggregationsAttributWert(attribut,
					DUAKonstanten.NICHT_ERMITTELBAR_BZW_FEHLERHAFT, 0);
			final Collection<GWert> gueteWerte = new ArrayList<>();

			if (attribut.isGeschwindigkeitsAttribut()) {
				long summe = 0;
				long anzahl = 0;

				for (final AggregationsFahrStreifen fahrStreifen : fsDaten.keySet()) {
					final Collection<AggregationsDatum> fsQuellDatum = fsDaten.get(fahrStreifen);

					long summeInFs = 0;
					long anzahlInFs = 0;
					final Collection<GWert> gueteWerteInFs = new ArrayList<>();
					for (final AggregationsDatum basisDatum : fsQuellDatum) {
						final AggregationsAttributWert basisWert = basisDatum.getWert(attribut);
						if ((basisWert != null) && (basisWert.getWert() >= 0)) {
							anzahlInFs++;
							summeInFs += basisWert.getWert();
							gueteWerteInFs.add(basisWert.getGuete());
							interpoliert |= basisWert.isInterpoliert();
							nichtErfasst |= basisWert.isNichtErfasst();
						}
					}

					if (anzahlInFs > 0) {
						anzahl++;
						summe += Math.round((double) summeInFs / (double) anzahlInFs);
					}
					try {
						gueteWerte.add(GueteVerfahren.summe(gueteWerteInFs.toArray(new GWert[0])));
					} catch (final GueteException e) {
						LOGGER.error("Guete von " + fahrStreifen.getObjekt() + " fuer " + attribut
								+ " konnte nicht berechnet werden", e);
						e.printStackTrace();
					}
				}

				if (anzahl > 0) {
					exportWert.setWert(Math.round((double) summe / (double) anzahl));
				}
			} else {
				double summe = 0;

				for (final Collection<AggregationsDatum> fsQuellDatum : fsDaten.values()) {
					double erfassungsIntervall = 0;
					double zwischenSumme = 0;
					for (final AggregationsDatum basisDatum : fsQuellDatum) {
						final AggregationsAttributWert basisWert = basisDatum.getWert(attribut);
						if ((basisWert != null) && (basisWert.getWert() >= 0)) {
							erfassungsIntervall += basisDatum.getT();
							zwischenSumme += basisWert.getWert();
							gueteWerte.add(basisWert.getGuete());
							interpoliert |= basisWert.isInterpoliert();
							nichtErfasst |= basisWert.isNichtErfasst();
						}
					}
					if (erfassungsIntervall > 0) {
						summe += (zwischenSumme * Constants.MILLIS_PER_HOUR) / erfassungsIntervall;
					}
				}

				if (gueteWerte.size() > 0) {
					exportWert.setWert(Math.round(summe));
				}
			}

			if (gueteWerte.size() > 0) {
				exportWert.setInterpoliert(interpoliert);
				if (AggregationLVE.NICHT_ERFASST) {
					exportWert.setNichtErfasst(nichtErfasst);
				}
				try {
					exportWert.setGuete(GueteVerfahren.summe(gueteWerte.toArray(new GWert[0])));
				} catch (final GueteException e) {
					LOGGER.error("Guete von " + objekt + " fuer " + attribut
							+ " konnte nicht berechnet werden", e);
					e.printStackTrace();
				}
			}

			exportWert.exportiere(nutzDatum, isFahrstreifen());
		}
	}

	@Override
	protected void finalize() throws Throwable {
		LOGGER.warning("Der MQ " + mq + " wird nicht mehr aggregiert");
		super.finalize();
	}

	@Override
	public String toString() {
		return mq.toString();
	}

	@Override
	protected boolean isFahrstreifen() {
		return false;
	}

}
