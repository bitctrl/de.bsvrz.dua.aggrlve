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
package de.bsvrz.dua.aggrlve.vmq;

import java.util.HashMap;
import java.util.Map;

import de.bsvrz.dav.daf.main.ClientDavInterface;
import de.bsvrz.dav.daf.main.ClientReceiverInterface;
import de.bsvrz.dav.daf.main.ClientSenderInterface;
import de.bsvrz.dav.daf.main.Data;
import de.bsvrz.dav.daf.main.DataDescription;
import de.bsvrz.dav.daf.main.DataNotSubscribedException;
import de.bsvrz.dav.daf.main.OneSubscriptionPerSendData;
import de.bsvrz.dav.daf.main.ReceiveOptions;
import de.bsvrz.dav.daf.main.ReceiverRole;
import de.bsvrz.dav.daf.main.ResultData;
import de.bsvrz.dav.daf.main.SendSubscriptionNotConfirmed;
import de.bsvrz.dav.daf.main.SenderRole;
import de.bsvrz.dav.daf.main.config.Aspect;
import de.bsvrz.dav.daf.main.config.AttributeGroup;
import de.bsvrz.dav.daf.main.config.SystemObject;
import de.bsvrz.dua.guete.GWert;
import de.bsvrz.dua.guete.GueteException;
import de.bsvrz.dua.guete.GueteVerfahren;
import de.bsvrz.sys.funclib.bitctrl.daf.DaVKonstanten;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAUtensilien;
import de.bsvrz.sys.funclib.bitctrl.dua.MesswertUnskaliert;
import de.bsvrz.sys.funclib.debug.Debug;

/**
 * Abstrakte Implementierung zur Verwaltung der Daten eines virtuellen Messquerschnitts, dessen
 * Aggregationsdaten gebildet werden sollen.
 *
 * @author BitCtrl Systems GmbH, Uwe Peuker
 * @version $Id$
 */
public abstract class AbstractAggregationsVmq implements ClientReceiverInterface,
ClientSenderInterface {

	private static Debug logger = Debug.getLogger();

	/** das VMQ-Objekt. */
	private final SystemObject vmq;
	/** die Menge der Bestandteile des virtuellen MQ. */
	private final Map<SystemObject, VmqDataPart> mqParts = new HashMap<SystemObject, VmqDataPart>();

	/** die verwendete Datenverteilerverbindung. */
	private ClientDavInterface dav;

	/** die Parameterattributgruppe für die Grenzwerte der Aggregation. */
	private AttributeGroup paramAtg;
	/** die Parameterdaten für die Grenzwerte der Aggregation. */
	private Data paramData;

	/**
	 * das letzte veröffentlichte Datum (wird zur Ersatzwertbildung herangezogen).
	 */
	private ResultData letztesErgebnis;

	/**
	 * Konstruktor, erzeugt eine Instanz für das übergebene MQ-Objekt.
	 *
	 * @param obj
	 *            das Objekt
	 */
	public AbstractAggregationsVmq(final SystemObject obj) {
		vmq = obj;
	}

	/**
	 * Berechnet die Bemessungsdichte (<code>KB</code>) analog SE-02.00.00.00.00-AFo-4.0 S.120f.
	 *
	 * @param analyseDatum
	 *            das Datum in das die Daten eingetragen werden sollen
	 */
	protected final void berechneBemessungsdichte(final Data analyseDatum) {
		final MesswertUnskaliert kbAnalyse = new MesswertUnskaliert("KB");
		final MesswertUnskaliert qbWert = new MesswertUnskaliert("QB", analyseDatum);
		final MesswertUnskaliert vKfz = new MesswertUnskaliert("VKfz", analyseDatum);

		if (qbWert.isFehlerhaftBzwImplausibel() || vKfz.isFehlerhaftBzwImplausibel()) {
			kbAnalyse.setWertUnskaliert(DUAKonstanten.NICHT_ERMITTELBAR_BZW_FEHLERHAFT);
		} else {
			if ((vKfz.getWertUnskaliert() == 0)
					|| (vKfz.getWertUnskaliert() == DUAKonstanten.NICHT_ERMITTELBAR)) {

				if ((paramData != null) && (letztesErgebnis != null)
						&& (letztesErgebnis.getData() != null)) {

					final MesswertUnskaliert kbTMinus1 = new MesswertUnskaliert("KB",
							letztesErgebnis.getData());
					if (kbTMinus1.getWertUnskaliert() >= 0) {
						if (kbTMinus1.getWertUnskaliert() >= paramData.getItem("KB")
								.getUnscaledValue("Grenz").longValue()) {
							kbAnalyse.setWertUnskaliert(paramData.getItem("KB")
									.getUnscaledValue("Max").longValue());
						} else {
							kbAnalyse.setWertUnskaliert(0);
						}
					} else {
						kbAnalyse.setWertUnskaliert(DUAKonstanten.NICHT_ERMITTELBAR_BZW_FEHLERHAFT);
					}
				} else {
					kbAnalyse.setWertUnskaliert(DUAKonstanten.NICHT_ERMITTELBAR_BZW_FEHLERHAFT);
				}

			} else {
				// normal berechnen
				if (qbWert.getWertUnskaliert() == DUAKonstanten.NICHT_ERMITTELBAR) {
					kbAnalyse.setWertUnskaliert(DUAKonstanten.NICHT_ERMITTELBAR);
				} else {
					final long kbWert = Math.round((double) qbWert.getWertUnskaliert()
							/ (double) vKfz.getWertUnskaliert());

					if (DUAUtensilien.isWertInWerteBereich(
							analyseDatum.getItem("KB").getItem("Wert"), kbWert)) {
						final boolean interpoliert = qbWert.isInterpoliert()
								|| vKfz.isInterpoliert();
						GWert kbGuete = null;
						try {
							kbGuete = GueteVerfahren.quotient(new GWert(analyseDatum, "QB"),
									new GWert(analyseDatum, "VKfz"));
						} catch (final GueteException e) {
							Debug.getLogger().error(
									"Guete-Index fuer KB nicht berechenbar in " + analyseDatum, e);
							e.printStackTrace();
						}

						kbAnalyse.setWertUnskaliert(kbWert);
						kbAnalyse.setInterpoliert(interpoliert);
						if (kbGuete != null) {
							kbAnalyse.getGueteIndex().setWert(kbGuete.getIndexUnskaliert());
							kbAnalyse.setVerfahren(kbGuete.getVerfahren().getCode());
						}
					} else {
						kbAnalyse.setWertUnskaliert(DUAKonstanten.NICHT_ERMITTELBAR_BZW_FEHLERHAFT);
					}
				}
			}
		}

		kbAnalyse.kopiereInhaltNachModifiziereIndex(analyseDatum);
	}

	/**
	 * Berechnet die Bemessungsverkehrsstaerke (<code>QB</code>) analog SE-02.00.00.00.00-AFo-4.0
	 * S.120f.
	 *
	 * @param analyseDatum
	 *            das Datum in das die Daten eingetragen werden sollen
	 */
	protected final void berechneBemessungsVerkehrsstaerke(final Data analyseDatum) {
		final MesswertUnskaliert qbAnalyse = new MesswertUnskaliert("QB");
		final MesswertUnskaliert vPkw = new MesswertUnskaliert("VPkw", analyseDatum);
		final MesswertUnskaliert vLkw = new MesswertUnskaliert("VLkw", analyseDatum);
		final MesswertUnskaliert qPkw = new MesswertUnskaliert("QPkw", analyseDatum);
		final MesswertUnskaliert qLkw = new MesswertUnskaliert("QLkw", analyseDatum);

		if (vLkw.isFehlerhaftBzwImplausibel() || qLkw.isFehlerhaftBzwImplausibel()
				|| (vLkw.getWertUnskaliert() == DUAKonstanten.NICHT_ERMITTELBAR)
				|| (qLkw.getWertUnskaliert() == DUAKonstanten.NICHT_ERMITTELBAR)) {
			if ((vPkw.getWertUnskaliert() == DUAKonstanten.NICHT_ERMITTELBAR)
					|| (qPkw.getWertUnskaliert() == DUAKonstanten.NICHT_ERMITTELBAR)) {
				qbAnalyse.setWertUnskaliert(DUAKonstanten.NICHT_ERMITTELBAR);
			} else {
				long qbWert = DUAKonstanten.NICHT_ERMITTELBAR;
				GWert qbGuete = GueteVerfahren.STD_FEHLERHAFT_BZW_NICHT_ERMITTELBAR;

				qbWert = qPkw.getWertUnskaliert();
				if (DUAUtensilien.isWertInWerteBereich(analyseDatum.getItem("QB").getItem("Wert"),
						qbWert)) {
					qbGuete = new GWert(analyseDatum, "QPkw");

					qbAnalyse.setWertUnskaliert(qbWert);
					qbAnalyse.setInterpoliert(qPkw.isInterpoliert());
					qbAnalyse.getGueteIndex().setWert(qbGuete.getIndexUnskaliert());
					qbAnalyse.setVerfahren(qbGuete.getVerfahren().getCode());
				} else {
					qbAnalyse.setWertUnskaliert(DUAKonstanten.NICHT_ERMITTELBAR_BZW_FEHLERHAFT);
				}
			}
		} else {
			if (vPkw.isFehlerhaftBzwImplausibel() || qPkw.isFehlerhaftBzwImplausibel()) {
				qbAnalyse.setWertUnskaliert(DUAKonstanten.NICHT_ERMITTELBAR_BZW_FEHLERHAFT);
			} else if ((vPkw.getWertUnskaliert() == DUAKonstanten.NICHT_ERMITTELBAR)
					|| (qPkw.getWertUnskaliert() == DUAKonstanten.NICHT_ERMITTELBAR)) {
				qbAnalyse.setWertUnskaliert(DUAKonstanten.NICHT_ERMITTELBAR);
			} else {
				if (paramData != null) {
					final double k1 = paramData.getItem("fl").getScaledValue("k1").doubleValue();
					final double k2 = paramData.getItem("fl").getScaledValue("k2").doubleValue();

					double fL;
					if (vPkw.getWertUnskaliert() <= vLkw.getWertUnskaliert()) {
						fL = k1;
					} else {
						fL = k1 + (k2 * (vPkw.getWertUnskaliert() - vLkw.getWertUnskaliert()));
					}

					long qbWert = DUAKonstanten.NICHT_ERMITTELBAR;
					GWert qbGuete = GueteVerfahren.STD_FEHLERHAFT_BZW_NICHT_ERMITTELBAR;

					qbWert = qPkw.getWertUnskaliert() + Math.round(fL * qLkw.getWertUnskaliert());
					if (DUAUtensilien.isWertInWerteBereich(
							analyseDatum.getItem("QB").getItem("Wert"), qbWert)) {
						final GWert qPkwGuete = new GWert(analyseDatum, "QPkw");
						final GWert qLkwGuete = new GWert(analyseDatum, "QLkw");

						try {
							qbGuete = GueteVerfahren.summe(qPkwGuete,
									GueteVerfahren.gewichte(qLkwGuete, fL));
						} catch (final GueteException e) {
							Debug.getLogger().error(
									"Guete-Index fuer QB nicht berechenbar in " + analyseDatum, e);
							e.printStackTrace();
						}

						qbAnalyse.setWertUnskaliert(qbWert);
						qbAnalyse.setInterpoliert(qPkw.isInterpoliert() || qLkw.isInterpoliert());
						if (qbGuete != null) {
							qbAnalyse.getGueteIndex().setWert(qbGuete.getIndexUnskaliert());
							qbAnalyse.setVerfahren(qbGuete.getVerfahren().getCode());
						}
					} else {
						qbAnalyse.setWertUnskaliert(DUAKonstanten.NICHT_ERMITTELBAR_BZW_FEHLERHAFT);
					}

				} else {
					qbAnalyse.setWertUnskaliert(DUAKonstanten.NICHT_ERMITTELBAR_BZW_FEHLERHAFT);
				}
			}

		}

		qbAnalyse.kopiereInhaltNachModifiziereIndex(analyseDatum);
	}

	/**
	 * Berechnet die Verkehrsstärken (<code>Kxxx</code>) analog SE-02.00.00.00.00-AFo-4.0 S.119f.
	 *
	 * @param analyseDatum
	 *            das Datum in das die Daten eingetragen werden sollen
	 * @param attName
	 *            der Attributname des Verkehrswertes, der berechnet werden soll
	 */
	protected final void berechneDichte(final Data analyseDatum, final String attName) {
		final MesswertUnskaliert kAnalyse = new MesswertUnskaliert("K" + attName);
		final MesswertUnskaliert qWert = new MesswertUnskaliert("Q" + attName, analyseDatum);
		final MesswertUnskaliert vWert = new MesswertUnskaliert("V" + attName, analyseDatum);

		if (qWert.isFehlerhaftBzwImplausibel() || vWert.isFehlerhaftBzwImplausibel()) {
			kAnalyse.setWertUnskaliert(DUAKonstanten.NICHT_ERMITTELBAR_BZW_FEHLERHAFT);
		} else {
			if ((vWert.getWertUnskaliert() == 0)
					|| (vWert.getWertUnskaliert() == DUAKonstanten.NICHT_ERMITTELBAR)) {
				if ((paramData != null) && (letztesErgebnis != null)
						&& (letztesErgebnis.getData() != null)) {
					long grenz = -1;
					long max = -1;

					if (attName.startsWith("K")) {
						/* Kfz */
						grenz = paramData.getItem("KKfz").getUnscaledValue("Grenz").longValue();
						max = paramData.getItem("KKfz").getUnscaledValue("Max").longValue();
					} else if (attName.startsWith("L")) {
						/* Lkw */
						grenz = paramData.getItem("KLkw").getUnscaledValue("Grenz").longValue();
						max = paramData.getItem("KLkw").getUnscaledValue("Max").longValue();
					} else {
						/* Pkw */
						grenz = paramData.getItem("KPkw").getUnscaledValue("Grenz").longValue();
						max = paramData.getItem("KPkw").getUnscaledValue("Max").longValue();
					}

					final MesswertUnskaliert kTMinus1 = new MesswertUnskaliert("K" + attName,
							letztesErgebnis.getData());
					if (kTMinus1.isFehlerhaftBzwImplausibel()) {
						kAnalyse.setWertUnskaliert(DUAKonstanten.NICHT_ERMITTELBAR_BZW_FEHLERHAFT);
					} else if (kTMinus1.getWertUnskaliert() == DUAKonstanten.NICHT_ERMITTELBAR) {
						kAnalyse.setWertUnskaliert(DUAKonstanten.NICHT_ERMITTELBAR);
					} else {
						if (kTMinus1.getWertUnskaliert() < grenz) {
							kAnalyse.setWertUnskaliert(0);
						} else {
							kAnalyse.setWertUnskaliert(max);
						}
					}
				} else {
					kAnalyse.setWertUnskaliert(DUAKonstanten.NICHT_ERMITTELBAR_BZW_FEHLERHAFT);
				}

			} else {
				if (qWert.getWertUnskaliert() == DUAKonstanten.NICHT_ERMITTELBAR) {
					kAnalyse.setWertUnskaliert(DUAKonstanten.NICHT_ERMITTELBAR);
				} else {
					final long kWert = Math.round((double) qWert.getWertUnskaliert()
							/ (double) vWert.getWertUnskaliert());
					if (DUAUtensilien.isWertInWerteBereich(analyseDatum.getItem("K" + attName)
							.getItem("Wert"), kWert)) {
						final boolean interpoliert = qWert.isInterpoliert()
								|| vWert.isInterpoliert();
						GWert kGuete = null;

						try {
							kGuete = GueteVerfahren.quotient(
									new GWert(analyseDatum, "Q" + attName), new GWert(analyseDatum,
											"V" + attName));
						} catch (final GueteException e) {
							Debug.getLogger().error(
									"Guete-Index fuer K" + attName + " nicht berechenbar", e);
							e.printStackTrace();
						}

						kAnalyse.setWertUnskaliert(kWert);
						kAnalyse.setInterpoliert(interpoliert);
						if (kGuete != null) {
							kAnalyse.getGueteIndex().setWert(kGuete.getIndexUnskaliert());
							kAnalyse.setVerfahren(kGuete.getVerfahren().getCode());
						}
					} else {
						kAnalyse.setWertUnskaliert(DUAKonstanten.NICHT_ERMITTELBAR_BZW_FEHLERHAFT);
					}
				}
			}
		}

		kAnalyse.kopiereInhaltNachModifiziereIndex(analyseDatum);
	}

	/**
	 * Berechnet (<code>ALkw</code>) analog SE-02.00.00.00.00-AFo-4.0 S.119f.
	 *
	 * @param analyseDatum
	 *            das Datum in das die Daten eingetragen werden sollen
	 */
	protected final void berechneLkwAnteil(final Data analyseDatum) {
		final MesswertUnskaliert aLkwAnalyse = new MesswertUnskaliert("ALkw");
		final MesswertUnskaliert qLkw = new MesswertUnskaliert("QLkw", analyseDatum);
		final MesswertUnskaliert qKfz = new MesswertUnskaliert("QKfz", analyseDatum);

		if (qLkw.isFehlerhaftBzwImplausibel() || qKfz.isFehlerhaftBzwImplausibel()) {
			aLkwAnalyse.setWertUnskaliert(DUAKonstanten.NICHT_ERMITTELBAR_BZW_FEHLERHAFT);
		} else if ((qLkw.getWertUnskaliert() == DUAKonstanten.NICHT_ERMITTELBAR)
				|| (qKfz.getWertUnskaliert() == DUAKonstanten.NICHT_ERMITTELBAR)) {
			aLkwAnalyse.setWertUnskaliert(DUAKonstanten.NICHT_ERMITTELBAR);
		} else {
			GWert aLkwGuete = null;
			final long aLkwWert = Math.round(((double) qLkw.getWertUnskaliert() / (double) qKfz
					.getWertUnskaliert()) * 100.0);

			if (DUAUtensilien.isWertInWerteBereich(analyseDatum.getItem("ALkw").getItem("Wert"),
					aLkwWert)) {
				try {
					aLkwGuete = GueteVerfahren.quotient(new GWert(analyseDatum, "QLkw"), new GWert(
							analyseDatum, "QKfz"));
				} catch (final GueteException e) {
					Debug.getLogger().error(
							"Guete-Index fuer ALkw nicht berechenbar in " + analyseDatum, e);
					e.printStackTrace();
				}

				aLkwAnalyse.setWertUnskaliert(aLkwWert);
				aLkwAnalyse.setInterpoliert(qLkw.isInterpoliert() || qKfz.isInterpoliert());
				if (aLkwGuete != null) {
					aLkwAnalyse.getGueteIndex().setWert(aLkwGuete.getIndexUnskaliert());
					aLkwAnalyse.setVerfahren(aLkwGuete.getVerfahren().getCode());
				}
			} else {
				aLkwAnalyse.setWertUnskaliert(DUAKonstanten.NICHT_ERMITTELBAR_BZW_FEHLERHAFT);
			}
		}

		aLkwAnalyse.kopiereInhaltNachModifiziereIndex(analyseDatum);
	}

	/**
	 * berechnet den zusammengefassten Datensatz für einen virtuellen MQ aus den übergebenen
	 * aktuellen Daten.
	 *
	 * @param dataList
	 *            die aktuellen Daten
	 * @return der Datensatz oder null, wenn keiner gebildet werden konnte
	 */
	protected abstract ResultData calculateResultData(Map<SystemObject, ResultData> dataList);

	/**
	 * löscht alle zwischengespeicherten Daten die nicht jünger als der übergebene Zeitpunkt sind.
	 *
	 * @param dataTime
	 *            der Zeitpunkt
	 * @param aspect
	 *            der Aspekt für den die daten gelöscht werden sollen
	 */
	private void clearData(final long dataTime, final Aspect aspect) {
		for (final VmqDataPart part : getMqParts().values()) {
			part.clear(dataTime, aspect);
		}
	}

	@Override
	public void dataRequest(final SystemObject object, final DataDescription dataDescription,
			final byte state) {
		Debug.getLogger().finest("Sendesteuerung wird nicht unterstützt");
	}

	/**
	 * liefert die verwendete Datenverteilerverbindung.
	 *
	 * @return die Verbindung
	 */
	protected ClientDavInterface getDav() {
		return dav;
	}

	/**
	 * liefert das letzte publizierte Ergebnis.
	 *
	 * @return das Ergebnis oder <code>null</code>, wenn noch keines publiziert wurde
	 */
	protected ResultData getLetztesErgebnis() {
		return letztesErgebnis;
	}

	/**
	 * liefert die Bestandteile des virtuellen MQ.
	 *
	 * @return die Betsandteile
	 */
	protected Map<SystemObject, VmqDataPart> getMqParts() {
		return mqParts;
	}

	/**
	 * liefert das VMQ-Objekt.
	 *
	 * @return das Objekt
	 */
	public SystemObject getVmq() {
		return vmq;
	}

	/**
	 * initialisiert die Datenverteilerverbindung (Anmeldung der erforderlichen
	 * Datenspezifikationen.
	 *
	 * @param connection
	 *            die Datenverteilerverbindung
	 */
	public final void init(final ClientDavInterface connection) {
		dav = connection;

		paramAtg = connection.getDataModel()
				.getAttributeGroup("atg.verkehrsDatenKurzZeitAnalyseMq");
		connection.subscribeReceiver(this, vmq, new DataDescription(paramAtg, connection
				.getDataModel().getAspect(DaVKonstanten.ASP_PARAMETER_SOLL)), ReceiveOptions
				.normal(), ReceiverRole.receiver());

		for (final Aspect asp : VMqAggregator.getInstance().getSupportedAspects()) {
			final DataDescription dataDescription = new DataDescription(VMqAggregator.getInstance()
					.getSrcAtg(), asp);
			connection.subscribeReceiver(this, getMqParts().keySet(), dataDescription,
					ReceiveOptions.normal(), ReceiverRole.receiver());
			try {
				connection.subscribeSender(this, vmq, dataDescription, SenderRole.source());
			} catch (final OneSubscriptionPerSendData e) {
				Debug.getLogger().error(vmq + ": " + e.getLocalizedMessage());
			}
		}
	}

	@Override
	public boolean isRequestSupported(final SystemObject object,
			final DataDescription dataDescription) {
		Debug.getLogger().finest("Sendesteuerung wird nicht unterstützt");
		return false;
	}

	/**
	 * Ermittelt zu versendende Daten und versendet diese gegebenenfalls.
	 *
	 * @return das Ergebnis, <code>true</code>, wenn Daten versendet wurden
	 */
	public boolean sendNextCompletedResult() {

		boolean result = false;
		for (final Aspect aspect : VMqAggregator.getInstance().getSupportedAspects()) {

			long currentTime = 0;
			final Map<SystemObject, ResultData> dataList = new HashMap<SystemObject, ResultData>();
			boolean nextTime = false;

			for (final VmqDataPart part : getMqParts().values()) {
				final ResultData data = part.getNextValue(aspect);
				if (data != null) {
					final long dataTime = data.getDataTime();
					if (currentTime == 0) {
						currentTime = dataTime;
						dataList.put(data.getObject(), data);
					} else {
						if (dataTime > currentTime) {
							nextTime = true;
						} else if (dataTime < currentTime) {
							nextTime = true;
							currentTime = dataTime;
							dataList.clear();
							dataList.put(data.getObject(), data);
						} else {
							dataList.put(data.getObject(), data);
						}
					}
				}
			}

			if ((dataList.size() == getMqParts().size()) || nextTime) {
				final ResultData resultData = calculateResultData(dataList);
				if (resultData != null) {
					result = true;
					letztesErgebnis = resultData;
					try {
						Debug.getLogger()
						.finer(vmq + "(" + aspect + ") Sende Daten: " + resultData);
						dav.sendData(resultData);
					} catch (final DataNotSubscribedException ex) {
						logger.warning("Ein Datum konnte nicht versendet werden: " + resultData, ex);
					} catch (final SendSubscriptionNotConfirmed ex) {
						logger.warning("Ein Datum konnte nicht versendet werden: " + resultData, ex);
					}
				}
				clearData(currentTime, aspect);
			}
		}

		return result;
	}

	@Override
	public final void update(final ResultData[] results) {

		boolean check = false;
		for (final ResultData result : results) {

			if (result.getDataDescription().getAttributeGroup().equals(paramAtg)) {
				paramData = result.getData();
			} else {
				final SystemObject object = result.getObject();
				final VmqDataPart vmqDataPart = mqParts.get(object);
				if (vmqDataPart != null) {
					check = true;
					vmqDataPart.push(result);
				}
			}
		}

		if (check) {
			VMqAggregator.getInstance().push(this);
		}
	}

}
