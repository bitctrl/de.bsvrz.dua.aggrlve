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

import java.util.Map;
import java.util.Map.Entry;

import de.bsvrz.dav.daf.main.Data;
import de.bsvrz.dav.daf.main.Data.ReferenceValue;
import de.bsvrz.dav.daf.main.DataDescription;
import de.bsvrz.dav.daf.main.ResultData;
import de.bsvrz.dav.daf.main.config.SystemObject;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;
import de.bsvrz.sys.funclib.bitctrl.dua.MesswertUnskaliert;
import de.bsvrz.sys.funclib.debug.Debug;

/**
 * Repräsentation eines virtuellen MQ im Standardverfahren zur Berechnung der Aggregationswerte.
 *
 * @author BitCtrl Systems GmbH, Uwe Peuker
 */
public class StandardAggregationsVmq extends AbstractAggregationsVmq {

	private static final Debug LOGGER = Debug.getLogger();
	/** Konstante für die MQ-Lage: DAVOR. */
	private static final int VOR = 0;
	/** Konstante für die MQ-Lage: MITTE. */
	private static final int MITTE = 1;
	/** Konstante für die MQ-Lage: DANACH. */
	private static final int NACH = 2;
	// private static final int EINFAHRT = 3;
	// private static final int AUSFAHRT = 4;

	/** die konfigurierte Lage des MQ. */
	private int lage;
	/** der reale MQ davor. */
	private SystemObject mqVor;
	/** der reale MQ in der Mitte der Anschlussstelle/des Kreuzes. */
	private SystemObject mqMitte;
	/** der reale MQ danach. */
	private SystemObject mqNach;
	/** der reale MQ in der Ausfahrt. */
	private SystemObject mqAusfahrt;
	/** der reale MQ in der Einfahrt. */
	private SystemObject mqEinfahrt;

	/**
	 * Konstruktor, erzeugt eine Instanz der Klasse für das übergebene Systemobjekt mit den
	 * angegebenen Konfigurationsdaten.
	 *
	 * @param obj
	 *            das MQ-Objekt
	 * @param data
	 *            die konfigurierenden Daten
	 */
	public StandardAggregationsVmq(final SystemObject obj, final Data data) {
		super(obj);
		if (data != null) {

			lage = data.getUnscaledValue("Lage").intValue();

			final ReferenceValue referenceValue = data.getReferenceValue("MessQuerschnittVor");
			mqVor = referenceValue.getSystemObject();
			if (mqVor != null) {
				getMqParts().put(mqVor, new VmqDataPart());
			}

			mqMitte = data.getReferenceValue("MessQuerschnittMitte").getSystemObject();
			if (mqMitte != null) {
				getMqParts().put(mqMitte, new VmqDataPart());
			}

			mqNach = data.getReferenceValue("MessQuerschnittNach").getSystemObject();
			if (mqNach != null) {
				getMqParts().put(mqNach, new VmqDataPart());
			}

			mqAusfahrt = data.getReferenceValue("MessQuerschnittAusfahrt").getSystemObject();
			if (mqAusfahrt != null) {
				getMqParts().put(mqAusfahrt, new VmqDataPart());
			}

			mqEinfahrt = data.getReferenceValue("MessQuerschnittEinfahrt").getSystemObject();
			if (mqEinfahrt != null) {
				getMqParts().put(mqEinfahrt, new VmqDataPart());
			}
		}
	}

	@Override
	protected ResultData calculateResultData(final Map<SystemObject, ResultData> dataList) {
		ResultData result = null;
		if (!dataList.isEmpty()) {
			DataDescription desc = null;
			long time = 0;
			for (final Entry<SystemObject, ResultData> entry : dataList.entrySet()) {
				if (desc == null) {
					desc = entry.getValue().getDataDescription();
					time = entry.getValue().getDataTime();
				}
			}

			if (desc != null) {
				// result = new ResultData(getVmq(), desc, time, null);
				result = getErgebnisAufBasisAktuellerDaten(desc, time, dataList);
			}

		}
		return result;
	}

	/**
	 * Diese Methode geht davon aus, dass keine weiteren Werte zur Berechnung des Analysedatums
	 * eintreffen werden und berechnet mit allen im Moment gepufferten Daten das Analysedatum.
	 *
	 * @param dataList
	 *            die Liste mi den aktuellen Daten
	 * @param time
	 *            der Zeitpunkt für den Zieldatensatz
	 * @param desc
	 *            die Datenbeschreibung
	 *
	 * @return ein Analysedatum
	 */
	private synchronized ResultData getErgebnisAufBasisAktuellerDaten(final DataDescription desc,
			final long time, final Map<SystemObject, ResultData> dataList) {
		ResultData ergebnis = null;

		final Data analyseDatum = getDav().createData(desc.getAttributeGroup());

		/**
		 * Ermittle Werte für <code>VKfz, VLkw, VPkw, VgKfz, B, Bmax, SKfz</code> und
		 * <code>VDelta</code> via Ersetzung
		 */
		final String[] attErsetzung = new String[] { "VKfz", "VLkw", "VPkw", "VgKfz", "B", "BMax",
				"SKfz", "VDelta" };
		for (final String attName : attErsetzung) {
			final ResultData ersetzung = getErsatzDatum(attName, dataList);

			if (ersetzung != null) {
				new MesswertUnskaliert(attName, ersetzung.getData())
						.kopiereInhaltNachModifiziereIndex(analyseDatum);
			} else {
				LOGGER.error("Es konnte kein Ersetzungsdatum fuer " + getVmq() + " im Attribut "
						+ attName + " ermittelt werden");
				final MesswertUnskaliert mw = new MesswertUnskaliert(attName);
				mw.setWertUnskaliert(DUAKonstanten.NICHT_ERMITTELBAR_BZW_FEHLERHAFT);
				mw.kopiereInhaltNachModifiziereIndex(analyseDatum);
			}
		}

		/**
		 * Ermittle Werte für <code>QKfz, QLkw</code> und <code>QPkw</code>
		 */
		final String[] attBilanz = new String[] { "QKfz", "QLkw", "QPkw" };
		for (final String attName : attBilanz) {
			setBilanzDatum(analyseDatum, attName, dataList);
		}

		/**
		 * Berechne Werte für <code>ALkw, KKfz, KPkw, KLkw, QB</code> und <code>KB</code>
		 */
		berechneLkwAnteil(analyseDatum);
		berechneDichte(analyseDatum, "Kfz");
		berechneDichte(analyseDatum, "Lkw");
		berechneDichte(analyseDatum, "Pkw");
		berechneBemessungsVerkehrsstaerke(analyseDatum);
		berechneBemessungsdichte(analyseDatum);

		ergebnis = new ResultData(getVmq(), desc, time, analyseDatum);

		return ergebnis;
	}

	/**
	 * Erfragt das Ersatzdatum für diesen virtuellen Messquerschnitt in den Attributen
	 * <code>VKfz, VLkw, VPkw, VgKfz, B, Bmax, SKfz</code> und <code>VDelta</code>.
	 *
	 * @param attName
	 *            der Name des Attributs, für das ein Ersatzdatum gefunden werden soll
	 * @param dataList
	 *            die Liste mit den aktuellen Daten
	 * @return das Ersatzdatum für diesen virtuellen Messquerschnitt in den Attributen
	 *         <code>VKfz, VLkw, VPkw, VgKfz, B, Bmax, SKfz</code> und <code>VDelta</code> oder
	 *         <code>null</code>, wenn dieses nicht ermittelt werden konnte, weil z.B. alle MQs
	 *         erfasst sind (wäre ein Konfigurationsfehler)
	 */
	private ResultData getErsatzDatum(final String attName,
			final Map<SystemObject, ResultData> dataList) {
		ResultData ersatzDatum = null;

		if (lage == StandardAggregationsVmq.VOR) {
			/**
			 * 1. MQVor nicht direkt erfasst
			 */
			final ResultData mqDataMitte = getMQData(dataList, mqMitte);

			if (isDatumOk(mqDataMitte)) {
				ersatzDatum = mqDataMitte;
			}

			if (!isDatumNutzbar(ersatzDatum, attName)) {
				final ResultData mqDataNach = getMQData(dataList, mqNach);
				if (isDatumOk(mqDataNach)) {
					ersatzDatum = mqDataNach;
				}
			}
		} else if (lage == StandardAggregationsVmq.MITTE) {
			/**
			 * 2. MQMitte nicht direkt erfasst
			 */
			final ResultData mqDataVor = getMQData(dataList, mqVor);

			if (isDatumOk(mqDataVor)) {
				ersatzDatum = mqDataVor;
			}

			if (!isDatumNutzbar(ersatzDatum, attName)) {
				final ResultData mqDataNach = getMQData(dataList, mqNach);
				if (isDatumOk(mqDataNach)) {
					ersatzDatum = mqDataNach;
				}
			}
		} else if (lage == StandardAggregationsVmq.NACH) {
			/**
			 * 3. MQNach nicht direkt erfasst
			 */
			final ResultData mqDataMitte = getMQData(dataList, mqMitte);

			if (isDatumOk(mqDataMitte)) {
				ersatzDatum = mqDataMitte;
			}

			if (!isDatumNutzbar(ersatzDatum, attName)) {
				final ResultData mqDataVor = getMQData(dataList, mqVor);
				if (isDatumOk(mqDataVor)) {
					ersatzDatum = mqDataVor;
				}
			}
		}

		return ersatzDatum;
	}

	/**
	 * liefert für den übergebenen MQ den Datensatz aus der aktuellen Datenliste. Wenn keine
	 * entsprechenden Daten verfügbar sind wird <code>null</code> geliefert.
	 *
	 * @param dataList
	 *            die Liste mit den aktuellen Daten
	 * @param mq
	 *            der MQ
	 * @return das Ergebnis oder <code>null</code>
	 */
	private static ResultData getMQData(final Map<SystemObject, ResultData> dataList,
			final SystemObject mq) {
		ResultData result = null;
		if ((mq != null) && (dataList != null)) {
			result = dataList.get(mq);
		}
		return result;
	}

	/**
	 * liefert einen QWert für das übergebene Attribut und den defnierten MQ aus den aktuellen
	 * Daten. Wenn kein Wert ermittelt werden kann, wird <code>null</code> geliefert.
	 *
	 * @param dataList
	 *            die Liste mit den aktuellen Daten
	 * @param mq
	 *            der MQ
	 * @param attname
	 *            der Name des gesuchten Attributs
	 * @return das Ergebnis oder <code>null</code>
	 */
	private static QWert getQWert(final Map<SystemObject, ResultData> dataList,
			final SystemObject mq, final String attname) {
		final ResultData resultData = getMQData(dataList, mq);
		if (resultData == null) {
			return null;
		}
		return new QWert(resultData, attname);
	}

	/**
	 * Setzt die Verkehrsstärke für diesen virtuellen Messquerschnitt in den Attributen
	 * <code>QKfz, QLkw</code> und <code>QPkw</code>.
	 *
	 * @param analyseDatum
	 *            das Zeil für den Ergebniswert
	 * @param attName
	 *            der Name des Attributs, für das die Verkehrsstärke gesetzt werden soll
	 * @param dataList
	 *            die aktuellen Daten
	 */
	private void setBilanzDatum(final Data analyseDatum, final String attName,
			final Map<SystemObject, ResultData> dataList) {
		QWert qWert = null;

		if (lage == StandardAggregationsVmq.VOR) {
			/**
			 * 1. MQVor nicht direkt erfasst: Q(MQVor)=Q(MQMitte)+Q(MQAus). Wenn an MQMitte der
			 * jeweilige Wert nicht vorhanden ist, gilt: Q(MQVor)=Q(MQNach)+Q(MQAus)-Q(MQEin).
			 */
			final QWert qMitte = getQWert(dataList, mqMitte, attName);
			final QWert qAus = getQWert(dataList, mqAusfahrt, attName);
			if ((qMitte != null) && (qAus != null)) {
				qWert = QWert.summe(qMitte, qAus);
			}

			if ((qWert == null) || !qWert.isExportierbarNach(analyseDatum)
					|| !qWert.isVerrechenbar()) {
				final QWert qNach = getQWert(dataList, mqNach, attName);
				final QWert qEin = getQWert(dataList, mqEinfahrt, attName);

				if ((qNach != null) && (qEin != null) && qNach.isVerrechenbar()
						&& qEin.isVerrechenbar()) {
					if ((qWert == null) || !qWert.isExportierbarNach(analyseDatum)) {
						qWert = QWert.differenz(QWert.summe(qNach, qAus), qEin);
					} else {
						/**
						 * Also Q != null und Q ist exportierbar
						 */
						if (!qWert.isVerrechenbar()) {
							final QWert dummy = QWert.differenz(QWert.summe(qNach, qAus), qEin);
							if ((dummy != null) && dummy.isExportierbarNach(analyseDatum)
									&& dummy.isVerrechenbar()) {
								qWert = dummy;
							}
						}
					}
				}
			}
		} else if (lage == StandardAggregationsVmq.MITTE) {
			/**
			 * 2. MQMitte nicht direkt erfasst: Q(MQMitte)=Q(MQVor)-Q(MQAus). Wenn an MQVor der
			 * jeweilige Wert nicht vorhanden ist, gilt Q(MQMitte)=Q(MQNach)-Q(MQEin).
			 */
			final QWert qVor = getQWert(dataList, mqVor, attName);
			final QWert qAus = getQWert(dataList, mqAusfahrt, attName);

			qWert = QWert.differenz(qVor, qAus);

			if ((qWert == null) || !qWert.isExportierbarNach(analyseDatum)
					|| !qWert.isVerrechenbar()) {
				final QWert qNach = getQWert(dataList, mqNach, attName);
				final QWert qEin = getQWert(dataList, mqEinfahrt, attName);

				if ((qNach != null) && (qEin != null) && qNach.isVerrechenbar()
						&& qEin.isVerrechenbar()) {
					if ((qWert == null) || !qWert.isExportierbarNach(analyseDatum)) {
						qWert = QWert.differenz(qNach, qEin);
					} else {
						/**
						 * Also Q != null und Q ist exportierbar
						 */
						if (!qWert.isVerrechenbar()) {
							final QWert dummy = QWert.differenz(qNach, qEin);
							if ((dummy != null) && dummy.isExportierbarNach(analyseDatum)
									&& dummy.isVerrechenbar()) {
								qWert = dummy;
							}
						}
					}
				}
			}
		} else if (lage == StandardAggregationsVmq.NACH) {
			/**
			 * 3. MQNach nicht direkt erfasst Q(MQNach)=Q(MQMitte)+Q(MQEin). Wenn an MQMitte der
			 * jeweilige Wert nicht vorhanden ist, gilt Q(MQNach)=Q(MQVor)+Q(MQEin)-Q(MQAus).
			 */
			final QWert qMitte = getQWert(dataList, mqMitte, attName);
			final QWert qEin = getQWert(dataList, mqEinfahrt, attName);

			qWert = QWert.summe(qMitte, qEin);

			if ((qWert == null) || !qWert.isExportierbarNach(analyseDatum)
					|| !qWert.isVerrechenbar()) {
				final QWert qVor = getQWert(dataList, mqVor, attName);
				final QWert qAus = getQWert(dataList, mqAusfahrt, attName);

				if ((qVor != null) && (qAus != null) && qVor.isVerrechenbar()
						&& qAus.isVerrechenbar()) {
					if ((qWert == null) || !qWert.isExportierbarNach(analyseDatum)) {
						qWert = QWert.differenz(QWert.summe(qVor, qEin), qAus);
					} else {
						/**
						 * Also Q != null und Q ist exportierbar
						 */
						if (!qWert.isVerrechenbar()) {
							final QWert dummy = QWert.differenz(QWert.summe(qVor, qEin), qAus);
							if ((dummy != null) && dummy.isExportierbarNach(analyseDatum)
									&& dummy.isVerrechenbar()) {
								qWert = dummy;
							}
						}
					}
				}
			}
		}

		MesswertUnskaliert mw = new MesswertUnskaliert(attName);
		if ((qWert == null) || !qWert.isExportierbarNach(analyseDatum)) {
			mw.setWertUnskaliert(DUAKonstanten.NICHT_ERMITTELBAR_BZW_FEHLERHAFT);
		} else {
			mw = qWert.getWert();
		}
		mw.kopiereInhaltNachModifiziereIndex(analyseDatum);
	}

	/**
	 * Erfragt, ob das übergebene Datum im Sinne der Wertersetzung brauchbar ist. Dies ist dann der
	 * Fall, wenn das Datum Nutzdaten enthält und dessen Datenzeit echt älter als die des letzten
	 * publizierten Analysedatums ist.
	 *
	 * @param datum
	 *            ein Analysedatum eines MQ
	 * @return ob das übergebene Datum im Sinne der Wertersetzung brauchbar ist
	 */
	private boolean isDatumOk(final ResultData datum) {
		boolean ergebnis = false;

		if ((datum != null) && (datum.getData() != null)) {
			final long letzterAnalyseZeitStempel = getLetztesErgebnis() == null ? -1
					: getLetztesErgebnis().getDataTime();
			ergebnis = datum.getDataTime() > letzterAnalyseZeitStempel;
		}

		return ergebnis;
	}

	/**
	 * Erfragt, ob das übergebene Datum im übergebenen Attribut sinnvolle Nutzdaten (Werte &gt;= 0
	 * hat).
	 *
	 * @param datum
	 *            ein Analysedatum
	 * @param attName
	 *            der Name des Attributs
	 * @return ob das übergebene Datum im übergebenen Attribut sinnvolle Daten
	 */
	private static boolean isDatumNutzbar(final ResultData datum, final String attName) {
		boolean ergebnis = false;

		if ((datum != null) && (datum.getData() != null)) {
			ergebnis = new MesswertUnskaliert(attName, datum.getData()).getWertUnskaliert() >= 0;
		}

		return ergebnis;
	}
}
