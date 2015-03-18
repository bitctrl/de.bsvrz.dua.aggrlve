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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import de.bsvrz.dav.daf.main.Data;
import de.bsvrz.dav.daf.main.Data.Array;
import de.bsvrz.dav.daf.main.DataDescription;
import de.bsvrz.dav.daf.main.ResultData;
import de.bsvrz.dav.daf.main.config.SystemObject;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;
import de.bsvrz.sys.funclib.bitctrl.dua.MesswertUnskaliert;
import de.bsvrz.sys.funclib.debug.Debug;

/**
 * Klasse, die einen virtuellen MQ mit dem Verfahren VLage beschreibt, für den Aggregationswerte
 * übernommen werden sollen.
 *
 * @author BitCtrl Systems GmbH, Uwe Peuker
 * @version $Id$
 */
public class VLageAggregationsVmq extends AbstractAggregationsVmq {

	/**
	 * der MQ zur Bestimmung der Geschwindigkeit.
	 *
	 * TODO Momentan wird davon ausgegangen, das der MQ in der Liste der MQ des VMQ enthalten ist,
	 * die Verwendung eines zusätzlichen Ersatz-MQ wird nicht unterstützt.
	 */
	private SystemObject geschwindigkeitMq;

	/**
	 * Konstruktor.
	 *
	 * @param obj
	 *            das VMQ-Objekt
	 * @param data
	 *            die konfigurierenden Daten
	 */
	public VLageAggregationsVmq(final SystemObject obj, final Data data) {
		super(obj);
		if (data != null) {

			geschwindigkeitMq = data.getReferenceValue("MessQuerschnittGeschwindigkeit")
					.getSystemObject();

			final Array array = data.getArray("MessQuerSchnittBestandTeile");
			if (array != null) {
				for (int idx = 0; idx < array.getLength(); idx++) {
					final SystemObject mq = array.getItem(idx)
							.getReferenceValue("MessQuerschnittReferenz").getSystemObject();
					final double anteil = array.getItem(idx).getScaledValue("Anteil").doubleValue();
					getMqParts().put(mq, new VmqDataPart(anteil));
				}
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
	 * @param time
	 *            der Zeitstempel für den zu erstellenden Datensatz
	 * @param desc
	 *            die Datenbeschreibung der Zieldaten
	 * @param dataList
	 *            die aktuellen Basisdaten für die Berechnung
	 *
	 * @return ein Analysedatum
	 */
	private ResultData getErgebnisAufBasisAktuellerDaten(final DataDescription desc,
			final long time, final Map<SystemObject, ResultData> dataList) {

		final Data analyseDatum = getDav().createData(desc.getAttributeGroup());

		/**
		 * Ermittle Werte fuer <code>VKfz, VLkw, VPkw</code> und <code>VgKfz</code> via Ersetzung
		 */
		final ResultData ersetzung = dataList.get(geschwindigkeitMq);

		for (final String attName : new String[] { "VKfz", "VLkw", "VPkw", "VgKfz" }) {
			if ((ersetzung != null) && (ersetzung.getData() != null)) {
				new MesswertUnskaliert(attName, ersetzung.getData())
				.kopiereInhaltNachModifiziereIndex(analyseDatum);
			} else {
				Debug.getLogger().error(
						"Es konnte kein Ersetzungsdatum fuer " + getVmq() + " im Attribut "
								+ attName + " ermittelt werden");
				final MesswertUnskaliert mw = new MesswertUnskaliert(attName);
				mw.setWertUnskaliert(DUAKonstanten.NICHT_ERMITTELBAR_BZW_FEHLERHAFT);
				mw.kopiereInhaltNachModifiziereIndex(analyseDatum);
			}
		}

		/**
		 * Setze Rest (<code>B, BMax, SKfz</code> und <code>VDelta</code>) auf
		 * <code>nicht ermittelbar/fehlerhaft</code>
		 */
		for (final String attName : new String[] { "B", "BMax", "SKfz", "VDelta" }) {
			final MesswertUnskaliert mw = new MesswertUnskaliert(attName);
			mw.setWertUnskaliert(DUAKonstanten.NICHT_ERMITTELBAR_BZW_FEHLERHAFT);
			mw.kopiereInhaltNachModifiziereIndex(analyseDatum);
		}

		/**
		 * Ermittle Werte für <code>QKfz, QLkw</code> und <code>QPkw</code>
		 */
		for (final String attName : new String[] { "QKfz", "QLkw", "QPkw" }) {
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

		return new ResultData(getVmq(), desc, time, analyseDatum);
	}

	/**
	 * Setzt die Verkehrsstärke für diesen virtuellen Messquerschnitt in den Attributen
	 * <code>QKfz, QLkw</code> und <code>QPkw</code>.
	 *
	 * @param analyseDatum
	 *            das zu modifizierende Datum.
	 * @param attName
	 *            der Name des Attributs, für das die Verkehrsstärke gesetzt werden soll
	 * @param dataList
	 *            die aktuellen Basisdaten
	 */
	private void setBilanzDatum(final Data analyseDatum, final String attName,
			final Map<SystemObject, ResultData> dataList) {
		final List<QWert> qWerte = new ArrayList<QWert>();

		for (final SystemObject mq : getMqParts().keySet()) {
			final ResultData data = dataList.get(mq);
			if (data != null) {
				qWerte.add(new QWert(data, attName, getMqParts().get(mq).getAnteil()));
			}
		}

		QWert qQ = null;
		if (!qWerte.isEmpty()) {
			qQ = QWert.summe(qWerte.toArray(new QWert[qWerte.size()]));
		}

		MesswertUnskaliert mw = new MesswertUnskaliert(attName);
		if ((qQ == null) || !qQ.isExportierbarNach(analyseDatum)) {
			mw.setWertUnskaliert(DUAKonstanten.NICHT_ERMITTELBAR_BZW_FEHLERHAFT);
		} else {
			mw = qQ.getWert();
		}
		mw.kopiereInhaltNachModifiziereIndex(analyseDatum);
	}

}
