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
import de.bsvrz.dav.daf.main.archive.*;
import de.bsvrz.dav.daf.main.config.SystemObject;
import de.bsvrz.sys.funclib.debug.Debug;

import java.io.IOException;

/**
 * Datenpuffer fuer Daten, die zur Erzeugung von TV- und DTV-Werten (nur fuer
 * Messquerschnitte) benoetigt werden. Im Unterschied zu den normalen
 * Datenpuffern liest dieser Puffer seine Daten initial aus dem Archiv ein
 * 
 * @author BitCtrl Systems GmbH, Thierfelder
 */
public final class ArchivAggregationsPuffer extends AggregationsPuffer implements
		ArchiveAvailabilityListener {

	/**
	 * Millis in einer Stunde
	 */
	public static final long MILLIS_PER_HOUR = (long) (60 * 60 * 1000);
	private final AggregationsIntervall aggregationsIntervall;

	/**
	 * Standardkonstruktor.
	 * 
	 * @param dav
	 *            Verbindung zum Datenverteiler
	 * @param obj
	 *            das Objekt, dessen Daten gepuffert werden sollen
	 * @param intervall
	 *            das Aggregationsintervall, fuer das Daten in diesem Puffer
	 */
	public ArchivAggregationsPuffer(final ClientDavInterface dav,
			final SystemObject obj, final AggregationsIntervall intervall) {
		super(dav, obj, intervall);
		aggregationsIntervall = intervall;
		if (dav.getArchive().isArchiveAvailable()) {
			this.archiveAvailabilityChanged(dav
					.getArchive());
		} else {
			dav.getArchive()
					.addArchiveAvailabilityListener(this);
		}
	}

	@Override
	public void archiveAvailabilityChanged(final ArchiveRequestManager archiv) {
		if (archiv.isArchiveAvailable() && ringPufferisEmpty()) {
			final long jetzt = System.currentTimeMillis();
			long beginArchivAnfrage = aggregationsIntervall.getStartZeitStempel(aggregationsIntervall.getAggregationsZeitStempel(jetzt));
			final long endeArchivAnfrage = jetzt;

			final ArchiveTimeSpecification ats = new ArchiveTimeSpecification(
					TimingType.DATA_TIME, 
					false, 
					beginArchivAnfrage,
					endeArchivAnfrage
			);

			final ArchiveDataSpecification archivDatenBeschreibung = new ArchiveDataSpecification(
					ats, new ArchiveDataKindCombination(ArchiveDataKind.ONLINE),
					ArchiveOrder.BY_DATA_TIME,
					ArchiveRequestOption.NORMAL,
					aggregationsIntervall.getDatenBeschreibung(false),
					this.objekt
			);

			try {
				final ArchiveDataQueryResult result = archiv.request(
						ArchiveQueryPriority.MEDIUM,
						archivDatenBeschreibung);
				final ArchiveDataStream[] streams = result.getStreams();
				synchronized (this) {
					for (final ArchiveDataStream stream : streams) {
						ArchiveData archiveDatum = null;
						do {
							archiveDatum = stream.take();
							if ((archiveDatum != null)
									&& (archiveDatum.getData() != null)) {
								this.aktualisiere(new AggregationsDatum(archiveDatum, dav));
							}
						} while (archiveDatum != null);
					}
				}
			} catch (final IOException | InterruptedException e) {
				Debug.getLogger().error("Fehler bei Archivanfrage", e);
			}
		}
	}

}
