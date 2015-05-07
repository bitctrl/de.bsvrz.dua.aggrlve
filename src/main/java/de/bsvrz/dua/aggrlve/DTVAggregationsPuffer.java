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

import java.io.IOException;

import com.bitctrl.Constants;

import de.bsvrz.dav.daf.main.ClientDavInterface;
import de.bsvrz.dav.daf.main.archive.ArchiveAvailabilityListener;
import de.bsvrz.dav.daf.main.archive.ArchiveData;
import de.bsvrz.dav.daf.main.archive.ArchiveDataKind;
import de.bsvrz.dav.daf.main.archive.ArchiveDataKindCombination;
import de.bsvrz.dav.daf.main.archive.ArchiveDataQueryResult;
import de.bsvrz.dav.daf.main.archive.ArchiveDataSpecification;
import de.bsvrz.dav.daf.main.archive.ArchiveDataStream;
import de.bsvrz.dav.daf.main.archive.ArchiveOrder;
import de.bsvrz.dav.daf.main.archive.ArchiveQueryPriority;
import de.bsvrz.dav.daf.main.archive.ArchiveRequestManager;
import de.bsvrz.dav.daf.main.archive.ArchiveRequestOption;
import de.bsvrz.dav.daf.main.archive.ArchiveTimeSpecification;
import de.bsvrz.dav.daf.main.archive.TimingType;
import de.bsvrz.dav.daf.main.config.SystemObject;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAInitialisierungsException;
import de.bsvrz.sys.funclib.debug.Debug;

/**
 * Datenpuffer fuer Daten, die zur Erzeugung von TV- und DTV-Werten (nur fuer Messquerschnitte)
 * benoetigt werden. Im Unterschied zu den normalen Datenpuffern liest dieser Puffer seine Daten
 * initial aus dem Archiv ein
 *
 * @author BitCtrl Systems GmbH, Thierfelder
 */
public class DTVAggregationsPuffer extends AggregationsPuffer
		implements ArchiveAvailabilityListener {

	private static final Debug LOGGER = Debug.getLogger();

	/**
	 * Standardkonstruktor.
	 *
	 * @param dav
	 *            Verbindung zum Datenverteiler
	 * @param obj
	 *            das Objekt, dessen Daten gepuffert werden sollen
	 * @param intervall
	 *            das Aggregationsintervall, fuer das Daten in diesem Puffer
	 * @throws DUAInitialisierungsException
	 *             wenn dieses Objekt nicht vollstaendig initialisiert werden konnte
	 */
	public DTVAggregationsPuffer(final ClientDavInterface dav, final SystemObject obj,
			final AggregationsIntervall intervall) throws DUAInitialisierungsException {
		super(dav, obj, intervall);
		if (AbstraktAggregationsPuffer.sDAV.getArchive().isArchiveAvailable()) {
			archiveAvailabilityChanged(AbstraktAggregationsPuffer.sDAV.getArchive());
		} else {
			AbstraktAggregationsPuffer.sDAV.getArchive().addArchiveAvailabilityListener(this);
		}
	}

	@Override
	public void archiveAvailabilityChanged(final ArchiveRequestManager archiv) {
		if (archiv.isArchiveAvailable() && ringPufferisEmpty()) {
			final long jetzt = System.currentTimeMillis();
			long beginArchivAnfrage = -1;
			final long endeArchivAnfrage = jetzt;

			if (aggregationsIntervall.equals(AggregationsIntervall.aGG60MINUTE)) {
				/**
				 * Zum Start der Applikation sollen moeglichst die Datensaetze der letzten 24
				 * Stunden bereitstehen
				 */
				beginArchivAnfrage = jetzt
						- (Constants.MILLIS_PER_HOUR * aggregationsIntervall.getMaxPufferGroesse());
			} else if (aggregationsIntervall.equals(AggregationsIntervall.aGGDTVTAG)) {
				/**
				 * Zum Start der Applikation sollen moeglichst die Datensaetze der letzten 50 Tage
				 * bereitstehen
				 */
				beginArchivAnfrage = jetzt - (Constants.MILLIS_PER_HOUR * 24L
						* aggregationsIntervall.getMaxPufferGroesse());
			} else if (aggregationsIntervall.equals(AggregationsIntervall.aGGDTVMONAT)) {
				/**
				 * Zum Start der Applikation sollen moeglichst die Datensaetze der letzten 15 Monate
				 * bereitstehen
				 */
				beginArchivAnfrage = jetzt - (Constants.MILLIS_PER_HOUR * 24L * 31L
						* aggregationsIntervall.getMaxPufferGroesse());
			} else {
				beginArchivAnfrage = jetzt - (Constants.MILLIS_PER_HOUR * 24L * 370L);
			}

			if (beginArchivAnfrage > 0) {
				final ArchiveTimeSpecification zeit = new ArchiveTimeSpecification(
						TimingType.DATA_TIME, false, beginArchivAnfrage, endeArchivAnfrage);

				final ArchiveDataSpecification archivDatenBeschreibung = new ArchiveDataSpecification(
						zeit, new ArchiveDataKindCombination(ArchiveDataKind.ONLINE),
						ArchiveOrder.BY_DATA_TIME, ArchiveRequestOption.NORMAL,
						aggregationsIntervall.getDatenBeschreibung(false), objekt);

				try {
					final ArchiveDataQueryResult result = archiv
							.request(ArchiveQueryPriority.MEDIUM, archivDatenBeschreibung);
					final ArchiveDataStream[] streams = result.getStreams();
					synchronized (this) {
						for (final ArchiveDataStream stream : streams) {
							ArchiveData archiveDatum = null;
							do {
								archiveDatum = stream.take();
								if ((archiveDatum != null) && (archiveDatum.getData() != null)) {
									aktualisiere(archiveDatum);
								}
							} while (archiveDatum != null);
						}
					}
				} catch (final IOException e) {
					LOGGER.error(Constants.EMPTY_STRING, e);
					e.printStackTrace();
				} catch (final InterruptedException e) {
					LOGGER.error(Constants.EMPTY_STRING, e);
					e.printStackTrace();
				}

			}
		}
	}

}
