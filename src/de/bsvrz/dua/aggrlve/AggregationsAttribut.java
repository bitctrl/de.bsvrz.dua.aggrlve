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

import java.util.HashSet;
import java.util.Set;

/**
 * Container fuer Attribute die zur Aggregation herangezogen werden (jeweils
 * fuer Fahrstreifen bzw. Messquerschnitte):<br>
 * <code>qKfz</code> bzw. <code>QKfz</code>,<br>
 * <code>qLkw</code> bzw. <code>QLkw</code>,<br>
 * <code>qPkw</code> bzw. <code>QPkw</code>,<br>
 * <code>vKfz</code> bzw. <code>VKfz</code>,<br>
 * <code>vLkw</code> bzw. <code>VLkw</code> und<br>
 * <code>vPkw</code> bzw. <code>VPkw</code>
 * 
 * @author BitCtrl Systems GmbH, Thierfelder
 * 
 * @verison $Id$
 */
public class AggregationsAttribut {

	/**
	 * Wertebereich
	 */
	private static Set<AggregationsAttribut> WERTE_BEREICH = new HashSet<AggregationsAttribut>();

	/**
	 * Attribut <code>qKfz</code> bzw. <code>QKfz</code>
	 */
	public static final AggregationsAttribut Q_KFZ = new AggregationsAttribut(
			"qKfz", "QKfz"); //$NON-NLS-1$ //$NON-NLS-2$

	/**
	 * Attribut <code>qLkw</code> bzw. <code>QLkw</code>
	 */
	public static final AggregationsAttribut Q_LKW = new AggregationsAttribut(
			"qLkw", "QLkw"); //$NON-NLS-1$ //$NON-NLS-2$

	/**
	 * Attribut <code>qPkw</code> bzw. <code>QPkw</code>
	 */
	public static final AggregationsAttribut Q_PKW = new AggregationsAttribut(
			"qPkw", "QPkw"); //$NON-NLS-1$ //$NON-NLS-2$

	/**
	 * Attribut <code>vKfz</code> bzw. <code>VKfz</code>
	 */
	public static final AggregationsAttribut V_KFZ = new AggregationsAttribut(
			"vKfz", "VKfz"); //$NON-NLS-1$ //$NON-NLS-2$

	/**
	 * Attribut <code>vLkw</code> bzw. <code>VLkw</code>
	 */
	public static final AggregationsAttribut V_LKW = new AggregationsAttribut(
			"vLkw", "VLkw"); //$NON-NLS-1$ //$NON-NLS-2$

	/**
	 * Attribut <code>vPkw</code> bzw. <code>VPkw</code>
	 */
	public static final AggregationsAttribut V_PKW = new AggregationsAttribut(
			"vPkw", "VPkw"); //$NON-NLS-1$ //$NON-NLS-2$

	/**
	 * der Name des Attributs (FS)
	 */
	private String nameFS = null;

	/**
	 * der Name des Attributs (MQ)
	 */
	private String nameMQ = null;

	/**
	 * indiziert, ob es sich bei diesem Attribut um ein Geschwindigkeitsattribut
	 * handelt
	 */
	private boolean geschwindigkeitsAttribut = false;

	/**
	 * Standardkonstruktor
	 * 
	 * @param nameFS
	 *            der Attributname bei Fahrstreifendaten z.B. <code>qKfz</code>
	 *            oder <code>vKfz</code>
	 * @param nameMQ
	 *            der Attributname bei Messquerschnittdaten z.B.
	 *            <code>QKfz</code> oder <code>VKfz</code>
	 */
	private AggregationsAttribut(final String nameFS, final String nameMQ) {
		this.nameFS = nameFS;
		this.nameMQ = nameMQ;
		this.geschwindigkeitsAttribut = nameFS.startsWith("v"); //$NON-NLS-1$
		WERTE_BEREICH.add(this);
	}

	/**
	 * Erfragt, ob es sich bei diesem Attribut um ein Geschwindigkeitsattribut
	 * handelt
	 * 
	 * @return ob es sich bei diesem Attribut um ein Geschwindigkeitsattribut
	 *         handelt
	 */
	public final boolean isGeschwindigkeitsAttribut() {
		return this.geschwindigkeitsAttribut;
	}

	/**
	 * Erfragt den Namen dieses Attributs
	 * 
	 * @param fuerFahrStreifen
	 *            das Objekt, fuer den der Name dieses Attributs erfragt wird
	 * @return der Name dieses Attributs
	 */
	public String getAttributName(final boolean fuerFahrStreifen) {
		return fuerFahrStreifen ? this.nameFS : this.nameMQ;
	}

	/**
	 * Erfragt alle statischen Instanzen dieser Klasse
	 * 
	 * @return alle statischen Instanzen dieser Klasse
	 */
	public static final Set<AggregationsAttribut> getInstanzen() {
		return WERTE_BEREICH;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return this.nameFS + " (" + this.nameMQ + ")"; //$NON-NLS-1$ //$NON-NLS-2$
	}

}