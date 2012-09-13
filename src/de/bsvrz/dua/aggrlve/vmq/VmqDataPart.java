package de.bsvrz.dua.aggrlve.vmq;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import de.bsvrz.dav.daf.main.ResultData;
import de.bsvrz.dav.daf.main.config.Aspect;

/**
 * Klasse zum Speichern der Daten eines MQ, der ein Bestandteil eines VMQ ist.
 * 
 * @author BitCtrl Systems GmbH, Uwe Peuker
 * @version $Id$
 */
public class VmqDataPart {

	/** der Anteil des MQ am virtuellen Messquerschnitt. */
	private final double anteil;
	/** die Liste der für den MQ empfangenen Daten nach Aspekten geordnet. */
	private final Map<Aspect, SortedMap<Long, ResultData>> dataList = new HashMap<Aspect, SortedMap<Long, ResultData>>();

	/**
	 * Konstruktor.
	 * 
	 * @param anteil
	 *            der Anteil des MQ am VMQ
	 */
	public VmqDataPart(final double anteil) {
		this.anteil = anteil;
	}

	/** Standardkonstruktor mit Anteil 1 - 100 Prozent. */
	public VmqDataPart() {
		this(1.0);
	}

	/**
	 * fügt einen Ergebnisdatensatz hinzu.
	 * 
	 * @param result
	 *            der Datensatz
	 */
	public void push(final ResultData result) {
		if (result.hasData()) {
			final Aspect aspect = result.getDataDescription().getAspect();
			SortedMap<Long, ResultData> map = dataList.get(aspect);
			if (map == null) {
				map = new TreeMap<Long, ResultData>();
				dataList.put(aspect, map);
			}
			map.put(result.getDataTime(), result);
		}
	}

	/**
	 * liefert den nächsten verfügbaren noch nicht verarbeiteten Wert ( nach
	 * Zeit geordnet).
	 * 
	 * @param aspect
	 *            der Aspekt für den ein Wert bestimmt werden soll
	 * @return der Wert oder <code>null</code>
	 */
	public ResultData getNextValue(final Aspect aspect) {

		final SortedMap<Long, ResultData> map = dataList.get(aspect);
		if ((map != null) && (!map.isEmpty())) {
			return map.get(map.firstKey());
		}
		return null;
	}

	/**
	 * löscht für den angegebenen Aspekte alle Werte, die nicht jünger als der
	 * angebene Zeitstempel sind.
	 * 
	 * @param dataTime
	 *            der Zeitstempel
	 * @param aspect
	 *            der Aspekt
	 */
	public void clear(final long dataTime, final Aspect aspect) {

		final SortedMap<Long, ResultData> map = dataList.get(aspect);
		if (map != null) {
			while (!map.isEmpty() && (map.firstKey() <= dataTime)) {
				map.remove(map.firstKey());
			}
		}
	}

	/**
	 * liefert den Anteil des MQ am VMQ.
	 * 
	 * @return der Anteil
	 */
	public double getAnteil() {
		return anteil;
	}
}
