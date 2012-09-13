package de.bsvrz.dua.aggrlve.vmq;

import de.bsvrz.dav.daf.main.Data;
import de.bsvrz.dav.daf.main.config.SystemObject;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;

/**
 * Factory zum Anlegen von Instanzen für die Verwaltung der Aggregationsdaten
 * von virtuellen MQ.
 * 
 * @author BitCtrl Systems GmbH, Uwe Peuker
 * @version $Id$
 */
public final class AggregationsVmqFactory {

	/** privater Konstruktor. */
	private AggregationsVmqFactory() {
		// es werden keine Instanzen der Factory benötigt
	}

	/**
	 * erzeugt eine Instanz zur Kombination der Aggregationsdaten eines
	 * virtuellen Messquertschnitts.
	 * 
	 * @param obj
	 *            das zu Grunde liegende Systemobjekt aus der
	 *            Datenverteilerkonfiguration
	 * @return das Objekt oder null, wenn der übergebene Systemobjekttyp nicht
	 *         unterstützt wird
	 */
	public static AbstractAggregationsVmq create(final SystemObject obj) {

		AbstractAggregationsVmq result = null;

		Data data = obj.getConfigurationData(obj.getDataModel()
				.getAttributeGroup(DUAKonstanten.ATG_MQ_VIRTUELL_STANDARD));
		if (data != null) {
			result = new StandardAggregationsVmq(obj, data);
		} else {
			data = obj.getConfigurationData(obj.getDataModel()
					.getAttributeGroup(DUAKonstanten.ATG_MQ_VIRTUELL_V_LAGE));
			if (data != null) {
				result = new VLageAggregationsVmq(obj, data);
			}
		}

		return result;
	}
}
