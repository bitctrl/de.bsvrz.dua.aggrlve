package de.bsvrz.dua.aggrlve.vmq;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import de.bsvrz.dav.daf.main.ClientDavInterface;
import de.bsvrz.dav.daf.main.config.Aspect;
import de.bsvrz.dav.daf.main.config.AttributeGroup;
import de.bsvrz.dav.daf.main.config.SystemObject;
import de.bsvrz.sys.funclib.bitctrl.dua.DUAKonstanten;
import de.bsvrz.sys.funclib.debug.Debug;

/**
 * Managerklasse, in der virtuelle Messquerschnitte verwaltet werden, dren
 * Aggregationswerte aus den beteiligten realen MQ kombiniert werden.
 * 
 * @author BitCtrl Systems GmbH, Uwe Peuker
 * @version $Id$
 */
public final class VMqAggregator extends Thread {

	/** globale Instanz des Aggregators. */
	private static final VMqAggregator INSTANCE = new VMqAggregator();

	/**
	 * liefert die globale Instanz des Aggregators.
	 * 
	 * @return die Instanz
	 */
	public static VMqAggregator getInstance() {
		return VMqAggregator.INSTANCE;
	}

	/** MQ mit neuen Daten zur Überprüfung und Zussammenfassung. */
	private final Set<AbstractAggregationsVmq> reqSet = new HashSet<AbstractAggregationsVmq>();

	/** Objekt zur Synchronisation der Auftragswarteschlange. */
	private final Object locker = new Object();

	/** die Liste der Aspekte für die Daten übertragen werden. */
	private final Set<Aspect> supportedAspects = new HashSet<Aspect>();

	/** die Attributgruppe für die Quelldaten. */
	private AttributeGroup srcAtg;

	/** privater Konstruktor. */
	private VMqAggregator() {
		// es wird nur die globale SINGLETON-Instanz verwendet
		super("VMQ-Aggregator");
		setDaemon(true);
		start();
	}

	/**
	 * liefert die Attributgruppe der Quelldaten.
	 * 
	 * @return die Attributgruppe
	 */
	public AttributeGroup getSrcAtg() {
		return srcAtg;
	}

	/**
	 * liefert die Menge der behandelten Aspekte.
	 * 
	 * @return die Menge
	 */
	public Set<Aspect> getSupportedAspects() {
		return supportedAspects;
	}

	/**
	 * initialisiert den Aggregator.
	 * 
	 * Für alle virtuellen Messquerschnitte erfolgt eine Anmeldung auf die
	 * Aggregationsdaten der beteiligten MQ. Die Daten werden von den
	 * entsprechenden Vertreterobjekten empfangen und zusammengefasst.
	 * 
	 * @param connection
	 *            die verwendete Datenverteilerverbindung
	 */
	public void init(final ClientDavInterface connection) {

		srcAtg = connection.getDataModel().getAttributeGroup(
				DUAKonstanten.ATG_KURZZEIT_MQ);
		final Collection<Aspect> aspects = srcAtg.getAspects();
		for (final Aspect asp : aspects) {
			if (asp.getPid().startsWith("asp.agre")) {
				supportedAspects.add(asp);
			}
		}

		for (final SystemObject obj : connection.getDataModel()
				.getType(DUAKonstanten.TYP_MQ_VIRTUELL).getObjects()) {
			final AbstractAggregationsVmq vmq = AggregationsVmqFactory
					.create(obj);
			if (vmq != null) {
				vmq.init(connection);
			}
		}
	}

	/**
	 * markiert einen VMQ zur potentiellen Ergebnis-Publikation in der
	 * Auftragswarteschlange.
	 * 
	 * @param aggregationsVmq
	 *            der MQ
	 */
	public void push(final AbstractAggregationsVmq aggregationsVmq) {
		synchronized (locker) {
			reqSet.add(aggregationsVmq);
			locker.notifyAll();
		}
	}

	@Override
	public void run() {
		while (true) {
			AbstractAggregationsVmq nextVmq = null;
			synchronized (locker) {
				final Iterator<AbstractAggregationsVmq> iterator = reqSet
						.iterator();
				if (iterator.hasNext()) {
					nextVmq = iterator.next();
					reqSet.remove(nextVmq);
				}
			}

			if (nextVmq != null) {
				while (nextVmq.sendNextCompletedResult()) {
					Debug.getLogger().finest("Daten publiziert");
				}
			}

			synchronized (locker) {
				if (reqSet.size() <= 0) {
					try {
						locker.wait(30000);
					} catch (final InterruptedException e) {
						Debug.getLogger().warning(e.getLocalizedMessage());
					}
				}
			}
		}
	}
}
