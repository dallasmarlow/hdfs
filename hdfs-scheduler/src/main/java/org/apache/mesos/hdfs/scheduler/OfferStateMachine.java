package org.apache.mesos.hdfs.scheduler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.hdfs.scheduler.OfferStateMachine.OfferState;
import org.apache.mesos.hdfs.scheduler.OfferStateMachine.OfferEvent;

import org.squirrelframework.foundation.fsm.impl.AbstractStateMachine;
import org.squirrelframework.foundation.fsm.StateMachineBuilder;
import org.squirrelframework.foundation.fsm.StateMachineBuilderFactory;

/**
 * OfferStateMachine.
 */
public class OfferStateMachine extends AbstractStateMachine<OfferStateMachine, OfferState, OfferEvent, String> {
  private final Log log = LogFactory.getLog(OfferStateMachine.class);

  /**
   * OfferEvent.
   */
  public enum OfferEvent {
    ToInitial,
    ToReserved,
    ToVolumeCreated,
    ToLaunched
  }

  /**
   * OfferState.
   */
  public enum OfferState {
    Initial,
    Reserved,
    VolumeCreated,
    Launched
  }

  private void logTransition(OfferState from, OfferState to, OfferEvent event, String context) {
    log.info(event + "event triggered transition from " + from + " to " + to + " with " + context);
  }

  public static OfferStateMachine create() {
    StateMachineBuilder<OfferStateMachine, OfferState, OfferEvent, String> builder =
      StateMachineBuilderFactory.create(
          OfferStateMachine.class,
          OfferState.class,
          OfferEvent.class,
          String.class);

    builder.externalTransition()
      .from(OfferState.Initial)
      .to(OfferState.Reserved)
      .on(OfferEvent.ToReserved)
      .callMethod("logTransition");

    builder.externalTransition()
      .from(OfferState.Reserved)
      .to(OfferState.VolumeCreated)
      .on(OfferEvent.ToVolumeCreated)
      .callMethod("logTransition");

    builder.externalTransition()
      .from(OfferState.VolumeCreated)
      .to(OfferState.Launched)
      .on(OfferEvent.ToLaunched)
      .callMethod("logTransition");

    builder.externalTransition()
      .from(OfferState.Launched)
      .to(OfferState.VolumeCreated)
      .on(OfferEvent.ToVolumeCreated)
      .callMethod("logTransition");

    builder.externalTransition()
      .from(OfferState.VolumeCreated)
      .to(OfferState.Reserved)
      .on(OfferEvent.ToReserved)
      .callMethod("logTransition");

    builder.externalTransition()
      .from(OfferState.Reserved)
      .to(OfferState.Initial)
      .on(OfferEvent.ToInitial)
      .callMethod("logTransition");

    return builder.newStateMachine(OfferState.Initial);
  }
}
