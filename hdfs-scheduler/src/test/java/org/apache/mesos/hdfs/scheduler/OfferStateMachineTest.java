package org.apache.mesos.hdfs.scheduler;

import org.apache.mesos.hdfs.scheduler.OfferStateMachine.OfferState;
import org.apache.mesos.hdfs.scheduler.OfferStateMachine.OfferEvent;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class OfferStateMachineTest {
  private OfferStateMachine fsm = OfferStateMachine.create();

  @Test
  public void stateTransitions() {
    fsm.start();
    assertEquals(OfferState.Initial, fsm.getCurrentState());

    fsm.fire(OfferEvent.ToReserved, "reserved-context");
    assertEquals(OfferState.Reserved, fsm.getCurrentState());

    fsm.fire(OfferEvent.ToVolumeCreated, "volume-context");
    assertEquals(OfferState.VolumeCreated, fsm.getCurrentState());

    fsm.fire(OfferEvent.ToLaunched, "launched-context");
    assertEquals(OfferState.Launched, fsm.getCurrentState());

    fsm.fire(OfferEvent.ToVolumeCreated, "volume-context");
    assertEquals(OfferState.VolumeCreated, fsm.getCurrentState());

    fsm.fire(OfferEvent.ToReserved, "reserved-context");
    assertEquals(OfferState.Reserved, fsm.getCurrentState());

    fsm.fire(OfferEvent.ToInitial, "initial-context");
    assertEquals(OfferState.Initial, fsm.getCurrentState());
  }
}
