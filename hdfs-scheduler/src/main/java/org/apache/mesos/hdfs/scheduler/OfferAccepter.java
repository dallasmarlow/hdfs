package org.apache.mesos.hdfs.scheduler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Protos.*;
import org.apache.mesos.Protos.Offer.Operation;
import org.apache.mesos.Protos.Offer.Operation.Create;
import org.apache.mesos.Protos.Offer.Operation.Launch;
import org.apache.mesos.Protos.Offer.Operation.Reserve;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.hdfs.state.TaskRecord;

import java.util.Arrays;
import java.util.List;

/**
 * OfferAccepter.
 */
public class OfferAccepter {
  private final Log log = LogFactory.getLog(HdfsNode.class);

  public void reserve(SchedulerDriver driver, Offer offer, List<Resource> resources) {
    log.info("Reserving Resources: " + resources);

    Reserve reserve = getReserve(resources);

    Operation op = Operation.newBuilder()
      .setType(Operation.Type.RESERVE)
      .setReserve(reserve)
      .build();

    driver.acceptOffers(
        getOfferIds(offer),
        getOperations(op),
        getFilters());
  }

  public void createVolume(SchedulerDriver driver, Offer offer, List<Resource> volumes) {
    log.info("Creating Volumes: " + volumes);

    Create create = getCreate(volumes);

    Operation op = Operation.newBuilder()
      .setType(Operation.Type.CREATE)
      .setCreate(create)
      .build();

    driver.acceptOffers(
        getOfferIds(offer),
        getOperations(op),
        getFilters());
  }

  public void launch(SchedulerDriver driver, Offer offer, List<TaskRecord> tasks) {
    log.info("Launching Tasks: " + tasks);

    Launch launch = getLaunch(tasks);

    Operation op = Operation.newBuilder()
      .setType(Operation.Type.LAUNCH)
      .setLaunch(launch)
      .build();

    driver.acceptOffers(
        getOfferIds(offer),
        getOperations(op),
        getFilters());
  }

  private List<OfferID> getOfferIds(Offer offer) {
    return Arrays.asList(offer.getId());
  }

  private List<Operation> getOperations(Operation operation) {
    return Arrays.asList(operation);
  }

  private Filters getFilters() {
    return Filters.newBuilder().setRefuseSeconds(1).build();
  }

  private Reserve getReserve(List<Resource> resources) {
    Reserve.Builder builder = Reserve.newBuilder();

    for (Resource resource : resources) {
      builder.addResources(resource);
    }

    return builder.build();
  }

  private Create getCreate(List<Resource> volumes) {
    Create.Builder builder = Create.newBuilder();

    for (Resource volume : volumes) {
      builder.addVolumes(volume);
    }

    return builder.build();
  }

  private Launch getLaunch(List<TaskRecord> tasks) {
    Launch.Builder builder = Launch.newBuilder();

    for (TaskRecord task : tasks) {
      builder.addTaskInfos(task.getInfo());
    }

    return builder.build();
  }
}
