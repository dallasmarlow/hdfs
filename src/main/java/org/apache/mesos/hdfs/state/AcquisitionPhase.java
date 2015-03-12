package org.apache.mesos.hdfs.state;

public enum AcquisitionPhase {
  RECONCILING_TASKS, // Waits here for the timeout on (re)registration
  JOURNAL_NODES, // Launches and waits for all journalnodes to start
  WAIT_FOR_JOURNAL_NODES, // waits for journal nodes to be available via DNS
  START_NAME_NODES, // Launches the both namenodes
  WAIT_FOR_NAME_NODES, // waits for name nodes to become available via DNS
  FORMAT_NAME_NODES, // Formats both namenodes (first with initialize, second with bootstrap
  DATA_NODES // If everything is healthy the scheduler stays here and tries to launch
             // datanodes on any slave that doesn't have an hdfs task running on it
}
