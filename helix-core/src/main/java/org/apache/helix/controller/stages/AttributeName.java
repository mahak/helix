package org.apache.helix.controller.stages;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

public enum AttributeName {
  RESOURCES,
  RESOURCES_TO_REBALANCE,
  BEST_POSSIBLE_STATE,
  CURRENT_STATE,
  CURRENT_STATE_EXCLUDING_UNKNOWN,
  CUSTOMIZED_STATE,
  INTERMEDIATE_STATE,
  MESSAGES_ALL,
  MESSAGES_SELECTED,
  MESSAGES_THROTTLE,
  LOCAL_STATE,
  EVENT_CREATE_TIME,
  helixmanager,
  clusterStatusMonitor,
  changeContext,
  instanceName,
  eventData,
  AsyncFIFOWorkerPool,
  PipelineType,
  LastRebalanceFinishTimeStamp,
  ControllerDataProvider,
  STATEFUL_REBALANCER,

  /** This is the cluster manager's session id when event is received. */
  EVENT_SESSION,

  /** Represents cluster's status, used in management mode pipeline. */
  CLUSTER_STATUS,

  // This attribute should only be used in TaskGarbageCollectionStage, misuse could cause race conditions.
  TO_BE_PURGED_WORKFLOWS,
  // This attribute should only be used in TaskGarbageCollectionStage, misuse could cause race conditions.
  JOBS_WITHOUT_CONFIG,
  // This attribute should only be used in TaskGarbageCollectionStage, misuse could cause race conditions.
  TO_BE_PURGED_JOBS_MAP
}
