package org.apache.helix.util;

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

import java.util.UUID;

import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Message utils to operate on message such creating messages.
 */
public class MessageUtil {
  private static final Logger LOG = LoggerFactory.getLogger(MessageUtil.class);

  // TODO: Make the message retry count configurable through the Cluster Config or IdealStates.
  public final static int DEFAULT_STATE_TRANSITION_MESSAGE_RETRY_COUNT = 3;

  public static Message createStateTransitionCancellationMessage(String srcInstanceName,
      String srcSessionId, Resource resource, String partitionName, String instanceName,
      String sessionId, String stateModelDefName, String fromState, String toState,
      String nextState, Message cancellationMessage, boolean isCancellationEnabled,
      String currentState) {
    if (isCancellationEnabled && cancellationMessage == null) {
      LOG.info("Create cancellation message of the state transition for {}.{} on {}, "
              + "currentState: {}, nextState: {},  toState: {}", resource.getResourceName(),
          partitionName, instanceName, currentState, nextState == null ? "N/A" : nextState,
          toState);

      Message message =
          createBasicStateTransitionMessage(Message.MessageType.STATE_TRANSITION_CANCELLATION,
              srcInstanceName, srcSessionId, resource, partitionName, instanceName, currentState,
              nextState, sessionId, stateModelDefName);

      message.setFromState(fromState);
      message.setToState(toState);
      return message;
    }

    return null;
  }

  /**
   * Creates a message to change participant status
   * {@link org.apache.helix.model.LiveInstance.LiveInstanceStatus}
   *
   * @param currentState current status of the live instance
   * @param nextState next status that will be changed to
   * @param srcInstanceName source instance name
   * @param srcSessionId session id for the source instance
   * @param tgtInstanceName target instance name
   * @param tgtSessionId target instance session id
   * @return participant status change message
   */
  public static Message createStatusChangeMessage(LiveInstance.LiveInstanceStatus currentState,
      LiveInstance.LiveInstanceStatus nextState, String srcInstanceName, String srcSessionId,
      String tgtInstanceName, String tgtSessionId) {
    return createBasicMessage(Message.MessageType.PARTICIPANT_STATUS_CHANGE, srcInstanceName,
        srcSessionId, tgtInstanceName, tgtSessionId, currentState.name(), nextState.name());
  }

  /* Creates a message that that has the least required fields. */
  private static Message createBasicMessage(Message.MessageType messageType, String srcInstanceName,
      String srcSessionId, String tgtInstanceName, String tgtSessionId, String currentState,
      String nextState) {
    String uuid = UUID.randomUUID().toString();

    Message message = new Message(messageType, uuid);
    message.setSrcName(srcInstanceName);
    message.setTgtName(tgtInstanceName);
    message.setMsgState(Message.MessageState.NEW);
    message.setFromState(currentState);
    message.setToState(nextState);
    message.setTgtSessionId(tgtSessionId);
    message.setSrcSessionId(srcSessionId);
    message.setExpectedSessionId(srcSessionId);

    return message;
  }

  /* Creates state transition or state transition cancellation message */
  private static Message createBasicStateTransitionMessage(Message.MessageType messageType,
      String srcInstanceName, String srcSessionId, Resource resource, String partitionName,
      String instanceName, String currentState, String nextState, String tgtSessionId,
      String stateModelDefName) {
    Message message =
        createBasicMessage(messageType, srcInstanceName, srcSessionId, instanceName, tgtSessionId,
            currentState, nextState);
    message.setPartitionName(partitionName);
    message.setStateModelDef(stateModelDefName);
    message.setResourceName(resource.getResourceName());
    message.setStateModelFactoryName(resource.getStateModelFactoryname());
    message.setBucketSize(resource.getBucketSize());

    return message;
  }

  /**
   * Create a state transition message with replica prioritization metadata
   * @param srcInstanceName source instance name
   * @param srcSessionId source session id
   * @param resource resource
   * @param partitionName partition name
   * @param instanceName target instance name
   * @param currentState current state
   * @param nextState next state
   * @param tgtSessionId target session id
   * @param stateModelDefName state model definition name
   * @param currentActiveReplicaNumber The number of replicas currently in active states
   *          for this partition before any state transitions occur. This metadata
   *          enables participant-side message prioritization by indicating the
   *          current availability level (e.g., 0→1 recovery scenarios get higher
   *          priority than 2→3 load balancing scenarios). Set to -1 for transitions
   *          that should not be prioritized (downward transitions).
   *          Active states include top states, secondary top states (for single-top
   *          state models), and ERROR states since they are still considered active by Helix.
   * @return state transition message
   */
  public static Message createStateTransitionMessage(String srcInstanceName, String srcSessionId,
      Resource resource, String partitionName, String instanceName, String currentState,
      String nextState, String tgtSessionId, String stateModelDefName,
      int currentActiveReplicaNumber) {
    Message message = createBasicStateTransitionMessage(Message.MessageType.STATE_TRANSITION,
        srcInstanceName, srcSessionId, resource, partitionName, instanceName, currentState,
        nextState, tgtSessionId, stateModelDefName);

    // Set the retry count for state transition messages.
    // TODO: make the retry count configurable in ClusterConfig or IdealState
    message.setRetryCount(DEFAULT_STATE_TRANSITION_MESSAGE_RETRY_COUNT);

    if (resource.getResourceGroupName() != null) {
      message.setResourceGroupName(resource.getResourceGroupName());
    }
    if (resource.getResourceTag() != null) {
      message.setResourceTag(resource.getResourceTag());
    }

    // Set current active replica number for participant-side prioritization
    message.setCurrentActiveReplicaNumber(currentActiveReplicaNumber);

    return message;
  }

  /**
   * Create a state transition message (backward compatibility)
   * @param srcInstanceName source instance name
   * @param srcSessionId source session id
   * @param resource resource
   * @param partitionName partition name
   * @param instanceName target instance name
   * @param currentState current state
   * @param nextState next state
   * @param tgtSessionId target session id
   * @param stateModelDefName state model definition name
   * @return state transition message
   */
  public static Message createStateTransitionMessage(String srcInstanceName, String srcSessionId,
      Resource resource, String partitionName, String instanceName, String currentState,
      String nextState, String tgtSessionId, String stateModelDefName) {
    // currentActiveReplicaNumber is set to -1 for ST messages needing no prioritization metadata
    // (backward compatibility)
    return createStateTransitionMessage(srcInstanceName, srcSessionId, resource, partitionName,
        instanceName, currentState, nextState, tgtSessionId, stateModelDefName, -1);
  }
}
