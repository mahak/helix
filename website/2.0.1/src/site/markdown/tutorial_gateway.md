<!---
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

<head>
  <title>Tutorial - Helix Gateway Service</title>
</head>

## [Helix Tutorial](./Tutorial.html): Helix Gateway Service

The Helix Gateway Service, introduced in Helix 2.0.0, lets applications participate in a Helix-managed cluster **without embedding the Helix library**. Instead of running a JVM participant that connects directly to ZooKeeper, your application opens a gRPC stream to the Gateway and exchanges simple shard-level messages.

This is useful when your application is not written in Java, when you want to keep ZooKeeper connections off your application nodes, or when you want many lightweight instances to share a small number of Helix connections.

The Gateway service lives in the `helix-gateway` module.

### An Additional Participation Mode, Not a Replacement

It is worth being precise about what the Gateway changes, because the 2.0.0 release notes describe it as "replacing the legacy state transition message approach".

At the code level, nothing about Helix's coordination model is replaced:

* **`helix-core` contains no reference to the Gateway at all**, and has no dependency on the `helix-gateway` module — the dependency runs the other way. The controller cannot distinguish a Gateway-backed participant from any other, so it computes assignments and sends **ordinary state transition messages** exactly as in 1.x.
* The Gateway joins through the **ordinary participant API**: it registers a state model factory on the manager's `StateMachineEngine` and then calls `connect()` — the same calls described in [Participant](./tutorial_participant.html).
* `HelixGatewayMultiTopStateStateModel` extends the same `StateModel` class your own participants use, and registers a catch-all `@Transition(to = "*", from = "*")` handler. That handler receives a standard Helix `Message` and translates it into a gRPC request for your application.
* The classic embedded participant API — `HelixManagerFactory`, `InstanceType.PARTICIPANT`, `StateModelFactory`, `@Transition` — is fully present and **not deprecated** in 2.x.

So the accurate framing is that the Gateway **adds a second way to participate**, and moves the Helix integration out of your application and into a shared service. Existing 1.x participants keep working unchanged, and remain the right choice for most JVM applications.

What genuinely changes is *where the Helix client lives* and *how your application is told what to do* — not how the controller coordinates the cluster.

### How It Works

The Gateway joins the Helix cluster as a **proxy participant** on your application's behalf. It runs a real Helix participant (`HelixGatewayParticipant`, backed by a `ZKHelixManager` with `InstanceType.PARTICIPANT`), so from the controller's point of view nothing unusual is happening.

![Gateway message flow](./images/gateway/gateway-message-flow.png)

The flow for a state change is:

1. The Helix controller computes the assignment and sends a state transition message to the Gateway, exactly as it would to any participant.
2. The Gateway translates that message into a `ShardChangeRequest` and pushes it to your application over the gRPC stream.
3. Your application performs the work and reports the shard's resulting current state back on the same stream.
4. The Gateway completes the Helix state transition using the reported state.

Because your application only ever sees `ADD_SHARD`, `DELETE_SHARD`, and `CHANGE_ROLE` requests, it does not need to implement a Helix state model or understand Helix's state machine.

The Gateway keeps a local cache (`GatewayCurrentStateCache`) of every connected participant's shard states, so it can compute what actually changed and avoid forwarding redundant transitions.

#### Transition Translation

The Gateway derives the request type from the transition's `fromState` and `toState`. A shard that the Gateway has never seen assigned is tracked as the special state `UNASSIGNED`.

![Transition translation](./images/gateway/gateway-transition-translation.png)

| From state | To state | Request sent to application |
| ---------- | -------- | --------------------------- |
| not `UNASSIGNED` | `DROPPED` | `DELETE_SHARD` |
| `UNASSIGNED` | not `DROPPED` | `ADD_SHARD` |
| any other combination | | `CHANGE_ROLE` |

### Supported State Models

The Gateway currently supports the **`OnlineOffline`** state model only. This is enforced by `GatewayServiceManager.SUPPORTED_MULTI_STATE_MODEL_TYPES`. Registering a resource with any other state model will not work through the Gateway.

### Starting the Gateway Service

The simplest way to start the Gateway is `HelixGatewayMain`, which takes the ZooKeeper address and the gRPC server port as its two arguments:

```
java -cp "helix-gateway-2.0.1.jar:<dependencies>" \
  org.apache.helix.gateway.HelixGatewayMain localhost:2181 50051
```

The `helix-gateway` module also builds a `helix-gateway-pkg` distribution (via the `pkg` assembly) containing `bin`, `conf`, and `repo` directories, which bundles the runtime dependencies for you.

To embed the Gateway in your own process, or to change any of its defaults, construct a `GatewayServiceManager` directly:

```
import org.apache.helix.gateway.channel.GatewayServiceChannelConfig;
import org.apache.helix.gateway.service.GatewayServiceManager;

GatewayServiceChannelConfig config =
    new GatewayServiceChannelConfig.GatewayServiceProcessorConfigBuilder()
        .setGrpcServerPort(50051)
        .build();

GatewayServiceManager manager = new GatewayServiceManager("localhost:2181", config);
manager.startService();

// on shutdown
manager.stopService();
```

A single-argument constructor, `new GatewayServiceManager(zkAddress)`, is also available when the defaults are acceptable.

### The gRPC Contract

The service is defined in `HelixGatewayService.proto` as a single bidirectional streaming call:

```
service HelixGatewayService {
  rpc report(stream ShardStateMessage) returns (stream ShardChangeRequests) {}
}
```

Your application sends `ShardStateMessage`, which carries one of two payloads:

* `ShardState` — sent on initial connection. Reports the instance name, cluster name, and the current state of every shard the instance already holds. Reporting accurate initial state lets the Gateway skip transitions your instance has effectively already completed.
* `ShardTransitionStatus` — sent in response to a change request. Reports the resulting `currentState` for each shard. If the operation failed, report the state the shard should be considered to be in.

The Gateway sends back `ShardChangeRequests`, a batch of `SingleShardChangeRequest` entries, each containing:

| Field | Meaning |
| ----- | ------- |
| `stateChangeRequestType` | `ADD_SHARD`, `DELETE_SHARD`, or `CHANGE_ROLE` |
| `resourceName` | The Helix resource the shard belongs to |
| `shardName` | The shard (partition) to act on |
| `targetState` | The state the shard should end up in |

The stream is the unit of liveness: when it closes, the Gateway treats the instance as disconnected and the controller reassigns its shards.

### Deployment Topology

The Gateway changes where the Helix client — and therefore the ZooKeeper connection — lives.

![Deployment topology](./images/gateway/gateway-topology-comparison.png)

In 1.x, every application node embeds Helix and connects to ZooKeeper directly. With the Gateway, application nodes hold no Helix dependency and no ZooKeeper connection; they speak gRPC to the Gateway, which connects to ZooKeeper on their behalf.

One detail worth understanding for capacity planning: the Gateway creates a **separate `HelixGatewayParticipant` for every connected application instance**, and each one constructs its own `ZKHelixManager` and calls `connect()` (`GatewayServiceManager` keeps them in a per-cluster, per-instance map). N connected app nodes therefore mean N ZooKeeper connections — *consolidated into the Gateway process*, not eliminated. Size the Gateway accordingly, and note that it becomes a component whose availability affects every application instance behind it.

The controller is unchanged and unaware of the difference.

### Connecting an Application

Your application connects as a gRPC client, opens the `report` stream, and reacts to change requests. The sketch below mirrors the flow exercised in `TestGatewayServiceConnection`:

```
ManagedChannel channel = ManagedChannelBuilder.forAddress(gatewayHost, 50051)
    .usePlaintext()
    .keepAliveTime(30, TimeUnit.SECONDS)
    .keepAliveWithoutCalls(true)
    .build();

HelixGatewayServiceGrpc.HelixGatewayServiceStub stub =
    HelixGatewayServiceGrpc.newStub(channel);

StreamObserver<ShardStateMessage> toGateway =
    stub.report(new StreamObserver<ShardChangeRequests>() {
      @Override
      public void onNext(ShardChangeRequests requests) {
        for (SingleShardChangeRequest r : requests.getRequestList()) {
          switch (r.getStateChangeRequestType()) {
            case ADD_SHARD:    /* load the shard  */ break;
            case DELETE_SHARD: /* drop the shard  */ break;
            case CHANGE_ROLE:  /* change its role */ break;
          }
          // Report the resulting state back on the same stream.
        }
      }

      @Override public void onError(Throwable t) { /* reconnect */ }
      @Override public void onCompleted() { }
    });

// On connect, report the shards this instance already holds.
toGateway.onNext(ShardStateMessage.newBuilder()
    .setShardState(ShardState.newBuilder()
        .setInstanceName("instance1")
        .setClusterName("MY_CLUSTER")
        .build())
    .build());
```

Two things matter for correctness:

* **Always answer a change request.** The Gateway completes the corresponding Helix state transition only when your application reports the resulting state. Silence leaves the transition pending until the client times out.
* **Report accurate state on connect.** The Gateway diffs your reported state against its cache, so a correct initial report avoids redundant `ADD_SHARD` requests for shards you already hold.

Closing the stream (or losing the connection) marks the instance offline, and the controller reassigns its shards.

### How This Differs From a 1.x Participant

In 1.x, the only way to participate was to embed Helix in your process: create a `HelixManager` with `InstanceType.PARTICIPANT`, register a `StateModelFactory`, and implement `@Transition` callbacks. That is still fully supported and remains the right choice for most JVM applications — see [Participant](./tutorial_participant.html).

The Gateway adds a second option:

| | 1.x embedded participant | 2.0 Gateway client |
| --- | --- | --- |
| Application language | JVM only | Any language with gRPC |
| Helix dependency in your app | `helix-core` on the classpath | none |
| ZooKeeper connection | every application node connects | only the Gateway connects |
| Receiving transitions | `@Transition` state model callbacks | `ShardChangeRequests` on a gRPC stream |
| Reporting state | Helix writes the current state for you | your app reports it on the stream |
| State models | any (`MasterSlave`, `LeaderStandby`, `OnlineOffline`, custom) | `OnlineOffline` only |

The equivalent of this 1.x registration:

```
manager = HelixManagerFactory.getZKHelixManager(
    clusterName, instanceName, InstanceType.PARTICIPANT, zkConnectString);
manager.getStateMachineEngine()
    .registerStateModelFactory("OnlineOffline", new OnlineOfflineStateModelFactory());
manager.connect();
```

is, with the Gateway, no Helix code in your application at all: run the Gateway service pointing at the same ZooKeeper, then open the `report` stream shown above. `ADD_SHARD` takes the place of your `OFFLINE -> ONLINE` callback and `DELETE_SHARD` the place of `ONLINE -> OFFLINE`.

Because only `OnlineOffline` is supported, applications using `MasterSlave`, `LeaderStandby`, or a custom state model must continue to use the embedded participant.

### Channel Configuration

The Gateway supports more than one transport arrangement, selected through `GatewayServiceChannelConfig`.

`ChannelMode` controls how the Gateway learns about participants:

* `PUSH_MODE` *(default)* — participants connect and push their state to the Gateway.
* `POLL_MODE` — the Gateway actively polls participant state.

`ChannelType` selects the transport for participant liveness detection and for shard state exchange. Both default to `GRPC_SERVER`; `GRPC_CLIENT` and `FILE` are also available and may be set independently.

Note that mixed modes are not supported: the mode applies to inbound information as a whole. Outbound shard change requests are always pushed by the Gateway.

Frequently used builder options and their defaults:

| Option | Default | Notes |
| ------ | ------- | ----- |
| `setGrpcServerPort(int)` | *(none)* | Required for gRPC server mode |
| `setChannelMode(ChannelMode)` | `PUSH_MODE` | |
| `setServerHeartBeatInterval(int)` | `60` | Seconds |
| `setClientTimeout(int)` | `300` | Seconds |
| `setEnableReflectionService(boolean)` | `true` | gRPC server reflection |
| `setPollIntervalSec(int)` | `60` | `POLL_MODE` only |
| `setPollStartDelaySec(int)` | `60` | `POLL_MODE` only |
| `setPollHealthCheckTimeout(int)` | `60` | `POLL_MODE` only |

When running in `POLL_MODE` with `FILE` channels, the file locations are supplied with `addPollModeConfig`:

```
new GatewayServiceChannelConfig.GatewayServiceProcessorConfigBuilder()
    .setChannelMode(GatewayServiceChannelConfig.ChannelMode.POLL_MODE)
    .addPollModeConfig(
        GatewayServiceChannelConfig.FileBasedConfigType.PARTICIPANT_CURRENT_STATE_PATH,
        "/path/to/current-state")
    .addPollModeConfig(
        GatewayServiceChannelConfig.FileBasedConfigType.SHARD_TARGET_STATE_PATH,
        "/path/to/target-state")
    .build();
```

### Registering a Participant Programmatically

`HelixGatewayParticipant.Builder` is used internally when an application instance connects, and is also available directly if you are extending the Gateway:

```
HelixGatewayParticipant participant =
    new HelixGatewayParticipant.Builder(channel, instanceName, clusterName, zkAddress,
        onDisconnectedCallback, gatewayServiceManager)
        .addMultiTopStateStateModelDefinition("OnlineOffline")
        .setInitialShardState(initialShardStateMap)
        .build();
```

`setInitialShardState` seeds the participant with shard states the instance already holds, which suppresses unnecessary transitions on reconnect.

### Notes and Current Limitations

* Only the `OnlineOffline` state model is supported.
* Hybrid channel modes (for example, push for liveness and poll for shard state) are not supported.
* Participants cannot poll for state transition requests; those are always pushed by the Gateway.

### Related

* [Participant](./tutorial_participant.html) — the traditional embedded participant, for JVM applications that connect to Helix directly.
* [Helix REST Service 2.0](./tutorial_rest_service.html) — administrative REST API.
