/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.azure.eventhub.checkpoint;

import com.azure.core.util.CoreUtils;
import com.azure.messaging.eventhubs.CheckpointStore;
import com.azure.messaging.eventhubs.models.Checkpoint;
import com.azure.messaging.eventhubs.models.PartitionOwnership;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.processor.exception.ProcessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;
import static org.apache.nifi.processors.azure.eventhub.checkpoint.ComponentStateCheckpointStoreUtils.KEY_CHECKPOINT;
import static org.apache.nifi.processors.azure.eventhub.checkpoint.ComponentStateCheckpointStoreUtils.KEY_OWNERSHIP;
import static org.apache.nifi.processors.azure.eventhub.checkpoint.ComponentStateCheckpointStoreUtils.checkpointToString;
import static org.apache.nifi.processors.azure.eventhub.checkpoint.ComponentStateCheckpointStoreUtils.convertOwnership;
import static org.apache.nifi.processors.azure.eventhub.checkpoint.ComponentStateCheckpointStoreUtils.createCheckpointKey;
import static org.apache.nifi.processors.azure.eventhub.checkpoint.ComponentStateCheckpointStoreUtils.createCheckpointValue;
import static org.apache.nifi.processors.azure.eventhub.checkpoint.ComponentStateCheckpointStoreUtils.createOwnershipKey;
import static org.apache.nifi.processors.azure.eventhub.checkpoint.ComponentStateCheckpointStoreUtils.createOwnershipValue;
import static org.apache.nifi.processors.azure.eventhub.checkpoint.ComponentStateCheckpointStoreUtils.ownershipListToString;
import static org.apache.nifi.processors.azure.eventhub.checkpoint.ComponentStateCheckpointStoreUtils.ownershipToString;

/**
 * The {@link com.azure.messaging.eventhubs.CheckpointStore} is responsible for managing the storage of partition ownership and checkpoint information for Azure Event Hubs consumers.
 * The underlying storage has to be persistent and centralized (shared across the consumer clients in the consumer group).
 * <p>
 * There exist one ownership entry and one checkpoint entry for each partition in the store. They represent {@link com.azure.messaging.eventhubs.models.PartitionOwnership}
 * and {@link com.azure.messaging.eventhubs.models.Checkpoint} entities in a storage-specific serialized form.
 * <p>
 * The checkpoint store is plugged into {@link com.azure.messaging.eventhubs.EventProcessorClient} and directly used by the load balancer algorithm running in each consumer client instance.
 * <p>
 * {@code ComponentStateCheckpointStore} stores the partition ownership and checkpoint information in the component's (that is {@link org.apache.nifi.processors.azure.eventhub.ConsumeAzureEventHub}
 * processor's) state using NiFi's {@link org.apache.nifi.components.state.StateManager} in the background.
 * <p>
 * The format of the ownership entry in the state map:
 * <pre>    ownership/event-hub-namespace/event-hub-name/consumer-group/partition-id -> client-id/last-modified-time/etag</pre>
 * <p>
 * The format of the checkpoint entry in the state map:
 * <pre>    checkpoint/event-hub-namespace/event-hub-name/consumer-group/partition-id -> offset/sequence-number</pre>
 * <p>
 * The checkpoint store is required to provide optimistic locking mechanism in order to avoid concurrent updating of the same ownership entry and therefore owning the same partition
 * by multiple client instances at the same time. The optimistic locking is supposed to be based on the <code>eTag</code> field of {@link com.azure.messaging.eventhubs.models.PartitionOwnership}
 * and should be supported at entry level (only updating the same partition ownership is conflicting, claiming ownership of 2 different partitions or updating 2 checkpoints in parallel are
 * valid operations as they are independent changes).
 * <p>
 * {@link org.apache.nifi.components.state.StateManager#replace(StateMap, Map, Scope)} method supports optimistic locking but only globally, in the scope of the whole state map (which may or may not
 * contain conflicting changes after update). For this reason, the state update had to be implemented in 2 phases in {@link ComponentStateCheckpointStore#claimOwnership(List)}:
 * <ul>
 *     <li>in the 1st phase the algorithm gets the current state and tries to set the ownership in memory based on <code>eTag</code>, the claim request is skipped if <code>eTag</code>
 *     does not match (the original <code>eTag</code> was retrieved in {@link ComponentStateCheckpointStore#listOwnership(String, String, String)})</li>
 *     <li>in the 2nd phase {@link org.apache.nifi.components.state.StateManager#replace(StateMap, Map, Scope)} is called to persist the new state and if it is not successful - meaning
 *     that another client instance changed the state in the meantime which may or may not be conflicting -, then the whole process needs to be started over with the 1st phase</li>
 * </ul>
 */
public class ComponentStateCheckpointStore implements CheckpointStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(ComponentStateCheckpointStore.class);

    private final String clientId;

    private final StateManager stateManager;

    public ComponentStateCheckpointStore(String clientId, StateManager stateManager) {
        this.clientId = clientId;
        this.stateManager = stateManager;
    }

    @Override
    public Flux<PartitionOwnership> listOwnership(String fullyQualifiedNamespace, String eventHubName, String consumerGroup) {
        return getState()
                .doFirst(() -> debug("listOwnership() -> Entering [{}, {}, {}]", fullyQualifiedNamespace, eventHubName, consumerGroup))
                .flatMapMany(this::getOwnerships)
                .filter(ownership ->
                        ownership.getFullyQualifiedNamespace().equals(fullyQualifiedNamespace)
                                && ownership.getEventHubName().equals(eventHubName)
                                && ownership.getConsumerGroup().equals(consumerGroup)
                )
                .doOnNext(partitionOwnership -> debug("listOwnership() -> Returning {}", ownershipToString(partitionOwnership)))
                .doOnComplete(() -> debug("listOwnership() -> Succeeded"))
                .doOnError(throwable -> debug("listOwnership() -> Failed: {}", throwable.getMessage()));
    }

    @Override
    public Flux<PartitionOwnership> claimOwnership(List<PartitionOwnership> requestedPartitionOwnerships) {
        return getState()
                .doFirst(() -> debug("claimOwnership() -> Entering [{}]", ownershipListToString(requestedPartitionOwnerships)))
                .flatMapMany(oldState -> {
                    Map<String, String> newMap = new HashMap<>(oldState.toMap());

                    List<PartitionOwnership> claimedOwnerships = new ArrayList<>();

                    long timestamp = System.currentTimeMillis();

                    for (PartitionOwnership requestedPartitionOwnership : requestedPartitionOwnerships) {
                        String key = createOwnershipKey(requestedPartitionOwnership);

                        if (oldState.get(key) != null) {
                            PartitionOwnership oldPartitionOwnership = convertOwnership(key, oldState.get(key));

                            String oldETag = oldPartitionOwnership.getETag();
                            String reqETag = requestedPartitionOwnership.getETag();
                            if (StringUtils.isNotEmpty(oldETag) && !oldETag.equals(reqETag)) {
                                debug("claimOwnership() -> Already claimed {}", ownershipToString(oldPartitionOwnership));
                                continue;
                            }
                        }

                        String newETag = CoreUtils.randomUuid().toString();

                        PartitionOwnership partitionOwnership = new PartitionOwnership()
                                .setFullyQualifiedNamespace(requestedPartitionOwnership.getFullyQualifiedNamespace())
                                .setEventHubName(requestedPartitionOwnership.getEventHubName())
                                .setConsumerGroup(requestedPartitionOwnership.getConsumerGroup())
                                .setPartitionId(requestedPartitionOwnership.getPartitionId())
                                .setOwnerId(requestedPartitionOwnership.getOwnerId())
                                .setLastModifiedTime(timestamp)
                                .setETag(newETag);

                        claimedOwnerships.add(partitionOwnership);

                        newMap.put(key, createOwnershipValue(partitionOwnership));

                        debug("claimOwnership() -> Claiming {}", ownershipToString(partitionOwnership));
                    }

                    if (claimedOwnerships.isEmpty()) {
                        return Flux.empty();
                    }

                    return updateState(oldState, newMap)
                            .thenMany(Flux.fromIterable(claimedOwnerships));
                })
                .doOnNext(partitionOwnership -> debug("claimOwnership() -> Returning {}", ownershipToString(partitionOwnership)))
                .doOnComplete(() -> debug("claimOwnership() -> Succeeded"))
                .doOnError(throwable -> debug("claimOwnership() -> Failed: {}", throwable.getMessage()))
                .retryWhen(createRetrySpec())
                .doOnError(ConcurrentStateModificationException.class, throwable -> debug("claimOwnership() -> Retry failed: {}", throwable.getMessage()));
    }

    @Override
    public Flux<Checkpoint> listCheckpoints(String fullyQualifiedNamespace, String eventHubName, String consumerGroup) {
        return getState()
                .doFirst(() -> debug("listCheckpoints() -> Entering [{}, {}, {}]", fullyQualifiedNamespace, eventHubName, consumerGroup))
                .flatMapMany(this::getCheckpoints)
                .filter(checkpoint ->
                        checkpoint.getFullyQualifiedNamespace().equals(fullyQualifiedNamespace)
                                && checkpoint.getEventHubName().equals(eventHubName)
                                && checkpoint.getConsumerGroup().equals(consumerGroup)
                )
                .doOnNext(checkpoint -> debug("listCheckpoints() -> Returning {}", checkpointToString(checkpoint)))
                .doOnComplete(() -> debug("listCheckpoints() -> Succeeded"))
                .doOnError(throwable -> debug("listCheckpoints() -> Failed: {}", throwable.getMessage()));
    }

    @Override
    public Mono<Void> updateCheckpoint(Checkpoint checkpoint) {
        return getState()
                .doFirst(() -> debug("updateCheckpoint() -> Entering [{}]", checkpointToString(checkpoint)))
                .flatMap(oldState -> {
                    Map<String, String> newMap = new HashMap<>(oldState.toMap());

                    newMap.put(createCheckpointKey(checkpoint), createCheckpointValue(checkpoint));

                    return updateState(oldState, newMap);
                })
                .doOnSuccess(__ -> debug("updateCheckpoint() -> Succeeded"))
                .doOnError(throwable -> debug("updateCheckpoint() -> Failed: {}", throwable.getMessage()))
                .retryWhen(createRetrySpec())
                .doOnError(ConcurrentStateModificationException.class, throwable -> debug("updateCheckpoint() -> Retry failed: {}", throwable.getMessage()));
    }

    private Retry createRetrySpec() {
        return Retry.backoff(10, Duration.ofMillis(100))
                .maxBackoff(Duration.ofMillis(400))
                .filter(t -> t instanceof ConcurrentStateModificationException)
                .onRetryExhaustedThrow(((retryBackoffSpec, retrySignal) -> new ConcurrentStateModificationException(
                        String.format("Retrials of concurrent state modifications has been exhausted (%d retrials)", 10))));
    }

    private Flux<PartitionOwnership> getOwnerships(StateMap state) {
        return getEntries(state, KEY_OWNERSHIP, ComponentStateCheckpointStoreUtils::convertOwnership);
    }

    private Flux<Checkpoint> getCheckpoints(StateMap state) {
        return getEntries(state, KEY_CHECKPOINT, ComponentStateCheckpointStoreUtils::convertCheckpoint);
    }

    private <T> Flux<T> getEntries(StateMap state, String kind, BiFunction<String, String, T> converter) throws ProcessException {
        return state.toMap().entrySet().stream()
                .filter(e -> e.getKey().startsWith(kind))
                .map(e -> converter.apply(e.getKey(), e.getValue()))
                .collect(collectingAndThen(toList(), Flux::fromIterable));
    }

    private void debug(String message, Object... arguments) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[clientId={}] " + message, ArrayUtils.addFirst(arguments, clientId));
        }
    }

    private Mono<StateMap> getState() {
        return Mono.defer(
                () -> {
                    try {
                        StateMap state = stateManager.getState(Scope.CLUSTER);
                        return Mono.just(state);
                    } catch (IOException e) {
                        return Mono.error(e);
                    }
                }
        );
    }

    private Mono<Void> updateState(StateMap oldState, Map<String, String> newMap) {
        return Mono.defer(
                () -> {
                    try {
                        boolean success = stateManager.replace(oldState, newMap, Scope.CLUSTER);
                        if (success) {
                            return Mono.empty();
                        } else {
                            return Mono.error(new ConcurrentStateModificationException(
                                    String.format("Component state with version [%s] has been modified by another instance" , oldState.getStateVersion().orElse("new"))));
                        }
                    } catch (IOException e) {
                        return Mono.error(e);
                    }
                }
        );
    }

    static class ConcurrentStateModificationException extends Exception {
        ConcurrentStateModificationException(String message) {
            super(message);
        }
    }
}
