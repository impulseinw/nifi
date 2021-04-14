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

package org.apache.nifi.cluster.lifecycle;

import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.node.DisconnectionCode;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.coordination.node.OffloadCode;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.controller.DecommissionTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ClusterDecommissionTask implements DecommissionTask {
    private static final Logger logger = LoggerFactory.getLogger(ClusterDecommissionTask.class);
    private static final int delaySeconds = 3;

    private final ClusterCoordinator clusterCoordinator;
    private NodeIdentifier localNodeIdentifier;

    public ClusterDecommissionTask(final ClusterCoordinator clusterCoordinator) {
        this.clusterCoordinator = clusterCoordinator;
    }

    @Override
    public synchronized void decommission() throws InterruptedException {
        if (clusterCoordinator == null) {
            throw new IllegalStateException("Cannot decommission Node because it is not part of a cluster");
        }

        logger.info("Decommissioning Node...");
        localNodeIdentifier = clusterCoordinator.getLocalNodeIdentifier();
        if (localNodeIdentifier == null) {
            throw new IllegalStateException("Node has not yet connected to the cluster");
        }

        disconnectNode();
        logger.info("Successfully disconnected node from cluster");

        offloadNode();
        logger.info("Successfully triggered Node Offload. Will wait for offload to complete");

        waitForOffloadToFinish();
        logger.info("Offload has successfully completed.");

        removeFromCluster();
        logger.info("Node successfully removed from cluster. Decommission is complete.");
    }

    private void disconnectNode() throws InterruptedException {
        logger.info("Requesting that Node disconnect from cluster");

        while (true) {
            final Future<Void> future = clusterCoordinator.requestNodeDisconnect(localNodeIdentifier, DisconnectionCode.USER_DISCONNECTED, "Node is being decommissioned");
            try {
                future.get();
                return;
            } catch (final ExecutionException e) {
                final Throwable cause = e.getCause();
                logger.error("Failed when attempting to disconnect node from cluster", cause);
            }
        }
    }

    private void offloadNode() throws InterruptedException {
        logger.info("Requesting that Node be offloaded");

        while (true) {
            final Future<Void> future = clusterCoordinator.requestNodeOffload(localNodeIdentifier, OffloadCode.OFFLOADED, "Node is being decommissioned");
            try {
                future.get();
                break;
            } catch (final ExecutionException e) {
                final Throwable cause = e.getCause();
                logger.error("Failed when attempting to disconnect node from cluster", cause);
            }
        }

        // Wait until status changes to either OFFLOADING or OFFLOADED.
        while (true) {
            final NodeConnectionStatus status = clusterCoordinator.getConnectionStatus(localNodeIdentifier);
            final NodeConnectionState state = status.getState();
            logger.debug("Node state is {}", state);

            if (state == NodeConnectionState.OFFLOADING || state == NodeConnectionState.OFFLOADED) {
                return;
            }

            TimeUnit.SECONDS.sleep(delaySeconds);
        }
    }

    private void waitForOffloadToFinish() throws InterruptedException {
        logger.info("Waiting for Node to finish offloading");

        while (true) {
            final NodeConnectionStatus status = clusterCoordinator.getConnectionStatus(localNodeIdentifier);
            final NodeConnectionState state = status.getState();
            if (state == NodeConnectionState.OFFLOADED) {
                return;
            }

            if (state != NodeConnectionState.OFFLOADING) {
                throw new IllegalStateException("Expected state of Node to be OFFLOADING but Node is now in a state of " + state);
            }

            logger.debug("Node state is OFFLOADING. Will wait {} seconds and check again", delaySeconds);
            TimeUnit.SECONDS.sleep(delaySeconds);
        }
    }

    private void removeFromCluster() {
        clusterCoordinator.removeNode(localNodeIdentifier, "<Local Decommission>");
    }
}
