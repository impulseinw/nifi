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

package org.apache.nifi.tests.system.clustering;

import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.tests.system.SpawnedClusterNiFiInstanceFactory;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.web.api.dto.NodeDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.entity.ClusterEntity;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class OffloadIT extends NiFiSystemIT {
    private static final Logger logger = LoggerFactory.getLogger(OffloadIT.class);

    @Override
    protected NiFiInstanceFactory getInstanceFactory() {
        return new SpawnedClusterNiFiInstanceFactory(
            "src/test/resources/conf/clustered/node1/bootstrap.conf",
            "src/test/resources/conf/clustered/node2/bootstrap.conf");
    }

    @Test
    public void testOffload() throws InterruptedException, IOException, NiFiClientException {
        for (int i=0; i < 5; i++) {
            logger.info("Running iteration {}", i);
            testIteration();
            logger.info("Node reconnected to cluster");
            destroyFlow();
        }
    }

    private void testIteration() throws NiFiClientException, IOException, InterruptedException {
        ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        ProcessorEntity sleep = getClientUtil().createProcessor("Sleep");
        ConnectionEntity connectionEntity = getClientUtil().createConnection(generate, sleep, "success");

        getClientUtil().setAutoTerminatedRelationships(sleep, "success");
        generate = getClientUtil().updateProcessorProperties(generate, Collections.singletonMap("File Size", "1 KB"));
        final ProcessorConfigDTO configDto = generate.getComponent().getConfig();
        configDto.setSchedulingPeriod("0 sec");
        getClientUtil().updateProcessorConfig(generate, configDto);

        getClientUtil().updateProcessorProperties(sleep, Collections.singletonMap("onTrigger Sleep Time", "100 ms"));


        getClientUtil().startProcessGroupComponents("root");

        waitForQueueNotEmpty(connectionEntity.getId());

        final NodeDTO node2Dto = getNodeDTO(5672);

        disconnectNode(node2Dto);

        final String nodeId = node2Dto.getNodeId();
        getClientUtil().offloadNode(nodeId);
        waitFor(this::isNodeOffloaded);

        getClientUtil().connectNode(nodeId);
        waitForAllNodesConnected();
    }

    private boolean isNodeOffloaded() {
        final ClusterEntity clusterEntity;
        try {
            clusterEntity = getNifiClient().getControllerClient().getNodes();
        } catch (final Exception e) {
            logger.error("Failed to determine if node is offloaded", e);
            return false;
        }

        final Collection<NodeDTO> nodeDtos = clusterEntity.getCluster().getNodes();

        for (final NodeDTO dto : nodeDtos) {
            final String status = dto.getStatus();
            if (status.equalsIgnoreCase("OFFLOADED")) {
                return true;
            }
        }

        return false;
    }

    private NodeDTO getNodeDTO(final int apiPort) throws NiFiClientException, IOException {
        final ClusterEntity clusterEntity = getNifiClient().getControllerClient().getNodes();
        final NodeDTO node2Dto = clusterEntity.getCluster().getNodes().stream()
            .filter(nodeDto -> nodeDto.getApiPort() == apiPort)
            .findAny()
            .orElseThrow(() -> new RuntimeException("Could not locate Node 2"));

        return node2Dto;
    }


    private void disconnectNode(final NodeDTO nodeDto) throws NiFiClientException, IOException, InterruptedException {
        getClientUtil().disconnectNode(nodeDto.getNodeId());

        final Integer apiPort = nodeDto.getApiPort();
        waitFor(() -> {
            try {
                final NodeDTO dto = getNodeDTO(apiPort);
                final String status = dto.getStatus();
                return "DISCONNECTED".equals(status);
            } catch (final Exception e) {
                logger.error("Failed to determine if node is disconnected", e);
            }

            return false;
        });
    }

}
