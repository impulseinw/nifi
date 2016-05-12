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

package org.apache.nifi.cluster.coordination.http.endpoints;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.nifi.cluster.coordination.http.EndpointResponseMerger;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.dto.flow.FlowDTO;
import org.apache.nifi.web.api.entity.FlowEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupEntity;

public class FlowSnippetEndpointMerger implements EndpointResponseMerger {
    public static final Pattern TEMPLATE_INSTANCE_URI_PATTERN = Pattern.compile("/nifi-api/process-groups/(?:(?:root)|(?:[a-f0-9\\-]{36}))/template-instance");
    public static final Pattern FLOW_SNIPPET_INSTANCE_URI_PATTERN = Pattern.compile("/nifi-api/process-groups/(?:(?:root)|(?:[a-f0-9\\-]{36}))/snippet-instance");

    @Override
    public boolean canHandle(final URI uri, final String method) {
        return "POST".equalsIgnoreCase(method) && (TEMPLATE_INSTANCE_URI_PATTERN.matcher(uri.getPath()).matches()
            || FLOW_SNIPPET_INSTANCE_URI_PATTERN.matcher(uri.getPath()).matches());
    }

    @Override
    public NodeResponse merge(final URI uri, final String method, Set<NodeResponse> successfulResponses, final Set<NodeResponse> problematicResponses, final NodeResponse clientResponse) {
        final FlowEntity responseEntity = clientResponse.getClientResponse().getEntity(FlowEntity.class);
        final FlowDTO flowDto = responseEntity.getFlow();

        if (flowDto == null) {
            return clientResponse;
        } else {
            final Map<String, Map<NodeIdentifier, ProcessorEntity>> processorMap = new HashMap<>();
            final Map<String, Map<NodeIdentifier, RemoteProcessGroupEntity>> remoteProcessGroupMap = new HashMap<>();

            for (final NodeResponse nodeResponse : successfulResponses) {
                final FlowEntity nodeResponseEntity = nodeResponse == clientResponse ? responseEntity : nodeResponse.getClientResponse().getEntity(FlowEntity.class);
                final FlowDTO nodeContents = nodeResponseEntity.getFlow();

                for (final ProcessorEntity nodeProcessor : nodeContents.getProcessors()) {
                    Map<NodeIdentifier, ProcessorEntity> innerMap = processorMap.get(nodeProcessor.getId());
                    if (innerMap == null) {
                        innerMap = new HashMap<>();
                        processorMap.put(nodeProcessor.getId(), innerMap);
                    }

                    innerMap.put(nodeResponse.getNodeId(), nodeProcessor);
                }

                for (final RemoteProcessGroupEntity nodeRemoteProcessGroup : nodeContents.getRemoteProcessGroups()) {
                    Map<NodeIdentifier, RemoteProcessGroupEntity> innerMap = remoteProcessGroupMap.get(nodeRemoteProcessGroup.getId());
                    if (innerMap == null) {
                        innerMap = new HashMap<>();
                        remoteProcessGroupMap.put(nodeRemoteProcessGroup.getId(), innerMap);
                    }

                    innerMap.put(nodeResponse.getNodeId(), nodeRemoteProcessGroup);
                }
            }

            final ProcessorEndpointMerger procMerger = new ProcessorEndpointMerger();
            for (final ProcessorEntity processor : flowDto.getProcessors()) {
                final String procId = processor.getId();
                final Map<NodeIdentifier, ProcessorEntity> mergeMap = processorMap.get(procId);

                procMerger.mergeResponses(processor, mergeMap, successfulResponses, problematicResponses);
            }

            final RemoteProcessGroupEndpointMerger rpgMerger = new RemoteProcessGroupEndpointMerger();
            for (final RemoteProcessGroupEntity remoteProcessGroup : flowDto.getRemoteProcessGroups()) {
                final String remoteProcessGroupId = remoteProcessGroup.getId();
                final Map<NodeIdentifier, RemoteProcessGroupEntity> mergeMap = remoteProcessGroupMap.get(remoteProcessGroupId);

                rpgMerger.mergeResponses(remoteProcessGroup, mergeMap, successfulResponses, problematicResponses);
            }
        }

        // create a new client response
        return new NodeResponse(clientResponse, responseEntity);
    }

}
