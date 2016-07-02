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
package org.apache.nifi.cluster.manager;

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.status.ProcessorStatusDTO;
import org.apache.nifi.web.api.entity.ProcessorEntity;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ProcessorEntityMerger implements ComponentEntityMerger<ProcessorEntity>, ComponentEntityStatusMerger<ProcessorStatusDTO> {
    @Override
    public void merge(ProcessorEntity clientEntity, Map<NodeIdentifier, ProcessorEntity> entityMap) {
        ComponentEntityMerger.super.merge(clientEntity, entityMap);
        merge(clientEntity.getStatus(), entityMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getStatus())));
    }

    /**
     * Merges the ProcessorEntity responses.
     *
     * @param clientEntity the entity being returned to the client
     * @param entityMap all node responses
     */
    public void mergeComponents(final ProcessorEntity clientEntity, final Map<NodeIdentifier, ProcessorEntity> entityMap) {
        final ProcessorDTO clientDto = clientEntity.getComponent();
        final Map<NodeIdentifier, ProcessorDTO> dtoMap = new HashMap<>();
        for (final Map.Entry<NodeIdentifier, ProcessorEntity> entry : entityMap.entrySet()) {
            final ProcessorEntity nodeProcEntity = entry.getValue();
            final ProcessorDTO nodeProcDto = nodeProcEntity.getComponent();
            dtoMap.put(entry.getKey(), nodeProcDto);
        }

        mergeDtos(clientDto, dtoMap);
    }

    @Override
    public void merge(ProcessorStatusDTO clientEntityStatus, Map<NodeIdentifier, ProcessorStatusDTO> entityStatusMap) {
        for (final Map.Entry<NodeIdentifier, ProcessorStatusDTO> entry : entityStatusMap.entrySet()) {
            final NodeIdentifier nodeId = entry.getKey();
            final ProcessorStatusDTO entityStatus = entry.getValue();
            if (entityStatus != clientEntityStatus) {
                StatusMerger.merge(clientEntityStatus, entityStatus, nodeId.getId(), nodeId.getApiAddress(), nodeId.getApiPort());
            }
        }
    }

    private static void mergeDtos(final ProcessorDTO clientDto, final Map<NodeIdentifier, ProcessorDTO> dtoMap) {
        // if unauthorized for the client dto, simple return
        if (clientDto == null) {
            return;
        }

        final Map<String, Set<NodeIdentifier>> validationErrorMap = new HashMap<>();

        for (final Map.Entry<NodeIdentifier, ProcessorDTO> nodeEntry : dtoMap.entrySet()) {
            final ProcessorDTO nodeProcessor = nodeEntry.getValue();

            // merge the validation errors, if authorized
            if (nodeProcessor != null) {
                final NodeIdentifier nodeId = nodeEntry.getKey();
                ErrorMerger.mergeErrors(validationErrorMap, nodeId, nodeProcessor.getValidationErrors());
            }
        }

        // set the merged the validation errors
        clientDto.setValidationErrors(ErrorMerger.normalizedMergedErrors(validationErrorMap, dtoMap.size()));
    }
}
