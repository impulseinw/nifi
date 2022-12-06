/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.processors.gcp.vision;

import static org.apache.nifi.processors.gcp.util.GoogleUtils.GCP_CREDENTIALS_PROVIDER_SERVICE;

import com.google.longrunning.Operation;
import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.rpc.Status;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

abstract public class AbstractGetGcpVisionAnnotateOperationStatus extends AbstractGcpVisionProcessor {

    private static final String GCP_OPERATION_KEY = "operationKey";

    public static final Relationship REL_RUNNING = new Relationship.Builder()
            .name("running")
            .description("The job is currently still being processed")
            .build();
    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("Upon successful completion, the original FlowFile will be routed to this relationship.")
            .autoTerminateDefault(true)
            .build();

    private static final Set<Relationship> relationships = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            REL_ORIGINAL,
            REL_SUCCESS,
            REL_FAILURE,
            REL_RUNNING
    )));

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.unmodifiableList(Arrays.asList(
                GCP_CREDENTIALS_PROVIDER_SERVICE)
        );
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        try {
            String operationKey = flowFile.getAttribute(GCP_OPERATION_KEY);
            Operation operation = getVisionClient().getOperationsClient().getOperation(operationKey);
            getLogger().info(operation.toString());
            if (operation.getDone() && !operation.hasError()) {
                GeneratedMessageV3 response = deserializeResponse(operation.getResponse().getValue());
                FlowFile childFlowFile = session.create(flowFile);
                session.write(childFlowFile, out -> out.write(JsonFormat.printer().print(response).getBytes(StandardCharsets.UTF_8)));
                session.transfer(flowFile, REL_ORIGINAL);
                session.transfer(childFlowFile, REL_SUCCESS);
            } else if (!operation.getDone()) {
                session.transfer(flowFile, REL_RUNNING);
            } else {
                Status error = operation.getError();
                getLogger().error("Failed to execute vision operation. Error code: {}, Error message: {}", error.getCode(), error.getMessage());
                session.transfer(flowFile, REL_FAILURE);
            }
        } catch (Exception e) {
            getLogger().error("Fail to get GCP Vision operation's status", e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    abstract protected GeneratedMessageV3 deserializeResponse(ByteString responseValue) throws InvalidProtocolBufferException;
}
