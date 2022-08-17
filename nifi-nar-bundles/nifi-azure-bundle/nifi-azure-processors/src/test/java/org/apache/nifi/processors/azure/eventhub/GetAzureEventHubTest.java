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
package org.apache.nifi.processors.azure.eventhub;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.models.LastEnqueuedEventProperties;
import com.azure.messaging.eventhubs.models.PartitionContext;
import com.azure.messaging.eventhubs.models.PartitionEvent;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class GetAzureEventHubTest {
    private static final String DOMAIN_NAME = "servicebus";
    private static final String EVENT_HUB_NAMESPACE = "NAMESPACE";
    private static final String EVENT_HUB_NAME = "NAME";
    private static final String POLICY_NAME = "POLICY";
    private static final String POLICY_KEY = "POLICY-KEY";
    private static final String CONSUMER_GROUP = "$Default";
    private static final Instant ENQUEUED_TIME = Instant.now();
    private static final long SEQUENCE_NUMBER = 32;
    private static final long OFFSET = 64;
    private static final String PARTITIONS = "1";
    private static final String PARTITION_ID = "0";
    private static final String CONTENT = String.class.getSimpleName();

    private List<PartitionEvent> partitionEvents;

    private TestRunner testRunner;

    @BeforeEach
    public void setUp() throws Exception {
        partitionEvents = new ArrayList<>();
        testRunner = TestRunners.newTestRunner(new MockGetAzureEventHub());
    }

    @Test
    public void testProperties() {
        testRunner.setProperty(GetAzureEventHub.EVENT_HUB_NAME, EVENT_HUB_NAME);
        testRunner.assertNotValid();
        testRunner.setProperty(GetAzureEventHub.NAMESPACE, EVENT_HUB_NAMESPACE);
        testRunner.assertNotValid();
        testRunner.setProperty(GetAzureEventHub.ACCESS_POLICY, POLICY_NAME);
        testRunner.assertNotValid();
        testRunner.setProperty(GetAzureEventHub.POLICY_PRIMARY_KEY, POLICY_KEY);
        testRunner.assertNotValid();
        testRunner.setProperty(GetAzureEventHub.NUM_PARTITIONS, PARTITIONS);
        testRunner.assertValid();
        testRunner.setProperty(GetAzureEventHub.ENQUEUE_TIME, ENQUEUED_TIME.toString());
        testRunner.assertValid();
        testRunner.setProperty(GetAzureEventHub.RECEIVER_FETCH_SIZE, "5");
        testRunner.assertValid();
        testRunner.setProperty(GetAzureEventHub.RECEIVER_FETCH_TIMEOUT, "10000");
        testRunner.assertValid();
    }

    @Test
    public void testPropertiesManagedIdentity() {
        testRunner.setProperty(GetAzureEventHub.EVENT_HUB_NAME, EVENT_HUB_NAME);
        testRunner.assertNotValid();
        testRunner.setProperty(GetAzureEventHub.NAMESPACE, EVENT_HUB_NAMESPACE);
        testRunner.assertNotValid();
        testRunner.setProperty(GetAzureEventHub.NUM_PARTITIONS, PARTITIONS);
        testRunner.assertNotValid();
        testRunner.setProperty(PutAzureEventHub.USE_MANAGED_IDENTITY, Boolean.TRUE.toString());
        testRunner.assertValid();
    }

    @Test
    public void testRunNoEventsReceived(){
        setProperties();

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(GetAzureEventHub.REL_SUCCESS, 0);
    }

    @Test
    public void testRunEventsReceived() {
        setProperties();

        final PartitionEvent partitionEvent = createPartitionEvent();
        partitionEvents.add(partitionEvent);

        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(GetAzureEventHub.REL_SUCCESS, 1);

        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(GetAzureEventHub.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(CONTENT);
        flowFile.assertAttributeEquals("eventhub.enqueued.timestamp", ENQUEUED_TIME.toString());
        flowFile.assertAttributeEquals("eventhub.offset", Long.toString(OFFSET));
        flowFile.assertAttributeEquals("eventhub.sequence", Long.toString(SEQUENCE_NUMBER));
        flowFile.assertAttributeEquals("eventhub.name", EVENT_HUB_NAME);
    }

    private class MockGetAzureEventHub extends GetAzureEventHub {

        @Override
        protected Iterable<PartitionEvent> receiveEvents(final String partitionId) {
            return partitionEvents;
        }
    }

    private PartitionEvent createPartitionEvent() {
        final PartitionContext partitionContext = new PartitionContext(DOMAIN_NAME, EVENT_HUB_NAME, CONSUMER_GROUP, PARTITION_ID);
        final EventData eventData = new EventData(CONTENT);
        final LastEnqueuedEventProperties lastEnqueuedEventProperties = new LastEnqueuedEventProperties(SEQUENCE_NUMBER, OFFSET, ENQUEUED_TIME, ENQUEUED_TIME);
        return new PartitionEvent(partitionContext, eventData, lastEnqueuedEventProperties);
    }

    private void setProperties() {
        testRunner.setProperty(GetAzureEventHub.EVENT_HUB_NAME, EVENT_HUB_NAME);
        testRunner.setProperty(GetAzureEventHub.NAMESPACE, EVENT_HUB_NAMESPACE);
        testRunner.setProperty(GetAzureEventHub.ACCESS_POLICY, POLICY_NAME);
        testRunner.setProperty(GetAzureEventHub.POLICY_PRIMARY_KEY, POLICY_KEY);
        testRunner.setProperty(GetAzureEventHub.NUM_PARTITIONS, PARTITIONS);
        testRunner.assertValid();
    }
}
