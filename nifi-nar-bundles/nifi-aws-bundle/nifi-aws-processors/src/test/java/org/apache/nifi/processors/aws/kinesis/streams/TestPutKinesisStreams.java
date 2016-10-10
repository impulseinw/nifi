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
package org.apache.nifi.processors.aws.kinesis.streams;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertNotNull;

public class TestPutKinesisStreams {
    private TestRunner runner;
    protected final static String CREDENTIALS_FILE = System.getProperty("user.home") + "/aws-credentials.properties";

    @Before
    public void setUp() throws Exception {
        runner = TestRunners.newTestRunner(PutKinesisStreams.class);
        runner.setProperty(PutKinesisStreams.ACCESS_KEY, "abcd");
        runner.setProperty(PutKinesisStreams.SECRET_KEY, "secret key");
        runner.setProperty(PutKinesisStreams.KINESIS_STREAMS_DELIVERY_STREAM_NAME, "deliveryName");
        runner.assertValid();
    }

    @After
    public void tearDown() throws Exception {
        runner = null;
    }

    @Test
    public void testCustomValidateBatchSize1Valid() {
        runner.setProperty(PutKinesisStreams.NR_SHARDS, "1");
        runner.assertValid();
    }

    @Test
    public void testWithSizeGreaterThan1MB() {
        runner.setProperty(PutKinesisStreams.NR_SHARDS, "1");
        runner.assertValid();
        byte [] bytes = new byte[(PutKinesisStreams.MAX_MESSAGE_SIZE + 1)];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = 'a';
        }
        runner.enqueue(bytes);
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutKinesisStreams.REL_FAILURE, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutKinesisStreams.REL_FAILURE);

        assertNotNull(flowFiles.get(0).getAttribute(PutKinesisStreams.AWS_KINESIS_STREAMS_ERROR_MESSAGE));
    }
}
