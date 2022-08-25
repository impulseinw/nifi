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

package org.apache.nifi.processors.snowflake;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import net.snowflake.ingest.utils.StagedFileWrapper;
import org.apache.commons.io.FileUtils;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processors.snowflake.common.Attributes;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

public class SnowflakePipeIT implements SnowflakeConfigAware {

    @Test
    void shouldPutIntoInternalStage() throws Exception {
        final PutSnowflakeInternalStage processor = new PutSnowflakeInternalStage();

        final TestRunner runner = TestRunners.newTestRunner(processor);
        final SnowflakeConnectionProviderService connectionProviderService = createConnectionProviderService(runner);

        runner.setProperty(PutSnowflakeInternalStage.SNOWFLAKE_CONNECTION_PROVIDER, connectionProviderService.getIdentifier());
        runner.setProperty(PutSnowflakeInternalStage.INTERNAL_STAGE_NAME, internalStageName);

        final String uuid = UUID.randomUUID().toString();
        final String fileName = filePath.getFileName().toString();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), fileName);
        attributes.put(CoreAttributes.PATH.key(), uuid + "/");
        runner.enqueue(filePath, attributes);

        runner.run();

        final Set<String> checkedAttributes = new HashSet<>(Arrays.asList(Attributes.ATTRIBUTE_STAGED_FILE_PATH));
        final Map<String, String> expectedAttributesMap = new HashMap<>();
        expectedAttributesMap.put(Attributes.ATTRIBUTE_STAGED_FILE_PATH, uuid + "/" + fileName);
        final Set<Map<String, String>> expectedAttributes = new HashSet<>(Arrays.asList(expectedAttributesMap));

        runner.assertAllFlowFilesTransferred(StartSnowflakeIngest.REL_SUCCESS);
        runner.assertAttributes(StartSnowflakeIngest.REL_SUCCESS, checkedAttributes, expectedAttributes);
    }

    @Test
    void shouldStartFileIngestion() throws Exception {
        final StartSnowflakeIngest processor = new StartSnowflakeIngest();

        final TestRunner runner = TestRunners.newTestRunner(processor);
        final SnowflakeConnectionProviderService connectionProviderService = createConnectionProviderService(runner);
        final SnowflakeIngestManagerProviderService ingestManagerProviderService = createIngestManagerProviderService(runner);

        final String uuid = UUID.randomUUID().toString();
        final String fileName = filePath.getFileName().toString();

        runner.setProperty(StartSnowflakeIngest.INGEST_MANAGER_PROVIDER, ingestManagerProviderService.getIdentifier());

        try (final SnowflakeConnectionWrapper snowflakeConnection = connectionProviderService.getSnowflakeConnection()) {
            snowflakeConnection.unwrap().uploadStream(internalStageName,
                    uuid,
                    FileUtils.openInputStream(filePath.toFile()),
                    fileName,
                    false);
        }

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(Attributes.ATTRIBUTE_STAGED_FILE_PATH, uuid + "/" + stagedFilePath);
        runner.enqueue("", attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(StartSnowflakeIngest.REL_SUCCESS);
    }

    @Test
    void shouldAwaitSnowflakePipeIngestion() throws Exception {
        final GetSnowflakeIngestStatus processor = new GetSnowflakeIngestStatus();

        final TestRunner runner = TestRunners.newTestRunner(processor);
        final SnowflakeConnectionProviderService connectionProviderService = createConnectionProviderService(runner);
        final SnowflakeIngestManagerProviderService ingestManagerProviderService = createIngestManagerProviderService(runner);

        final String uuid = UUID.randomUUID().toString();
        final String fileName = filePath.getFileName().toString();

        runner.setProperty(GetSnowflakeIngestStatus.INGEST_MANAGER_PROVIDER, ingestManagerProviderService.getIdentifier());

        try (final SnowflakeConnectionWrapper snowflakeConnection = connectionProviderService.getSnowflakeConnection()) {
            snowflakeConnection.unwrap().uploadStream(internalStageName,
                    uuid,
                    FileUtils.openInputStream(filePath.toFile()),
                    fileName,
                    false);
        }

        final String stagedFilePathAttribute = uuid + "/" + stagedFilePath;

        final StagedFileWrapper stagedFile = new StagedFileWrapper(stagedFilePathAttribute);
        ingestManagerProviderService.getIngestManager().ingestFile(stagedFile, null);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(Attributes.ATTRIBUTE_STAGED_FILE_PATH, stagedFilePathAttribute);
        runner.enqueue("", attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(GetSnowflakeIngestStatus.REL_RETRY);
    }
}
