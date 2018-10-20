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


package org.apache.nifi.testharness.samples;


import org.apache.nifi.testharness.TestNiFiInstance;
import org.apache.nifi.testharness.samples.mock.GetHTTPMock;
import org.apache.nifi.testharness.util.FileUtils;
import org.apache.nifi.testharness.SimpleNiFiFlowDefinitionEditor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.util.List;

import static org.junit.Assert.assertTrue;


/**
 * This test demonstrates how to mock the source data by mocking the processor
 * itself in the flow definition.
 */
public class NiFiMockFlowTest {

    private static final InputStream DEMO_DATA_AS_STREAM =
            NiFiFlowTest.class.getResourceAsStream("/sample_technology_rss.xml");


    // We have a dedicated class. It has to be public static
    // so that NiFi engine can instantiate it.
    public static class MockedGetHTTP extends GetHTTPMock {

        public MockedGetHTTP() {
            super("text/xml; charset=utf-8", () -> DEMO_DATA_AS_STREAM);
        }
    }


    private static final SimpleNiFiFlowDefinitionEditor CONFIGURE_MOCKS_IN_NIFI_FLOW = SimpleNiFiFlowDefinitionEditor.builder()
            .setClassOfSingleProcessor("GetHTTP", MockedGetHTTP.class)
            .build();


    private final TestNiFiInstance testNiFiInstance;


    public NiFiMockFlowTest() {
        try {
            if (!Constants.NIFI_ZIP_DIR.exists()) {
                throw new IllegalStateException("NiFi distribution ZIP file not found at the expected location: "
                        + Constants.NIFI_ZIP_DIR.getCanonicalPath());
            }

            File nifiZipFile = TestUtils.getBinaryDistributionZipFile(Constants.NIFI_ZIP_DIR);

            this.testNiFiInstance = TestNiFiInstance.builder()
                    .setNiFiBinaryDistributionZip(nifiZipFile)
                    .setFlowXmlToInstallForTesting(Constants.FLOW_XML_FILE)
                    .modifyFlowXmlBeforeInstalling(CONFIGURE_MOCKS_IN_NIFI_FLOW)
                    .build();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Before
    public void bootstrapNiFi() throws Exception {

        if (Constants.OUTPUT_DIR.exists()) {
            FileUtils.deleteDirectoryRecursive(Constants.OUTPUT_DIR.toPath());
        }

        testNiFiInstance.install();
        testNiFiInstance.start();
    }

    @Test
    public void testFlowCreatesFilesInCorrectLocation() throws IOException {

        // We deleted the output directory: our NiFi flow should create it

        assertTrue("Output directory not found: " + Constants.OUTPUT_DIR, Constants.OUTPUT_DIR.exists());

        File outputFile = new File(Constants.OUTPUT_DIR, "bbc-world.rss.xml");

        assertTrue("Output file not found: " + outputFile, outputFile.exists());

        List<String> strings = Files.readAllLines(outputFile.toPath());

        boolean atLeastOneLineContainsBBC = strings.stream().anyMatch(line -> line.toLowerCase().contains("bbc"));

        assertTrue("There was no line containing BBC", atLeastOneLineContainsBBC);

        boolean atLeastOneLineContainsIPhone = strings.stream().anyMatch(line -> line.toLowerCase().contains("iphone"));

        assertTrue("There was no line containing IPhone", atLeastOneLineContainsIPhone);

    }

    @After
    public void shutdownNiFi() {

        testNiFiInstance.stopAndCleanup();
    }
}
