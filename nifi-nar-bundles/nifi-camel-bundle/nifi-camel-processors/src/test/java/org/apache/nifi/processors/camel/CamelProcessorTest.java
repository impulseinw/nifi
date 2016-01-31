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
package org.apache.nifi.processors.camel;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CamelProcessorTest {

    private static final Logger LOGGER =
        LoggerFactory.getLogger(CamelProcessorTest.class);
    private TestRunner testRunner;
    private static final int THREAD_COUNT=1;
    private static final int ITERATION=1;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(CamelProcessor.class);
        testRunner.setThreadCount(THREAD_COUNT);
    }


    /**
     * This test will up a dummy camel route to send a Flow File as camel exchange.
     * Returned response FlowFile will be tested for the same content as original.
     */
    @Test
    public void testProcessorBasic() {
        testRunner.setProperty(CamelProcessor.CAMEL_SPRING_CONTEXT_FILE_PATH,"classpath*:/META-INF/camel-application-context.xml");
        testRunner.setProperty(CamelProcessor.CAMEL_ENTRY_POINT_URI, "direct-vm:nifiEntryPoint");
        String content="Hello NiFi, said Camel";
        testRunner.enqueue(content);
        testRunner.run(ITERATION);
        testRunner.assertAllFlowFilesTransferred(CamelProcessor.SUCCESS, 1);
        final MockFlowFile processedFlowFile = testRunner
                .getFlowFilesForRelationship(CamelProcessor.SUCCESS).get(0);
        processedFlowFile.assertContentEquals(content);
        LOGGER.debug("Content Processed Successully");
    }

    /**
     * This test will up a dummy camel route to send a Flow File as camel exchange.
     * Modified response FlowFile will be tested for a string , pushed from camel.
     */
    @Test
    public void testProcessorFlowFileMod() {
        testRunner.setProperty(CamelProcessor.CAMEL_SPRING_CONTEXT_FILE_PATH,"classpath*:/META-INF/camel-application-context.xml");
        testRunner.setProperty(CamelProcessor.CAMEL_ENTRY_POINT_URI, "direct-vm:nifiEntryPoint2");
        testRunner.enqueue("Hello");
        testRunner.run(ITERATION);
        testRunner.assertAllFlowFilesTransferred(CamelProcessor.SUCCESS, 1);
        final MockFlowFile processedFlowFile = testRunner
                .getFlowFilesForRelationship(CamelProcessor.SUCCESS).get(0);
        processedFlowFile.assertContentEquals("Hello NiFi");
        LOGGER.debug("Content Processed Successully");
    }

    /**
     * This test will up a dummy camel route with dependency[camel-stream], which is not avail in classpath.
     * Inside the Processor Groovy-Grape will be used to satisfy that dependency by downloading from maven repository.
     * Then the enqueued {@link MockFlowFile} will be captured back at output of the {@link CamelProcessor}
     * and will be tested for a string , pushed from camel.
     */
    @Test
    public void testProcessorGrapeGrab() {
        testRunner.setProperty(CamelProcessor.CAMEL_SPRING_CONTEXT_FILE_PATH,
                               "classpath*:/META-INF/camel-undefined-dependency-application-context.xml");
        testRunner.setProperty(CamelProcessor.CAMEL_ENTRY_POINT_URI, "direct-vm:nifiEntryPoint3");
        testRunner.setProperty(CamelProcessor.EXT_LIBRARIES, "org.apache.camel/camel-stream/2.16.1");
        testRunner.enqueue("Hello");
        testRunner.run(ITERATION);
        testRunner.assertAllFlowFilesTransferred(CamelProcessor.SUCCESS, 1);
        final MockFlowFile processedFlowFile = testRunner
                .getFlowFilesForRelationship(CamelProcessor.SUCCESS).get(0);
        processedFlowFile.assertContentEquals("Hello");
        LOGGER.debug("Content Processed Successully");
    }


    @Test
    public void testValidatorWithNull(){
        Assert.assertTrue(CamelProcessor.GrapeGrabValidator.INSTANCE.validate("GrapeGrabValidation", null, null).isValid());
    }

    @Test
    public void testValidatorWithEmptyString(){
        String input ="";
        Assert.assertTrue(CamelProcessor.GrapeGrabValidator.INSTANCE.validate("GrapeGrabValidation", input, null).isValid());
    }

    @Test
    public void testValidatorWithInvalidGrapeURL(){
        String input ="a/b/c/1.2.1";
        Assert.assertFalse(CamelProcessor.GrapeGrabValidator.INSTANCE.validate("GrapeGrabValidation", input, null).isValid());
    }

    @Test
    public void testValidatorWithValidGrapeURL(){
        String input ="a/b/1.2.1";
        Assert.assertTrue(CamelProcessor.GrapeGrabValidator.INSTANCE.validate("GrapeGrabValidation", input, null).isValid());
    }

}
