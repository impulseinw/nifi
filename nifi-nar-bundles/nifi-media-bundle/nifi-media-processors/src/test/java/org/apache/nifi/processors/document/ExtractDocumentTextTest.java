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
package org.apache.nifi.processors.document;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;


public class ExtractDocumentTextTest {
    private TestRunner testRunner;

    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(ExtractDocumentText.class);
    }

    @Test
    @DisplayName("Should support PDF types without exceptions being thrown")
    public void processorShouldSupportPDF() throws Exception {
        final String filename = "simple.pdf";
        MockFlowFile flowFile = testRunner.enqueue(new FileInputStream("src/test/resources/" + filename));
        Map<String, String> attrs = Collections.singletonMap("filename", filename);
        flowFile.putAttributes(attrs);

        testRunner.assertValid();
        testRunner.run();
        testRunner.assertTransferCount(ExtractDocumentText.REL_FAILURE, 0);

        List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(ExtractDocumentText.REL_EXTRACTED);
        for (MockFlowFile mockFile : successFiles) {
            String result = new String(mockFile.toByteArray(), "UTF-8");
            String trimmedResult = result.trim();
            assertTrue(trimmedResult.startsWith("A Simple PDF File"));
        }
    }

    @Test
    @DisplayName("Should support MS Word DOC types without throwing exceptions")
    public void processorShouldSupportDOC() throws Exception {
        final String filename = "simple.doc";
        MockFlowFile flowFile = testRunner.enqueue(new FileInputStream("src/test/resources/" + filename));
        Map<String, String> attrs = Collections.singletonMap("filename", filename);
        flowFile.putAttributes(attrs);

        testRunner.assertValid();
        testRunner.run();
        testRunner.assertTransferCount(ExtractDocumentText.REL_FAILURE, 0);

        List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(ExtractDocumentText.REL_EXTRACTED);
        for (MockFlowFile mockFile : successFiles) {
            String result = new String(mockFile.toByteArray(), "UTF-8");
            String trimmedResult = result.trim();
            assertTrue(trimmedResult.startsWith("A Simple WORD DOC File"));
        }
    }

    @Test
    @DisplayName("Should support MS Word DOCX types without exception")
    public void processorShouldSupportDOCX() throws Exception {
        final String filename = "simple.docx";
        MockFlowFile flowFile = testRunner.enqueue(new FileInputStream("src/test/resources/" + filename));
        Map<String, String> attrs = Collections.singletonMap("filename", filename);
        flowFile.putAttributes(attrs);

        testRunner.assertValid();
        testRunner.run();
        testRunner.assertTransferCount(ExtractDocumentText.REL_FAILURE, 0);

        List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(ExtractDocumentText.REL_EXTRACTED);
        for (MockFlowFile mockFile : successFiles) {
            String result = new String(mockFile.toByteArray(), "UTF-8");
            String trimmedResult = result.trim();
            assertTrue(trimmedResult.startsWith("A Simple WORD DOCX File"));
        }
    }

    @Test
    @DisplayName("The PDF mime type should be discovered when examining a PDF")
    public void shouldFindPDFMimeTypeWhenProcessingPDFs() throws Exception {
        final String filename = "simple.pdf";
        MockFlowFile flowFile = testRunner.enqueue(new FileInputStream("src/test/resources/" + filename));
        Map<String, String> attrs = Collections.singletonMap("filename", filename);
        flowFile.putAttributes(attrs);

        testRunner.assertValid();
        testRunner.run();

        testRunner.assertTransferCount(ExtractDocumentText.REL_FAILURE, 0);
        testRunner.assertTransferCount(ExtractDocumentText.REL_EXTRACTED, 1);
        testRunner.assertTransferCount(ExtractDocumentText.REL_ORIGINAL, 1);
        List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(ExtractDocumentText.REL_EXTRACTED);
        for (MockFlowFile mockFile : successFiles) {
            mockFile.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
            mockFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "text/plain");
        }
    }

    @Test
    @DisplayName("DOC mime type should be discovered when processing a MS Word doc file")
    public void shouldFindDOCMimeTypeWhenProcessingMSWordDoc() throws Exception {
        final String filename = "simple.doc";
        MockFlowFile flowFile = testRunner.enqueue(new FileInputStream("src/test/resources/" + filename));
        Map<String, String> attrs = Collections.singletonMap("filename", filename);
        flowFile.putAttributes(attrs);

        testRunner.assertValid();
        testRunner.run();

        testRunner.assertTransferCount(ExtractDocumentText.REL_FAILURE, 0);
        testRunner.assertTransferCount(ExtractDocumentText.REL_EXTRACTED, 1);
        testRunner.assertTransferCount(ExtractDocumentText.REL_ORIGINAL, 1);
        List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(ExtractDocumentText.REL_EXTRACTED);
        for (MockFlowFile mockFile : successFiles) {
            mockFile.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
            mockFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "text/plain");
        }
    }

    @Test
    @DisplayName("Should discover DOCX mime type when processing docx file")
    public void shouldFindDOCXMimeType() throws Exception {
        final String filename = "simple.docx";
        MockFlowFile flowFile = testRunner.enqueue(new FileInputStream("src/test/resources/" + filename));
        Map<String, String> attrs = Collections.singletonMap("filename", filename);
        flowFile.putAttributes(attrs);

        testRunner.assertValid();
        testRunner.run();

        testRunner.assertTransferCount(ExtractDocumentText.REL_FAILURE, 0);
        testRunner.assertTransferCount(ExtractDocumentText.REL_EXTRACTED, 1);
        testRunner.assertTransferCount(ExtractDocumentText.REL_ORIGINAL, 1);
        List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(ExtractDocumentText.REL_EXTRACTED);
        for (MockFlowFile mockFile : successFiles) {
            mockFile.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
            mockFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "text/plain");
        }
    }

    @Test
    @DisplayName("Unlimited text length should be the default setting")
    public void unlimitedTextShouldBeDefault() throws Exception {
        final String filename = "big.pdf";
        MockFlowFile flowFile = testRunner.enqueue(new FileInputStream("src/test/resources/" + filename));
        Map<String, String> attrs = Collections.singletonMap("filename", filename);
        flowFile.putAttributes(attrs);

        testRunner.assertValid();
        testRunner.run();

        testRunner.assertTransferCount(ExtractDocumentText.REL_FAILURE, 0);
        testRunner.assertTransferCount(ExtractDocumentText.REL_EXTRACTED, 1);
        testRunner.assertTransferCount(ExtractDocumentText.REL_ORIGINAL, 1);
        List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(ExtractDocumentText.REL_EXTRACTED);
        for (MockFlowFile mockFile : successFiles) {
            String result = new String(mockFile.toByteArray(), "UTF-8");
            assertTrue(result.length() > 100);
        }
    }
}