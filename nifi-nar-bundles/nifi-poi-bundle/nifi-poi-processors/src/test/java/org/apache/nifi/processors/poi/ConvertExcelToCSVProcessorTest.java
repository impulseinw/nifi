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
package org.apache.nifi.processors.poi;

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class ConvertExcelToCSVProcessorTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(ConvertExcelToCSVProcessor.class);
    }

    @Test
    public void testMultipleSheetsGeneratesMultipleFlowFiles() throws Exception {

        testRunner.enqueue(new File("src/test/resources/TwoSheets.xlsx").toPath());
        testRunner.run();

        testRunner.assertTransferCount(ConvertExcelToCSVProcessor.SUCCESS, 2);
        testRunner.assertTransferCount(ConvertExcelToCSVProcessor.ORIGINAL, 1);
        testRunner.assertTransferCount(ConvertExcelToCSVProcessor.FAILURE, 0);

        MockFlowFile ffSheetA = testRunner.getFlowFilesForRelationship(ConvertExcelToCSVProcessor.SUCCESS).get(0);
        Long rowsSheetA = new Long(ffSheetA.getAttribute(ConvertExcelToCSVProcessor.ROW_NUM));
        assertTrue(rowsSheetA == 4l);
        assertTrue(ffSheetA.getAttribute(ConvertExcelToCSVProcessor.SHEET_NAME).equalsIgnoreCase("TestSheetA"));

        MockFlowFile ffSheetB = testRunner.getFlowFilesForRelationship(ConvertExcelToCSVProcessor.SUCCESS).get(1);
        Long rowsSheetB = new Long(ffSheetB.getAttribute(ConvertExcelToCSVProcessor.ROW_NUM));
        assertTrue(rowsSheetB == 3l);
        assertTrue(ffSheetB.getAttribute(ConvertExcelToCSVProcessor.SHEET_NAME).equalsIgnoreCase("TestSheetB"));
    }

    @Test
    public void testProcessAllSheets() throws Exception {

        testRunner.enqueue(new File("src/test/resources/CollegeScorecard.xlsx").toPath());
        testRunner.run();

        testRunner.assertTransferCount(ConvertExcelToCSVProcessor.SUCCESS, 1);
        testRunner.assertTransferCount(ConvertExcelToCSVProcessor.ORIGINAL, 1);
        testRunner.assertTransferCount(ConvertExcelToCSVProcessor.FAILURE, 0);

        MockFlowFile ff = testRunner.getFlowFilesForRelationship(ConvertExcelToCSVProcessor.SUCCESS).get(0);
        Long l = new Long(ff.getAttribute(ConvertExcelToCSVProcessor.ROW_NUM));
        assertTrue(l == 7805l);
    }

    @Test
    public void testProcessASpecificSheetThatDoesExist() throws Exception {

        testRunner.setProperty(ConvertExcelToCSVProcessor.DESIRED_SHEETS, "Scorecard");
        testRunner.enqueue(new File("src/test/resources/CollegeScorecard.xlsx").toPath());
        testRunner.run();

        testRunner.assertTransferCount(ConvertExcelToCSVProcessor.SUCCESS, 1);
        testRunner.assertTransferCount(ConvertExcelToCSVProcessor.ORIGINAL, 1);
        testRunner.assertTransferCount(ConvertExcelToCSVProcessor.FAILURE, 0);

        MockFlowFile ff = testRunner.getFlowFilesForRelationship(ConvertExcelToCSVProcessor.SUCCESS).get(0);
        Long l = new Long(ff.getAttribute(ConvertExcelToCSVProcessor.ROW_NUM));
        assertTrue(l == 7805l);
    }

    /**
     * We do want to allow this to be a success relationship because if arbitrary Excel
     * @throws Exception
     *  Any exception thrown
     */
    @Test
    public void testNonExistantSpecifiedSheetName() throws Exception {

        testRunner.setProperty(ConvertExcelToCSVProcessor.DESIRED_SHEETS, "NopeIDoNotExist");
        testRunner.enqueue(new File("src/test/resources/CollegeScorecard.xlsx").toPath());
        testRunner.run();

        testRunner.assertTransferCount(ConvertExcelToCSVProcessor.SUCCESS, 0);  //We aren't expecting any output to success here because the sheet doesn't exist
        testRunner.assertTransferCount(ConvertExcelToCSVProcessor.ORIGINAL, 1);
        testRunner.assertTransferCount(ConvertExcelToCSVProcessor.FAILURE, 0);
    }
}
