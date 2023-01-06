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
package org.apache.nifi.processors;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.sql.Timestamp;
import java.util.List;

public class PutIoTDBIT {
    private static TestRunner testRunner;
    private static MockRecordParser recordReader;
    private static Session session;

    @BeforeEach
    public void init() throws IoTDBConnectionException {
        testRunner = TestRunners.newTestRunner(PutIoTDBRecord.class);
        recordReader = new MockRecordParser();
        testRunner.setProperty(PutIoTDBRecord.RECORD_READER_FACTORY, "reader");
        testRunner.setProperty("Host", "127.0.0.1");
        testRunner.setProperty("Username", "root");
        testRunner.setProperty("Password", "root");
        testRunner.setProperty("Max Row Number", "1024");
        EnvironmentUtils.envSetUp();
        session = new Session.Builder().build();
        session.open();
    }

    @AfterEach
    public void release() throws Exception {
        testRunner.shutdown();
        recordReader.disabled();
        session.close();
        EnvironmentUtils.cleanEnv();
        EnvironmentUtils.shutdownDaemon();
    }

    private void setUpStandardTestConfig() throws InitializationException {
        testRunner.addControllerService("reader", recordReader);
        testRunner.enableControllerService(recordReader);
    }

    @Test
    public void testInsertByNativeSchemaWithSingleDevice()
            throws IoTDBConnectionException, StatementExecutionException, InitializationException {
        setUpStandardTestConfig();

        // create schema
        recordReader.addSchemaField("TIME", RecordFieldType.LONG);
        recordReader.addSchemaField("s1", RecordFieldType.INT);
        recordReader.addSchemaField("s2", RecordFieldType.LONG);
        recordReader.addSchemaField("s3", RecordFieldType.FLOAT);
        recordReader.addSchemaField("s4", RecordFieldType.DOUBLE);
        recordReader.addSchemaField("s5", RecordFieldType.BOOLEAN);
        recordReader.addSchemaField("s6", RecordFieldType.STRING);

        // add record
        recordReader.addRecord(1L, 1, 2L, 3.0F, 4.0D, true, "abc");
        recordReader.addRecord(2L, 1, 2L, 3.0F, 4.0D, true, "abc");
        recordReader.addRecord(3L, 1, 2L, 3.0F, 4.0D, true, "abc");
        recordReader.addRecord(4L, 1, 2L, 3.0F, 4.0D, true, "abc");
        recordReader.addRecord(5L, 1, 2L, 3.0F, 4.0D, true, "abc");
        recordReader.addRecord(6L, 1, 2L, 3.0F, 4.0D, true, "abc");
        recordReader.addRecord(7L, 1, 2L, 3.0F, 4.0D, true, "abc");

        // call the PutIoTDBProcessor
        testRunner.setProperty("Time Field", "TIME");
        testRunner.setProperty("Prefix", "root.sg0.d1.");
        testRunner.enqueue("");
        testRunner.run();

        // test whether transferred successfully?
        testRunner.assertAllFlowFilesTransferred(PutIoTDBRecord.REL_SUCCESS, 1);

        // test whether data is correct?
        Object[][] exceptedResult = {
                {1L, 1, 2L, 3.0F, 4.0D, true, "abc"},
                {2L, 1, 2L, 3.0F, 4.0D, true, "abc"},
                {3L, 1, 2L, 3.0F, 4.0D, true, "abc"},
                {4L, 1, 2L, 3.0F, 4.0D, true, "abc"},
                {5L, 1, 2L, 3.0F, 4.0D, true, "abc"},
                {6L, 1, 2L, 3.0F, 4.0D, true, "abc"},
                {7L, 1, 2L, 3.0F, 4.0D, true, "abc"},
        };
        SessionDataSet dataSet =
                session.executeQueryStatement("select s1,s2,s3,s4,s5,s6 from root.sg0.**");
        Object[][] actualResult = new Object[7][7];
        int index = 0;
        while (dataSet.hasNext()) {
            RowRecord record = dataSet.next();
            actualResult[index][0] = record.getTimestamp();
            List<Field> fields = record.getFields();
            actualResult[index][1] = fields.get(0).getIntV();
            actualResult[index][2] = fields.get(1).getLongV();
            actualResult[index][3] = fields.get(2).getFloatV();
            actualResult[index][4] = fields.get(3).getDoubleV();
            actualResult[index][5] = fields.get(4).getBoolV();
            actualResult[index][6] = fields.get(5).getBinaryV().getStringValue();
            index++;
        }
        Assertions.assertTrue(EqualsBuilder.reflectionEquals(exceptedResult,actualResult));
    }

    @Test
    public void testInsertByNativeSchemaWithTimeStamp()
            throws IoTDBConnectionException, StatementExecutionException, InitializationException {
        setUpStandardTestConfig();

        // create schema
        recordReader.addSchemaField("Time", RecordFieldType.TIMESTAMP);
        recordReader.addSchemaField("s1", RecordFieldType.INT);
        recordReader.addSchemaField("s2", RecordFieldType.LONG);
        recordReader.addSchemaField("s3", RecordFieldType.FLOAT);
        recordReader.addSchemaField("s4", RecordFieldType.DOUBLE);
        recordReader.addSchemaField("s5", RecordFieldType.BOOLEAN);
        recordReader.addSchemaField("s6", RecordFieldType.STRING);

        // add record
        recordReader.addRecord(new Timestamp(1L), 1, 2L, 3.0F, 4.0D, true, "abc");
        recordReader.addRecord(new Timestamp(2L), 1, 2L, 3.0F, 4.0D, true, "abc");
        recordReader.addRecord(new Timestamp(3L), 1, 2L, 3.0F, 4.0D, true, "abc");
        recordReader.addRecord(new Timestamp(4L), 1, 2L, 3.0F, 4.0D, true, "abc");
        recordReader.addRecord(new Timestamp(5L), 1, 2L, 3.0F, 4.0D, true, "abc");
        recordReader.addRecord(new Timestamp(6L), 1, 2L, 3.0F, 4.0D, true, "abc");
        recordReader.addRecord(new Timestamp(7L), 1, 2L, 3.0F, 4.0D, true, "abc");

        // call the PutIoTDBProcessor
        testRunner.setProperty("Prefix", "root.sg1.d1.");
        testRunner.enqueue("");
        testRunner.run();

        // test whether transferred successfully?
        testRunner.assertAllFlowFilesTransferred(PutIoTDBRecord.REL_SUCCESS, 1);

        // test whether data is correct?
        Object[][] exceptedResult = {
                {1L, 1, 2L, 3.0F, 4.0D, true, "abc"},
                {2L, 1, 2L, 3.0F, 4.0D, true, "abc"},
                {3L, 1, 2L, 3.0F, 4.0D, true, "abc"},
                {4L, 1, 2L, 3.0F, 4.0D, true, "abc"},
                {5L, 1, 2L, 3.0F, 4.0D, true, "abc"},
                {6L, 1, 2L, 3.0F, 4.0D, true, "abc"},
                {7L, 1, 2L, 3.0F, 4.0D, true, "abc"},
        };
        SessionDataSet dataSet =
                session.executeQueryStatement("select s1,s2,s3,s4,s5,s6 from root.sg1.**");
        Object[][] actualResult = new Object[7][7];
        int index = 0;
        while (dataSet.hasNext()) {
            RowRecord record = dataSet.next();
            actualResult[index][0] = record.getTimestamp();
            List<Field> fields = record.getFields();
            actualResult[index][1] = fields.get(0).getIntV();
            actualResult[index][2] = fields.get(1).getLongV();
            actualResult[index][3] = fields.get(2).getFloatV();
            actualResult[index][4] = fields.get(3).getDoubleV();
            actualResult[index][5] = fields.get(4).getBoolV();
            actualResult[index][6] = fields.get(5).getBinaryV().getStringValue();
            index++;
        }
        Assertions.assertTrue(EqualsBuilder.reflectionEquals(exceptedResult,actualResult));
    }

    @Test
    public void testInsertByNativeSchemaWithNullValue()
            throws InitializationException, IoTDBConnectionException, StatementExecutionException {
        setUpStandardTestConfig();

        // create schema
        recordReader.addSchemaField("Time", RecordFieldType.LONG);
        recordReader.addSchemaField("s1", RecordFieldType.INT);
        recordReader.addSchemaField("s2", RecordFieldType.LONG);

        // add record
        recordReader.addRecord(1L, 1, 2L);
        recordReader.addRecord(2L, 1, 2L);
        recordReader.addRecord(3L, 1, null);

        // call the PutIoTDBProcessor
        testRunner.setProperty("Prefix", "root.sg2.d1.");
        testRunner.enqueue("");
        testRunner.run();

        // test whether transferred successfully?
        testRunner.assertAllFlowFilesTransferred(PutIoTDBRecord.REL_SUCCESS, 1);

        // test whether data is correct?
        Object[][] exceptedResult = {
                {1L, 1, 2L},
                {2L, 1, 2L},
                {3L, 1, null}
        };
        SessionDataSet dataSet = session.executeQueryStatement("select s1,s2 from root.sg2.d1");
        Object[][] actualResult = new Object[3][3];
        int index = 0;
        while (dataSet.hasNext()) {
            RowRecord record = dataSet.next();
            actualResult[index][0] = record.getTimestamp();
            List<Field> fields = record.getFields();
            actualResult[index][1] = fields.get(0).getIntV();
            actualResult[index][2] =
                    fields.get(1).getObjectValue(TSDataType.INT64) != null ? fields.get(1).getLongV() : null;
            index++;
        }
        Assertions.assertTrue(EqualsBuilder.reflectionEquals(exceptedResult,actualResult));
    }

    @Test
    public void testInsertByNativeSchemaWithEmptyValue()
            throws InitializationException, IoTDBConnectionException, StatementExecutionException {
        setUpStandardTestConfig();

        // create schema
        recordReader.addSchemaField("Time", RecordFieldType.LONG);
        recordReader.addSchemaField("s1", RecordFieldType.INT);
        recordReader.addSchemaField("s2", RecordFieldType.LONG);

        testRunner.setProperty("Prefix", "root.sg3.d1.");
        // call the PutIoTDBProcessor
        testRunner.enqueue("");
        testRunner.run();

        // test whether transferred successfully?
        testRunner.assertAllFlowFilesTransferred(PutIoTDBRecord.REL_SUCCESS, 1);
    }

    @Test
    public void testInsertByNativeSchemaWithUnsupportedDataType() throws InitializationException {
        setUpStandardTestConfig();

        // create schema
        recordReader.addSchemaField("Time", RecordFieldType.LONG);
        recordReader.addSchemaField("s1", RecordFieldType.ARRAY);

        recordReader.addRecord(1L, new String[]{"1"});

        testRunner.setProperty("Prefix", "root.sg4.d1.");
        // call the PutIoTDBProcessor
        testRunner.enqueue("");
        testRunner.run();

        // test whether transferred successfully?
        testRunner.assertAllFlowFilesTransferred(PutIoTDBRecord.REL_FAILURE, 1);
    }

    @Test
    public void testInsertByNativeSchemaWithoutTimeField() throws InitializationException {
        setUpStandardTestConfig();

        // create schema
        recordReader.addSchemaField("s1", RecordFieldType.INT);
        recordReader.addSchemaField("s2", RecordFieldType.INT);
        recordReader.addRecord(1, 1);

        testRunner.setProperty("Prefix", "root.sg5.d1.");
        // call the PutIoTDBProcessor
        testRunner.enqueue("");
        testRunner.run();

        // test whether transferred successfully?
        testRunner.assertAllFlowFilesTransferred(PutIoTDBRecord.REL_FAILURE, 1);
    }

    @Test
    public void testInsertByNativeSchemaWithWrongTimeType() throws InitializationException {
        setUpStandardTestConfig();

        // create schema
        recordReader.addSchemaField("Time", RecordFieldType.INT);
        recordReader.addSchemaField("s1", RecordFieldType.INT);

        recordReader.addRecord(1, 1);

        // call the PutIoTDBProcessor
        testRunner.setProperty("Prefix", "root.sg5.d1.");
        testRunner.enqueue("");
        testRunner.run();

        // test whether transferred successfully?
        testRunner.assertAllFlowFilesTransferred(PutIoTDBRecord.REL_FAILURE, 1);
    }

    @Test
    public void testInsertByNativeSchemaNotStartWithRoot() throws InitializationException {
        setUpStandardTestConfig();

        // create schema
        recordReader.addSchemaField("Time", RecordFieldType.LONG);
        recordReader.addSchemaField("s1", RecordFieldType.INT);

        recordReader.addRecord(1L, 1);

        // call the PutIoTDBProcessor
        testRunner.setProperty("Prefix", "sg6.d1.");
        testRunner.enqueue("");
        testRunner.run();

        // test whether transferred successfully?
        testRunner.assertAllFlowFilesTransferred(PutIoTDBRecord.REL_FAILURE, 1);
    }
}
