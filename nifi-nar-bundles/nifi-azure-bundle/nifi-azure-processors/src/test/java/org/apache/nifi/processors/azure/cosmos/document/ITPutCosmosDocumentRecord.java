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
package org.apache.nifi.processors.azure.cosmos.document;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import com.azure.cosmos.CosmosItemProperties;
import com.azure.cosmos.FeedOptions;
import com.azure.cosmos.FeedResponse;

import org.apache.nifi.processor.Processor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ITPutCosmosDocumentRecord extends ITAbstractCosmosDocument {
    static Logger logger = Logger.getLogger(ITPutCosmosDocumentRecord.class.getName());

    @Override
    protected Class<? extends Processor> getProcessorClass() {
        return PutCosmosDocumentRecord.class;
    }

    @Before
    public void setUp() throws Exception {
        resetTestCosmosConnection();
    }

    @After
    public void cleanupTestCase() {
        try{
            clearTestData();
            closeClient();
        }catch(Exception e) {

        }
    }
    private List<CosmosItemProperties> getDataFromTestDB() {
        logger.info("getDataFromTestDB for test result validation");
        FeedOptions queryOptions = new FeedOptions();
        queryOptions.setEnableCrossPartitionQuery(true);
        List<CosmosItemProperties> results = new ArrayList<>();

        Iterator<FeedResponse<CosmosItemProperties>> response = container.queryItems(
            "select * from c", queryOptions );
        while(response.hasNext()) {

            FeedResponse<CosmosItemProperties> page = response.next();
            for(CosmosItemProperties doc: page.getResults()){
                results.add(doc);
            }
        }
        return results;
    }

    private MockRecordParser recordReader;

    private void setupRecordReader() throws InitializationException {
        recordReader = new MockRecordParser();
        runner.addControllerService("reader", recordReader);
        runner.enableControllerService(recordReader);
        runner.setProperty(PutCosmosDocumentRecord.RECORD_READER_FACTORY, "reader");
    }

    @Test
    public void testOnTriggerWithNestedRecords() throws InitializationException {
        setupRecordReader();
        recordReader.addSchemaField("id", RecordFieldType.STRING);

        final List<RecordField> personFields = new ArrayList<>();
        final RecordField nameField = new RecordField("name", RecordFieldType.STRING.getDataType());
        final RecordField ageField = new RecordField("age", RecordFieldType.INT.getDataType());
        final RecordField sportField = new RecordField("sport", RecordFieldType.STRING.getDataType());
        final RecordField categoryField = new RecordField(TEST_COSMOS_PARTITION_KEY_FIELD_NAME, RecordFieldType.STRING.getDataType());
        personFields.add(nameField);
        personFields.add(ageField);
        personFields.add(sportField);
        personFields.add(categoryField);
        final RecordSchema personSchema = new SimpleRecordSchema(personFields);
        recordReader.addSchemaField("person", RecordFieldType.RECORD);

        recordReader.addRecord("1", new MapRecord(personSchema, new HashMap<String,Object>() {
            private static final long serialVersionUID = -3185956498135742190L;

            {
            put("name", "John Doe");
            put("age", 48);
            put("sport", "Soccer");
            put(TEST_COSMOS_PARTITION_KEY_FIELD_NAME, "A");
        }}));
        recordReader.addRecord("2", new MapRecord(personSchema, new HashMap<String,Object>() {
            private static final long serialVersionUID = 1L;

            {
            put("name", "Jane Doe");
            put("age", 47);
            put("sport", "Tennis");
            put(TEST_COSMOS_PARTITION_KEY_FIELD_NAME, "B");
        }}));
        recordReader.addRecord("3", new MapRecord(personSchema, new HashMap<String,Object>() {

            private static final long serialVersionUID = -1329194249439570573L;

            {
            put("name", "Sally Doe");
            put("age", 47);
            put("sport", "Curling");
            put(TEST_COSMOS_PARTITION_KEY_FIELD_NAME, "A");
        }}));
        recordReader.addRecord("4", new MapRecord(personSchema, new HashMap<String,Object>() {
            private static final long serialVersionUID = -1329194249439570574L;

            {
            put("name", "Jimmy Doe");
            put("age", 14);
            put("sport", null);
            put(TEST_COSMOS_PARTITION_KEY_FIELD_NAME, "C");
        }}));

        runner.enqueue("");
        runner.run();
        runner.assertAllFlowFilesTransferred(PutCosmosDocumentRecord.REL_SUCCESS, 1);
        assertEquals(4, getDataFromTestDB().size());
    }

    @Test
    public void testOnTriggerWithFlatRecords() throws InitializationException {
        setupRecordReader();
        recordReader.addSchemaField("id", RecordFieldType.STRING);
        recordReader.addSchemaField(TEST_COSMOS_PARTITION_KEY_FIELD_NAME, RecordFieldType.STRING);
        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("age", RecordFieldType.INT);
        recordReader.addSchemaField("sport", RecordFieldType.STRING);

        recordReader.addRecord("1", "A", "John Doe", 48, "Soccer");
        recordReader.addRecord("2", "B","Jane Doe", 47, "Tennis");
        recordReader.addRecord("3", "B", "Sally Doe", 47, "Curling");
        recordReader.addRecord("4", "A", "Jimmy Doe", 14, null);
        recordReader.addRecord("5", "C","Pizza Doe", 14, null);

        runner.enqueue("");
        runner.run();
        runner.assertAllFlowFilesTransferred(PutCosmosDocumentRecord.REL_SUCCESS, 1);
        assertEquals(5, getDataFromTestDB().size());
    }



}