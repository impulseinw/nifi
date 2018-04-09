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

package org.apache.nifi.mongodb;

import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bson.Document;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MongoDBLookupServiceIT {
    private static final String DB_NAME = String.format("nifi_test-%d", Calendar.getInstance().getTimeInMillis());
    private static final String COL_NAME = String.format("nifi_test-%d", Calendar.getInstance().getTimeInMillis());

    private TestRunner runner;
    private MongoDBLookupService service;

    @Before
    public void before() throws Exception {
        runner = TestRunners.newTestRunner(TestLookupServiceProcessor.class);
        service = new MongoDBLookupService();
        runner.addControllerService("Client Service", service);
        runner.setProperty(service, MongoDBLookupService.DATABASE_NAME, DB_NAME);
        runner.setProperty(service, MongoDBLookupService.COLLECTION_NAME, COL_NAME);
        runner.setProperty(service, MongoDBLookupService.URI, "mongodb://localhost:27017");
        runner.setProperty(service, MongoDBLookupService.LOOKUP_VALUE_FIELD, "message");
    }

    @After
    public void after() {
        service.dropDatabase();
        service.onDisable();
    }

    @Test
    public void testInit() {
        runner.enableControllerService(service);
        runner.assertValid(service);
    }

    @Test
    public void testLookupSingle() throws Exception {
        runner.setProperty(service, MongoDBLookupService.LOOKUP_VALUE_FIELD, "message");
        runner.enableControllerService(service);
        Document document = service.convertJson("{ \"uuid\": \"x-y-z\", \"message\": \"Hello, world\" }");
        service.insert(document);

        Map<String, Object> criteria = new HashMap<>();
        criteria.put("uuid", "x-y-z");
        Optional result = service.lookup(criteria);

        Assert.assertNotNull("The value was null.", result.get());
        Assert.assertEquals("The value was wrong.", "Hello, world", result.get());

        Map<String, Object> clean = new HashMap<>();
        clean.putAll(criteria);
        service.delete(new Document(clean));

        try {
            result = service.lookup(criteria);
        } catch (LookupFailureException ex) {
            Assert.fail();
        }

        Assert.assertTrue(!result.isPresent());
    }

    @Test
    public void testLookupRecord() throws Exception {
        runner.setProperty(service, MongoDBLookupService.LOOKUP_VALUE_FIELD, "");
        runner.setProperty(service, MongoDBLookupService.PROJECTION, "{ \"_id\": 0 }");
        runner.enableControllerService(service);

        Date d = new Date();
        Timestamp ts = new Timestamp(new Date().getTime());
        List list = Arrays.asList("a", "b", "c", "d", "e");

        service.insert(new Document()
            .append("uuid", "x-y-z")
            .append("dateField", d)
            .append("longField", 10000L)
            .append("stringField", "Hello, world")
            .append("timestampField", ts)
            .append("decimalField", Double.MAX_VALUE / 2.0)
            .append("subrecordField", new Document()
                .append("nestedString", "test")
                .append("nestedLong", new Long(1000)))
            .append("arrayField", list)
        );

        Map<String, Object> criteria = new HashMap<>();
        criteria.put("uuid", "x-y-z");
        Optional result = service.lookup(criteria);

        Assert.assertNotNull("The value was null.", result.get());
        Assert.assertTrue("The value was wrong.", result.get() instanceof MapRecord);
        MapRecord record = (MapRecord)result.get();
        RecordSchema subSchema = ((RecordDataType)record.getSchema().getField("subrecordField").get().getDataType()).getChildSchema();

        Assert.assertEquals("The value was wrong.", "Hello, world", record.getValue("stringField"));
        Assert.assertEquals("The value was wrong.", "x-y-z", record.getValue("uuid"));
        Assert.assertEquals(new Long(10000), record.getValue("longField"));
        Assert.assertEquals((Double.MAX_VALUE / 2.0), record.getValue("decimalField"));
        Assert.assertEquals(d, record.getValue("dateField"));
        Assert.assertEquals(ts.getTime(), ((Date)record.getValue("timestampField")).getTime());

        Record subRecord = record.getAsRecord("subrecordField", subSchema);
        Assert.assertNotNull(subRecord);
        Assert.assertEquals("test", subRecord.getValue("nestedString"));
        Assert.assertEquals(new Long(1000), subRecord.getValue("nestedLong"));
        Assert.assertEquals(list, record.getValue("arrayField"));

        Map<String, Object> clean = new HashMap<>();
        clean.putAll(criteria);
        service.delete(new Document(clean));

        try {
            result = service.lookup(criteria);
        } catch (LookupFailureException ex) {
            Assert.fail();
        }

        Assert.assertTrue(!result.isPresent());
    }

    @Test
    public void testServiceParameters() {
        runner.enableControllerService(service);
        Document document = service.convertJson("{ \"uuid\": \"x-y-z\", \"message\": \"Hello, world\" }");
        service.insert(document);

        Map<String, Object> criteria = new HashMap<>();
        criteria.put("uuid", "x-y-z");

        boolean error = false;
        try {
            service.lookup(criteria);
        } catch(Exception ex) {
            error = true;
        }

        Assert.assertFalse("An error was thrown when no error should have been thrown.", error);
        error = false;

        try {
            service.lookup(new HashMap());
        } catch (Exception ex) {
            error = true;
            Assert.assertTrue("The exception was the wrong type", ex instanceof LookupFailureException);
        }

        Assert.assertTrue("An error was not thrown when the input was empty", error);
    }
}
