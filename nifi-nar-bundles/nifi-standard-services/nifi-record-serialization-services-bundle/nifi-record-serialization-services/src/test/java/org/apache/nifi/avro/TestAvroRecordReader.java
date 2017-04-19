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

package org.apache.nifi.avro;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.Test;

public class TestAvroRecordReader {


    @Test
    public void testLogicalTypes() throws IOException, ParseException, MalformedRecordException {
        final Schema schema = new Schema.Parser().parse(new File("src/test/resources/avro/logical-types.avsc"));

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        final String expectedTime = "2017-04-04 14:20:33.000";
        final DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        df.setTimeZone(TimeZone.getTimeZone("gmt"));
        final long timeLong = df.parse(expectedTime).getTime();

        final long secondsSinceMidnight = 33 + (20 * 60) + (14 * 60 * 60);
        final long millisSinceMidnight = secondsSinceMidnight * 1000L;


        final byte[] serialized;
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
            final DataFileWriter<GenericRecord> writer = dataFileWriter.create(schema, baos)) {

            final GenericRecord record = new GenericData.Record(schema);
            record.put("timeMillis", millisSinceMidnight);
            record.put("timeMicros", millisSinceMidnight * 1000L);
            record.put("timestampMillis", timeLong);
            record.put("timestampMicros", timeLong * 1000L);
            record.put("date", 17260);

            writer.append(record);
            writer.flush();

            serialized = baos.toByteArray();
        }

        try (final InputStream in = new ByteArrayInputStream(serialized)) {
            final AvroRecordReader reader = new AvroRecordReader(in);
            final RecordSchema recordSchema = reader.getSchema();

            assertEquals(RecordFieldType.TIME, recordSchema.getDataType("timeMillis").get().getFieldType());
            assertEquals(RecordFieldType.TIME, recordSchema.getDataType("timeMicros").get().getFieldType());
            assertEquals(RecordFieldType.TIMESTAMP, recordSchema.getDataType("timestampMillis").get().getFieldType());
            assertEquals(RecordFieldType.TIMESTAMP, recordSchema.getDataType("timestampMicros").get().getFieldType());
            assertEquals(RecordFieldType.DATE, recordSchema.getDataType("date").get().getFieldType());

            final Record record = reader.nextRecord();
            assertEquals(new java.sql.Time(millisSinceMidnight), record.getValue("timeMillis"));
            assertEquals(new java.sql.Time(millisSinceMidnight), record.getValue("timeMicros"));
            assertEquals(new java.sql.Timestamp(timeLong), record.getValue("timestampMillis"));
            assertEquals(new java.sql.Timestamp(timeLong), record.getValue("timestampMicros"));
            final DateFormat noTimeOfDayDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            noTimeOfDayDateFormat.setTimeZone(TimeZone.getTimeZone("gmt"));
            assertEquals(new java.sql.Date(timeLong).toString(), noTimeOfDayDateFormat.format(record.getValue("date")));
        }
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testDataTypes() throws IOException, MalformedRecordException {
        final List<Field> accountFields = new ArrayList<>();
        accountFields.add(new Field("accountId", Schema.create(Type.LONG), null, (Object) null));
        accountFields.add(new Field("accountName", Schema.create(Type.STRING), null, (Object) null));
        final Schema accountSchema = Schema.createRecord("account", null, null, false);
        accountSchema.setFields(accountFields);

        final List<Field> catFields = new ArrayList<>();
        catFields.add(new Field("catTailLength", Schema.create(Type.INT), null, (Object) null));
        catFields.add(new Field("catName", Schema.create(Type.STRING), null, (Object) null));
        final Schema catSchema = Schema.createRecord("cat", null, null, false);
        catSchema.setFields(catFields);

        final List<Field> dogFields = new ArrayList<>();
        dogFields.add(new Field("dogTailLength", Schema.create(Type.INT), null, (Object) null));
        dogFields.add(new Field("dogName", Schema.create(Type.STRING), null, (Object) null));
        final Schema dogSchema = Schema.createRecord("dog", null, null, false);
        dogSchema.setFields(dogFields);

        final List<Field> fields = new ArrayList<>();
        fields.add(new Field("name", Schema.create(Type.STRING), null, (Object) null));
        fields.add(new Field("age", Schema.create(Type.INT), null, (Object) null));
        fields.add(new Field("balance", Schema.create(Type.DOUBLE), null, (Object) null));
        fields.add(new Field("rate", Schema.create(Type.FLOAT), null, (Object) null));
        fields.add(new Field("debt", Schema.create(Type.BOOLEAN), null, (Object) null));
        fields.add(new Field("nickname", Schema.create(Type.NULL), null, (Object) null));
        fields.add(new Field("binary", Schema.create(Type.BYTES), null, (Object) null));
        fields.add(new Field("fixed", Schema.createFixed("fixed", null, null, 5), null, (Object) null));
        fields.add(new Field("map", Schema.createMap(Schema.create(Type.STRING)), null, (Object) null));
        fields.add(new Field("array", Schema.createArray(Schema.create(Type.LONG)), null, (Object) null));
        fields.add(new Field("account", accountSchema, null, (Object) null));
        fields.add(new Field("desiredbalance", Schema.createUnion( // test union of NULL and other type with no value
            Arrays.asList(Schema.create(Type.NULL), Schema.create(Type.DOUBLE))),
            null, (Object) null));
        fields.add(new Field("dreambalance", Schema.createUnion( // test union of NULL and other type with a value
            Arrays.asList(Schema.create(Type.NULL), Schema.create(Type.DOUBLE))),
            null, (Object) null));
        fields.add(new Field("favAnimal", Schema.createUnion(Arrays.asList(catSchema, dogSchema)), null, (Object) null));
        fields.add(new Field("otherFavAnimal", Schema.createUnion(Arrays.asList(catSchema, dogSchema)), null, (Object) null));

        final Schema schema = Schema.createRecord("record", null, null, false);
        schema.setFields(fields);

        final byte[] source;
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        final Map<String, String> map = new HashMap<>();
        map.put("greeting", "hello");
        map.put("salutation", "good-bye");

        final List<RecordField> mapFields = new ArrayList<>();
        mapFields.add(new RecordField("greeting", RecordFieldType.STRING.getDataType()));
        mapFields.add(new RecordField("salutation", RecordFieldType.STRING.getDataType()));
        final RecordSchema mapSchema = new SimpleRecordSchema(mapFields);
        final Record expectedRecord = new MapRecord(mapSchema, (Map) map);

        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
            final DataFileWriter<GenericRecord> writer = dataFileWriter.create(schema, baos)) {

            final GenericRecord record = new GenericData.Record(schema);
            record.put("name", "John");
            record.put("age", 33);
            record.put("balance", 1234.56D);
            record.put("rate", 0.045F);
            record.put("debt", false);
            record.put("binary", ByteBuffer.wrap("binary".getBytes(StandardCharsets.UTF_8)));
            record.put("fixed", new GenericData.Fixed(Schema.create(Type.BYTES), "fixed".getBytes(StandardCharsets.UTF_8)));
            record.put("map", map);
            record.put("array", Arrays.asList(1L, 2L));
            record.put("dreambalance", 10_000_000.00D);

            final GenericRecord accountRecord = new GenericData.Record(accountSchema);
            accountRecord.put("accountId", 83L);
            accountRecord.put("accountName", "Checking");
            record.put("account", accountRecord);

            final GenericRecord catRecord = new GenericData.Record(catSchema);
            catRecord.put("catTailLength", 1);
            catRecord.put("catName", "Meow");
            record.put("otherFavAnimal", catRecord);

            final GenericRecord dogRecord = new GenericData.Record(dogSchema);
            dogRecord.put("dogTailLength", 14);
            dogRecord.put("dogName", "Fido");
            record.put("favAnimal", dogRecord);

            writer.append(record);
        }

        source = baos.toByteArray();

        try (final InputStream in = new ByteArrayInputStream(source)) {
            final AvroRecordReader reader = new AvroRecordReader(in);
            final RecordSchema recordSchema = reader.getSchema();
            assertEquals(15, recordSchema.getFieldCount());

            assertEquals(RecordFieldType.STRING, recordSchema.getDataType("name").get().getFieldType());
            assertEquals(RecordFieldType.INT, recordSchema.getDataType("age").get().getFieldType());
            assertEquals(RecordFieldType.DOUBLE, recordSchema.getDataType("balance").get().getFieldType());
            assertEquals(RecordFieldType.FLOAT, recordSchema.getDataType("rate").get().getFieldType());
            assertEquals(RecordFieldType.BOOLEAN, recordSchema.getDataType("debt").get().getFieldType());
            assertEquals(RecordFieldType.RECORD, recordSchema.getDataType("nickname").get().getFieldType());
            assertEquals(RecordFieldType.ARRAY, recordSchema.getDataType("binary").get().getFieldType());
            assertEquals(RecordFieldType.ARRAY, recordSchema.getDataType("fixed").get().getFieldType());
            assertEquals(RecordFieldType.RECORD, recordSchema.getDataType("map").get().getFieldType());
            assertEquals(RecordFieldType.ARRAY, recordSchema.getDataType("array").get().getFieldType());
            assertEquals(RecordFieldType.RECORD, recordSchema.getDataType("account").get().getFieldType());
            assertEquals(RecordFieldType.DOUBLE, recordSchema.getDataType("desiredbalance").get().getFieldType());
            assertEquals(RecordFieldType.DOUBLE, recordSchema.getDataType("dreambalance").get().getFieldType());
            assertEquals(RecordFieldType.CHOICE, recordSchema.getDataType("favAnimal").get().getFieldType());
            assertEquals(RecordFieldType.CHOICE, recordSchema.getDataType("otherFavAnimal").get().getFieldType());

            final Object[] values = reader.nextRecord().getValues();
            assertEquals(15, values.length);
            assertEquals("John", values[0]);
            assertEquals(33, values[1]);
            assertEquals(1234.56D, values[2]);
            assertEquals(0.045F, values[3]);
            assertEquals(false, values[4]);
            assertEquals(null, values[5]);
            assertArrayEquals(toObjectArray("binary".getBytes(StandardCharsets.UTF_8)), (Object[]) values[6]);
            assertArrayEquals(toObjectArray("fixed".getBytes(StandardCharsets.UTF_8)), (Object[]) values[7]);
            assertEquals(expectedRecord, values[8]);
            assertArrayEquals(new Object[] {1L, 2L}, (Object[]) values[9]);

            final Map<String, Object> accountValues = new HashMap<>();
            accountValues.put("accountName", "Checking");
            accountValues.put("accountId", 83L);

            final List<RecordField> accountRecordFields = new ArrayList<>();
            accountRecordFields.add(new RecordField("accountId", RecordFieldType.LONG.getDataType()));
            accountRecordFields.add(new RecordField("accountName", RecordFieldType.STRING.getDataType()));

            final RecordSchema accountRecordSchema = new SimpleRecordSchema(accountRecordFields);
            final Record mapRecord = new MapRecord(accountRecordSchema, accountValues);

            assertEquals(mapRecord, values[10]);

            assertNull(values[11]);
            assertEquals(10_000_000.0D, values[12]);

            final Map<String, Object> dogMap = new HashMap<>();
            dogMap.put("dogName", "Fido");
            dogMap.put("dogTailLength", 14);

            final List<RecordField> dogRecordFields = new ArrayList<>();
            dogRecordFields.add(new RecordField("dogTailLength", RecordFieldType.INT.getDataType()));
            dogRecordFields.add(new RecordField("dogName", RecordFieldType.STRING.getDataType()));
            final RecordSchema dogRecordSchema = new SimpleRecordSchema(dogRecordFields);
            final Record dogRecord = new MapRecord(dogRecordSchema, dogMap);

            assertEquals(dogRecord, values[13]);

            final Map<String, Object> catMap = new HashMap<>();
            catMap.put("catName", "Meow");
            catMap.put("catTailLength", 1);

            final List<RecordField> catRecordFields = new ArrayList<>();
            catRecordFields.add(new RecordField("catTailLength", RecordFieldType.INT.getDataType()));
            catRecordFields.add(new RecordField("catName", RecordFieldType.STRING.getDataType()));
            final RecordSchema catRecordSchema = new SimpleRecordSchema(catRecordFields);
            final Record catRecord = new MapRecord(catRecordSchema, catMap);

            assertEquals(catRecord, values[14]);
        }
    }

    private Object[] toObjectArray(final byte[] bytes) {
        final Object[] array = new Object[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            array[i] = Byte.valueOf(bytes[i]);
        }
        return array;
    }

    public static enum Status {
        GOOD, BAD;
    }
}
