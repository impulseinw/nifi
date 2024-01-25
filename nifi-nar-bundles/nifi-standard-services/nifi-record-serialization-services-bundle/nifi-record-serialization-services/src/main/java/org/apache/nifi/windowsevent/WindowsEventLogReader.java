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
package org.apache.nifi.windowsevent;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.RecordFieldType;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;


@Tags({"xml", "windows", "event", "log", "record", "reader", "parser"})
@CapabilityDescription("Reads Windows Event Log data as XML content having been generated by ConsumeWindowsEventLog, ParseEvtx, etc. (see Additional Details) and creates Record object(s). If the "
        + "root tag of the input XML is 'Events', the child content is expected to be a series of 'Event' tags, each of which will constitute a single record. If the root tag is 'Event', the "
        + "content is expected to be a single 'Event' and thus a single record. No other root tags are valid. Only events of type 'System' are currently supported.")
public class WindowsEventLogReader extends AbstractControllerService implements RecordReaderFactory {

    private static final String DATE_FORMAT = RecordFieldType.DATE.getDefaultFormat();
    private static final String TIME_FORMAT = RecordFieldType.TIME.getDefaultFormat();
    private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS";

    @Override
    public RecordReader createRecordReader(Map<String, String> variables, InputStream in, long inputLength, ComponentLog logger)
            throws MalformedRecordException, IOException, SchemaNotFoundException {
        return new WindowsEventLogRecordReader(in, DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT, logger);
    }
}
