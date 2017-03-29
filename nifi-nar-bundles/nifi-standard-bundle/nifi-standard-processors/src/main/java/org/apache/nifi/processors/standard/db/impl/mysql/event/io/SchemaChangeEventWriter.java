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
package org.apache.nifi.processors.standard.db.impl.mysql.event.io;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processors.standard.CaptureChangeMySQL;
import org.apache.nifi.processors.standard.db.impl.mysql.event.SchemaChangeEventInfo;

/**
 * A writer class to output MySQL binlog "schema change" (ALTER TABLE, e.g.) events to flow file(s).
 */
public class SchemaChangeEventWriter extends AbstractBinlogTableEventWriter<SchemaChangeEventInfo> {

    @Override
    public long writeEvent(ProcessSession session, SchemaChangeEventInfo eventInfo, long currentSequenceId) {
        FlowFile flowFile = session.create();
        flowFile = session.write(flowFile, (outputStream) -> {
            super.startJson(outputStream, eventInfo);
            super.writeJson(eventInfo);
            jsonGenerator.writeStringField("query", eventInfo.getQuery());
            super.endJson();
        });
        flowFile = session.putAllAttributes(flowFile, getCommonAttributes(currentSequenceId, eventInfo));
        session.transfer(flowFile, CaptureChangeMySQL.REL_SUCCESS);
        return currentSequenceId + 1;
    }
}
