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
package org.apache.nifi.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.io.SerializedString;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.schema.inference.RecordSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

public class JsonRecordSource implements RecordSource<JsonNode> {
    private static final Logger logger = LoggerFactory.getLogger(JsonRecordSource.class);
    private static final JsonFactory jsonFactory;
    private final JsonParser jsonParser;
    private final String skipToNestedJsonField;

    static {
        jsonFactory = new JsonFactory();
        jsonFactory.setCodec(new ObjectMapper());
    }

    public JsonRecordSource(final InputStream in, final String skipToNestedJsonField) throws IOException {
        jsonParser = jsonFactory.createParser(in);
        this.skipToNestedJsonField = skipToNestedJsonField;
    }

    @Override
    public void init() throws IOException {
        if (skipToNestedJsonField != null) {
            while (!jsonParser.nextFieldName(new SerializedString(skipToNestedJsonField))) {
                // go to nested field if specified
                if (!jsonParser.hasCurrentToken()) {
                    throw new IOException("The defined skipTo json field is not found when inferring json schema.");
                }
            }
            logger.debug("Skipped to specified json field [{}] while inferring json schema.", skipToNestedJsonField);
        }
    }

    @Override
    public JsonNode next() throws IOException {
        JsonToken token = jsonParser.nextToken();
        if (skipToNestedJsonField != null && !jsonParser.isExpectedStartArrayToken() && token != JsonToken.START_OBJECT) {
            logger.debug("Specified json field [{}] to skip to is not found. Schema infer will start from the next nested json object or array.", skipToNestedJsonField);
        }
        while (true) {
            if (token == null) {
                return null;
            }
            if (token == JsonToken.START_OBJECT) {
                return jsonParser.readValueAsTree();
            }
            token = jsonParser.nextToken();
        }
    }
}
