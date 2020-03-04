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

package org.apache.nifi.schema.access;

import java.io.IOException;
import java.io.OutputStream;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;

public class HortonworksAttributeSchemaReferenceWriter implements SchemaAccessWriter {
    private static final Set<SchemaField> requiredSchemaFields = EnumSet.of(SchemaField.SCHEMA_IDENTIFIER, SchemaField.SCHEMA_VERSION);
    static final int LATEST_PROTOCOL_VERSION = 3;
    static final String SCHEMA_BRANCH_ATTRIBUTE = "schema.branch";

    @Override
    public void writeHeader(RecordSchema schema, OutputStream out) throws IOException {
    }

    @Override
    public Map<String, String> getAttributes(final RecordSchema schema) {
        final Map<String, String> attributes = new HashMap<>(4);
        final SchemaIdentifier id = schema.getIdentifier();

        final Long schemaId = id.getIdentifier().getAsLong();
        final Integer schemaVersion = id.getVersion().getAsInt();

        attributes.put(HortonworksAttributeSchemaReferenceStrategy.SCHEMA_ID_ATTRIBUTE, String.valueOf(schemaId));
        attributes.put(HortonworksAttributeSchemaReferenceStrategy.SCHEMA_VERSION_ATTRIBUTE, String.valueOf(schemaVersion));
        attributes.put(HortonworksAttributeSchemaReferenceStrategy.SCHEMA_PROTOCOL_VERSION_ATTRIBUTE, String.valueOf(id.getProtocol()));

        if (id.getBranch().isPresent()) {
            attributes.put(SCHEMA_BRANCH_ATTRIBUTE, id.getBranch().get());
        }

        if (id.getSchemaVersionId().isPresent()) {
            attributes.put(HortonworksAttributeSchemaReferenceStrategy.SCHEMA_VERSION_ID_ATTRIBUTE, String.valueOf(id.getSchemaVersionId().getAsLong()));
        }

        return attributes;
    }

    @Override
    public void validateSchema(final RecordSchema schema) throws SchemaNotFoundException {
        final SchemaIdentifier identifier = schema.getIdentifier();

        if(!identifier.getSchemaVersionId().isPresent()) {
            if (!identifier.getIdentifier().isPresent()) {
                throw new SchemaNotFoundException("Cannot write Encoded Schema Reference because the Schema Identifier is not known");
            }
            if (!identifier.getVersion().isPresent()) {
                throw new SchemaNotFoundException("Cannot write Encoded Schema Reference because the Schema Version is not known");
            }
        }
    }

    @Override
    public Set<SchemaField> getRequiredSchemaFields() {
        return requiredSchemaFields;
    }

}
