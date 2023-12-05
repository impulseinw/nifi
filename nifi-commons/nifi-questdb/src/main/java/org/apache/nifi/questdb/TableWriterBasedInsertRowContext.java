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
package org.apache.nifi.questdb;

import io.questdb.cairo.TableWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

final class TableWriterBasedInsertRowContext implements InsertRowContext {
    public static final Logger LOGGER = LoggerFactory.getLogger(TableWriterBasedInsertRowContext.class);

    private final TableWriter tableWriter;
    private TableWriter.Row actualRow;

    TableWriterBasedInsertRowContext(final TableWriter tableWriter) {
        this.tableWriter = tableWriter;
    }

    void addRow(final InsertRowDataSource rowDataSource) throws DatabaseException {
        rowDataSource.fillRowData(this);
        actualRow.append();
        actualRow = null;

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Appending new row to the table writer: {}", actualRow.toString());
        }
    }

    @Override
    public void initializeRow(final Instant capturedAt) {
        actualRow = tableWriter.newRow(instantToMicros(capturedAt));
    }

    @Override
    public void addLong(final int position, final long value) {
        actualRow.putLong(position, value);
    }

    @Override
    public void addInt(final int position, final int value) {
        actualRow.putInt(position, value);
    }

    @Override
    public void addShort(final int position, final short value) {
        actualRow.putShort(position, value);
    }

    @Override
    public void addString(final int position, final String value) {
        actualRow.putSym(position, value);
    }

    @Override
    public void addInstant(final int position, final Instant value) {
        actualRow.putTimestamp(position, instantToMicros(value));
    }

    @Override
    public void addTimestamp(final int position, final long value) {
        actualRow.putTimestamp(position, value);
    }

    private static long instantToMicros(final Instant instant) {
        return ChronoUnit.MICROS.between(Instant.EPOCH, instant);
    }
}
