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
package org.apache.nifi.controller.status.history.storage;

import io.questdb.cairo.sql.Record;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.commons.math3.util.Pair;
import org.apache.nifi.controller.status.history.ComponentDetailsStorage;
import org.apache.nifi.controller.status.history.MetricDescriptor;
import org.apache.nifi.controller.status.history.StandardStatusHistory;
import org.apache.nifi.controller.status.history.StatusHistory;
import org.apache.nifi.controller.status.history.StatusSnapshot;
import org.apache.nifi.controller.status.history.questdb.QuestDbContext;
import org.apache.nifi.controller.status.history.questdb.QuestDbEntityReadingTemplate;
import org.apache.nifi.controller.status.history.questdb.QuestDbEntityWritingTemplate;
import org.apache.nifi.controller.status.history.questdb.QuestDbStatusSnapshotMapper;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Component specific implementation of the {@link ComponentStatusStorage}.
 *
 * @param <T> Component status entry type.
 */
abstract class QuestDbComponentStatusStorage<T> implements ComponentStatusStorage<T> {
    private static final FastDateFormat DATE_FORMAT = FastDateFormat.getInstance(StatusStorage.CAPTURE_DATE_FORMAT);

    /**
     * In case of component status entries the first two columns are fixed (measurement time and component id) and all
     * the following fields are metric values using long format. The list of these are specified by the implementation class.
     */
    private final QuestDbEntityWritingTemplate<T> writingTemplate =  new QuestDbEntityWritingTemplate<>(
        getTableName(),
        (statusEntry, row) -> {
            row.putSym(1, extractId(statusEntry));
            getMetrics().keySet().forEach(ordinal -> row.putLong(ordinal, getMetrics().get(ordinal).getValueFunction().getValue(statusEntry)));
    });

    private final Function<Record, StatusSnapshot> statusSnapshotMapper = new QuestDbStatusSnapshotMapper(getMetrics());

    private final QuestDbEntityReadingTemplate<StatusSnapshot, List<StatusSnapshot>> readingTemplate
            = new QuestDbEntityReadingTemplate<>(QUERY_TEMPLATE, statusSnapshotMapper, e -> e, e -> Collections.emptyList());

    private final QuestDbContext dbContext;
    private final ComponentDetailsStorage componentDetailsStorage;

    protected QuestDbComponentStatusStorage(final QuestDbContext dbContext, final ComponentDetailsStorage componentDetailsStorage) {
        this.dbContext = dbContext;
        this.componentDetailsStorage = componentDetailsStorage;
    }

    /**
     * Extracts unique identifier from the status entry.
     *
     * @param statusEntry The status entry.
     *
     * @return The identifier.
     */
    abstract protected String extractId(final T statusEntry);

    /**
     * Specifies the metrics being stored for the given kind of component.
     * .
     * @return A map of {@link MetricDescriptor} instances defines the metrics to store. The keys in the map server as column index.
     */
    abstract protected Map<Integer, MetricDescriptor<T>> getMetrics();

    /**
     * Returns the database table which is used to store the data.
     *
     * @return Database table name.
     */
    abstract protected String getTableName();

    @Override
    public StatusHistory read(final String componentId, final Date start, final Date end, final int preferredDataPoints) {
        final String formattedStart = DATE_FORMAT.format(Optional.ofNullable(start).orElse(DateUtils.addDays(new Date(), -1)));
        final String formattedEnd = DATE_FORMAT.format(Optional.ofNullable(end).orElse(new Date()));
        final List<StatusSnapshot> snapshots = readingTemplate.read(
                dbContext.getEngine(),
                dbContext.getSqlExecutionContext(),
                Arrays.asList(getTableName(), componentId, formattedStart, formattedEnd));
        return new StandardStatusHistory(
                snapshots.subList(Math.max(snapshots.size() - preferredDataPoints, 0), snapshots.size()),
                componentDetailsStorage.getDetails(componentId),
                new Date()
        );
    }

    @Override
    public void store(final List<Pair<Date, T>> statusEntries) {
        writingTemplate.insert(dbContext.getEngine(), dbContext.getSqlExecutionContext(), statusEntries);
    }
}
