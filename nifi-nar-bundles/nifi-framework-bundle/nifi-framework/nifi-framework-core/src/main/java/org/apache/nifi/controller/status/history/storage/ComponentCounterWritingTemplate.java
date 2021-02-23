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

import io.questdb.cairo.TableWriter;
import org.apache.commons.math3.util.Pair;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.history.questdb.QuestDbWritingTemplate;

import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

class ComponentCounterWritingTemplate extends QuestDbWritingTemplate<Pair<Date, ProcessorStatus>> {

    public ComponentCounterWritingTemplate() {
        super("componentCounter");
    }

    @Override
    protected void addRows(final TableWriter tableWriter, final Collection<Pair<Date, ProcessorStatus>> entries) {
        for (final Pair<Date, ProcessorStatus> entry : entries) {
            final Map<String, Long> counters = entry.getSecond().getCounters();

            if (counters != null && counters.size() > 0) {
                for (final Map.Entry<String, Long> counter : counters.entrySet()) {
                    final long measuredAt = TimeUnit.MILLISECONDS.toMicros(entry.getFirst().getTime());
                    final TableWriter.Row counterRow = tableWriter.newRow(measuredAt);
                    counterRow.putSym(1,  entry.getSecond().getId());
                    counterRow.putSym(2,  counter.getKey());
                    counterRow.putLong(3,  counter.getValue());
                    counterRow.append();
                }
            }
        }
    }
}
