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
package org.apache.nifi.processors.iceberg;

import com.google.common.collect.ImmutableList;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.WriteResult;

import java.util.Arrays;

/**
 * This class is responsible for appending task writers result to Iceberg tables.
 */
public class IcebergFileCommitter {

    private Table table;

    IcebergFileCommitter(Table table) {
        this.table = table;
    }

    public void commit(WriteResult result) {
        RowDelta rowDelta = table.newRowDelta().validateDataFilesExist(ImmutableList.copyOf(result.referencedDataFiles()));

        Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);

        rowDelta.commit();
    }
}
