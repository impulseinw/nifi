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
package org.apache.nifi.processors.deltalake.storage;

import io.delta.standalone.DeltaLog;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface StorageAdapter {
    DeltaLog getDeltaLog();
    String getDataPath();
    FileSystem getFileSystem();
    String getEngineInfo();
    Configuration getConfiguration();

    default void copyFile(InputStream inputStream, Path outputPath) {
        try {
            OutputStream outputStream = getFileSystem().create(outputPath);
            IOUtils.copyBytes(inputStream, outputStream, getConfiguration());
        } catch (IOException e) {
            throw new RuntimeException("Error moving file to data storage", e);
        }
    }


}
