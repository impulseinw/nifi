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
package org.apache.nifi.bootstrap.process;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public abstract class AbstractFileBasedRuntimeValidator implements RuntimeValidator {
    protected final File configurationFile;

    AbstractFileBasedRuntimeValidator(final File configurationFile) {
        this.configurationFile = configurationFile;
    }

    @Override
    public List<RuntimeValidatorResult> validate() {
        final List<RuntimeValidatorResult> results = new ArrayList<>();
        if (!canReadConfigurationFile(results)) {
            return results;
        }

        performChecks(results);

        processResults(results);
        return results;
    }
    
    protected File getConfigurationFile() {
        return configurationFile;
    }

    protected String getContents() throws IOException {
        try (Scanner scanner = new Scanner(configurationFile)) {
            final StringBuilder builder = new StringBuilder();
            while (scanner.hasNextLine()) {
                builder.append(scanner.nextLine());
                builder.append("\n");
            }
            return builder.toString();
        }
    }

    protected boolean canReadConfigurationFile(final List<RuntimeValidatorResult> results) {
        final File configurationFile = getConfigurationFile();
        if (configurationFile == null) {
            final RuntimeValidatorResult result = new RuntimeValidatorResult.Builder()
                    .subject(this.getClass().getName())
                    .outcome(RuntimeValidatorResult.Outcome.SKIPPED)
                    .explanation("Configuration file is null")
                    .build();
            results.add(result);
            return false;
        }
        if (!configurationFile.canRead()) {
            final RuntimeValidatorResult result = new RuntimeValidatorResult.Builder()
                    .subject(this.getClass().getName())
                    .outcome(RuntimeValidatorResult.Outcome.SKIPPED)
                    .explanation(String.format("Configuration file [%s] cannot be read", configurationFile.getAbsolutePath()))
                    .build();
            results.add(result);
            return false;
        }
        return true;
    }

    protected abstract void performChecks(final List<RuntimeValidatorResult> results);

    protected void processResults(final List<RuntimeValidatorResult> results) {
        if (results.isEmpty()) {
            final RuntimeValidatorResult result = new RuntimeValidatorResult.Builder()
                    .subject(this.getClass().getName())
                    .outcome(RuntimeValidatorResult.Outcome.SUCCESSFUL)
                    .build();
            results.add(result);
        }
    }
}
