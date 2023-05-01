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
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AvailablePorts extends AbstractFileBasedRuntimeValidator {
    private static final String FILE_PATH = "/proc/sys/net/ipv4/ip_local_port_range";
    private static final Pattern PATTERN = Pattern.compile("(\\d+)\\s+(\\d+)");
    private static final int RECOMMENDED_AVAILABLE_PORTS = 55000;

    public AvailablePorts() {
        super(new File(FILE_PATH));
    }

    AvailablePorts(final File configurationFile) {
        super(configurationFile);
    }

    @Override
    protected Pattern getPattern() {
        return PATTERN;
    }

    @Override
    protected void performChecks(final Matcher matcher, final List<RuntimeValidatorResult> results) {
        if (matcher.find()) {
            final int lowerPort = Integer.valueOf(matcher.group(1));
            final int higherPort = Integer.valueOf(matcher.group(2));
            final int availablePorts = higherPort - lowerPort;
            if (availablePorts < RECOMMENDED_AVAILABLE_PORTS) {
                final RuntimeValidatorResult result = new RuntimeValidatorResult.Builder()
                        .subject(this.getClass().getName())
                        .outcome(RuntimeValidatorResult.Outcome.FAILED)
                        .explanation(String.format("Number of available ports [%d] is less than the recommended number of available ports [%d]", availablePorts, RECOMMENDED_AVAILABLE_PORTS))
                        .build();
                results.add(result);
            }
        } else {
            final RuntimeValidatorResult result = new RuntimeValidatorResult.Builder()
                    .subject(this.getClass().getName())
                    .outcome(RuntimeValidatorResult.Outcome.FAILED)
                    .explanation(String.format("Configuration file [%s] cannot be parsed", getConfigurationFile().getAbsolutePath()))
                    .build();
            results.add(result);
        }
    }
}
