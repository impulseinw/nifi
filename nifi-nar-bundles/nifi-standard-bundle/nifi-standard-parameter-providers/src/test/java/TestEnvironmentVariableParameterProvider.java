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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.parameter.AbstractEnvironmentVariableParameterProvider;
import org.apache.nifi.parameter.EnvironmentVariableNonSensitiveParameterProvider;
import org.apache.nifi.parameter.EnvironmentVariableSensitiveParameterProvider;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.parameter.ParameterProvider;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockParameterProviderInitializationContext;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestEnvironmentVariableParameterProvider {
    private ParameterProvider getParameterProvider(final boolean sensitive) {
        return sensitive ? new EnvironmentVariableSensitiveParameterProvider() : new EnvironmentVariableNonSensitiveParameterProvider();
    }

    private void runProviderTest(final boolean sensitive, final String includePattern, final String excludePattern) throws InitializationException {
        final Map<String, String> env = System.getenv();
        final Map<String, String> filteredVariables = env.entrySet().stream()
                .filter(entry -> entry.getKey().matches(includePattern))
                .filter(entry -> excludePattern == null || !entry.getKey().matches(excludePattern))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        final ParameterProvider parameterProvider = getParameterProvider(sensitive);
        final MockParameterProviderInitializationContext initContext = new MockParameterProviderInitializationContext("id", "name",
                new MockComponentLog("providerId", parameterProvider));
        parameterProvider.initialize(initContext);

        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(AbstractEnvironmentVariableParameterProvider.INCLUDE_REGEX, includePattern);
        if (excludePattern != null) {
            properties.put(AbstractEnvironmentVariableParameterProvider.EXCLUDE_REGEX, excludePattern);
        }
        final MockConfigurationContext mockConfigurationContext = new MockConfigurationContext(properties, null);

        final Map<ParameterDescriptor, Parameter> parameters = parameterProvider.fetchParameters(mockConfigurationContext);

        assertEquals(filteredVariables.size(), parameters.size());
        for(final Map.Entry<ParameterDescriptor, Parameter> entry : parameters.entrySet()) {
            assertEquals(sensitive, entry.getKey().isSensitive());
            assertNotNull(entry.getValue().getValue());
        }
    }

    @Test
    public void testSensitiveParameterProvider() throws InitializationException {
        runProviderTest(true, "P.*", null);
    }

    @Test
    public void testNonSensitiveParameterProvider() throws InitializationException {
        runProviderTest(false, ".*", "P.*");
    }
}
