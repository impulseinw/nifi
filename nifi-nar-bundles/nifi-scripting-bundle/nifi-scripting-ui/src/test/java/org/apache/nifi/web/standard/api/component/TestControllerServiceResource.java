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
package org.apache.nifi.web.standard.api.component;

import com.bazaarvoice.jolt.JsonUtils;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.nifi.web.ComponentDescriptor;
import org.apache.nifi.web.ComponentDetails;
import org.apache.nifi.web.NiFiWebConfigurationContext;
import org.apache.nifi.web.NiFiWebConfigurationRequestContext;
import org.apache.nifi.web.NiFiWebRequestContext;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.inmemory.InMemoryTestContainerFactory;
import org.glassfish.jersey.test.spi.TestContainerFactory;
import org.junit.Test;
import org.mockito.Mockito;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;

public class TestControllerServiceResource extends JerseyTest {

    public static final ServletContext servletContext = mock(ServletContext.class);
    public static final HttpServletRequest requestContext = mock(HttpServletRequest.class);

    @Override
    protected Application configure() {
        final ResourceConfig config = new ResourceConfig();
        config.register(ControllerServiceResource.class);
        config.register(JacksonFeature.class);
        config.register(new AbstractBinder() {
            @Override
            public void configure() {
                bindFactory(MockRequestContext.class).to(HttpServletRequest.class);
            }
        });
        config.register(new AbstractBinder() {
            @Override
            public void configure() {
                bindFactory(MockServletContext.class).to(ServletContext.class);
            }
        });
        return config;
    }

    @Override
    public TestContainerFactory getTestContainerFactory() {
        return new InMemoryTestContainerFactory();
    }

    @Test
    public void testSetControllerServiceProperties() {
        final NiFiWebConfigurationContext nifiWebConfigurationContext = mock(NiFiWebConfigurationContext.class);
        final Map<String, String> properties = new HashMap<>();
        properties.put("Script Body", "def x = '1'");
        final ComponentDetails componentDetails = new ComponentDetails.Builder().properties(properties).build();

        Mockito.when(servletContext.getAttribute(Mockito.anyString())).thenReturn(nifiWebConfigurationContext);
        Mockito.when(nifiWebConfigurationContext.updateComponent(any(NiFiWebConfigurationRequestContext.class), any(String.class), any(Map.class))).thenReturn(componentDetails);

        final Response response = client().target(getBaseUri())
                .path("/standard/controller-service/properties")
                .queryParam("controllerServiceId", "1")
                .queryParam("clientId", "1")
                .queryParam("revisionId", "1")
                .request()
                .put(Entity.json(JsonUtils.toJsonString(properties)));

        assertNotNull(response);
        final JsonNode jsonNode = response.readEntity(JsonNode.class);
        assertNotNull(jsonNode);
        assertTrue(jsonNode.get("properties").get("Script Body").asText().equals("def x = '1'"));
    }

    @Test
    public void testGetControllerServiceDetails() {
        final NiFiWebConfigurationContext nifiWebConfigurationContext = mock(NiFiWebConfigurationContext.class);
        final Map<String, String> allowableValues = new HashMap<>();
        final ComponentDescriptor descriptor = new ComponentDescriptor.Builder().name("test-name").allowableValues(allowableValues).build();
        final Map<String, ComponentDescriptor> descriptors = new HashMap<>();
        descriptors.put("Script Body", descriptor);
        final ComponentDetails componentDetails = new ComponentDetails.Builder().name("my-script").type("org.apache.nifi.record.script.ScriptedReader")
                .descriptors(descriptors)
                .build();

        Mockito.when(servletContext.getAttribute(Mockito.anyString())).thenReturn(nifiWebConfigurationContext);
        Mockito.when(nifiWebConfigurationContext.getComponentDetails(any(NiFiWebRequestContext.class))).thenReturn(componentDetails);

        final JsonNode value = client().target(getBaseUri())
                .path("/standard/controller-service/details")
                .queryParam("controllerServiceId", "1")
                .request()
                .get(JsonNode.class);

        assertNotNull(value);

        try {
            assertTrue(value.get("name").asText().equals("my-script"));
        } catch (Exception e) {
            fail("Failed due to: " + e.toString());
        }
    }

    public static class MockRequestContext implements Factory<HttpServletRequest> {
        @Override
        public HttpServletRequest provide() {
            return requestContext;
        }

        @Override
        public void dispose(HttpServletRequest t) {
        }
    }

    public static class MockServletContext implements Factory<ServletContext> {
        @Override
        public ServletContext provide() {
            return servletContext;
        }

        @Override
        public void dispose(ServletContext t) {
        }
    }
}
