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
package org.apache.nifi.tests.system.controllerservice;

import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ControllerServicesClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.PropertyDescriptorDTO;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServiceRunStatusEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.PropertyDescriptorEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ControllerServiceApiValidationIT extends NiFiSystemIT {

    private static final String SENSITIVE_PROPERTY_NAME = "Credentials";

    private static final String SENSITIVE_PROPERTY_VALUE = "Token";

    private static final Set<String> SENSITIVE_DYNAMIC_PROPERTY_NAMES = Collections.singleton(SENSITIVE_PROPERTY_NAME);

    @Test
    public void testMatchingControllerService() throws NiFiClientException, IOException {
        final ControllerServiceEntity fakeServiceEntity = getClientUtil().createControllerService("FakeControllerService1");
        final ProcessorEntity fakeProcessorEntity = getClientUtil().createProcessor("FakeProcessor");
        fakeProcessorEntity.getComponent().getConfig().setProperties(Collections.singletonMap("Fake Service", fakeServiceEntity.getId()));
        getNifiClient().getProcessorClient().updateProcessor(fakeProcessorEntity);
        final ControllerServiceRunStatusEntity runStatusEntity = new ControllerServiceRunStatusEntity();
        runStatusEntity.setState("ENABLED");
        runStatusEntity.setRevision(fakeServiceEntity.getRevision());
        getNifiClient().getControllerServicesClient().activateControllerService(fakeServiceEntity.getId(), runStatusEntity);
        getClientUtil().waitForControllerSerivcesEnabled("root");
        String controllerStatus = getNifiClient().getControllerServicesClient().getControllerService(fakeServiceEntity.getId()).getStatus().getRunStatus();
        String processorStatus = getNifiClient().getProcessorClient().getProcessor(fakeProcessorEntity.getId()).getStatus().getRunStatus();

        assertEquals("ENABLED", controllerStatus);
        assertEquals("Stopped", processorStatus);
    }

    @Test
    public void testMatchingDynamicPropertyControllerService() throws NiFiClientException, IOException {
        final ControllerServiceEntity fakeServiceEntity = getClientUtil().createControllerService("FakeControllerService1");
        final ProcessorEntity fakeProcessorEntity = getClientUtil().createProcessor("FakeDynamicPropertiesProcessor");
        fakeProcessorEntity.getComponent().getConfig().setProperties(Collections.singletonMap("FCS.fakeControllerService", fakeServiceEntity.getId()));
        getNifiClient().getProcessorClient().updateProcessor(fakeProcessorEntity);
        final ControllerServiceRunStatusEntity runStatusEntity = new ControllerServiceRunStatusEntity();
        runStatusEntity.setState("ENABLED");
        runStatusEntity.setRevision(fakeServiceEntity.getRevision());
        getNifiClient().getControllerServicesClient().activateControllerService(fakeServiceEntity.getId(), runStatusEntity);
        getClientUtil().waitForControllerSerivcesEnabled("root");
        String controllerStatus = getNifiClient().getControllerServicesClient().getControllerService(fakeServiceEntity.getId()).getStatus().getRunStatus();
        String processorStatus = getNifiClient().getProcessorClient().getProcessor(fakeProcessorEntity.getId()).getStatus().getRunStatus();

        assertEquals("ENABLED", controllerStatus);
        assertEquals("Stopped", processorStatus);
    }

    @Test
    public void testNonMatchingControllerService() throws NiFiClientException, IOException, InterruptedException {
        final ControllerServiceEntity controllerService = getClientUtil().createControllerService(
                NiFiSystemIT.TEST_CS_PACKAGE + ".FakeControllerService2",
                "root",
                NiFiSystemIT.NIFI_GROUP_ID,
                "nifi-system-test-extensions2-nar",
                getNiFiVersion());

        final ProcessorEntity processor = getClientUtil().createProcessor("FakeProcessor");
        getClientUtil().updateProcessorProperties(processor, Collections.singletonMap("Fake Service", controllerService.getId()));
        getClientUtil().enableControllerService(controllerService);

        getClientUtil().waitForControllerSerivcesEnabled("root");

        final String controllerStatus = getNifiClient().getControllerServicesClient().getControllerService(controllerService.getId()).getStatus().getRunStatus();
        assertEquals("ENABLED", controllerStatus);

        getClientUtil().waitForInvalidProcessor(processor.getId());
    }

    @Test
    public void testNonMatchingDynamicPropertyControllerService() throws NiFiClientException, IOException, InterruptedException {
        final ControllerServiceEntity controllerService = getClientUtil().createControllerService(
                NiFiSystemIT.TEST_CS_PACKAGE + ".FakeControllerService2",
                "root",
                NiFiSystemIT.NIFI_GROUP_ID,
                "nifi-system-test-extensions2-nar",
                getNiFiVersion());

        final ProcessorEntity processor = getClientUtil().createProcessor("FakeDynamicPropertiesProcessor");
        processor.getComponent().getConfig().setProperties(Collections.singletonMap("FCS.fakeControllerService", controllerService.getId()));
        getNifiClient().getProcessorClient().updateProcessor(processor);
        final ControllerServiceRunStatusEntity runStatusEntity = new ControllerServiceRunStatusEntity();
        runStatusEntity.setState("ENABLED");
        runStatusEntity.setRevision(controllerService.getRevision());
        getNifiClient().getControllerServicesClient().activateControllerService(controllerService.getId(), runStatusEntity);
        getClientUtil().waitForControllerSerivcesEnabled("root");

        final String controllerStatus = getNifiClient().getControllerServicesClient().getControllerService(controllerService.getId()).getStatus().getRunStatus();
        assertEquals("ENABLED", controllerStatus);

        getClientUtil().waitForInvalidProcessor(processor.getId());
    }

    @Test
    public void testMatchingGenericControllerService() throws NiFiClientException, IOException {
        final ControllerServiceEntity fakeServiceEntity = getClientUtil().createControllerService("FakeControllerService1");
        final ProcessorEntity fakeProcessorEntity = getClientUtil().createProcessor(
                NiFiSystemIT.TEST_PROCESSORS_PACKAGE + ".FakeGenericProcessor",
                "root",
                NiFiSystemIT.NIFI_GROUP_ID,
                "nifi-system-test-extensions2-nar",
                getNiFiVersion());
        fakeProcessorEntity.getComponent().getConfig().setProperties(Collections.singletonMap("Fake Service", fakeServiceEntity.getId()));
        getNifiClient().getProcessorClient().updateProcessor(fakeProcessorEntity);
        final ControllerServiceRunStatusEntity runStatusEntity = new ControllerServiceRunStatusEntity();
        runStatusEntity.setState("ENABLED");
        runStatusEntity.setRevision(fakeServiceEntity.getRevision());
        getNifiClient().getControllerServicesClient().activateControllerService(fakeServiceEntity.getId(), runStatusEntity);
        getClientUtil().waitForControllerSerivcesEnabled("root");
        String controllerStatus = getNifiClient().getControllerServicesClient().getControllerService(fakeServiceEntity.getId()).getStatus().getRunStatus();
        String processorStatus = getNifiClient().getProcessorClient().getProcessor(fakeProcessorEntity.getId()).getStatus().getRunStatus();

        assertEquals("ENABLED", controllerStatus);
        assertEquals("Stopped", processorStatus);
    }

    @Test
    public void testMatchingGenericDynamicPropertyControllerService() throws NiFiClientException, IOException {
        final ControllerServiceEntity fakeServiceEntity = getClientUtil().createControllerService("FakeControllerService1");
        final ProcessorEntity fakeProcessorEntity = getClientUtil().createProcessor("FakeDynamicPropertiesProcessor");
        fakeProcessorEntity.getComponent().getConfig().setProperties(Collections.singletonMap("CS.fakeControllerService", fakeServiceEntity.getId()));
        getNifiClient().getProcessorClient().updateProcessor(fakeProcessorEntity);
        final ControllerServiceRunStatusEntity runStatusEntity = new ControllerServiceRunStatusEntity();
        runStatusEntity.setState("ENABLED");
        runStatusEntity.setRevision(fakeServiceEntity.getRevision());
        getNifiClient().getControllerServicesClient().activateControllerService(fakeServiceEntity.getId(), runStatusEntity);
        getClientUtil().waitForControllerSerivcesEnabled("root");
        String controllerStatus = getNifiClient().getControllerServicesClient().getControllerService(fakeServiceEntity.getId()).getStatus().getRunStatus();
        String processorStatus = getNifiClient().getProcessorClient().getProcessor(fakeProcessorEntity.getId()).getStatus().getRunStatus();

        assertEquals("ENABLED", controllerStatus);
        assertEquals("Stopped", processorStatus);
    }

    @Test
    void testGetPropertyDescriptor() throws NiFiClientException, IOException {
        final ControllerServiceEntity controllerServiceEntity = getClientUtil().createControllerService("SensitiveDynamicPropertiesService");

        final ControllerServicesClient servicesClient = getNifiClient().getControllerServicesClient();
        final PropertyDescriptorEntity propertyDescriptorEntity = servicesClient.getPropertyDescriptor(controllerServiceEntity.getId(), SENSITIVE_PROPERTY_NAME, null);
        final PropertyDescriptorDTO propertyDescriptor = propertyDescriptorEntity.getPropertyDescriptor();
        assertFalse(propertyDescriptor.isSensitive());
        assertTrue(propertyDescriptor.isDynamic());

        final PropertyDescriptorEntity sensitivePropertyDescriptorEntity = servicesClient.getPropertyDescriptor(controllerServiceEntity.getId(), SENSITIVE_PROPERTY_NAME, true);
        final PropertyDescriptorDTO sensitivePropertyDescriptor = sensitivePropertyDescriptorEntity.getPropertyDescriptor();
        assertTrue(sensitivePropertyDescriptor.isSensitive());
        assertTrue(sensitivePropertyDescriptor.isDynamic());
    }

    @Test
    public void testSensitiveDynamicPropertiesNotSupported() throws NiFiClientException, IOException {
        final ControllerServiceEntity controllerServiceEntity = getClientUtil().createControllerService("StandardCountService");
        final ControllerServiceDTO component = controllerServiceEntity.getComponent();
        assertFalse(component.getSupportsSensitiveDynamicProperties());

        component.setSensitiveDynamicPropertyNames(SENSITIVE_DYNAMIC_PROPERTY_NAMES);

        getClientUtil().updateControllerService(controllerServiceEntity, Collections.singletonMap(SENSITIVE_PROPERTY_NAME, SENSITIVE_PROPERTY_VALUE));

        getClientUtil().waitForControllerServiceValidationStatus(controllerServiceEntity.getId(), ControllerServiceDTO.INVALID);
    }

    @Test
    public void testSensitiveDynamicPropertiesSupportedConfigured() throws NiFiClientException, IOException {
        final ControllerServiceEntity controllerServiceEntity = getClientUtil().createControllerService("SensitiveDynamicPropertiesService");
        final ControllerServiceDTO component = controllerServiceEntity.getComponent();
        assertTrue(component.getSupportsSensitiveDynamicProperties());

        component.setSensitiveDynamicPropertyNames(SENSITIVE_DYNAMIC_PROPERTY_NAMES);
        component.setProperties(Collections.singletonMap(SENSITIVE_PROPERTY_NAME, SENSITIVE_PROPERTY_VALUE));

        getNifiClient().getControllerServicesClient().updateControllerService(controllerServiceEntity);

        final ControllerServiceEntity updatedControllerServiceEntity = getNifiClient().getControllerServicesClient().getControllerService(controllerServiceEntity.getId());
        final ControllerServiceDTO updatedComponent = updatedControllerServiceEntity.getComponent();

        final Map<String, String> properties = updatedComponent.getProperties();
        assertNotSame(SENSITIVE_PROPERTY_VALUE, properties.get(SENSITIVE_PROPERTY_NAME));

        final Map<String, PropertyDescriptorDTO> descriptors = updatedComponent.getDescriptors();
        final PropertyDescriptorDTO descriptor = descriptors.get(SENSITIVE_PROPERTY_NAME);
        assertNotNull(descriptor);
        assertTrue(descriptor.isSensitive());
        assertTrue(descriptor.isDynamic());

        getClientUtil().waitForControllerServiceValidationStatus(controllerServiceEntity.getId(), ControllerServiceDTO.VALID);
    }
}
