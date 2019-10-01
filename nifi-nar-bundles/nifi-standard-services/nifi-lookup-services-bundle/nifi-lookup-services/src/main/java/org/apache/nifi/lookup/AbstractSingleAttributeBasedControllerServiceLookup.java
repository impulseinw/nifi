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
 * See the License for the specific language governing permissions andf
 * limitations under the License.
 */
package org.apache.nifi.lookup;

import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * {@link LookupService} that can select a ControllerService of the given type {@link S}.
 * <p>
 * Selection is based on a single {@linkplain String} lookup key.
 * <p>
 * Lookup key is provided as a value in an attribute map (usually coming form a flowfile)
 * with a predefined key (see {@link #lookupAttribute()}).
 *
 * @param <S> The type of service to be looked up
 */
public abstract class AbstractSingleAttributeBasedControllerServiceLookup<S extends ControllerService> extends AbstractControllerService implements LookupService<S> {
    protected volatile Map<String, S> serviceMap;

    /**
     * @return the Class that represents the type of service that will be returned by {@link #lookup(Map)}
     */
    public abstract Class<S> getServiceType();

    /**
     * @return the name of attribute (usually from a flowfile) the value of which serves as the lookup key
     * for the desired service (of type {@link S})
     */
    protected abstract String lookupAttribute();

    @Override
    public Class<S> getValueType() {
        return getServiceType();
    }

    public S lookupService(Map<String, String> attributes) {
        S service = lookup(new HashMap<>(), attributes)
                .orElseThrow(() -> new ProcessException("No " + getServiceName() + " found for " + lookupAttribute()));

        return service;
    }

    @Override
    public Optional<S> lookup(Map<String, Object> unsupported) {
        throw new UnsupportedOperationException("This service only supports attribute-based lookup!");
    }

    @Override
    public Optional<S> lookup(Map<String, Object> unused, Map<String, String> attributes) {
        if (attributes == null) {
            throw new ProcessException("Attributes map is null");
        } else if (!attributes.containsKey(lookupAttribute())) {
            throw new ProcessException("Attributes must contain an attribute name '" + lookupAttribute() + "'");
        }

        Object lookupKey = Optional.of(lookupAttribute())
                .map(attributes::get)
                .orElseThrow(() -> new ProcessException(lookupAttribute() + " cannot be null or blank"));

        S service = serviceMap.get(lookupKey);

        return Optional.ofNullable(service);
    }

    @Override
    public Set<String> getRequiredKeys() {
        return new HashSet<>(Collections.singletonList(lookupAttribute()));
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        Map<String, S> serviceMap = new HashMap<>();

        context.getProperties().keySet().stream()
                .filter(PropertyDescriptor::isDynamic)
                .forEach(propertyDescriptor -> {
                    S service = context.getProperty(propertyDescriptor).asControllerService(getServiceType());

                    serviceMap.put(propertyDescriptor.getName(), service);
                });

        this.serviceMap = Collections.unmodifiableMap(serviceMap);
    }

    @OnDisabled
    public void onDisabled() {
        serviceMap = null;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return lookupKeyPropertyDescriptor(propertyDescriptorName);
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        return validateForAtLeastOneService(context);
    }

    protected PropertyDescriptor lookupKeyPropertyDescriptor(String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .description("The " + getServiceName() + " to return when " + lookupAttribute() + " = '" + propertyDescriptorName + "'")
                .identifiesControllerService(getServiceType())
                .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
                .build();
    }

    private Collection<ValidationResult> validateForAtLeastOneService(ValidationContext context) {
        final List<ValidationResult> results = new ArrayList<>();

        int numDefinedServices = 0;
        for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
            if (descriptor.isDynamic()) {
                numDefinedServices++;
            }

            final String referencedId = context.getProperty(descriptor).getValue();
            if (this.getIdentifier().equals(referencedId)) {
                numDefinedServices--;

                results.add(new ValidationResult.Builder()
                        .subject(descriptor.getDisplayName())
                        .explanation("the current service cannot be registered as a " + getServiceName() + " to lookup")
                        .valid(false)
                        .build());
            }
        }

        if (numDefinedServices == 0) {
            results.add(new ValidationResult.Builder()
                    .subject(this.getClass().getSimpleName())
                    .explanation("at least one " + getServiceName() + " must be defined via dynamic properties")
                    .valid(false)
                    .build());
        }

        return results;
    }

    protected String getServiceName() {
        return getServiceType().getSimpleName();
    }
}
