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
package org.apache.nifi.web.dao.impl;

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.controller.ParameterProviderNode;
import org.apache.nifi.controller.ReloadComponent;
import org.apache.nifi.controller.exception.ValidationException;
import org.apache.nifi.controller.parameter.ParameterProviderInstantiationException;
import org.apache.nifi.controller.parameter.ParameterProviderProvider;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.util.BundleUtils;
import org.apache.nifi.web.NiFiCoreException;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.ParameterProviderDTO;
import org.apache.nifi.web.dao.ComponentStateDAO;
import org.apache.nifi.web.dao.ParameterProviderDAO;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StandardParameterProviderDAO extends ComponentDAO implements ParameterProviderDAO {

    private ParameterProviderProvider parameterProviderProvider;
    private ComponentStateDAO componentStateDAO;
    private ReloadComponent reloadComponent;

    private ParameterProviderNode locateParameterProvider(final String parameterProviderId) {
        // get the parameter provider
        final ParameterProviderNode parameterProvider = parameterProviderProvider.getParameterProviderNode(parameterProviderId);

        // ensure the parameter provider exists
        if (parameterProvider == null) {
            throw new ResourceNotFoundException(String.format("Unable to locate parameter provider with id '%s'.", parameterProviderId));
        }

        return parameterProvider;
    }

    @Override
    public void verifyCreate(final ParameterProviderDTO parameterProviderDTO) {
        verifyCreate(parameterProviderProvider.getExtensionManager(), parameterProviderDTO.getType(), parameterProviderDTO.getBundle());
    }

    @Override
    public ParameterProviderNode createParameterProvider(final ParameterProviderDTO parameterProviderDTO) {
        // ensure the type is specified
        if (parameterProviderDTO.getType() == null) {
            throw new IllegalArgumentException("The parameter provider type must be specified.");
        }

        try {
            // create the parameter provider
            final ExtensionManager extensionManager = parameterProviderProvider.getExtensionManager();
            final BundleCoordinate bundleCoordinate = BundleUtils.getBundle(extensionManager, parameterProviderDTO.getType(), parameterProviderDTO.getBundle());
            final ParameterProviderNode parameterProvider = parameterProviderProvider.createParameterProvider(
                    parameterProviderDTO.getType(), parameterProviderDTO.getId(), bundleCoordinate, true);

            // ensure we can perform the update
            verifyUpdate(parameterProvider, parameterProviderDTO);

            // perform the update
            configureParameterProvider(parameterProvider, parameterProviderDTO);

            return parameterProvider;
        } catch (final ParameterProviderInstantiationException e) {
            throw new NiFiCoreException(e.getMessage(), e);
        }
    }

    @Override
    public ParameterProviderNode getParameterProvider(final String parameterProviderId) {
        return locateParameterProvider(parameterProviderId);
    }

    @Override
    public boolean hasParameterProvider(final String parameterProviderId) {
        return parameterProviderProvider.getParameterProviderNode(parameterProviderId) != null;
    }

    @Override
    public Set<ParameterProviderNode> getParameterProviders() {
        return parameterProviderProvider.getAllParameterProviders();
    }

    @Override
    public ParameterProviderNode updateParameterProvider(final ParameterProviderDTO parameterProviderDTO) {
        // get the parameter provider
        final ParameterProviderNode parameterProvider = locateParameterProvider(parameterProviderDTO.getId());

        // ensure we can perform the update
        verifyUpdate(parameterProvider, parameterProviderDTO);

        // perform the update
        configureParameterProvider(parameterProvider, parameterProviderDTO);

        // attempt to change the underlying processor if an updated bundle is specified
        // updating the bundle must happen after configuring so that any additional classpath resources are set first
        updateBundle(parameterProvider, parameterProviderDTO);

        return parameterProvider;
    }

    @Override
    public ParameterProviderNode fetchParameters(final String parameterProviderId) {
        final ParameterProviderNode parameterProviderNode = locateParameterProvider(parameterProviderId);

        parameterProviderNode.fetchParameters();
        return parameterProviderNode;
    }

    @Override
    public void verifyCanApplyParameters(final String parameterProviderId, final Set<String> parameterNames) {
        final ParameterProviderNode parameterProviderNode = locateParameterProvider(parameterProviderId);
        parameterProviderNode.verifyCanApplyParameters(parameterNames);
    }

    @Override
    public Map<ParameterContext, Map<String, Parameter>> getFetchedParametersToApply(final String parameterProviderId, final Set<String> parameterNames) {
        final ParameterProviderNode parameterProviderNode = locateParameterProvider(parameterProviderId);

        return parameterProviderNode.getFetchedParametersToApply(parameterNames);
    }

    private void updateBundle(final ParameterProviderNode parameterProvider, final ParameterProviderDTO parameterProviderDTO) {
        final BundleDTO bundleDTO = parameterProviderDTO.getBundle();
        if (bundleDTO != null) {
            final ExtensionManager extensionManager = parameterProviderProvider.getExtensionManager();
            final BundleCoordinate incomingCoordinate = BundleUtils.getBundle(extensionManager, parameterProvider.getCanonicalClassName(), bundleDTO);
            final BundleCoordinate existingCoordinate = parameterProvider.getBundleCoordinate();
            if (!existingCoordinate.getCoordinate().equals(incomingCoordinate.getCoordinate())) {
                try {
                    // we need to use the property descriptors from the temp component here in case we are changing from a ghost component to a real component
                    final ConfigurableComponent tempComponent = extensionManager.getTempComponent(parameterProvider.getCanonicalClassName(), incomingCoordinate);
                    final Set<URL> additionalUrls = parameterProvider.getAdditionalClasspathResources(tempComponent.getPropertyDescriptors());
                    reloadComponent.reload(parameterProvider, parameterProvider.getCanonicalClassName(), incomingCoordinate, additionalUrls);
                } catch (final ParameterProviderInstantiationException e) {
                    throw new NiFiCoreException(String.format("Unable to update parameter provider %s from %s to %s due to: %s",
                            parameterProviderDTO.getId(), parameterProvider.getBundleCoordinate().getCoordinate(), incomingCoordinate.getCoordinate(), e.getMessage()), e);
                }
            }
        }
    }

    // A placeholder in case validation is required in the future
    private List<String> validateProposedConfiguration(final ParameterProviderNode parameterProvider, final ParameterProviderDTO parameterProviderDTO) {
        final List<String> validationErrors = new ArrayList<>();

        return validationErrors;
    }

    @Override
    public void verifyDelete(final String parameterProviderId) {
        // Throw ResourceNotFoundException if the provider does not exist
        final ParameterProviderNode parameterProviderNode = locateParameterProvider(parameterProviderId);
        parameterProviderNode.verifyCanDelete();
    }

    @Override
    public void verifyCanFetchParameters(final String parameterProviderId) {
        final ParameterProviderNode parameterProviderNode = locateParameterProvider(parameterProviderId);
        parameterProviderNode.verifyCanFetchParameters();
    }

    @Override
    public void verifyUpdate(final ParameterProviderDTO parameterProviderDTO) {
        final ParameterProviderNode parameterProvider = locateParameterProvider(parameterProviderDTO.getId());
        verifyUpdate(parameterProvider, parameterProviderDTO);
    }

    private void verifyUpdate(final ParameterProviderNode parameterProvider, final ParameterProviderDTO parameterProviderDTO) {
        if (isAnyNotNull(parameterProviderDTO.getName(),
                parameterProviderDTO.getAnnotationData(),
                parameterProviderDTO.getProperties(),
                parameterProviderDTO.getBundle())) {

            // validate the request
            final List<String> requestValidation = validateProposedConfiguration(parameterProvider, parameterProviderDTO);

            // ensure there was no validation errors
            if (!requestValidation.isEmpty()) {
                throw new ValidationException(requestValidation);
            }
        }

        final BundleDTO bundleDTO = parameterProviderDTO.getBundle();
        if (bundleDTO != null) {
            // ensures all nodes in a cluster have the bundle, throws exception if bundle not found for the given type
            final BundleCoordinate bundleCoordinate = BundleUtils.getBundle(
                    parameterProviderProvider.getExtensionManager(), parameterProvider.getCanonicalClassName(), bundleDTO);
            // ensure we are only changing to a bundle with the same group and id, but different version
            parameterProvider.verifyCanUpdateBundle(bundleCoordinate);
        }
    }

    private void configureParameterProvider(final ParameterProviderNode parameterProvider, final ParameterProviderDTO parameterProviderDTO) {
        final String name = parameterProviderDTO.getName();
        final String annotationData = parameterProviderDTO.getAnnotationData();
        final String comments = parameterProviderDTO.getComments();
        final Map<String, String> properties = parameterProviderDTO.getProperties();

        parameterProvider.pauseValidationTrigger(); // avoid triggering validation multiple times
        try {
            if (isNotNull(name)) {
                parameterProvider.setName(name);
            }
            if (isNotNull(annotationData)) {
                parameterProvider.setAnnotationData(annotationData);
            }
            if (isNotNull(comments)) {
                parameterProvider.setComments(comments);
            }
            if (isNotNull(properties)) {
                parameterProvider.setProperties(properties);
            }
        } finally {
            parameterProvider.resumeValidationTrigger();
        }
    }

    @Override
    public StateMap getState(final String parameterProviderId, final Scope scope) {
        final ParameterProviderNode parameterProvider = locateParameterProvider(parameterProviderId);
        return componentStateDAO.getState(parameterProvider, scope);
    }

    @Override
    public void verifyClearState(final String parameterProviderId) {
        final ParameterProviderNode parameterProvider = locateParameterProvider(parameterProviderId);
        parameterProvider.verifyCanClearState();
    }

    @Override
    public void clearState(final String parameterProviderId) {
        final ParameterProviderNode parameterProvider = locateParameterProvider(parameterProviderId);
        componentStateDAO.clearState(parameterProvider);
    }

    @Override
    public void deleteParameterProvider(final String parameterProviderId) {
        final ParameterProviderNode parameterProvider = locateParameterProvider(parameterProviderId);
        parameterProviderProvider.removeParameterProvider(parameterProvider);
    }

    /* setters */
    public void setParameterProviderProvider(final ParameterProviderProvider parameterProviderProvider) {
        this.parameterProviderProvider = parameterProviderProvider;
    }

    public void setComponentStateDAO(final ComponentStateDAO componentStateDAO) {
        this.componentStateDAO = componentStateDAO;
    }

    public void setReloadComponent(final ReloadComponent reloadComponent) {
        this.reloadComponent = reloadComponent;
    }
}
