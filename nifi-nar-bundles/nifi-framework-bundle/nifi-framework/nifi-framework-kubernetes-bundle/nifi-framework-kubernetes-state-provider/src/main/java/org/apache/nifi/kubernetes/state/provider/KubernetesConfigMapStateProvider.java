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
package org.apache.nifi.kubernetes.state.provider;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.StatusDetails;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.Resource;
import org.apache.nifi.components.AbstractConfigurableComponent;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.components.state.StateProvider;
import org.apache.nifi.components.state.StateProviderInitializationContext;
import org.apache.nifi.kubernetes.client.ServiceAccountNamespaceProvider;
import org.apache.nifi.kubernetes.client.StandardKubernetesClientProvider;
import org.apache.nifi.logging.ComponentLog;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * State Provider implementation based on Kubernetes ConfigMaps with Base64 encoded keys to meet Kubernetes constraints
 */
public class KubernetesConfigMapStateProvider extends AbstractConfigurableComponent implements StateProvider {
    private static final Scope[] SUPPORTED_SCOPES = { Scope.CLUSTER };

    private static final long UNKNOWN_VERSION = 0;

    private static final Charset KEY_CHARACTER_SET = StandardCharsets.UTF_8;

    private static final String CONFIG_MAP_NAME_FORMAT = "nifi-component-%s";

    /** Encode ConfigMap keys using URL Encoder without padding characters for compliance with Kubernetes naming */
    private static final Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();

    private static final Base64.Decoder decoder = Base64.getUrlDecoder();

    private final AtomicBoolean enabled = new AtomicBoolean();

    private KubernetesClient kubernetesClient;

    private String namespace;

    private String identifier;

    private ComponentLog logger;

    /**
     * Get configured component identifier
     *
     * @return Component Identifier
     */
    @Override
    public String getIdentifier() {
        return identifier;
    }

    /**
     * Initialize Provider using configured properties
     *
     * @param context Initialization Context
     */
    @Override
    public void initialize(final StateProviderInitializationContext context) {
        this.identifier = context.getIdentifier();
        this.logger = context.getLogger();
        this.kubernetesClient = getKubernetesClient();
        this.namespace = new ServiceAccountNamespaceProvider().getNamespace();
    }

    /**
     * Shutdown Provider
     */
    @Override
    public void shutdown() {
        kubernetesClient.close();
        logger.info("Provider shutdown");
    }

    /**
     * Set State as ConfigMap based on Component Identifier
     *
     * @param state State Map
     * @param componentId Component Identifier
     * @throws IOException Thrown on failure to set State Map
     */
    @Override
    public void setState(final Map<String, String> state, final String componentId) throws IOException {
        try {
            final ConfigMap configMap = createConfigMapBuilder(state, componentId).build();
            final ConfigMap configMapCreated = kubernetesClient.configMaps().resource(configMap).createOrReplace();
            final long version = getVersion(configMapCreated);
            logger.debug("Set State Component ID [{}] Version [{}]", componentId, version);
        } catch (final KubernetesClientException e) {
            if (isNotFound(e.getCode())) {
                logger.debug("State not found for Component ID [{}]", componentId, e);
            } else {
                throw new IOException(String.format("Set failed for Component ID [%s]", componentId), e);
            }
        } catch (final RuntimeException e) {
            throw new IOException(String.format("Set failed for Component ID [%s]", componentId), e);
        }
    }

    /**
     * Get State Map for Component Identifier
     *
     * @param componentId Component Identifier of State to be retrieved
     * @return State Map
     * @throws IOException Thrown on failure to get State Map
     */
    @Override
    public StateMap getState(final String componentId) throws IOException {
        try {
            final ConfigMap configMap = configMapResource(componentId).get();
            final Map<String, String> data = configMap == null ? Collections.emptyMap() : getDecodedMap(configMap.getData());
            final long version = configMap == null ? UNKNOWN_VERSION : getVersion(configMap);
            return new StandardStateMap(data, version);
        } catch (final RuntimeException e) {
            throw new IOException(String.format("Get failed for Component ID [%s]", componentId), e);
        }
    }

    /**
     * Replace State ConfigMap with new State based on current resource version
     *
     * @param currentState Current State Map with version
     * @param state New State Map
     * @param componentId Component Identifier
     * @return Replace operation status
     */
    @Override
    public boolean replace(final StateMap currentState, final Map<String, String> state, final String componentId) throws IOException {
        final String resourceVersion = Long.toString(currentState.getVersion());
        final ConfigMap configMap = createConfigMapBuilder(state, componentId)
                .editOrNewMetadata()
                .withResourceVersion(resourceVersion)
                .endMetadata()
                .build();

        try {
            final ConfigMap configMapReplaced = kubernetesClient.configMaps().resource(configMap).replace();
            final long version = getVersion(configMapReplaced);
            logger.debug("Replaced State Component ID [{}] Version [{}]", componentId, version);
            return true;
        } catch (final KubernetesClientException e) {
            if (isNotFoundOrConflict(e.getCode())) {
                logger.debug("Replace State Failed Component ID [{}] Version [{}]", componentId, resourceVersion, e);
                return false;
            } else {
                throw new IOException(String.format("Replace failed for Component ID [%s]", componentId), e);
            }
        } catch (final RuntimeException e) {
            throw new IOException(String.format("Replace failed for Component ID [%s]", componentId), e);
        }
    }

    /**
     * Clear state information for specified Component Identifier
     *
     * @param componentId the id of the component for which state is being cleared
     * @throws IOException Thrown on failure to clear state for Component Identifier
     */
    @Override
    public void clear(final String componentId) throws IOException {
        try {
            setState(Collections.emptyMap(), componentId);
        } catch (final RuntimeException e) {
            throw new IOException(String.format("Clear failed for Component ID [%s]", componentId), e);
        }
    }

    /**
     * Remove state information for specified Component Identifier
     *
     * @param componentId Identifier of component removed from the configuration
     * @throws IOException Thrown on failure to remove state for Component Identifier
     */
    @Override
    public void onComponentRemoved(final String componentId) throws IOException {
        try {
            final List<StatusDetails> deleteStatus = configMapResource(componentId).delete();
            logger.debug("Config Map [{}] deleted {}", componentId, deleteStatus);
        } catch (final RuntimeException e) {
            throw new IOException(String.format("Remove failed for Component ID [%s]", componentId), e);
        }
    }

    /**
     * Enable Provider
     */
    @Override
    public void enable() {
        enabled.getAndSet(true);
    }

    /**
     * Disable Provider
     */
    @Override
    public void disable() {
        enabled.getAndSet(false);
    }

    /**
     * Get Enabled status
     *
     * @return Enabled status
     */
    @Override
    public boolean isEnabled() {
        return enabled.get();
    }

    /**
     * Get Supported Scopes returns CLUSTER
     *
     * @return Supported Scopes including CLUSTER
     */
    @Override
    public Scope[] getSupportedScopes() {
        return SUPPORTED_SCOPES;
    }

    /**
     * Get Kubernetes Client using standard configuration
     *
     * @return Kubernetes Client
     */
    protected KubernetesClient getKubernetesClient() {
        return new StandardKubernetesClientProvider().getKubernetesClient();
    }

    private Resource<ConfigMap> configMapResource(final String componentId) {
        final String name = getConfigMapName(componentId);
        return kubernetesClient.configMaps().inNamespace(namespace).withName(name);
    }

    private ConfigMapBuilder createConfigMapBuilder(final Map<String, String> state, final String componentId) {
        final Map<String, String> encodedData = getEncodedMap(state);
        final String name = getConfigMapName(componentId);
        return new ConfigMapBuilder()
                .withNewMetadata()
                .withNamespace(namespace)
                .withName(name)
                .endMetadata()
                .withData(encodedData);
    }

    private String getConfigMapName(final String componentId) {
        return String.format(CONFIG_MAP_NAME_FORMAT, componentId);
    }

    private long getVersion(final ConfigMap configMap) {
        final ObjectMeta metadata = configMap.getMetadata();
        final String resourceVersion = metadata.getResourceVersion();
        try {
            return resourceVersion == null ? UNKNOWN_VERSION : Long.parseLong(resourceVersion);
        } catch (final NumberFormatException e) {
            logger.debug("ConfigMap [{}] Resource Version [{}] parsing failed", metadata.getName(), resourceVersion);
            return UNKNOWN_VERSION;
        }
    }

    private Map<String, String> getEncodedMap(final Map<String, String> stateMap) {
        final Map<String, String> encodedMap = new LinkedHashMap<>();
        stateMap.forEach((key, value) -> {
            final byte[] keyBytes = key.getBytes(KEY_CHARACTER_SET);
            final String encodedKey = encoder.encodeToString(keyBytes);
            encodedMap.put(encodedKey, value);
        });
        return encodedMap;
    }

    private Map<String, String> getDecodedMap(final Map<String, String> configMap) {
        final Map<String, String> decodedMap = new LinkedHashMap<>();
        configMap.forEach((key, value) -> {
            final byte[] keyBytes = decoder.decode(key);
            final String decodedKey = new String(keyBytes, KEY_CHARACTER_SET);
            decodedMap.put(decodedKey, value);
        });
        return decodedMap;
    }

    private boolean isNotFound(final int code) {
        return HttpURLConnection.HTTP_NOT_FOUND == code;
    }

    private boolean isNotFoundOrConflict(final int code) {
        return isNotFound(code) || HttpURLConnection.HTTP_CONFLICT == code;
    }
}
