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

package org.apache.nifi.groups;

import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReloadComponent;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.registry.flow.FlowRegistryClient;
import org.apache.nifi.registry.flow.mapping.FlowMappingOptions;

import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class ProcessGroupSynchronizationContext {
    private final ComponentIdGenerator componentIdGenerator;
    private final FlowManager flowManager;
    private final FlowRegistryClient flowRegistryClient;
    private final ReloadComponent reloadComponent;
    private final ControllerServiceProvider controllerServiceProvider;
    private final ExtensionManager extensionManager;
    private final ComponentScheduler componentScheduler;
    private final FlowMappingOptions flowMappingOptions;
    private final Function<ProcessorNode, ProcessContext> processContextFactory;


    private ProcessGroupSynchronizationContext(final Builder builder) {
        this.componentIdGenerator = builder.componentIdGenerator;
        this.flowManager = builder.flowManager;
        this.flowRegistryClient = builder.flowRegistryClient;
        this.reloadComponent = builder.reloadComponent;
        this.controllerServiceProvider = builder.controllerServiceProvider;
        this.extensionManager = builder.extensionManager;
        this.componentScheduler = builder.componentScheduler;
        this.flowMappingOptions = builder.flowMappingOptions;
        this.processContextFactory = builder.processContextFactory;
    }

    public ComponentIdGenerator getComponentIdGenerator() {
        return componentIdGenerator;
    }

    public FlowManager getFlowManager() {
        return flowManager;
    }

    public FlowRegistryClient getFlowRegistryClient() {
        return flowRegistryClient;
    }

    public ReloadComponent getReloadComponent() {
        return reloadComponent;
    }

    public ControllerServiceProvider getControllerServiceProvider() {
        return controllerServiceProvider;
    }

    public ExtensionManager getExtensionManager() {
        return extensionManager;
    }

    public ComponentScheduler getComponentScheduler() {
        return componentScheduler;
    }

    public FlowMappingOptions getFlowMappingOptions() {
        return flowMappingOptions;
    }

    public Function<ProcessorNode, ProcessContext> getProcessContextFactory() {
        return processContextFactory;
    }

    public static class Builder {
        private ComponentIdGenerator componentIdGenerator;
        private FlowManager flowManager;
        private FlowRegistryClient flowRegistryClient;
        private ReloadComponent reloadComponent;
        private ControllerServiceProvider controllerServiceProvider;
        private ExtensionManager extensionManager;
        private ComponentScheduler componentScheduler;
        private FlowMappingOptions flowMappingOptions;
        private Function<ProcessorNode, ProcessContext> processContextFactory;

        public Builder componentIdGenerator(final ComponentIdGenerator componentIdGenerator) {
            this.componentIdGenerator = componentIdGenerator;
            return this;
        }

        public Builder flowManager(final FlowManager flowManager) {
            this.flowManager = flowManager;
            return this;
        }

        public Builder flowRegistryClient(final FlowRegistryClient client) {
            this.flowRegistryClient = client;
            return this;
        }

        public Builder reloadComponent(final ReloadComponent reloadComponent) {
            this.reloadComponent = reloadComponent;
            return this;
        }

        public Builder controllerServiceProvider(final ControllerServiceProvider provider) {
            this.controllerServiceProvider = provider;
            return this;
        }

        public Builder extensionManager(final ExtensionManager extensionManager) {
            this.extensionManager = extensionManager;
            return this;
        }

        public Builder componentScheduler(final ComponentScheduler scheduler) {
            this.componentScheduler = scheduler;
            return this;
        }

        public Builder flowMappingOptions(final FlowMappingOptions flowMappingOptions) {
            this.flowMappingOptions = flowMappingOptions;
            return this;
        }

        public Builder processContextFactory(final Function<ProcessorNode, ProcessContext> processContextFactory) {
            this.processContextFactory = processContextFactory;
            return this;
        }

        public ProcessGroupSynchronizationContext build() {
            requireNonNull(componentIdGenerator, "Component ID Generator must be set");
            requireNonNull(flowManager, "Flow Manager must be set");
            requireNonNull(flowRegistryClient, "Flow Registry Client must be set");
            requireNonNull(reloadComponent, "Reload Component must be set");
            requireNonNull(controllerServiceProvider, "Controller Service Provider must be set");
            requireNonNull(extensionManager, "Extension Manager must be set");
            requireNonNull(componentScheduler, "Component Scheduler must be set");
            requireNonNull(flowMappingOptions, "Flow Mapping Options must be set");
            requireNonNull(processContextFactory, "Process Context Factory must be set");
            return new ProcessGroupSynchronizationContext(this);
        }
    }

}
