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

import { createFeatureSelector, createSelector } from '@ngrx/store';
import { extensionTypesFeatureKey, ExtensionTypesState } from './index';
import { DocumentedType, RequiredPermission } from '../shared';

export const selectExtensionTypesState = createFeatureSelector<ExtensionTypesState>(extensionTypesFeatureKey);

export const selectProcessorTypes = createSelector(
    selectExtensionTypesState,
    (state: ExtensionTypesState) => state.processorTypes
);

export const selectControllerServiceTypes = createSelector(
    selectExtensionTypesState,
    (state: ExtensionTypesState) => state.controllerServiceTypes
);

export const selectPrioritizerTypes = createSelector(
    selectExtensionTypesState,
    (state: ExtensionTypesState) => state.prioritizerTypes
);

export const selectReportingTaskTypes = createSelector(
    selectExtensionTypesState,
    (state: ExtensionTypesState) => state.reportingTaskTypes
);

export const selectRegistryClientTypes = createSelector(
    selectExtensionTypesState,
    (state: ExtensionTypesState) => state.registryClientTypes
);

export const selectTypesToIdentifyComponentRestrictions = createSelector(
    selectExtensionTypesState,
    (state: ExtensionTypesState) => {
        const types: DocumentedType[] = [];

        if (state.processorTypes) {
            types.push(...state.processorTypes);
        }
        if (state.controllerServiceTypes) {
            types.push(...state.controllerServiceTypes);
        }
        if (state.reportingTaskTypes) {
            types.push(...state.reportingTaskTypes);
        }
        if (state.parameterProviderTypes) {
            types.push(...state.parameterProviderTypes);
        }
        if (state.flowAnalysisRuleTypes) {
            types.push(...state.flowAnalysisRuleTypes);
        }

        return types;
    }
);

export const selectRequiredPermissions = createSelector(
    selectTypesToIdentifyComponentRestrictions,
    (documentedTypes: DocumentedType[]) => {
        const requiredPermissions: Map<string, RequiredPermission> = new Map<string, RequiredPermission>();

        documentedTypes
            .filter((documentedType) => documentedType.restricted)
            .forEach((documentedType) => {
                if (documentedType.explicitRestrictions) {
                    documentedType.explicitRestrictions.forEach((explicitRestriction) => {
                        const requiredPermission: RequiredPermission = explicitRestriction.requiredPermission;

                        if (!requiredPermissions.has(requiredPermission.id)) {
                            requiredPermissions.set(requiredPermission.id, requiredPermission);
                        }
                    });
                }
            });

        return Array.from(requiredPermissions.values());
    }
);
