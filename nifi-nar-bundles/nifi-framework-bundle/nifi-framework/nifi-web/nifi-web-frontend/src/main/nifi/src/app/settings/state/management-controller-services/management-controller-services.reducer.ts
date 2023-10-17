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

import { createReducer, on } from '@ngrx/store';
import { ManagementControllerServicesState } from './index';
import {
    configureControllerServiceSuccess,
    createControllerServiceSuccess,
    deleteControllerServiceSuccess,
    loadManagementControllerServices,
    loadManagementControllerServicesSuccess,
    managementControllerServicesApiError
} from './management-controller-services.actions';
import { produce } from 'immer';

export const initialState: ManagementControllerServicesState = {
    controllerServices: [],
    loadedTimestamp: '',
    error: null,
    status: 'pending'
};

export const managementControllerServicesReducer = createReducer(
    initialState,
    on(loadManagementControllerServices, (state) => ({
        ...state,
        status: 'loading' as const
    })),
    on(loadManagementControllerServicesSuccess, (state, { response }) => ({
        ...state,
        controllerServices: response.controllerServices,
        loadedTimestamp: response.loadedTimestamp,
        error: null,
        status: 'success' as const
    })),
    on(managementControllerServicesApiError, (state, { error }) => ({
        ...state,
        error,
        status: 'error' as const
    })),
    on(createControllerServiceSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            draftState.controllerServices.push(response.controllerService);
        });
    }),
    on(configureControllerServiceSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const componentIndex: number = draftState.controllerServices.findIndex((f: any) => response.id === f.id);
            if (componentIndex > -1) {
                draftState.controllerServices[componentIndex] = response.controllerService;
            }
        });
    }),
    on(deleteControllerServiceSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const componentIndex: number = draftState.controllerServices.findIndex(
                (f: any) => response.controllerService.id === f.id
            );
            if (componentIndex > -1) {
                draftState.controllerServices.splice(componentIndex, 1);
            }
        });
    })
);
