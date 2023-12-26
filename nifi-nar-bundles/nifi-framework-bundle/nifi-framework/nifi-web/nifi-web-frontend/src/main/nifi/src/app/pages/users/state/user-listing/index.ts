/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import { AccessPolicySummaryEntity, Permissions, Revision } from '../../../../state/shared';

export interface UserEntity {
    id: string;
    permissions: Permissions;
    component: User;
    revision: Revision;
    uri: string;
}

export interface User extends Tenant {
    userGroups: TenantEntity[];
    accessPolicies: AccessPolicySummaryEntity[];
}

export interface UserGroupEntity {
    id: string;
    permissions: Permissions;
    component: UserGroup;
    revision: Revision;
    uri: string;
}

export interface UserGroup extends Tenant {
    users: TenantEntity[];
    accessPolicies: AccessPolicySummaryEntity[];
}

export interface TenantEntity {
    component: Tenant;
}

export interface Tenant {
    id: string;
    identity: string;
    configurable: boolean;
}

export interface SelectedTenant {
    id: string;
    user?: UserEntity;
    userGroup?: UserGroupEntity;
}

export interface LoadTenantsSuccess {
    users: UserEntity[];
    userGroups: UserGroupEntity[];
    loadedTimestamp: string;
}

export interface EditUserRequest {
    user: UserEntity;
}

export interface EditUserGroupRequest {
    userGroup: UserGroupEntity;
}

export interface UserAccessPoliciesRequest {
    user: UserEntity;
}

export interface UserGroupAccessPoliciesRequest {
    userGroup: UserGroupEntity;
}

export interface DeleteUserRequest {
    user: UserEntity;
}

export interface DeleteUserGroupRequest {
    userGroup: UserGroupEntity;
}

export interface UserListingState {
    users: UserEntity[];
    userGroups: UserGroupEntity[];
    saving: boolean;
    loadedTimestamp: string;
    error: string | null;
    status: 'pending' | 'loading' | 'error' | 'success';
}
