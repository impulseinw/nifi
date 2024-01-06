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

import { RouterModule, Routes } from '@angular/router';
import { NgModule } from '@angular/core';
import { AccessPolicies } from './access-policies.component';
import { authorizationGuard } from '../../../service/guard/authorization.guard';
import { CurrentUser } from '../../../state/current-user';

const routes: Routes = [
    {
        path: '',
        component: AccessPolicies,
        canMatch: [authorizationGuard((user: CurrentUser) => user.tenantsPermissions.canRead)],
        children: [
            {
                path: 'global',
                loadChildren: () =>
                    import('../ui/global-access-policies/global-access-policies.module').then(
                        (m) => m.GlobalAccessPoliciesModule
                    )
            },
            {
                path: '',
                loadChildren: () =>
                    import('../ui/component-access-policies/component-access-policies.module').then(
                        (m) => m.ComponentAccessPoliciesModule
                    )
            }
        ]
    }
];

@NgModule({
    imports: [RouterModule.forChild(routes)],
    exports: [RouterModule]
})
export class AccessPoliciesRoutingModule {}
