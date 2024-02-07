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

import { Component, EventEmitter, Input, Output } from '@angular/core';
import { MatTableDataSource } from '@angular/material/table';
import { Sort } from '@angular/material/sort';
import { NiFiCommon } from '../../../../../service/nifi-common.service';
import { ParameterContextEntity } from '../../../state/parameter-context-listing';
import { FlowConfiguration } from '../../../../../state/flow-configuration';
import { CurrentUser } from '../../../../../state/current-user';
import { ParameterProviderConfigurationEntity } from '../../../../../state/shared';

@Component({
    selector: 'parameter-context-table',
    templateUrl: './parameter-context-table.component.html',
    styleUrls: ['./parameter-context-table.component.scss']
})
export class ParameterContextTable {
    @Input() initialSortColumn: 'name' | 'provider' | 'description' = 'name';
    @Input() initialSortDirection: 'asc' | 'desc' = 'asc';
    activeSort: Sort = {
        active: this.initialSortColumn,
        direction: this.initialSortDirection
    };

    @Input() set parameterContexts(parameterContextEntities: ParameterContextEntity[]) {
        this.dataSource.data = this.sortEntities(parameterContextEntities, this.activeSort);
    }

    @Input() selectedParameterContextId!: string;
    @Input() flowConfiguration!: FlowConfiguration;
    @Input() currentUser!: CurrentUser;

    @Output() selectParameterContext: EventEmitter<ParameterContextEntity> = new EventEmitter<ParameterContextEntity>();
    @Output() editParameterContext: EventEmitter<ParameterContextEntity> = new EventEmitter<ParameterContextEntity>();
    @Output() deleteParameterContext: EventEmitter<ParameterContextEntity> = new EventEmitter<ParameterContextEntity>();

    displayedColumns: string[] = ['moreDetails', 'name', 'provider', 'description', 'actions'];
    dataSource: MatTableDataSource<ParameterContextEntity> = new MatTableDataSource<ParameterContextEntity>();

    constructor(private nifiCommon: NiFiCommon) {}

    canRead(entity: ParameterContextEntity): boolean {
        return entity.permissions.canRead;
    }

    canWrite(entity: ParameterContextEntity): boolean {
        return entity.permissions.canWrite;
    }

    formatName(entity: ParameterContextEntity): string {
        return this.canRead(entity) ? entity.component.name : entity.id;
    }

    formatProvider(entity: ParameterContextEntity): string {
        const paramProvider = entity.component.parameterProviderConfiguration;
        if (!this.canRead(entity) || !paramProvider) {
            return '';
        }
        return `${paramProvider.component.parameterGroupName} from ${paramProvider.component.parameterProviderName}`;
    }

    formatDescription(entity: ParameterContextEntity): string {
        return this.canRead(entity) ? entity.component.description : '';
    }

    editClicked(entity: ParameterContextEntity, event: MouseEvent): void {
        event.stopPropagation();
        this.editParameterContext.next(entity);
    }

    canDelete(entity: ParameterContextEntity): boolean {
        const canModifyParameterContexts: boolean =
            this.currentUser.parameterContextPermissions.canRead &&
            this.currentUser.parameterContextPermissions.canWrite;
        return canModifyParameterContexts && this.canRead(entity) && this.canWrite(entity);
    }

    deleteClicked(entity: ParameterContextEntity, event: MouseEvent): void {
        event.stopPropagation();
        this.deleteParameterContext.next(entity);
    }

    canManageAccessPolicies(): boolean {
        return this.flowConfiguration.supportsManagedAuthorizer && this.currentUser.tenantsPermissions.canRead;
    }

    canGoToParameterProvider(entity: ParameterContextEntity): boolean {
        return !!entity.component.parameterProviderConfiguration;
    }

    getParameterProviderLink(entity: ParameterContextEntity): string[] {
        if (!entity.component.parameterProviderConfiguration) {
            return [];
        }
        return ['/settings', 'parameter-providers', entity.component.parameterProviderConfiguration.id];
    }

    getPolicyLink(entity: ParameterContextEntity): string[] {
        return ['/access-policies', 'read', 'component', 'parameter-contexts', entity.id];
    }

    select(entity: ParameterContextEntity): void {
        this.selectParameterContext.next(entity);
    }

    isSelected(entity: ParameterContextEntity): boolean {
        if (this.selectedParameterContextId) {
            return entity.id == this.selectedParameterContextId;
        }
        return false;
    }

    sortData(sort: Sort) {
        this.activeSort = sort;
        this.dataSource.data = this.sortEntities(this.dataSource.data, sort);
    }

    private sortEntities(data: ParameterContextEntity[], sort: Sort): ParameterContextEntity[] {
        if (!data) {
            return [];
        }
        return data.slice().sort((a, b) => {
            const isAsc = sort.direction === 'asc';
            let retVal = 0;

            switch (sort.active) {
                case 'name':
                    retVal = this.nifiCommon.compareString(this.formatName(a), this.formatName(b));
                    break;
                case 'provider':
                    retVal = this.nifiCommon.compareString(this.formatProvider(a), this.formatProvider(b));
                    break;
                case 'description':
                    retVal = this.nifiCommon.compareString(this.formatDescription(a), this.formatDescription(b));
                    break;
                default:
                    return 0;
            }
            return retVal * (isAsc ? 1 : -1);
        });
    }
}
