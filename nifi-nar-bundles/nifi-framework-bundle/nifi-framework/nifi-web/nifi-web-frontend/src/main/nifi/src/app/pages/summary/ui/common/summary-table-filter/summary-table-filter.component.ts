/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { AfterViewInit, Component, DestroyRef, EventEmitter, inject, Input, Output } from '@angular/core';
import { FormBuilder, FormGroup } from '@angular/forms';
import { debounceTime } from 'rxjs';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { NodeSearchResult } from '../../../../../state/cluster-summary';

export interface SummaryTableFilterColumn {
    key: string;
    label: string;
}

export interface SummaryTableFilterArgs {
    filterTerm: string;
    filterColumn: string;
    filterStatus?: string;
    primaryOnly?: boolean;
    clusterNode?: NodeSearchResult;
}

@Component({
    selector: 'summary-table-filter',
    templateUrl: './summary-table-filter.component.html',
    styleUrls: ['./summary-table-filter.component.scss']
})
export class SummaryTableFilter implements AfterViewInit {
    filterForm: FormGroup;
    private _filteredCount = 0;
    private _totalCount = 0;
    private _initialFilterColumn = 'name';
    private destroyRef: DestroyRef = inject(DestroyRef);

    showFilterMatchedLabel = false;
    allNodes: NodeSearchResult = {
        id: 'All',
        address: 'All Nodes'
    };

    private _clusterNodes: NodeSearchResult[] = [];
    private _selectedNode: NodeSearchResult | null = this.allNodes;

    @Input() filterableColumns: SummaryTableFilterColumn[] = [];
    @Input() includeStatusFilter = false;
    @Input() includePrimaryNodeOnlyFilter = false;

    @Input() set selectedNode(node: NodeSearchResult | null) {
        const n: NodeSearchResult = node ? (node.id !== 'All' ? node : this.allNodes) : this.allNodes;
        // find it in the available nodes
        const found = this._clusterNodes.find((node) => node.id === n.id);
        if (found) {
            this.filterForm.get('clusterNode')?.setValue(found);
            this._selectedNode = found;
        }
    }

    @Input() set clusterNodes(nodes: NodeSearchResult[] | null) {
        if (!nodes) {
            this._clusterNodes = [];
        } else {
            // test if the nodes have changed
            if (!this.areSame(this._clusterNodes, nodes)) {
                this._clusterNodes = [this.allNodes, ...nodes];
                if (this._selectedNode) {
                    this.selectedNode = this._selectedNode;
                }
            }
        }
    }
    get clusterNodes(): NodeSearchResult[] {
        return this._clusterNodes;
    }

    private areSame(a: NodeSearchResult[], b: NodeSearchResult[]) {
        if (a.length !== b.length) {
            return false;
        }

        const noMatch = a.filter((node) => b.findIndex((n) => n.id === node.id) < 0);
        return noMatch.length === 0 || (noMatch.length === 1 && noMatch[0].id === 'All');
    }

    private areEqual(a: NodeSearchResult, b: NodeSearchResult) {
        return a.id === b.id;
    }

    @Output() filterChanged: EventEmitter<SummaryTableFilterArgs> = new EventEmitter<SummaryTableFilterArgs>();

    @Input() set filterTerm(term: string) {
        this.filterForm.get('filterTerm')?.value(term);
    }

    @Input() set filterColumn(column: string) {
        this._initialFilterColumn = column;
        if (this.filterableColumns?.length > 0) {
            if (this.filterableColumns.findIndex((col) => col.key === column) >= 0) {
                this.filterForm.get('filterColumn')?.setValue(column);
            } else {
                this.filterForm.get('filterColumn')?.setValue(this.filterableColumns[0].key);
            }
        } else {
            this.filterForm.get('filterColumn')?.setValue(this._initialFilterColumn);
        }
    }

    @Input() set filterStatus(status: string) {
        if (this.includeStatusFilter) {
            this.filterForm.get('filterStatus')?.value(status);
        }
    }

    @Input() set filteredCount(count: number) {
        this._filteredCount = count;
    }

    get filteredCount(): number {
        return this._filteredCount;
    }

    @Input() set totalCount(total: number) {
        this._totalCount = total;
    }

    get totalCount(): number {
        return this._totalCount;
    }

    constructor(private formBuilder: FormBuilder) {
        this.filterForm = this.formBuilder.group({
            filterTerm: '',
            filterColumn: this._initialFilterColumn || 'name',
            filterStatus: 'All',
            primaryOnly: false,
            clusterNode: this.allNodes
        });
    }

    ngAfterViewInit(): void {
        this.filterForm
            .get('filterTerm')
            ?.valueChanges.pipe(debounceTime(500), takeUntilDestroyed(this.destroyRef))
            .subscribe((filterTerm: string) => {
                const filterColumn = this.filterForm.get('filterColumn')?.value;
                const filterStatus = this.filterForm.get('filterStatus')?.value;
                const primaryOnly = this.filterForm.get('primaryOnly')?.value;
                const clusterNode = this.filterForm.get('clusterNode')?.value;
                this.applyFilter(filterTerm, filterColumn, filterStatus, primaryOnly, clusterNode);
            });

        this.filterForm
            .get('filterColumn')
            ?.valueChanges.pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe((filterColumn: string) => {
                const filterTerm = this.filterForm.get('filterTerm')?.value;
                const filterStatus = this.filterForm.get('filterStatus')?.value;
                const primaryOnly = this.filterForm.get('primaryOnly')?.value;
                const clusterNode = this.filterForm.get('clusterNode')?.value;
                this.applyFilter(filterTerm, filterColumn, filterStatus, primaryOnly, clusterNode);
            });

        this.filterForm
            .get('filterStatus')
            ?.valueChanges.pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe((filterStatus: string) => {
                const filterTerm = this.filterForm.get('filterTerm')?.value;
                const filterColumn = this.filterForm.get('filterColumn')?.value;
                const primaryOnly = this.filterForm.get('primaryOnly')?.value;
                const clusterNode = this.filterForm.get('clusterNode')?.value;
                this.applyFilter(filterTerm, filterColumn, filterStatus, primaryOnly, clusterNode);
            });

        this.filterForm
            .get('primaryOnly')
            ?.valueChanges.pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe((primaryOnly: boolean) => {
                const filterTerm = this.filterForm.get('filterTerm')?.value;
                const filterColumn = this.filterForm.get('filterColumn')?.value;
                const filterStatus = this.filterForm.get('filterStatus')?.value;
                const clusterNode = this.filterForm.get('clusterNode')?.value;
                this.applyFilter(filterTerm, filterColumn, filterStatus, primaryOnly, clusterNode);
            });

        this.filterForm
            .get('clusterNode')
            ?.valueChanges.pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe((clusterNode) => {
                this._selectedNode = clusterNode;
                const filterTerm = this.filterForm.get('filterTerm')?.value;
                const filterColumn = this.filterForm.get('filterColumn')?.value;
                const filterStatus = this.filterForm.get('filterStatus')?.value;
                const primaryOnly = this.filterForm.get('primaryOnly')?.value;
                this.applyFilter(filterTerm, filterColumn, filterStatus, primaryOnly, clusterNode);
            });
    }

    applyFilter(
        filterTerm: string,
        filterColumn: string,
        filterStatus: string,
        primaryOnly: boolean,
        clusterNode: NodeSearchResult
    ) {
        this.filterChanged.next({
            filterColumn,
            filterStatus,
            filterTerm,
            primaryOnly,
            clusterNode
        });
        this.showFilterMatchedLabel =
            filterTerm?.length > 0 ||
            filterStatus !== 'All' ||
            primaryOnly ||
            (clusterNode ? clusterNode.id !== 'All' : false);
    }
}
