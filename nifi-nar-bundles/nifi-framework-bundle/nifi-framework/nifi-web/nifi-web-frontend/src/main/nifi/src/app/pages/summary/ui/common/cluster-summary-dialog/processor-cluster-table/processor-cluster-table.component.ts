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

import { Component } from '@angular/core';
import { NodeProcessorStatusSnapshot } from '../../../../state';
import { MatSortModule, Sort } from '@angular/material/sort';
import { ComponentClusterTable } from '../component-cluster-table/component-cluster-table.component';
import { MatTableModule } from '@angular/material/table';
import { NgClass } from '@angular/common';

@Component({
    selector: 'processor-cluster-table',
    standalone: true,
    imports: [MatTableModule, MatSortModule, NgClass],
    templateUrl: './processor-cluster-table.component.html',
    styleUrl: './processor-cluster-table.component.scss'
})
export class ProcessorClusterTable extends ComponentClusterTable<NodeProcessorStatusSnapshot> {
    displayedColumns: string[] = ['node', 'runStatus', 'in', 'readWrite', 'out', 'tasks'];

    constructor() {
        super();
    }

    supportsMultiValuedSort(sort: Sort): boolean {
        switch (sort.active) {
            case 'in':
            case 'out':
            case 'readWrite':
            case 'tasks':
                return true;
            default:
                return false;
        }
    }

    sortEntities(data: NodeProcessorStatusSnapshot[], sort: Sort): NodeProcessorStatusSnapshot[] {
        if (!data) {
            return [];
        }
        return data.slice().sort((a, b) => {
            const isAsc = sort.direction === 'asc';
            switch (sort.active) {
                case 'node':
                    return this.compare(a.address, b.address, isAsc);
                case 'runStatus':
                    return this.compare(this.formatRunStatus(a), this.formatRunStatus(b), isAsc);
                case 'in':
                    if (this.multiSort.sortValueIndex === 0) {
                        return this.compare(a.statusSnapshot.flowFilesIn, b.statusSnapshot.flowFilesIn, isAsc);
                    } else {
                        return this.compare(a.statusSnapshot.bytesIn, b.statusSnapshot.bytesIn, isAsc);
                    }
                case 'out':
                    if (this.multiSort.sortValueIndex === 0) {
                        return this.compare(a.statusSnapshot.flowFilesOut, b.statusSnapshot.flowFilesOut, isAsc);
                    } else {
                        return this.compare(a.statusSnapshot.bytesOut, b.statusSnapshot.bytesOut, isAsc);
                    }
                case 'readWrite':
                    if (this.multiSort.sortValueIndex === 0) {
                        return this.compare(a.statusSnapshot.bytesRead, b.statusSnapshot.bytesRead, isAsc);
                    } else {
                        return this.compare(a.statusSnapshot.bytesWritten, b.statusSnapshot.bytesWritten, isAsc);
                    }
                case 'tasks':
                    if (this.multiSort.sortValueIndex === 0) {
                        return this.compare(a.statusSnapshot.taskCount, b.statusSnapshot.taskCount, isAsc);
                    } else {
                        return this.compare(
                            a.statusSnapshot.tasksDurationNanos,
                            b.statusSnapshot.tasksDurationNanos,
                            isAsc
                        );
                    }
                default:
                    return 0;
            }
        });
    }

    formatNode(processor: NodeProcessorStatusSnapshot): string {
        return `${processor.address}:${processor.apiPort}`;
    }

    formatRunStatus(processor: NodeProcessorStatusSnapshot): string {
        return processor.statusSnapshot.runStatus;
    }

    formatIn(processor: NodeProcessorStatusSnapshot): string {
        return processor.statusSnapshot.input;
    }

    formatOut(processor: NodeProcessorStatusSnapshot): string {
        return processor.statusSnapshot.output;
    }

    formatReadWrite(processor: NodeProcessorStatusSnapshot): string {
        return `${processor.statusSnapshot.read} | ${processor.statusSnapshot.written}`;
    }

    formatTasks(processor: NodeProcessorStatusSnapshot): string {
        return `${processor.statusSnapshot.tasks} | ${processor.statusSnapshot.tasksDuration}`;
    }

    getRunStatusIcon(processor: NodeProcessorStatusSnapshot): string {
        switch (processor.statusSnapshot.runStatus.toLowerCase()) {
            case 'running':
                return 'fa fa-play running';
            case 'stopped':
                return 'fa fa-stop stopped';
            case 'enabled':
                return 'fa fa-flash enabled';
            case 'disabled':
                return 'icon icon-enable-false disabled';
            case 'validating':
                return 'fa fa-spin fa-circle-notch validating';
            case 'invalid':
                return 'fa fa-warning invalid';
            default:
                return '';
        }
    }
}
