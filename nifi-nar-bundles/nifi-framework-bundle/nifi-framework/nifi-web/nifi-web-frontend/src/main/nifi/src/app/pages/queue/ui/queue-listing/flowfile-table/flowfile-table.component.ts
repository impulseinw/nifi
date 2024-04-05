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
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { TextTip } from '../../../../../ui/common/tooltips/text-tip/text-tip.component';
import { BulletinsTip } from '../../../../../ui/common/tooltips/bulletins-tip/bulletins-tip.component';
import { ValidationErrorsTip } from '../../../../../ui/common/tooltips/validation-errors-tip/validation-errors-tip.component';
import { NiFiCommon } from '../../../../../service/nifi-common.service';

import { RouterLink } from '@angular/router';
import { FlowFileSummary, ListingRequest } from '../../../state/queue-listing';
import { CurrentUser } from '../../../../../state/current-user';
import { ErrorBanner } from '../../../../../ui/common/error-banner/error-banner.component';

@Component({
    selector: 'flowfile-table',
    standalone: true,
    templateUrl: './flowfile-table.component.html',
    imports: [MatTableModule, RouterLink, ErrorBanner],
    styleUrls: ['./flowfile-table.component.scss']
})
export class FlowFileTable {
    @Input() connectionLabel!: string;

    @Input() set listingRequest(listingRequest: ListingRequest) {
        if (listingRequest.flowFileSummaries) {
            this.dataSource.data = this.sortFlowFiles(listingRequest.flowFileSummaries);

            this.displayObjectCount = this.dataSource.data.length;
            this.queueSizeObjectCount = listingRequest.queueSize.objectCount;
            this.queueSizeByteCount = listingRequest.queueSize.byteCount;

            this.sourceRunning = listingRequest.sourceRunning;
            this.destinationRunning = listingRequest.destinationRunning;
        }
    }

    @Input() currentUser!: CurrentUser;
    @Input() contentViewerAvailable!: boolean;

    @Output() viewFlowFile: EventEmitter<FlowFileSummary> = new EventEmitter<FlowFileSummary>();
    @Output() downloadContent: EventEmitter<FlowFileSummary> = new EventEmitter<FlowFileSummary>();
    @Output() viewContent: EventEmitter<FlowFileSummary> = new EventEmitter<FlowFileSummary>();

    protected readonly TextTip = TextTip;
    protected readonly BulletinsTip = BulletinsTip;
    protected readonly ValidationErrorsTip = ValidationErrorsTip;

    // TODO - conditionally include the cluster column
    displayedColumns: string[] = [
        'moreDetails',
        'position',
        'flowFileUuid',
        'fileName',
        'fileSize',
        'queuedDuration',
        'lineageDuration',
        'penalized',
        'actions'
    ];
    dataSource: MatTableDataSource<FlowFileSummary> = new MatTableDataSource<FlowFileSummary>();
    selectedUuid: string | null = null;

    sourceRunning = false;
    destinationRunning = false;

    displayObjectCount = 0;
    queueSizeObjectCount = 0;
    queueSizeByteCount = 0;

    constructor(private nifiCommon: NiFiCommon) {}

    sortFlowFiles(summaries: FlowFileSummary[]): FlowFileSummary[] {
        const data: FlowFileSummary[] = summaries.slice();
        return data.sort((a: FlowFileSummary, b: FlowFileSummary) => {
            const aIsUndefined: boolean = typeof a.position === 'undefined';
            const bIsUndefined: boolean = typeof b.position === 'undefined';

            if (aIsUndefined && bIsUndefined) {
                return 0;
            } else if (aIsUndefined) {
                return 1;
            } else if (bIsUndefined) {
                return -1;
            }

            // @ts-ignore
            return this.nifiCommon.compareNumber(a.position, b.position);
        });
    }

    formatBytes(size: number): string {
        return this.nifiCommon.formatDataSize(size);
    }

    formatCount(count: number): string {
        return this.nifiCommon.formatInteger(count);
    }

    formatDuration(duration: number): string {
        return this.nifiCommon.formatDuration(duration);
    }

    select(summary: FlowFileSummary): void {
        this.selectedUuid = summary.uuid;
    }

    isSelected(summary: FlowFileSummary): boolean {
        if (this.selectedUuid) {
            return summary.uuid == this.selectedUuid;
        }
        return false;
    }

    viewFlowFileClicked(summary: FlowFileSummary): void {
        this.viewFlowFile.next(summary);
    }

    downloadContentClicked(summary: FlowFileSummary): void {
        this.downloadContent.next(summary);
    }

    viewContentClicked(summary: FlowFileSummary): void {
        this.viewContent.next(summary);
    }
}
