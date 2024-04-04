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

import { DestroyRef, inject, Injectable } from '@angular/core';
import { Store } from '@ngrx/store';
import { ClusterSummaryState } from '../state/cluster-summary';
import { selectDisconnectionAcknowledged } from '../state/cluster-summary/cluster-summary.selectors';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

@Injectable({
    providedIn: 'root'
})
export class ClusterConnectionService {
    private destroyRef = inject(DestroyRef);
    private disconnectionAcknowledged = false;

    constructor(private store: Store<ClusterSummaryState>) {
        this.store
            .select(selectDisconnectionAcknowledged)
            .pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe((disconnectionAcknowledged) => {
                this.disconnectionAcknowledged = disconnectionAcknowledged;
            });
    }

    isDisconnectionAcknowledged(): boolean {
        return this.disconnectionAcknowledged;
    }
}
