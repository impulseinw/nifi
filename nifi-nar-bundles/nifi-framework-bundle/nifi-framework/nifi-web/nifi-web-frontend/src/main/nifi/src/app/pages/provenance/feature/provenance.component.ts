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

import { Component, OnDestroy, OnInit } from '@angular/core';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../state';
import { startCurrentUserPolling, stopCurrentUserPolling } from '../../../state/current-user/current-user.actions';
import { loadProvenanceOptions } from '../state/provenance-event-listing/provenance-event-listing.actions';
import { loadAbout } from '../../../state/about/about.actions';

@Component({
    selector: 'provenance',
    templateUrl: './provenance.component.html',
    styleUrls: ['./provenance.component.scss']
})
export class Provenance implements OnInit, OnDestroy {
    constructor(private store: Store<NiFiState>) {}

    ngOnInit(): void {
        this.store.dispatch(startCurrentUserPolling());
        this.store.dispatch(loadProvenanceOptions());
        this.store.dispatch(loadAbout());
    }

    ngOnDestroy(): void {
        this.store.dispatch(stopCurrentUserPolling());
    }
}
