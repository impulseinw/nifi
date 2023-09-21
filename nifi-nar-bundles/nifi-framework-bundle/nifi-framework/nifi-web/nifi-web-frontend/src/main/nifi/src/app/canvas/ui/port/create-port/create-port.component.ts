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

import { Component, Inject } from '@angular/core';
import { FormBuilder, FormControl, FormGroup, Validators } from '@angular/forms';
import { Store } from '@ngrx/store';
import { CanvasState } from '../../../state';
import { MAT_DIALOG_DATA } from '@angular/material/dialog';
import { selectParentProcessGroupId } from '../../../state/flow/flow.selectors';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { createPort } from 'src/app/canvas/state/flow/flow.actions';
import { CreateComponent } from '../../../state/flow';
import { ComponentType } from '../../../state/shared';

@Component({
    selector: 'create-port',
    templateUrl: './create-port.component.html',
    styleUrls: ['./create-port.component.scss']
})
export class CreatePort {
    createPortForm: FormGroup;
    isRootProcessGroup: boolean = false;
    portTypeLabel: string;

    allowRemoteAccessOptions = [
        {
            text: 'Local connections',
            value: 'false',
            description: 'Receive FlowFiles from components in parent process groups'
        },
        {
            text: 'Remote connections (site-to-site)',
            value: 'true',
            description: 'Receive FlowFiles from remote process group (site-to-site)'
        }
    ];

    constructor(
        @Inject(MAT_DIALOG_DATA) private request: CreateComponent,
        private formBuilder: FormBuilder,
        private store: Store<CanvasState>
    ) {
        // set the port type name
        if (ComponentType.InputPort == this.request.type) {
            this.portTypeLabel = 'Input Port';
        } else {
            this.portTypeLabel = 'Output Port';
        }

        // build the form
        this.createPortForm = this.formBuilder.group({
            newPortName: new FormControl('', Validators.required),
            newPortAllowRemoteAccess: new FormControl(this.allowRemoteAccessOptions[0].value, Validators.required)
        });

        // listen for changes to the parent process group id
        this.store
            .select(selectParentProcessGroupId)
            .pipe(takeUntilDestroyed())
            .subscribe((parentProcessGroupId) => {
                this.isRootProcessGroup = parentProcessGroupId == null;
            });
    }

    createPort() {
        this.store.dispatch(
            createPort({
                request: {
                    ...this.request,
                    name: this.createPortForm.get('newPortName')?.value,
                    allowRemoteAccess: this.createPortForm.get('newPortAllowRemoteAccess')?.value
                }
            })
        );
    }
}