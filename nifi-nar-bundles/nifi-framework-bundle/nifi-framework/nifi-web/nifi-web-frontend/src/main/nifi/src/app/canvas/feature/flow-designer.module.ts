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

import { NgModule } from '@angular/core';
import { CommonModule, NgOptimizedImage } from '@angular/common';
import { FlowDesignerComponent } from './flow-designer.component';
import { FlowDesignerRoutingModule } from './flow-designer-routing.module';
import { HeaderModule } from '../ui/header/header.module';
import { FooterModule } from '../ui/footer/footer.module';
import { CanvasModule } from '../ui/canvas/canvas.module';
import { StoreModule } from '@ngrx/store';
import { EffectsModule } from '@ngrx/effects';
import { FlowEffects } from '../state/flow/flow.effects';
import { TransformEffects } from '../state/transform/transform.effects';
import { CreatePort } from '../ui/port/create-port/create-port.component';
import { EditPort } from '../ui/port/edit-port/edit-port.component';
import { MatDialogModule } from '@angular/material/dialog';
import { MatButtonModule } from '@angular/material/button';
import { ReactiveFormsModule } from '@angular/forms';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatCardModule } from '@angular/material/card';
import { Banner } from '../ui/common/banner/banner.component';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { ValidationErrorsTip } from '../ui/common/tooltips/validation-errors-tip/validation-errors-tip.component';
import { TextTip } from '../ui/common/tooltips/text-tip/text-tip.component';
import { BulletinsTip } from '../ui/common/tooltips/bulletins-tip/bulletins-tip.component';
import { VersionControlTip } from '../ui/common/tooltips/version-control-tip/version-control-tip.component';
import { UnorderedListTip } from '../ui/common/tooltips/unordered-list-tip/unordered-list-tip.component';
import { canvasFeatureKey, reducers } from '../state';
import { EditCanvasItemComponent } from '../ui/edit-canvas-item/edit-canvas-item.component';

@NgModule({
    declarations: [
        FlowDesignerComponent,
        Banner,
        EditCanvasItemComponent,
        CreatePort,
        EditPort,
        ValidationErrorsTip,
        TextTip,
        UnorderedListTip,
        BulletinsTip,
        VersionControlTip
    ],
    exports: [FlowDesignerComponent],
    imports: [
        CommonModule,
        HeaderModule,
        CanvasModule,
        FooterModule,
        FlowDesignerRoutingModule,
        StoreModule.forFeature(canvasFeatureKey, reducers),
        EffectsModule.forFeature(FlowEffects, TransformEffects),
        MatFormFieldModule,
        MatDialogModule,
        MatButtonModule,
        MatInputModule,
        MatSelectModule,
        MatCheckboxModule,
        MatCardModule,
        ReactiveFormsModule,
        MatTooltipModule,
        NgOptimizedImage
    ]
})
export class FlowDesignerModule {}