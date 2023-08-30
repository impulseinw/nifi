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

/*
  Canvas Positioning/Transforms
 */

export enum ComponentType {
  Processor = 'Processor',
  Funnel = 'Funnel'
}

export interface Dimension {
  width: number,
  height: number
}

export interface Position {
  x: number,
  y: number
}

export interface CanvasTransform {
  translate: Position,
  scale: number
}

/*
  Update Requests
 */

export interface UpdateComponentPosition {
  id: string,
  type: ComponentType,
  uri: string,
  revision: any,
  position: Position
}

export interface UpdateConnectionPosition {

}

export interface UpdateComponentPositionResponse {
  id: string,
  type: ComponentType,
  response: any;
}

/*
  Application State
 */

export interface Permissions {
  canRead: boolean;
  canWrite: boolean;
}

export interface ComponentEntity {
  id: string;
  position: Position;
  component: any;
}

export interface Flow {
  processGroups: ComponentEntity[];
  remoteProcessGroups: ComponentEntity[];
  processors: ComponentEntity[];
  inputPorts: ComponentEntity[];
  outputPorts: ComponentEntity[];
  connections: ComponentEntity[];
  labels: ComponentEntity[];
  funnels: ComponentEntity[];
}

export interface ProcessGroupFlow {
  id: string;
  uri: string;
  parentGroupId: string;
  breadcrumb: any;
  flow: Flow;
  lastRefreshed: string
}

export interface ProcessGroupFlowEntity {
  permissions: Permissions;
  processGroupFlow: ProcessGroupFlow;
}

export interface FlowState {
  flow: ProcessGroupFlowEntity;
  selection: string[];
  transition: boolean;
  error: string | null;
  status: 'pending' | 'loading' | 'error' | 'success';
}

export interface CanvasState {
  flowState: FlowState;
  transform: CanvasTransform;
}
