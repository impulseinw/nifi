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

import { Injectable } from '@angular/core';
import * as d3 from 'd3';
import { select, Store } from '@ngrx/store';
import { CanvasState, ComponentType, Position, UpdateComponent } from '../../state';
import { INITIAL_SCALE } from '../../state/transform/transform.reducer';
import { selectTransform } from '../../state/transform/transform.selectors';
import { CanvasUtils } from '../canvas-utils.service';
import { updatePositions } from '../../state/flow/flow.actions';
import { Client } from '../client.service';
import { selectConnections } from '../../state/flow/flow.selectors';

@Injectable({
    providedIn: 'root'
})
export class DraggableBehavior {
    private readonly drag: any;
    private snapEnabled: boolean = false;
    private snapAlignmentPixels: number = 8;

    private scale: number = INITIAL_SCALE;

    private connections: any[] = [];
    private updateConnectionRequestId: number = 0;

    constructor(
        private store: Store<CanvasState>,
        private client: Client,
        private canvasUtils: CanvasUtils
    ) {
        const self: DraggableBehavior = this;

        // subscribe to scale updates
        this.store.pipe(select(selectTransform)).subscribe((transform) => {
            this.scale = transform.scale;
        });
        this.store.pipe(select(selectConnections)).subscribe((connections) => {
            this.connections = connections;
        });

        // handle component drag events
        this.drag = d3
            .drag()
            .on('start', function (event) {
                // stop further propagation
                event.sourceEvent.stopPropagation();
            })
            .on('drag', function (event) {
                const dragSelection = d3.select('rect.drag-selection');

                // lazily create the drag selection box
                if (dragSelection.empty()) {
                    // get the current selection
                    const selection = d3.selectAll('g.component.selected');

                    // determine the appropriate bounding box
                    let minX: number | null = null;
                    let maxX: number | null = null;
                    let minY: number | null = null;
                    let maxY: number | null = null;

                    selection.each(function (d: any) {
                        if (minX === null || d.position.x < minX) {
                            minX = d.position.x;
                        }
                        if (minY === null || d.position.y < minY) {
                            minY = d.position.y;
                        }
                        var componentMaxX = d.position.x + d.dimensions.width;
                        var componentMaxY = d.position.y + d.dimensions.height;
                        if (maxX === null || componentMaxX > maxX) {
                            maxX = componentMaxX;
                        }
                        if (maxY === null || componentMaxY > maxY) {
                            maxY = componentMaxY;
                        }
                    });

                    // create a selection box for the move
                    d3.select('#canvas')
                        .append('rect')
                        .attr('rx', 6)
                        .attr('ry', 6)
                        .attr('x', minX)
                        .attr('y', minY)
                        .attr('class', 'drag-selection')
                        .attr('pointer-events', 'none')
                        // @ts-ignore
                        .attr('width', maxX - minX)
                        // @ts-ignore
                        .attr('height', maxY - minY)
                        .attr('stroke-width', function () {
                            return 1 / self.scale;
                        })
                        .attr('stroke-dasharray', function () {
                            return 4 / self.scale;
                        })
                        .datum({
                            original: {
                                x: minX,
                                y: minY
                            },
                            x: minX,
                            y: minY
                        });
                } else {
                    // update the position of the drag selection
                    // snap align the position unless the user is holding shift
                    self.snapEnabled = !event.sourceEvent.shiftKey;
                    dragSelection
                        .attr('x', function (d: any) {
                            d.x += event.dx;
                            return self.snapEnabled
                                ? Math.round(d.x / self.snapAlignmentPixels) * self.snapAlignmentPixels
                                : d.x;
                        })
                        .attr('y', function (d: any) {
                            d.y += event.dy;
                            return self.snapEnabled
                                ? Math.round(d.y / self.snapAlignmentPixels) * self.snapAlignmentPixels
                                : d.y;
                        });
                }
            })
            .on('end', function (event) {
                // stop further propagation
                event.sourceEvent.stopPropagation();

                // get the drag selection
                const dragSelection: any = d3.select('rect.drag-selection');

                // ensure we found a drag selection
                if (dragSelection.empty()) {
                    return;
                }

                // get the destination group if applicable... remove the drop flag if necessary to prevent
                // subsequent drop events from triggering prior to this move's completion
                const group: any = d3.select('g.drop').classed('drop', false);

                // either move or update the selections group as appropriate
                if (group.empty()) {
                    self.updateComponentsPosition(dragSelection);
                } else {
                    console.log('updateComponentsGroup');
                    // updateComponentsGroup(group);
                }

                // remove the drag selection
                dragSelection.remove();
            });
    }

    /**
     * Updates the positioning of all selected components.
     *
     * @param {selection} dragSelection The current drag selection
     */
    private updateComponentsPosition(dragSelection: any): void {
        const self: DraggableBehavior = this;
        const componentUpdates: Map<string, UpdateComponent> = new Map();
        const connectionUpdates: Map<string, UpdateComponent> = new Map();

        // determine the drag delta
        const dragData = dragSelection.datum();
        const delta = {
            x: dragData.x - dragData.original.x,
            y: dragData.y - dragData.original.y
        };

        // if the component didn't move, return
        if (delta.x === 0 && delta.y === 0) {
            return;
        }

        const selectedConnections: any = d3.selectAll('g.connection.selected');
        const selectedComponents: any = d3.selectAll('g.component.selected');

        // ensure every component is writable
        if (!this.canvasUtils.canModify(selectedConnections) || !this.canvasUtils.canModify(selectedComponents)) {
            // TODO
            // nfDialog.showOkDialog({
            //   headerText: 'Component Position',
            //   dialogContent: 'Must be authorized to modify every component selected.'
            // });
            return;
        }

        // go through each selected connection
        selectedConnections.each(function (d: any) {
            const connectionUpdate = self.updateConnectionPosition(d, delta);
            if (connectionUpdate !== null) {
                connectionUpdates.set(d.id, connectionUpdate);
            }
        });

        // go through each selected component
        selectedComponents.each(function (d: any) {
            // consider any self looping connections
            const componentConnections = self.connections.filter((connection) => {
                return (
                    self.canvasUtils.getConnectionSourceComponentId(connection) === d.id ||
                    self.canvasUtils.getConnectionDestinationComponentId(connection) === d.id
                );
            });

            componentConnections.forEach((componentConnection) => {
                if (!connectionUpdates.has(componentConnection.id)) {
                    const connectionUpdate = self.updateConnectionPosition(componentConnection, delta);
                    if (connectionUpdate !== null) {
                        connectionUpdates.set(componentConnection.id, connectionUpdate);
                    }
                }
            });

            // consider the component itself
            componentUpdates.set(d.id, self.updateComponentPosition(d, delta));
        });

        // dispatch the position updates
        this.store.dispatch(
            updatePositions({
                request: {
                    requestId: this.updateConnectionRequestId++,
                    componentUpdates: Array.from(componentUpdates.values()),
                    connectionUpdates: Array.from(connectionUpdates.values())
                }
            })
        );
    }

    // /**
    //  * Updates the parent group of all selected components.
    //  *
    //  * @param {selection} the destination group
    //  */
    // var updateComponentsGroup = function (group) {
    //   // get the selection and deselect the components being moved
    //   var selection = d3.selectAll('g.component.selected, g.connection.selected').classed('selected', false);
    //
    //   if (nfCanvasUtils.canModify(selection) === false) {
    //     nfDialog.showOkDialog({
    //       headerText: 'Component Position',
    //       dialogContent: 'Must be authorized to modify every component selected.'
    //     });
    //     return;
    //   }
    //   if (nfCanvasUtils.canModify(group) === false) {
    //     nfDialog.showOkDialog({
    //       headerText: 'Component Position',
    //       dialogContent: 'Not authorized to modify the destination group.'
    //     });
    //     return;
    //   }
    //
    //   // move the seleciton into the group
    //   nfCanvasUtils.moveComponents(selection, group);
    // };

    private updateComponentPosition(d: any, delta: Position): UpdateComponent {
        const newPosition = {
            x: this.snapEnabled
                ? Math.round((d.position.x + delta.x) / this.snapAlignmentPixels) * this.snapAlignmentPixels
                : d.position.x + delta.x,
            y: this.snapEnabled
                ? Math.round((d.position.y + delta.y) / this.snapAlignmentPixels) * this.snapAlignmentPixels
                : d.position.y + delta.y
        };

        return {
            id: d.id,
            type: d.type,
            uri: d.uri,
            payload: {
                revision: this.client.getRevision(d),
                component: {
                    id: d.id,
                    position: newPosition
                }
            },
            restoreOnFailure: {
                position: d.position
            }
        };
    }

    /**
     * Update the connection's position
     *
     * @param connection     The connection
     * @param delta The change in position
     * @returns {*}
     */
    private updateConnectionPosition(connection: any, delta: any): UpdateComponent | null {
        // only update if necessary
        if (connection.bends.length === 0) {
            return null;
        }

        // calculate the new bend points
        const newBends: Position[] = connection.bends.map((bend: any) => {
            return {
                x: bend.x + delta.x,
                y: bend.y + delta.y
            };
        });

        return {
            id: connection.id,
            type: ComponentType.Connection,
            uri: connection.uri,
            payload: {
                revision: this.client.getRevision(connection),
                component: {
                    id: connection.id,
                    bends: newBends
                }
            },
            restoreOnFailure: {
                bends: connection.bends
            }
        };
    }

    public activate(components: any): void {
        components.classed('moveable', true).call(this.drag);
    }

    public deactivate(components: any): void {
        components.classed('moveable', false).on('.drag', null);
    }
}
