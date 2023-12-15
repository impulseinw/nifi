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

import { Component, DestroyRef, EventEmitter, inject, Input, OnDestroy, OnInit, Output } from '@angular/core';
import * as d3 from 'd3';
import { Lineage, LineageLink, LineageNode } from '../../../../state/lineage';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { ProvenanceEventRequest } from '../../../../state/provenance-event-listing';

@Component({
    selector: 'lineage',
    standalone: true,
    templateUrl: './lineage.component.html',
    styleUrls: ['./lineage.component.scss']
})
export class LineageComponent implements OnInit {
    private static readonly DEFAULT_NODE_SPACING: number = 100;
    private static readonly DEFAULT_LEVEL_DIFFERENCE: number = 120;

    private destroyRef = inject(DestroyRef);

    @Input() set lineage(lineage: Lineage) {
        if (lineage && lineage.finished) {
            this.addLineage(lineage.results.nodes, lineage.results.links);
        }
    }
    @Input() eventId: string | null = null;

    @Input() set reset(reset: EventEmitter<void>) {
        reset.pipe(takeUntilDestroyed(this.destroyRef)).subscribe(() => {
            this.nodeLookup.clear();
            this.linkLookup.clear();
            this.refresh();
        });
    }

    @Output() openEventDialog: EventEmitter<ProvenanceEventRequest> = new EventEmitter<ProvenanceEventRequest>();

    lineageElement: any;
    lineageContainerElement: any;
    private nodeLookup: Map<string, any> = new Map<string, any>();
    private linkLookup: Map<string, any> = new Map<string, any>();

    ngOnInit(): void {
        this.lineageElement = document.getElementById('lineage');

        // handle zoom behavior
        const lineageZoom: any = d3
            .zoom()
            .scaleExtent([0.2, 8])
            .on('zoom', function (event) {
                d3.select('g.lineage').attr('transform', function () {
                    return (
                        `translate(${event.transform.x}, ${event.transform.y}) scale(${event.transform.k})`
                    );
                });
            });

        // build the birdseye svg
        const svg = d3
            .select(this.lineageElement)
            .append('svg')
            .attr('width', '100%')
            .attr('height', '100%')
            .call(lineageZoom)
            .on('dblclick.zoom', null);

        svg.append('rect').attr('width', '100%').attr('height', '100%').attr('fill', '#f9fafb');

        svg.append('defs')
            .selectAll('marker')
            .data(['FLOWFILE', 'FLOWFILE-SELECTED', 'EVENT', 'EVENT-SELECTED'])
            .enter()
            .append('marker')
            .attr('id', function (d) {
                return d;
            })
            .attr('viewBox', '0 -3 6 6')
            .attr('refX', function (d) {
                if (d.indexOf('FLOWFILE') >= 0) {
                    return 16;
                } else {
                    return 11;
                }
            })
            .attr('refY', 0)
            .attr('markerWidth', 6)
            .attr('markerHeight', 6)
            .attr('orient', 'auto')
            .attr('fill', function (d) {
                if (d.indexOf('SELECTED') >= 0) {
                    return '#ba554a';
                } else {
                    return '#000000';
                }
            })
            .append('path')
            .attr('d', 'M0,-3 L6,0 L0,3');

        // group everything together
        this.lineageContainerElement = svg
            .append('g')
            .attr('transform', 'translate(0, 0) scale(1)')
            .attr('pointer-events', 'all')
            .attr('class', 'lineage');
    }

    private locateDescendants(nodeIds: string[], descendants: Set<string>, depth?: number): void {
        nodeIds.forEach((nodeId) => {
            const node: any = this.nodeLookup.get(nodeId);

            const children: string[] = [];
            node.outgoing.forEach((link: any) => {
                children.push(link.target.id);
                descendants.add(link.target.id);
            });

            if (depth == null) {
                this.locateDescendants(children, descendants);
            } else if (depth > 1) {
                this.locateDescendants(children, descendants, depth - 1);
            }
        });
    }

    private positionNodes(nodeIds: string[], depth: number, parents: string[], levelDifference: number): void {
        const { width } = this.lineageElement.getBoundingClientRect();

        const immediateSet: Set<string> = new Set(nodeIds);
        const childSet: Set<string> = new Set();
        const descendantSet: Set<string> = new Set();

        // locate children
        this.locateDescendants(nodeIds, childSet, 1);

        // locate all descendants (including children)
        this.locateDescendants(nodeIds, descendantSet);

        // push off processing a node until its deepest point
        // by removing any descendants from the immediate nodes.
        // in this case, a link is panning multiple levels
        descendantSet.forEach(function (d) {
            immediateSet.delete(d);
        });

        // convert the children to an array to ensure consistent
        // order when performing index of checks below
        const children: string[] = Array.from(childSet.values()).sort(d3.descending);

        // convert the immediate to allow for sorting below
        let immediate: string[] = Array.from(immediateSet.values());

        // attempt to identify fan in/out cases
        let nodesWithTwoParents: number = 0;
        immediate.forEach((nodeId) => {
            const node: any = this.nodeLookup.get(nodeId);

            // identify fanning cases
            if (node.incoming.length > 3) {
                levelDifference = LineageComponent.DEFAULT_LEVEL_DIFFERENCE;
            } else if (node.incoming.length >= 2) {
                nodesWithTwoParents++;
            }
        });

        // increase the level difference if more than two nodes have two or more parents
        if (nodesWithTwoParents > 2) {
            levelDifference = LineageComponent.DEFAULT_LEVEL_DIFFERENCE;
        }

        // attempt to sort the nodes to provide an optimum layout
        if (parents.length === 1) {
            immediate = immediate.sort((one: string, two: string) => {
                const oneNode: any = this.nodeLookup.get(one);
                const twoNode: any = this.nodeLookup.get(two);

                // try to order by children
                if (oneNode.outgoing.length > 0 && twoNode.outgoing.length > 0) {
                    const oneIndex: number = children.indexOf(oneNode.outgoing[0].target.id);
                    const twoIndex: number = children.indexOf(twoNode.outgoing[0].target.id);
                    if (oneIndex !== twoIndex) {
                        return oneIndex - twoIndex;
                    }
                }

                // try to order by parents
                if (oneNode.incoming.length > 0 && twoNode.incoming.length > 0) {
                    const oneIndex: number = oneNode.incoming[0].source.index;
                    const twoIndex: number = twoNode.incoming[0].source.index;
                    if (oneIndex !== twoIndex) {
                        return oneIndex - twoIndex;
                    }
                }

                // type of node
                if (oneNode.type !== twoNode.type) {
                    return oneNode.type > twoNode.type ? 1 : -1;
                }

                // type of event
                if (oneNode.eventType !== twoNode.eventType) {
                    return oneNode.eventType > twoNode.eventType ? 1 : -1;
                }

                // timestamp
                return oneNode.millis - twoNode.millis;
            });
        } else if (parents.length > 1) {
            immediate = immediate.sort((one: string, two: string) => {
                const oneNode: any = this.nodeLookup.get(one);
                const twoNode: any = this.nodeLookup.get(two);

                // try to order by parents
                if (oneNode.incoming.length > 0 && twoNode.incoming.length > 0) {
                    const oneIndex: number = oneNode.incoming[0].source.index;
                    const twoIndex: number = twoNode.incoming[0].source.index;
                    if (oneIndex !== twoIndex) {
                        return oneIndex - twoIndex;
                    }
                }

                // try to order by children
                if (oneNode.outgoing.length > 0 && twoNode.outgoing.length > 0) {
                    const oneIndex: number = children.indexOf(oneNode.outgoing[0].target.id);
                    const twoIndex: number = children.indexOf(twoNode.outgoing[0].target.id);
                    if (oneIndex !== twoIndex) {
                        return oneIndex - twoIndex;
                    }
                }

                // node type
                if (oneNode.type !== twoNode.type) {
                    return oneNode.type > twoNode.type ? 1 : -1;
                }

                // event type
                if (oneNode.eventType !== twoNode.eventType) {
                    return oneNode.eventType > twoNode.eventType ? 1 : -1;
                }

                // timestamp
                return oneNode.millis - twoNode.millis;
            });
        }

        let originX: number = width / 2;
        if (parents.length > 0) {
            const meanParentX: number | undefined = d3.mean(parents, (parentId: string) => {
                const parent = this.nodeLookup.get(parentId);
                return parent ? parent.x : undefined;
            });
            if (meanParentX) {
                originX = meanParentX;
            }
        }

        const depthWidth: number = (immediate.length - 1) * LineageComponent.DEFAULT_NODE_SPACING;
        immediate.forEach((nodeId: string, i: number) => {
            const node: any = this.nodeLookup.get(nodeId);

            // set the y position based on the depth
            node.y = levelDifference + depth - 25;

            // ensure the children won't position on top of one another
            // based on the number of parent nodes
            if (immediate.length <= parents.length) {
                if (node.incoming.length === 1) {
                    const parent: any = node.incoming[0].source;
                    if (parent.outgoing.length === 1) {
                        node.x = parent.x;
                        return;
                    }
                } else if (node.incoming.length > 1) {
                    const nodesOnPreviousLevel: any = node.incoming.filter((link: any) => {
                        return node.y - link.source.y <= LineageComponent.DEFAULT_LEVEL_DIFFERENCE;
                    });
                    node.x = d3.mean(nodesOnPreviousLevel, function (link: any) {
                        return link.source.x;
                    });
                    return;
                }
            }

            // evenly space the nodes under the origin
            node.x = i * LineageComponent.DEFAULT_NODE_SPACING + originX - depthWidth / 2;
        });

        // sort the immediate nodes after positioning by the x coordinate
        // so they can be shifted accordingly if necessary
        const sortedImmediate: string[] = immediate.slice().sort((one: string, two: string) => {
            const nodeOne: any = this.nodeLookup.get(one);
            const nodeTwo: any = this.nodeLookup.get(two);
            return nodeOne.x - nodeTwo.x;
        });

        // adjust the x positioning if necessary to avoid positioning on top
        // of one another, only need to consider the x coordinate since the
        // y coordinate will be the same for each node on this row
        for (let i = 0; i < sortedImmediate.length - 1; i++) {
            const first: any = this.nodeLookup.get(sortedImmediate[i]);
            const second: any = this.nodeLookup.get(sortedImmediate[i + 1]);
            const difference: number = second.x - first.x;

            if (difference < LineageComponent.DEFAULT_NODE_SPACING) {
                second.x += LineageComponent.DEFAULT_NODE_SPACING - difference;
            }
        }

        // if there are children to position
        if (children.length > 0) {
            let childLevelDifference: number = LineageComponent.DEFAULT_LEVEL_DIFFERENCE / 3;

            // resort the immediate values after each node has been positioned
            immediate = immediate.sort((one, two) => {
                const oneNode: any = this.nodeLookup.get(one);
                const twoNode: any = this.nodeLookup.get(two);
                return oneNode.x - twoNode.x;
            });

            // mark each nodes index so subsequent recursive calls can position children accordingly
            let nodesWithTwoChildren: number = 0;
            immediate.forEach((nodeId: string, i: number) => {
                const node: any = this.nodeLookup.get(nodeId);
                node.index = i;

                // precompute the next level difference since we have easy access to going here
                if (node.outgoing.length > 3) {
                    childLevelDifference = LineageComponent.DEFAULT_LEVEL_DIFFERENCE;
                } else if (node.outgoing.length >= 2) {
                    nodesWithTwoChildren++;
                }
            });

            // if there are at least two immediate nodes with two or more children, increase the level difference
            if (nodesWithTwoChildren > 2) {
                childLevelDifference = LineageComponent.DEFAULT_LEVEL_DIFFERENCE;
            }

            // position the children
            this.positionNodes(children, levelDifference + depth, immediate, childLevelDifference);
        }
    }

    private addLineage(nodes: LineageNode[], links: LineageLink[]): void {
        // add the new nodes
        nodes.forEach((node) => {
            if (this.nodeLookup.has(node.id)) {
                return;
            }

            // add values to the node to support rendering
            this.nodeLookup.set(node.id, {
                ...node,
                x: 0,
                y: 0,
                visible: true
            });
        });

        // add the new links
        links.forEach((link) => {
            const linkId: string = `${link.sourceId}-${link.targetId}`;

            // create the link object
            this.linkLookup.set(linkId, {
                id: linkId,
                source: this.nodeLookup.get(link.sourceId),
                target: this.nodeLookup.get(link.targetId),
                flowFileUuid: link.flowFileUuid,
                millis: link.millis,
                visible: true
            });
        });

        this.refresh();
    }

    private refresh(): void {
        // consider all nodes as starting points
        const startNodes: Set<string> = new Set(this.nodeLookup.keys());

        // go through the nodes to reset their outgoing links
        this.nodeLookup.forEach(function (node, id) {
            node.outgoing = [];
            node.incoming = [];

            // // ensure this event has an event time
            // if (nfCommon.isUndefined(minMillis) || minMillis > node.millis) {
            //     minMillis = node.millis;
            //     minTimestamp = node.timestamp;
            // }
            // if (nfCommon.isUndefined(maxMillis) || maxMillis < node.millis) {
            //     maxMillis = node.millis;
            // }
        });

        // go through the links in order to compute the new layout
        this.linkLookup.forEach(function (link, id) {
            // updating the nodes connections
            link.source.outgoing.push(link);
            link.target.incoming.push(link);

            // remove the target from being a potential starting node
            startNodes.delete(link.target.id);
        });

        // position the nodes
        this.positionNodes(Array.from(startNodes.values()), 1, [], 50);

        // // update the slider min/max/step values
        // var step = (maxMillis - minMillis) / config.sliderTickCount;
        // slider.slider('option', 'min', minMillis).slider('option', 'max', maxMillis).slider('option', 'step', step > 0 ? step : 1).slider('value', maxMillis);
        //
        // // populate the event timeline
        // $('#event-time').text(formatEventTime(maxMillis, provenanceTableCtrl));

        // update the layout
        this.update();
    }

    private formatEventTime(): void {}

    private renderFlowFile(flowfiles: any): void {
        const self: LineageComponent = this;

        flowfiles.classed('flowfile', true).on('mousedown', function (event: MouseEvent, d: any) {
            event.stopPropagation();
        });

        // node
        flowfiles
            .append('circle')
            .attr('r', 16)
            .attr('fill', '#fff')
            .attr('stroke', '#000')
            .attr('stroke-width', 1.0)
            .on('mouseover', function (event: MouseEvent, d: any) {
                self.lineageContainerElement
                    .selectAll('path.link')
                    .filter(function (linkDatum: any) {
                        return d.id === linkDatum.flowFileUuid;
                    })
                    .classed('selected', true)
                    .attr('marker-end', function (d: any) {
                        return `url(#${d.target.type}-SELECTED)`;
                    });
            })
            .on('mouseout', function (event: MouseEvent, d: any) {
                self.lineageContainerElement
                    .selectAll('path.link')
                    .filter(function (linkDatum: any) {
                        return d.id === linkDatum.flowFileUuid;
                    })
                    .classed('selected', false)
                    .attr('marker-end', function (d: any) {
                        return `url(#${d.target.type})`;
                    });
            });

        flowfiles
            .append('g')
            .attr('class', 'flowfile-icon')
            .attr('transform', function () {
                return 'translate(-9,-9)';
            })
            .append('text')
            .attr('font-family', 'flowfont')
            .attr('font-size', '18px')
            .attr('fill', '#ad9897')
            .attr('transform', function () {
                return 'translate(0,15)';
            })
            .on('mouseover', function (event: MouseEvent, d: any) {
                self.lineageContainerElement
                    .selectAll('path.link')
                    .filter(function (linkDatum: any) {
                        return d.id === linkDatum.flowFileUuid;
                    })
                    .classed('selected', true)
                    .attr('marker-end', function (d: any) {
                        return `url(#${d.target.type}-SELECTED)`;
                    });
            })
            .on('mouseout', function (event: MouseEvent, d: any) {
                self.lineageContainerElement
                    .selectAll('path.link')
                    .filter(function (linkDatum: any) {
                        return d.id === linkDatum.flowFileUuid;
                    })
                    .classed('selected', false)
                    .attr('marker-end', function (d: any) {
                        return `url(#${d.target.type})`;
                    });
            })
            .text(function () {
                return '\ue808';
            });
    }

    private renderEvent(events: any): void {
        const self: LineageComponent = this;

        events
            .on('contextmenu', function (event: MouseEvent, d: any) {
                // select the current node for a visible cue
                d3.select(`#event-node-${d.id}`).classed('context', true);

                // show the context menu
                // showContextMenu(d, provenanceTableCtrl);
            })
            .on('mousedown', function (event: MouseEvent) {
                event.stopPropagation();
            })
            .on('dblclick', function (event: MouseEvent, d: any) {
                // show the event details

                // TODO - cluster node id
                self.openEventDialog.next({
                    id: d.id
                });
            });

        events
            .classed('event', true)
            // join node to its label
            .append('rect')
            .attr('x', 0)
            .attr('y', -8)
            .attr('height', 16)
            .attr('width', 1)
            .attr('opacity', 0)
            .attr('id', function (d: any) {
                return `event-filler-${d.id}`;
            });

        events
            .append('circle')
            .classed('selected', function (d: any) {
                return d.id === self.eventId;
            })
            .attr('r', 8)
            .attr('fill', '#aabbc3')
            .attr('stroke', '#000')
            .attr('stroke-width', 1.0)
            .attr('id', function (d: any) {
                return `event-node-${d.id}`;
            });

        events
            .append('text')
            .attr('id', function (d: any) {
                return `event-text-${d.id}`;
            })
            .attr('class', 'event-type')
            .classed('expand-parents', function (d: any) {
                return d.eventType === 'SPAWN';
            })
            .classed('expand-children', function (d: any) {
                return d.eventType === 'SPAWN';
            })
            .each(function (this: any, d: any) {
                const label: any = d3.select(this);
                if (d.eventType === 'CONTENT_MODIFIED' || d.eventType === 'ATTRIBUTES_MODIFIED') {
                    const lines: string[] = [];
                    if (d.eventType === 'CONTENT_MODIFIED') {
                        lines.push('CONTENT');
                    } else {
                        lines.push('ATTRIBUTES');
                    }
                    lines.push('MODIFIED');

                    // append each line
                    lines.forEach((line) => {
                        label
                            .append('tspan')
                            .attr('x', '0')
                            .attr('dy', '1.2em')
                            .text(function () {
                                return line;
                            });
                    });
                    label.attr('transform', 'translate(10,-14)');
                } else {
                    label.text(d.eventType).attr('x', 10).attr('y', 4);
                }
            });
    }

    private update(): void {
        const { width } = this.lineageElement.getBoundingClientRect();

        let nodes: any = this.lineageContainerElement.selectAll('g.node');
        let links: any = this.lineageContainerElement.selectAll('path.link');

        // update the node data
        nodes = nodes.data(Array.from(this.nodeLookup.values()), function (d: any) {
            return d.id;
        });

        // exit
        nodes
            .exit()
            .transition()
            .delay(200)
            .duration(400)
            .attr('transform', function (d: any) {
                if (d.incoming.length === 0) {
                    return `translate(${width / 2},50)`;
                } else {
                    return `translate(${d.incoming[0].source.x},${d.incoming[0].source.y})`;
                }
            })
            .style('opacity', 0)
            .remove();

        // enter
        const nodesEntered: any = nodes
            .enter()
            .append('g')
            .attr('id', function (d: any) {
                return `lineage-group-${d.id}`;
            })
            .classed('node', true)
            .attr('transform', function (d: any) {
                if (d.incoming.length === 0) {
                    return `translate(${width / 2},50)`;
                } else {
                    return `translate(${d.incoming[0].source.x},${d.incoming[0].source.y})`;
                }
            })
            .style('opacity', 0);

        // treat flowfiles and events differently
        this.renderFlowFile(
            nodesEntered.filter(function (d: any) {
                return d.type === 'FLOWFILE';
            })
        );
        this.renderEvent(
            nodesEntered.filter(function (d: any) {
                return d.type === 'EVENT';
            })
        );

        // merge
        nodes = nodes.merge(nodesEntered);

        // update the nodes
        nodes
            .transition()
            .duration(400)
            .attr('transform', function (d: any) {
                return `translate(${d.x}, ${d.y})`;
            })
            .style('opacity', 1);

        // update the link data
        links = links.data(Array.from(this.linkLookup.values()), function (d: any) {
            return d.id;
        });

        // exit
        links
            .exit()
            .attr('marker-end', '')
            .transition()
            .duration(400)
            .attr('d', function (d: any) {
                return `M${d.source.x},${d.source.y}L${d.source.x},${d.source.y}`;
            })
            .style('opacity', 0)
            .remove();

        // add new links
        const linksEntered = links
            .enter()
            .insert('path', '.node')
            .attr('class', 'link')
            .attr('stroke-width', 1.5)
            .attr('stroke', '#000')
            .attr('fill', 'none')
            .attr('d', function (d: any) {
                return `M${d.source.x},${d.source.y}L${d.source.x},${d.source.y}`;
            })
            .style('opacity', 0);

        // merge
        links = links.merge(linksEntered).attr('marker-end', '');

        // update the links
        links
            .transition()
            .delay(200)
            .duration(400)
            .attr('marker-end', function (d: any) {
                return `url(#${d.target.type})`;
            })
            .attr('d', function (d: any) {
                return `M${d.source.x},${d.source.y}L${d.target.x},${d.target.y}`;
            })
            .style('opacity', 1);
    }
}
