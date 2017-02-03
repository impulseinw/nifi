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

/* global nf, define, module, require, exports */

(function (root, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['jquery',
                'nf.Client',
                'nf.Birdseye',
                'nf.Graph',
                'nf.Canvas',
                'nf.ErrorHandler'],
            function ($, client, birdseye, graph, canvas, errorHandler) {
                return (nf.ng.OutputPortComponent = factory($, client, birdseye, graph, canvas, errorHandler));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.ng.OutputPortComponent =
            factory(require('jquery'),
                require('nf.Client'),
                require('nf.Birdseye'),
                require('nf.Graph'),
                require('nf.Canvas'),
                require('nf.ErrorHandler')));
    } else {
        nf.ng.OutputPortComponent = factory(root.$,
            root.nf.Client,
            root.nf.Birdseye,
            root.nf.Graph,
            root.nf.Canvas,
            root.nf.ErrorHandler);
    }
}(this, function ($, client, birdseye, graph, canvas, errorHandler) {
    'use strict';

    return function (serviceProvider) {
        'use strict';

        /**
         * Create the input port and add to the graph.
         *
         * @argument {string} portName          The output port name.
         * @argument {object} pt                The point that the output port was dropped.
         */
        var createOutputPort = function (portName, pt) {
            var outputPortEntity = {
                'revision': client.getRevision({
                    'revision': {
                        'version': 0
                    }
                }),
                'component': {
                    'name': portName,
                    'position': {
                        'x': pt.x,
                        'y': pt.y
                    }
                }
            };

            // create a new processor of the defined type
            $.ajax({
                type: 'POST',
                url: serviceProvider.headerCtrl.toolboxCtrl.config.urls.api + '/process-groups/' + encodeURIComponent(canvas.getGroupId()) + '/output-ports',
                data: JSON.stringify(outputPortEntity),
                dataType: 'json',
                contentType: 'application/json'
            }).done(function (response) {
                // add the port to the graph
                graph.add({
                    'outputPorts': [response]
                }, {
                    'selectAll': true
                });

                // update component visibility
                canvas.View.updateVisibility();

                // update the birdseye
                birdseye.refresh();
            }).fail(errorHandler.handleAjaxError);
        };

        function OutputPortComponent() {

            this.icon = 'icon icon-port-out';

            this.hoverIcon = 'icon icon-port-out-add';

            /**
             * The output port component's modal.
             */
            this.modal = {

                /**
                 * Gets the modal element.
                 *
                 * @returns {*|jQuery|HTMLElement}
                 */
                getElement: function () {
                    return $('#new-port-dialog'); //Reuse the input port dialog....
                },

                /**
                 * Initialize the modal.
                 */
                init: function () {
                    //Reuse the input port dialog....
                },

                /**
                 * Updates the modal config.
                 *
                 * @param {string} name             The name of the property to update.
                 * @param {object|array} config     The config for the `name`.
                 */
                update: function (name, config) {
                    this.getElement().modal(name, config);
                },

                /**
                 * Show the modal.
                 */
                show: function () {
                    this.getElement().modal('show');
                },

                /**
                 * Hide the modal.
                 */
                hide: function () {
                    this.getElement().modal('hide');
                }
            };
        }

        OutputPortComponent.prototype = {
            constructor: OutputPortComponent,

            /**
             * Gets the component.
             *
             * @returns {*|jQuery|HTMLElement}
             */
            getElement: function () {
                return $('#port-out-component');
            },

            /**
             * Enable the component.
             */
            enabled: function () {
                this.getElement().attr('disabled', false);
            },

            /**
             * Disable the component.
             */
            disabled: function () {
                this.getElement().attr('disabled', true);
            },

            /**
             * Handler function for when component is dropped on the canvas.
             *
             * @argument {object} pt        The point that the component was dropped.
             */
            dropHandler: function (pt) {
                this.promptForOutputPortName(pt);
            },

            /**
             * The drag icon for the toolbox component.
             *
             * @param event
             * @returns {*|jQuery|HTMLElement}
             */
            dragIcon: function (event) {
                return $('<div class="icon icon-port-out-add"></div>');
            },

            /**
             * Prompts the user to enter the name for the output port.
             *
             * @argument {object} pt        The point that the output port was dropped.
             */
            promptForOutputPortName: function (pt) {
                var outputPortComponent = this;
                var addOutputPort = function () {
                    // get the name of the output port and clear the textfield
                    var portName = $('#new-port-name').val();

                    // hide the dialog
                    outputPortComponent.modal.hide();

                    // create the output port
                    createOutputPort(portName, pt);
                };

                this.modal.update('setButtonModel', [{
                    buttonText: 'Add',
                    color: {
                        base: '#728E9B',
                        hover: '#004849',
                        text: '#ffffff'
                    },
                    handler: {
                        click: addOutputPort
                    }
                },
                    {
                        buttonText: 'Cancel',
                        color: {
                            base: '#E3E8EB',
                            hover: '#C7D2D7',
                            text: '#004849'
                        },
                        handler: {
                            click: function () {
                                outputPortComponent.modal.hide();
                            }
                        }
                    }]);

                // update the port type
                $('#new-port-type').text('Output');

                // set the focus and show the dialog
                this.modal.show();

                // set up the focus and key handlers
                $('#new-port-name').focus().off('keyup').on('keyup', function (e) {
                    var code = e.keyCode ? e.keyCode : e.which;
                    if (code === $.ui.keyCode.ENTER) {
                        addOutputPort();
                    }
                });
            }
        }

        var outputPortComponent = new OutputPortComponent();
        return outputPortComponent;
    };
}));