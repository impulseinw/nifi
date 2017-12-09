/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

/**
 * Creates a scripted component service 'implementation'.
 *
 * @argument {object} $state       The current router state
 * @argument {object} $injector       Dependency retriever
 */
var ScriptedComponentFactory = function ScriptedComponentFactory($state, $injector) {

    // chose a service based on the given entity type
    switch ($state.params.entityType) {
        case 'processor':
            return $injector.get('ProcessorService');
        case 'controllerService':
            return $injector.get('ControllerServiceService');
        default:
            return $injector.get('ReportingTaskService');
    }
};

ScriptedComponentFactory.$inject = ['$state', '$injector'];

angular.module('standardUI').factory('ScriptedComponentFactory', ScriptedComponentFactory);