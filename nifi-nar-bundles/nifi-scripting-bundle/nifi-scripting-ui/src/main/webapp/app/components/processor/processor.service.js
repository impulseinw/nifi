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
 * Defines data service for a scripted processor.
 *
 * @argument {object} $http       HTTP service
 */
var ProcessorService = function ProcessorService($http) {

    return {
        'setProperties': setProperties,
        'getType': getType,
        'getDetails': getDetails
    };

    /**
     * Sets configuration for specified scripted processor.
     *
     * @argument {string} processorId       The processor ID
     * @argument {number} revisionId        The current processor revision ID
     * @argument {string} clientId        The current client ID
     * @argument {object} properties        New processor configuration
     */
    function setProperties(processorId, revisionId, clientId, properties) {
        var urlParams = 'processorId=' + processorId + '&revisionId=' + revisionId + '&clientId=' + clientId;
        return $http({
            url: "api/standard/processor/properties?" + urlParams,
            method: 'PUT',
            data: properties
        });
    }

    /**
     * Gets component type of specified scripted processor.
     *
     * @argument {string} id       The processor ID
     */
    function getType(id) {
        return $http({
            url: "api/standard/processor/details?processorId=" + id,
            method: 'GET',
            transformResponse: [function (data) {
                var obj = JSON.parse(data)
                var type = obj['type'];
                return type;
            }]
        });
    }

    /**
     * Gets configuration of specified scripted processor.
     *
     * @argument {string} id       The processor ID
     */
    function getDetails(id) {
        return $http({
            url: "api/standard/processor/details?processorId=" + id,
            method: 'GET'
        });
    }
};

ProcessorService.$inject = ['$http'];

angular.module('standardUI').service('ProcessorService', ProcessorService);