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

package org.apache.nifi.groups;

import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.service.ControllerServiceNode;

public interface FlowSynchronizationCallback {
    void onScheduledStateChange(ProcessorNode processor);

    void onScheduledStateChange(Port port);

    void onScheduledStateChange(ControllerServiceNode controllerService);

    void onScheduledStateChange(ReportingTaskNode reportingTask);

    FlowSynchronizationCallback EMPTY = new FlowSynchronizationCallback() {
        @Override
        public void onScheduledStateChange(ProcessorNode processor) {

        }

        @Override
        public void onScheduledStateChange(Port port) {

        }

        @Override
        public void onScheduledStateChange(ControllerServiceNode controllerService) {

        }

        @Override
        public void onScheduledStateChange(ReportingTaskNode reportingTask) {

        }
    };
}
