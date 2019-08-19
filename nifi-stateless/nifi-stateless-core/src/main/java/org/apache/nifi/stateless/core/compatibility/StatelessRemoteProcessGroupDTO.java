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
package org.apache.nifi.stateless.core.compatibility;

import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;

import java.util.Set;
import java.util.stream.Collectors;

public class StatelessRemoteProcessGroupDTO implements StatelessRemoteProcessGroup {

    private final RemoteProcessGroupDTO remoteProcessGroup;

    public StatelessRemoteProcessGroupDTO(RemoteProcessGroupDTO remoteProcessGroup) {
        this.remoteProcessGroup = remoteProcessGroup;
    }

    @Override
    public String getId() {
        return this.remoteProcessGroup.getId();
    }

    @Override
    public String getCommunicationsTimeout() {
        return this.remoteProcessGroup.getCommunicationsTimeout();
    }

    @Override
    public String getTargetUris() {
        return this.remoteProcessGroup.getTargetUris();
    }

    @Override
    public String getTransportProtocol() {
        return this.remoteProcessGroup.getTransportProtocol();
    }

    @Override
    public Set<? extends StatelessRemoteProcessGroupPort> getInputPorts() {
        return this.remoteProcessGroup.getContents().getInputPorts()
                .stream()
                .map(StatelessRemoteProcessGroupPortDTO::new)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<? extends StatelessRemoteProcessGroupPort> getOutputPorts() {
        return this.remoteProcessGroup.getContents().getOutputPorts()
                .stream()
                .map(StatelessRemoteProcessGroupPortDTO::new)
                .collect(Collectors.toSet());
    }
}
