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
package org.apache.nifi.irc;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processor.exception.ProcessException;
import org.kitteh.irc.client.library.Client;

import java.util.concurrent.atomic.AtomicBoolean;

@Tags({"example"})
@CapabilityDescription("Example Service API.")
public interface IRCClientService extends ControllerService {

    // public void execute()  throws ProcessException;

    public void joinChannel(String channel)  throws ProcessException;

    public Client getClient()  throws ProcessException;

    public AtomicBoolean getIsConnected()  throws ProcessException;

    public int setAndGetDelay(int i);
}
