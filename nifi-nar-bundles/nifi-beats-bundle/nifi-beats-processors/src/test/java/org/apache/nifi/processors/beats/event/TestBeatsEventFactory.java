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

package org.apache.nifi.processors.beats.event;

import org.apache.nifi.processor.util.listen.event.EventFactory;
import org.apache.nifi.processor.util.listen.response.ChannelResponder;
import org.apache.nifi.processor.util.listen.response.socket.SocketChannelResponder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class TestBeatsEventFactory {

    @Test
    public void testCreateLumberJackEvent() {
        final String sender = "testsender1";
        final byte[] data = "this is a test line".getBytes();
        final int seqNumber = 1;
        final String fields = "{\"file\":\"test\"}";


        final Map<String,String> metadata = new HashMap<>();
        metadata.put(EventFactory.SENDER_KEY, sender);
        metadata.put(BeatsMetadata.SEQNUMBER_KEY, String.valueOf(seqNumber));

        final ChannelResponder responder = new SocketChannelResponder(null);

        final EventFactory<BeatsEvent> factory = new BeatsEventFactory();

        final BeatsEvent event = factory.create(data, metadata, responder);

        Assertions.assertEquals(sender, event.getSender());
        Assertions.assertEquals(seqNumber, event.getSeqNumber());
        Assertions.assertEquals(data, event.getData());
    }
}
