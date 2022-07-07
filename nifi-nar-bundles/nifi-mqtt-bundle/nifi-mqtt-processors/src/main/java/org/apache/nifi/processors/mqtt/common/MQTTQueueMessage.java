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

package org.apache.nifi.processors.mqtt.common;

public class MQTTQueueMessage {
    private final String topic;

    private final byte[] payload;
    private final int qos;
    private final boolean retained;
    private final boolean duplicate;

    public MQTTQueueMessage(String topic, NifiMqttMessage message) {
        this.topic = topic;
        payload = message.getPayload();
        qos = message.getQos();
        retained = message.isRetained();
        duplicate = message.isDuplicate();
    }

    public String getTopic() {
        return topic;
    }

    public byte[] getPayload() {
        return payload;
    }

    public int getQos() {
        return qos;
    }

    public boolean isRetained() {
        return retained;
    }

    public boolean isDuplicate() {
        return duplicate;
    }
}
