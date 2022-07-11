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
package org.apache.nifi.flow.resource;

/**
 * Describes an available resource might be fetched from the external source.
 */
public interface ExternalResourceDescriptor {

    /**
     * @return The location of the resource, where the format depends on the actual provider implementation.
     */
    String getLocation();

    /**
     * @return Returns the modification time of the original resource file using Unix timestamp format.
     */
    long getLastModified();

    /**
     * @return The path of the resource, where the format depends on the actual provider implementation.
     */
    String getPath();

    /**
     * @return Returns whether the resource is a directory.
     */
    boolean isDirectory();
}