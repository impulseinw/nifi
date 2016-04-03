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
package org.apache.nifi.util.verifier;

import org.apache.nifi.util.MockFlowFile;

public class ContentEqual implements Condition {

    protected final String content;

    public ContentEqual(String content) {
        super();
        this.content = content;
    }

    @Override
    public boolean check(MockFlowFile flowFile) {
        try {
            flowFile.assertContentEquals(content);
        } catch (AssertionError cf) {
            return false;
        }
        return true;
    }

}
