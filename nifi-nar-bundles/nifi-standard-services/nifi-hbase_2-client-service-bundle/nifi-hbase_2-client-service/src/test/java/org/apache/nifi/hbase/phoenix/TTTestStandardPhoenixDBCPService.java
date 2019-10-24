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
package org.apache.nifi.hbase.phoenix;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class TTTestStandardPhoenixDBCPService {
    private String dbUrl;
    @Before
    public void init() {
        this.dbUrl="jdbc:phoenix:knhdp31.field.hortonworks.com:2181:/hbase-unsecure";
    }

    @Test
    public void testService() throws InitializationException {
        try {
        //TestProcessor processor = new TestProcessor();
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final StandardPhoenixDBCPService service = new StandardPhoenixDBCPService();
        runner.addControllerService("test-good", service);
        runner.setProperty(service, "phoenix.schema.isNameSpaceMappingEnabled", "true");
        runner.setProperty(service, StandardPhoenixDBCPService.DATABASE_URL,dbUrl);
        // runner.setProperty(service, StandardPhoenixDBCPService.DB_USER,
        // "hdfs");
        runner.setProperty(service, StandardPhoenixDBCPService.VALIDATION_QUERY,
                "SELECT 1 FROM SYSTEM.CATALOG LIMIT 1");
        runner.enableControllerService(service);
        runner.assertValid(service);
        }catch(Exception ex) {
            ex.printStackTrace();
        }
    }
}