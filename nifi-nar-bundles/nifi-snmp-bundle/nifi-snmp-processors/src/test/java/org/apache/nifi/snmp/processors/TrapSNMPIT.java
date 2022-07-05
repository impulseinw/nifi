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
package org.apache.nifi.snmp.processors;

import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.apache.nifi.snmp.configuration.V1TrapConfiguration;
import org.apache.nifi.snmp.configuration.V2TrapConfiguration;
import org.apache.nifi.snmp.helper.TrapConfigurationFactory;
import org.apache.nifi.snmp.helper.testrunners.SNMPTestRunnerFactory;
import org.apache.nifi.snmp.helper.testrunners.SNMPV1TestRunnerFactory;
import org.apache.nifi.snmp.helper.testrunners.SNMPV2cTestRunnerFactory;
import org.apache.nifi.snmp.utils.SNMPUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.snmp4j.mp.SnmpConstants;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TrapSNMPIT {

    protected static final String SYSTEM_DESCRIPTION_OID = "1.3.6.1.2.1.1.1.0";
    protected static final String SYSTEM_DESCRIPTION_OID_VALUE = "optionalTrapOidTestValue";

    @Test
    void testSendReceiveV1Trap() throws InterruptedException {
        final int listenPort = NetworkUtils.getAvailableUdpPort();

        final V1TrapConfiguration v1TrapConfiguration = TrapConfigurationFactory.getV1TrapConfiguration();
        final SNMPTestRunnerFactory v1TestRunnerFactory = new SNMPV1TestRunnerFactory();

        final TestRunner sendTrapTestRunner = v1TestRunnerFactory.createSnmpSendTrapTestRunner(listenPort, SYSTEM_DESCRIPTION_OID, SYSTEM_DESCRIPTION_OID_VALUE);
        final TestRunner listenTrapTestRunner = v1TestRunnerFactory.createSnmpListenTrapTestRunner(listenPort);

        listenTrapTestRunner.run(1, false);
        sendTrapTestRunner.run(1);

        Thread.sleep(50);

        final MockFlowFile successFF = listenTrapTestRunner.getFlowFilesForRelationship(GetSNMP.REL_SUCCESS).get(0);

        assertNotNull(successFF);
        assertEquals("Success", successFF.getAttribute(SNMPUtils.SNMP_PROP_PREFIX + "errorStatusText"));

        assertEquals(v1TrapConfiguration.getEnterpriseOid(), successFF.getAttribute(SNMPUtils.SNMP_PROP_PREFIX + "enterprise"));
        assertEquals(v1TrapConfiguration.getAgentAddress(), successFF.getAttribute(SNMPUtils.SNMP_PROP_PREFIX + "agentAddress"));
        assertEquals(String.valueOf(v1TrapConfiguration.getGenericTrapType()), successFF.getAttribute(SNMPUtils.SNMP_PROP_PREFIX + "genericTrapType"));
        assertEquals(String.valueOf(v1TrapConfiguration.getSpecificTrapType()), successFF.getAttribute(SNMPUtils.SNMP_PROP_PREFIX + "specificTrapType"));


        assertEquals(SYSTEM_DESCRIPTION_OID_VALUE, successFF.getAttribute(SNMPUtils.SNMP_PROP_PREFIX + SYSTEM_DESCRIPTION_OID
                + SNMPUtils.SNMP_PROP_DELIMITER + "4"));

        listenTrapTestRunner.shutdown();
    }

    @Test
    void testSendReceiveV2Trap() throws InterruptedException {
        final int listenPort = NetworkUtils.getAvailableUdpPort();

        final V2TrapConfiguration v2TrapConfiguration = TrapConfigurationFactory.getV2TrapConfiguration();
        final SNMPTestRunnerFactory v2cTestRunnerFactory = new SNMPV2cTestRunnerFactory();

        final TestRunner sendTrapTestRunner = v2cTestRunnerFactory.createSnmpSendTrapTestRunner(listenPort, SYSTEM_DESCRIPTION_OID, SYSTEM_DESCRIPTION_OID_VALUE);
        final TestRunner listenTrapTestRunner = v2cTestRunnerFactory.createSnmpListenTrapTestRunner(listenPort);

        listenTrapTestRunner.run(1, false);
        sendTrapTestRunner.run(1);

        Thread.sleep(50);

        final MockFlowFile successFF = listenTrapTestRunner.getFlowFilesForRelationship(GetSNMP.REL_SUCCESS).get(0);

        assertNotNull(successFF);
        assertEquals("Success", successFF.getAttribute(SNMPUtils.SNMP_PROP_PREFIX + "errorStatusText"));

        assertEquals(v2TrapConfiguration.getTrapOidValue(), successFF.getAttribute(SNMPUtils.SNMP_PROP_PREFIX + SnmpConstants.snmpTrapOID
                + SNMPUtils.SNMP_PROP_DELIMITER + "4"));

        assertEquals(SYSTEM_DESCRIPTION_OID_VALUE, successFF.getAttribute(SNMPUtils.SNMP_PROP_PREFIX + SYSTEM_DESCRIPTION_OID
                + SNMPUtils.SNMP_PROP_DELIMITER + "4"));

        listenTrapTestRunner.shutdown();
    }

    @Disabled("The ListenTrapSNMP and SendTrapSNMP processors use the same SecurityProtocols instance" +
            " and same USM (the USM is stored in a map by version), hence this case shall be manually tested." +
            " Check assertByVersion() to see what the trap payload must contain.")
    @Test
    void testReceiveV3Trap() {
    }
}
