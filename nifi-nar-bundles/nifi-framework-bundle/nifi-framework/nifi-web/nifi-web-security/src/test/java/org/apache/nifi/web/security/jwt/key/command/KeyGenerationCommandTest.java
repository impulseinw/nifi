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
package org.apache.nifi.web.security.jwt.key.command;

import com.nimbusds.jose.JWSAlgorithm;
import org.apache.nifi.web.security.jwt.jws.JwsSignerContainer;
import org.apache.nifi.web.security.jwt.jws.SignerListener;
import org.apache.nifi.web.security.jwt.key.VerificationKeyListener;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.security.Key;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class KeyGenerationCommandTest {
    private static final String KEY_ALGORITHM = "RSA";

    private static final JWSAlgorithm JWS_ALGORITHM = JWSAlgorithm.PS512;

    @Mock
    private SignerListener signerListener;

    @Mock
    private VerificationKeyListener verificationKeyListener;

    @Captor
    private ArgumentCaptor<JwsSignerContainer> signerCaptor;

    @Captor
    private ArgumentCaptor<String> keyIdentifierCaptor;

    @Captor
    private ArgumentCaptor<Key> keyCaptor;

    private KeyGenerationCommand command;

    @Before
    public void setCommand() {
        command = new KeyGenerationCommand(signerListener, verificationKeyListener);
    }

    @Test
    public void testRun() {
        command.run();

        verify(signerListener).onSignerUpdated(signerCaptor.capture());
        final JwsSignerContainer signerContainer = signerCaptor.getValue();
        assertEquals(JWS_ALGORITHM, signerContainer.getJwsAlgorithm());

        verify(verificationKeyListener).onVerificationKeyGenerated(keyIdentifierCaptor.capture(), keyCaptor.capture());
        final Key key = keyCaptor.getValue();
        assertEquals(KEY_ALGORITHM, key.getAlgorithm());
    }
}
