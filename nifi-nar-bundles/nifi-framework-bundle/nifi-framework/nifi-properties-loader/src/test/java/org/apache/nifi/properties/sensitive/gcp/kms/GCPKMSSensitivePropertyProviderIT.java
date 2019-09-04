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
package org.apache.nifi.properties.sensitive.gcp.kms;

import com.google.cloud.kms.v1.CryptoKey;
import com.google.cloud.kms.v1.CryptoKeyVersion;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.cloud.kms.v1.KeyRing;
import com.google.cloud.kms.v1.KeyRingName;
import com.google.cloud.kms.v1.LocationName;
import org.apache.nifi.properties.sensitive.AbstractSensitivePropertyProviderTest;
import org.apache.nifi.properties.sensitive.SensitivePropertyConfigurationException;
import org.apache.nifi.properties.sensitive.SensitivePropertyProvider;
import org.apache.nifi.security.util.CipherUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Tests the GCP KMS Sensitive Property Provider.
 *
 * These tests rely on an environment with GCP credentials stored as environment variables, and require that those credentials support
 * creating and using KMS keys.
 *
 * These tests create a key ring with a random name, and all of our tests use just that ring.  This ensures we cannot destroy and existing user key.

 Configure your test environment like:

 $ export GOOGLE_APPLICATION_CREDENTIALS=/var/run/nifi-gcp-it.json
 $ export GOOGLE_PROJECT=nifi-gcp-unit-tests-project
 */
public class GCPKMSSensitivePropertyProviderIT extends AbstractSensitivePropertyProviderTest {
    private static final Logger logger = LoggerFactory.getLogger(GCPKMSSensitivePropertyProviderIT.class);
    private static String keyId;

    private static final String locationId = "us-west2";
    private static final String keyRingId = "key-ring-" + CipherUtils.getRandomHex(10);
    private static final String cryptoKeyId = "key-name-" + CipherUtils.getRandomHex(10);

    private static KeyRing keyRing;
    private static CryptoKey createdKey;
    private static String projectId;

    @BeforeClass
    public static void createKeyRingAndKey() {
        Assume.assumeTrue(System.getenv("GOOGLE_APPLICATION_CREDENTIALS") != null);
        Assume.assumeTrue(System.getenv("GOOGLE_PROJECT") != null);

        projectId = System.getenv("GOOGLE_PROJECT");
        try (KeyManagementServiceClient client = KeyManagementServiceClient.create()) {
            String parent = LocationName.format(projectId, locationId);
            keyRing = client.createKeyRing(parent, keyRingId, KeyRing.newBuilder().build());
            CryptoKey cryptoKey = CryptoKey.newBuilder().setPurpose(CryptoKey.CryptoKeyPurpose.ENCRYPT_DECRYPT).build();
            createdKey = client.createCryptoKey(KeyRingName.format(projectId, locationId, keyRingId), cryptoKeyId, cryptoKey);
            keyId = "gcp/kms/" + createdKey.getName();
            logger.info("Created GCP key: {}", createdKey.getName());

        } catch (IOException e) {
            logger.warn("Error during create key ring and key: " + e);
        }
    }

    @AfterClass
    public static void deleteKeyRingAndKey() {
        // See GCP docs "Note: To prevent resource name collisions, key ring and key resources CANNOT be deleted."
        if (createdKey == null || keyRing == null) return;
        try (KeyManagementServiceClient client = KeyManagementServiceClient.create()) {
            for (CryptoKeyVersion version : client.listCryptoKeyVersions(createdKey.getName()).iterateAll()) {
                CryptoKeyVersion destroyedKey = client.destroyCryptoKeyVersion(version.getName());
                logger.info("Destroyed GCP key: {}", destroyedKey.getName());
            }
        } catch (IOException e) {
            logger.warn("Error during delete key ring and key: " + e);
        }
    }

    /**
     * These tests show that the provider enforces valid key formats.
     */
    @Test
    public void testShouldThrowExceptionsWithBadKeys() throws Exception {
        try {
            new GCPKMSSensitivePropertyProvider("");
        } catch (final SensitivePropertyConfigurationException e) {
            Assert.assertTrue(Pattern.compile("The key cannot be empty").matcher(e.getMessage()).matches());
        }

        try {
            new GCPKMSSensitivePropertyProvider("this is an invalid key and will not work");
        } catch (final SensitivePropertyConfigurationException e) {
            Assert.assertTrue(Pattern.compile("Invalid GCP key").matcher(e.getMessage()).matches());
        }
    }

    /**
     * These tests show that the provider can encrypt and decrypt values as expected.
     */
    @Test
    public void testProtectAndUnprotect() {
        SensitivePropertyProvider sensitivePropertyProvider = new GCPKMSSensitivePropertyProvider(keyId);
        int plainSize = CipherUtils.getRandomInt(32, 256);
        checkProviderCanProtectAndUnprotectValue(sensitivePropertyProvider, plainSize);
        logger.info("GCP SPP protected and unprotected string of " + plainSize + " bytes using material: " + keyId);
    }

    /**
     * These tests show that the provider cannot encrypt empty values.
     */
    @Test
    public void testShouldHandleProtectEmptyValue() throws Exception {
        SensitivePropertyProvider sensitivePropertyProvider = new GCPKMSSensitivePropertyProvider(keyId);
        checkProviderProtectDoesNotAllowBlankValues(sensitivePropertyProvider);
    }

    /**
     * These tests show that the provider cannot decrypt invalid ciphertext.
     */
    @Test
    public void testShouldUnprotectValue() throws Exception {
        SensitivePropertyProvider sensitivePropertyProvider = new GCPKMSSensitivePropertyProvider(keyId);
        checkProviderUnprotectDoesNotAllowInvalidBase64Values(sensitivePropertyProvider);
    }

    /**
     * These tests show that the provider cannot decrypt text encoded but not encrypted.
     */
    @Test
    public void testShouldThrowExceptionWithValidBase64EncodedTextInvalidCipherText() throws Exception {
        SensitivePropertyProvider sensitivePropertyProvider = new GCPKMSSensitivePropertyProvider(keyId);
        checkProviderUnprotectDoesNotAllowValidBase64InvalidCipherTextValues(sensitivePropertyProvider);
    }

    /**
     * These tests show we can use an AWS KMS key to encrypt/decrypt property values.
     */
    @Test
    public void testShouldProtectAndUnprotectProperties() throws Exception {
        SensitivePropertyProvider sensitivePropertyProvider = new GCPKMSSensitivePropertyProvider(keyId);
        checkProviderCanProtectAndUnprotectProperties(sensitivePropertyProvider);
    }
}
