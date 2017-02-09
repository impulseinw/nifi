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

package org.apache.nifi.toolkit.tls.util;

import org.apache.commons.lang3.StringUtils;
import org.bouncycastle.asn1.pkcs.PKCSObjectIdentifiers;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.ExtensionsGenerator;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaMiscPEMGenerator;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequest;
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequestBuilder;
import org.bouncycastle.util.io.pem.PemWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;

public class TlsHelper {
    private static final Logger logger = LoggerFactory.getLogger(TlsHelper.class);
    private static final int DEFAULT_MAX_ALLOWED_KEY_LENGTH = 128;
    public static final String JCE_URL = "http://www.oracle.com/technetwork/java/javase/downloads/jce8-download-2133166.html";
    public static final String ILLEGAL_KEY_SIZE = "illegal key size";
    private static boolean isUnlimitedStrengthCryptographyEnabled;

    // Evaluate an unlimited strength algorithm to determine if we support the capability we have on the system
    static {
        try {
            isUnlimitedStrengthCryptographyEnabled = (Cipher.getMaxAllowedKeyLength("AES") > DEFAULT_MAX_ALLOWED_KEY_LENGTH);
        } catch (NoSuchAlgorithmException e) {
            // if there are issues with this, we default back to the value established
            isUnlimitedStrengthCryptographyEnabled = false;
        }
    }

    private static void logTruncationWarning(File file) {
        String fileToString = file.toString();
        String fileName = file.getName();
        logger.warn("**********************************************************************************");
        logger.warn("                                    WARNING!!!!");
        logger.warn("**********************************************************************************");
        logger.warn("Unlimited JCE Policy is not installed which means we cannot utilize a");
        logger.warn("PKCS12 password longer than 7 characters.");
        logger.warn("Autogenerated password has been reduced to 7 characters.");
        logger.warn("");
        logger.warn("Please strongly consider installing Unlimited JCE Policy at");
        logger.warn(JCE_URL);
        logger.warn("");
        logger.warn("Another alternative is to add a stronger password with the openssl tool to the");
        logger.warn("resulting client certificate: " + fileToString);
        logger.warn("");
        logger.warn("openssl pkcs12 -in '" + fileToString + "' -out '/tmp/" + fileName + "'");
        logger.warn("openssl pkcs12 -export -in '/tmp/"  + fileName + "' -out '" + fileToString + "'");
        logger.warn("rm -f '/tmp/" + fileName + "'");
        logger.warn("");
        logger.warn("**********************************************************************************");

    }

    private TlsHelper() {

    }

    public static boolean isUnlimitedStrengthCryptographyEnabled() {
        return isUnlimitedStrengthCryptographyEnabled;
    }

    public static String writeKeyStore(KeyStore keyStore, OutputStreamFactory outputStreamFactory, File file, String password, boolean generatedPassword) throws IOException, GeneralSecurityException {
        try (OutputStream fileOutputStream = outputStreamFactory.create(file)) {
            keyStore.store(fileOutputStream, password.toCharArray());
        } catch (IOException e) {
            if (e.getMessage().toLowerCase().contains(ILLEGAL_KEY_SIZE) && !isUnlimitedStrengthCryptographyEnabled()) {
                if (generatedPassword) {
                    file.delete();
                    String truncatedPassword = password.substring(0, 7);
                    try (OutputStream fileOutputStream = outputStreamFactory.create(file)) {
                        keyStore.store(fileOutputStream, truncatedPassword.toCharArray());
                    }
                    logTruncationWarning(file);
                    return truncatedPassword;
                } else {
                    throw new GeneralSecurityException("Specified password for " + file + " too long to work without unlimited JCE policy installed."
                            + System.lineSeparator() + "Please see " + JCE_URL);
                }
            } else {
                throw e;
            }
        }
        return password;
    }

    private static KeyPairGenerator createKeyPairGenerator(String algorithm, int keySize) throws NoSuchAlgorithmException {
        KeyPairGenerator instance = KeyPairGenerator.getInstance(algorithm);
        instance.initialize(keySize);
        return instance;
    }

    public static byte[] calculateHMac(String token, PublicKey publicKey) throws GeneralSecurityException {
        SecretKeySpec keySpec = new SecretKeySpec(token.getBytes(StandardCharsets.UTF_8), "RAW");
        Mac mac = Mac.getInstance("Hmac-SHA256", BouncyCastleProvider.PROVIDER_NAME);
        mac.init(keySpec);
        return mac.doFinal(getKeyIdentifier(publicKey));
    }

    public static byte[] getKeyIdentifier(PublicKey publicKey) throws NoSuchAlgorithmException {
        return new JcaX509ExtensionUtils().createSubjectKeyIdentifier(publicKey).getKeyIdentifier();
    }

    public static String pemEncodeJcaObject(Object object) throws IOException {
        StringWriter writer = new StringWriter();
        try (PemWriter pemWriter = new PemWriter(writer)) {
            pemWriter.writeObject(new JcaMiscPEMGenerator(object));
        }
        return writer.toString();
    }

    public static JcaPKCS10CertificationRequest parseCsr(String pemEncodedCsr) throws IOException {
        try (PEMParser pemParser = new PEMParser(new StringReader(pemEncodedCsr))) {
            Object o = pemParser.readObject();
            if (!PKCS10CertificationRequest.class.isInstance(o)) {
                throw new IOException("Expecting instance of " + PKCS10CertificationRequest.class + " but got " + o);
            }
            return new JcaPKCS10CertificationRequest((PKCS10CertificationRequest) o);
        }
    }

    public static X509Certificate parseCertificate(Reader pemEncodedCertificate) throws IOException, CertificateException {
        return new JcaX509CertificateConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME).getCertificate(parsePem(X509CertificateHolder.class, pemEncodedCertificate));
    }

    public static KeyPair parseKeyPair(Reader pemEncodedKeyPair) throws IOException {
        return new JcaPEMKeyConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME).getKeyPair(parsePem(PEMKeyPair.class, pemEncodedKeyPair));
    }

    public static <T> T parsePem(Class<T> clazz, Reader pemReader) throws IOException {
        try (PEMParser pemParser = new PEMParser(pemReader)) {
            Object object = pemParser.readObject();
            if (!clazz.isInstance(object)) {
                throw new IOException("Expected " + clazz);
            }
            return (T) object;
        }
    }

    public static KeyPair generateKeyPair(String algorithm, int keySize) throws NoSuchAlgorithmException {
        return createKeyPairGenerator(algorithm, keySize).generateKeyPair();
    }

    public static JcaPKCS10CertificationRequest generateCertificationRequest(String requestedDn, String domainAlternativeName,
            KeyPair keyPair, String signingAlgorithm) throws OperatorCreationException {
        JcaPKCS10CertificationRequestBuilder jcaPKCS10CertificationRequestBuilder = new JcaPKCS10CertificationRequestBuilder(new X500Name(requestedDn), keyPair.getPublic());

        // add Subject Alternative Name
        if(StringUtils.isNotBlank(domainAlternativeName)) {
            try {
                List<GeneralName> namesList = new ArrayList<>();
                for(String alternativeName : domainAlternativeName.split(",")) {
                    namesList.add(new GeneralName(GeneralName.dNSName, alternativeName));
                }

                GeneralNames subjectAltName = new GeneralNames(namesList.toArray(new GeneralName [] {}));
                ExtensionsGenerator extGen = new ExtensionsGenerator();
                extGen.addExtension(Extension.subjectAlternativeName, false, subjectAltName);
                jcaPKCS10CertificationRequestBuilder.addAttribute(PKCSObjectIdentifiers.pkcs_9_at_extensionRequest, extGen.generate());
            } catch (IOException e) {
                throw new OperatorCreationException("Error while adding " + domainAlternativeName + " as Subject Alternative Name.", e);
            }
        }

        JcaContentSignerBuilder jcaContentSignerBuilder = new JcaContentSignerBuilder(signingAlgorithm);
        return new JcaPKCS10CertificationRequest(jcaPKCS10CertificationRequestBuilder.build(jcaContentSignerBuilder.build(keyPair.getPrivate())));
    }
}
