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
package org.apache.nifi.processors.standard.util.crypto;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.processor.exception.ProcessException;

import javax.crypto.Cipher;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CipherUtility {

    public static final int BUFFER_SIZE = 65536;

    /**
     * Returns the cipher algorithm from the full algorithm name. Useful for getting key lengths, etc.
     * <p/>
     * Ex: PBEWITHMD5AND128BITAES-CBC-OPENSSL -> AES
     *
     * @param algorithm the full algorithm name
     * @return the generic cipher name or the full algorithm if one cannot be extracted
     */
    public static String parseCipherFromAlgorithm(final String algorithm) {
        // This is not optimal but the algorithms do not have a standard format
        final String AES = "AES";
        final String TDES = "TRIPLEDES";
        final String DES = "DES";
        final String RC4 = "RC4";
        final String RC2 = "RC2";
        final String TWOFISH = "TWOFISH";
        final List<String> SYMMETRIC_CIPHERS = Arrays.asList(AES, TDES, DES, RC4, RC2, TWOFISH);

        // The algorithms contain "TRIPLEDES" but the cipher name is "DESede"
        final String ACTUAL_TDES_CIPHER = "DESede";

        for (String cipher : SYMMETRIC_CIPHERS) {
            if (algorithm.contains(cipher)) {
                if (cipher.equals(TDES)) {
                    return ACTUAL_TDES_CIPHER;
                } else {
                    return cipher;
                }
            }
        }

        return algorithm;
    }

    /**
     * Returns the cipher key length from the full algorithm name. Useful for getting key lengths, etc.
     * <p/>
     * Ex: PBEWITHMD5AND128BITAES-CBC-OPENSSL -> 128
     *
     * @param algorithm the full algorithm name
     * @return the key length or -1 if one cannot be extracted
     */
    public static int parseKeyLengthFromAlgorithm(final String algorithm) {
        int keyLength = parseActualKeyLengthFromAlgorithm(algorithm);
        if (keyLength != -1) {
            return keyLength;
        } else {
            // Key length not explicitly named in algorithm
            String cipher = parseCipherFromAlgorithm(algorithm);
            return getDefaultKeyLengthForCipher(cipher);
        }
    }

    private static int parseActualKeyLengthFromAlgorithm(final String algorithm) {
        Pattern pattern = Pattern.compile("([\\d]+)BIT");
        Matcher matcher = pattern.matcher(algorithm);
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1));
        } else {
            return -1;
        }
    }

    /**
     * Returns true if the provided key length is a valid key length for the provided cipher family. Does not reflect if the Unlimited Strength Cryptography Jurisdiction Policies are installed.
     * Does not reflect if the key length is correct for a specific combination of cipher and PBE-derived key length.
     * <p/>
     * Ex:
     * <p/>
     * 256 is valid for {@code AES/CBC/PKCS7Padding} but not {@code PBEWITHMD5AND128BITAES-CBC-OPENSSL}. However, this method will return {@code true} for both because it only gets the cipher
     * family, {@code AES}.
     * <p/>
     * 64, AES -> false
     * [128, 192, 256], AES -> true
     *
     * @param keyLength the key length in bits
     * @param cipher    the cipher family
     * @return true if this key length is valid
     */
    public static boolean isValidKeyLength(int keyLength, final String cipher) {
        if (StringUtils.isEmpty(cipher)) {
            return false;
        }
        switch (cipher.toUpperCase()) {
            case "DESEDE":
                // 3DES keys have the cryptographic strength of 7/8 because of parity bits, but are often represented with n*8 bytes
                final List<Integer> DESEDE_KLS = Arrays.asList(56, 64, 112, 128, 168, 192);
                return DESEDE_KLS.contains(keyLength);
            case "DES":
                return keyLength == 56 || keyLength == 64;
            case "RC2":
            case "RC4":
            case "RC5":
                /** These ciphers can have arbitrary length keys but that's a really bad idea, {@see http://crypto.stackexchange.com/a/9963/12569}.
                 * Also, RC* is deprecated and should be considered insecure */
                return keyLength >= 40 && keyLength <= 2048;
            case "AES":
            case "TWOFISH":
                return keyLength == 128 || keyLength == 192 || keyLength == 256;
            default:
                return false;
        }
    }

    /**
     * Returns true if the provided key length is a valid key length for the provided algorithm. Does not reflect if the Unlimited Strength Cryptography Jurisdiction Policies are installed.
     * <p/>
     * Ex:
     * <p/>
     * 256 is valid for {@code AES/CBC/PKCS7Padding} but not {@code PBEWITHMD5AND128BITAES-CBC-OPENSSL}.
     * <p/>
     * 64, AES/CBC/PKCS7Padding -> false
     * [128, 192, 256], AES/CBC/PKCS7Padding -> true
     * <p/>
     * 128, PBEWITHMD5AND128BITAES-CBC-OPENSSL -> true
     * [192, 256], PBEWITHMD5AND128BITAES-CBC-OPENSSL -> false
     *
     * @param keyLength the key length in bits
     * @param algorithm the specific algorithm
     * @return true if this key length is valid
     */
    public static boolean isValidKeyLengthForAlgorithm(int keyLength, final String algorithm) {
        if (StringUtils.isEmpty(algorithm)) {
            return false;
        }
        int parsedKeyLength = parseActualKeyLengthFromAlgorithm(algorithm);
        if (parsedKeyLength != -1) {
            return keyLength == parsedKeyLength;
        } else {
            // No explicit key length is named in the algorithm; use cipher family
            return isValidKeyLength(keyLength, parseCipherFromAlgorithm(algorithm));
        }
    }

    private static int getDefaultKeyLengthForCipher(String cipher) {
        if (StringUtils.isEmpty(cipher)) {
            return -1;
        }
        cipher = cipher.toUpperCase();
        switch (cipher) {
            case "DESEDE":
                return 112;
            case "DES":
                return 64;
            case "RC2":
            case "RC4":
            case "RC5":
            default:
                return 128;
        }
    }

    public static void processStreams(Cipher cipher, InputStream in, OutputStream out) {
        try {
            final byte[] buffer = new byte[BUFFER_SIZE];
            int len;
            while ((len = in.read(buffer)) > 0) {
                final byte[] decryptedBytes = cipher.update(buffer, 0, len);
                if (decryptedBytes != null) {
                    out.write(decryptedBytes);
                }
            }

            try {
                out.write(cipher.doFinal());
            } catch (final Exception e) {
                throw new ProcessException(e);
            }
        } catch (Exception e) {
            throw new ProcessException(e);
        }
    }
}