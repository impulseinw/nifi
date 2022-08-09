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
package org.apache.nifi.util.console;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

@CommandLine.Command(name = "PropertyEncryptor",
        usageHelpWidth=140,
        subcommands = {
        PropertyEncryptorEncrypt.class,
        PropertyEncryptorDecrypt.class,
        PropertyEncryptorMigrate.class,
        PropertyEncryptorTranslateForCLI.class}
)
class PropertyEncryptorCLI extends BaseCLICommand implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(PropertyEncryptorCLI.class);

    @Override
    public void run() {
        if (verboseLogging) {
            logger.info("Verbose logging enabled");
        }
    }

    public static void main(String[] args) {
        logger.warn("Warning: This Property Encryptor tool is currently considered experimental. " +
                "The original 'encrypt-config' tool should still be used to encrypt and manage your NiFi configuration.");
        System.exit(new CommandLine(new PropertyEncryptorCLI()).setCaseInsensitiveEnumValuesAllowed(true).execute(args));
    }
}