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
package org.apache.nifi.util.file;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

public class FileUtilities {

    public static String DEFAULT_CONF_DIR = "conf";
    public static String NIFI_PROPERTIES_DEFAULT_NAME = "nifi.properties";
    public static String NIFI_REGISTRY_DEFAULT_PROPERTIES_NAME = "nifi-registry.properties";

    public static File getTemporaryOutputFile(final String prefix, final File siblingFile) throws IOException {
        if (siblingFile != null && siblingFile.isFile()) {
            return File.createTempFile(prefix, siblingFile.getName(), siblingFile.getParentFile());
        } else {
            throw new IOException("Failed to create temporary output file because sibling file is null or is not a file");
        }
    }

    public static boolean isSafeToWrite(final File fileToWrite) {
        assert(fileToWrite != null);
        return (!fileToWrite.exists() && fileToWrite.getParentFile().canWrite() || (fileToWrite.exists() && fileToWrite.canWrite()));
    }

    public static boolean isNiFiConfDirectory(final Path baseDirectory) {
        return directoryContainsFilename(baseDirectory, NIFI_PROPERTIES_DEFAULT_NAME);
    }

    public static boolean isNiFiRegistryConfDirectory(final Path baseDirectory) {
        return directoryContainsFilename(baseDirectory, NIFI_REGISTRY_DEFAULT_PROPERTIES_NAME);
    }

    /**
     * If the baseDirectory given is ./nifi/conf, return the parent directory ./nifi
     * @param baseDirectory A given base directory to locate configuration files
     * @return The ./nifi directory
     */
    public static Path resolveAbsoluteConfDirectory(final Path baseDirectory) {
        if (!baseDirectory.toFile().isDirectory()) {
            throw new IllegalArgumentException(String.format("The base directory given [%s] does not exist or is not a directory", baseDirectory));
        }

        if (isNiFiConfDirectory(baseDirectory) || isNiFiRegistryConfDirectory(baseDirectory)) {
            return getAbsolutePath(baseDirectory);
        } else if (directoryContainsFilename(baseDirectory, DEFAULT_CONF_DIR)) {
            return getAbsolutePath(getDefaultConfDirectory(baseDirectory).toPath());
        } else {
            throw new IllegalArgumentException(
                    String.format("The configuration directory [%s]/ could not be found within [%s] or it did not contain a properties file", DEFAULT_CONF_DIR, baseDirectory));
        }
    }

    /**
     * Get the properties file either NiFi or NiFi Registry from within the configuration directory
     * @param confDirectory The ./conf directory
     * @return The NiFi or NiFi Registry properties file (eg. nifi.properties or nifi-registry.properties)
     */
    public static File resolvePropertiesFile(final Path confDirectory) {
        if (directoryContainsFilename(confDirectory, NIFI_PROPERTIES_DEFAULT_NAME)) {
            return getAbsolutePath(confDirectory.resolve(NIFI_PROPERTIES_DEFAULT_NAME)).toFile();
        } else if (directoryContainsFilename(confDirectory, NIFI_REGISTRY_DEFAULT_PROPERTIES_NAME)) {
            return getAbsolutePath(confDirectory.resolve(NIFI_REGISTRY_DEFAULT_PROPERTIES_NAME)).toFile();
        } else {
            throw new IllegalArgumentException(String.format("Could not find a properties file in [%s]", confDirectory));
        }
    }

    private static Path getAbsolutePath(final Path relativeFile) {
        final Path absolutePath = relativeFile.toAbsolutePath();
        if (absolutePath.toFile().exists() && absolutePath.toFile().canRead()) {
            return absolutePath;
        } else {
            throw new IllegalArgumentException(String.format("The file or directory [%s] does not exist", absolutePath));
        }
    }

    private static boolean directoryContainsFilename(final Path directory, final String filename) {
        return Arrays.stream(directory.toFile().listFiles()).anyMatch(file -> file.getName().equals(filename));
    }

    private static File getDefaultConfDirectory(final Path baseDirectory) {
        return baseDirectory.resolve(DEFAULT_CONF_DIR).toFile();
    }
}
