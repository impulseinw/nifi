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

import org.apache.nifi.registry.properties.NiFiRegistryProperties;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Resolve configuration files that need to be encrypted from a given ApplicationProperties
 */
public class NiFiRegistryConfigurationFileResolver implements ConfigurationFileResolver<NiFiRegistryProperties> {

    private Path confDirectory;

    public NiFiRegistryConfigurationFileResolver(final Path confDirectory) {
        this.confDirectory = confDirectory;
    }

    /**
     * Use the nifi.properties file to locate configuration files referenced by properties in the file
     *
     * @return List of application configuration files
     */
    @Override
    public List<File> resolveFilesFromApplicationProperties(NiFiRegistryProperties properties) throws ConfigurationFileResolverException {
        ArrayList<File> configurationFiles = new ArrayList<>();
        configurationFiles.add(ConfigurationFileUtils.getAbsoluteFile(confDirectory.toFile(), properties.getAuthorizersConfigurationFile()));
        configurationFiles.add(ConfigurationFileUtils.getAbsoluteFile(confDirectory.toFile(), properties.getProvidersConfigurationFile()));
        configurationFiles.add(ConfigurationFileUtils.getAbsoluteFile(confDirectory.toFile(), properties.getIdentityProviderConfigurationFile()));
        configurationFiles.add(ConfigurationFileUtils.getAbsoluteFile(confDirectory.toFile(), properties.getRegistryAliasConfigurationFile()));

        for (final File configFile : configurationFiles) {
            if (!isValidConfigurationFile(configFile)) {
                throw new ConfigurationFileResolverException(String.format("Failed to resolve configuration file [%s]", configFile.getName()));
            }
        }

        return configurationFiles;
    }
}
