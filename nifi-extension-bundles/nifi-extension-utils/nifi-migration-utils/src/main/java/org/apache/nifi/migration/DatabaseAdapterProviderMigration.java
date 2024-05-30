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
package org.apache.nifi.migration;

import static java.util.Collections.emptyMap;

import org.apache.nifi.components.PropertyDescriptor;

public final class DatabaseAdapterProviderMigration {

    static final String GENERIC_DATABASE_ADAPTER_NAME = "Generic";
    static final String MSSQL_2012_DATABASE_ADAPTER_NAME = "MS SQL 2012+";
    static final String MSSQL_2008_DATABASE_ADAPTER_NAME = "MS SQL 2008";
    static final String MYSQL_DATABASE_ADAPTER_NAME = "MySQL";
    static final String LEGACY_ORACLE_DATABASE_ADAPTER_NAME = "Oracle";
    static final String ORACLE_12_DATABASE_ADAPTER_NAME = "Oracle 12+";
    static final String PHOENIX_DATABASE_ADAPTER_NAME = "Phoenix";
    static final String POSTGRESQL_DATABASE_ADAPTER_NAME = "PostgreSQL";

    static final String GENERIC_DATABASE_ADAPTER_PROVIDER_CLASSNAME = "org.apache.nifi.db.GenericDatabaseAdapterProvider";
    static final String MSSQL_2012_DATABASE_ADAPTER_PROVIDER_CLASSNAME = "org.apache.nifi.db.MSSQL2012DatabaseAdapterProvider";
    static final String MSSQL_2008_DATABASE_ADAPTER_PROVIDER_CLASSNAME = "org.apache.nifi.db.MSSQL2008DatabaseAdapterProvider";
    static final String MYSQL_DATABASE_ADAPTER_PROVIDER_CLASSNAME = "org.apache.nifi.db.MySQLDatabaseAdapterProvider";
    static final String LEGACY_ORACLE_DATABASE_ADAPTER_PROVIDER_CLASSNAME = "org.apache.nifi.db.LegacyOracleDatabaseAdapterProvider";
    static final String ORACLE_12_DATABASE_ADAPTER_PROVIDER_CLASSNAME = "org.apache.nifi.db.Oracle12DatabaseAdapterProvider";
    static final String PHOENIX_DATABASE_ADAPTER_PROVIDER_CLASSNAME = "org.apache.nifi.db.PhoenixDatabaseAdapterProvider";
    static final String POSTGRESQL_DATABASE_ADAPTER_PROVIDER_CLASSNAME = "org.apache.nifi.db.PostgreSQLDatabaseAdapterProvider";

    static final String DB_TYPE_PROPERTY = "db-type";

    private DatabaseAdapterProviderMigration() { }

    /**
     * Migrates component level Database Type property to DatabaseAdapterProvider controller service.
     *
     * @param config the component's property config to be migrated
     * @param dbAdapterProviderProperty the component's property descriptor referencing DatabaseAdapterProvider service
     * @param dbTypeProperty the name of the component level Database Type property
     */
    public static void migrateProperties(final PropertyConfiguration config,
            final PropertyDescriptor dbAdapterProviderProperty, final String dbTypeProperty) {
        if (!config.isPropertySet(dbTypeProperty)) {
            return; // nothing to do
        }

        final String serviceId;
        switch (config.getRawPropertyValue(dbTypeProperty).orElseThrow()) {
            case GENERIC_DATABASE_ADAPTER_NAME:
                serviceId = config.createControllerService(GENERIC_DATABASE_ADAPTER_PROVIDER_CLASSNAME, emptyMap());
                break;
            case MSSQL_2012_DATABASE_ADAPTER_NAME:
                serviceId = config.createControllerService(MSSQL_2012_DATABASE_ADAPTER_PROVIDER_CLASSNAME, emptyMap());
                break;
            case MSSQL_2008_DATABASE_ADAPTER_NAME:
                serviceId = config.createControllerService(MSSQL_2008_DATABASE_ADAPTER_PROVIDER_CLASSNAME, emptyMap());
                break;
            case MYSQL_DATABASE_ADAPTER_NAME:
                serviceId = config.createControllerService(MYSQL_DATABASE_ADAPTER_PROVIDER_CLASSNAME, emptyMap());
                break;
            case LEGACY_ORACLE_DATABASE_ADAPTER_NAME:
                serviceId = config.createControllerService(LEGACY_ORACLE_DATABASE_ADAPTER_PROVIDER_CLASSNAME, emptyMap());
                break;
            case ORACLE_12_DATABASE_ADAPTER_NAME:
                serviceId = config.createControllerService(ORACLE_12_DATABASE_ADAPTER_PROVIDER_CLASSNAME, emptyMap());
                break;
            case PHOENIX_DATABASE_ADAPTER_NAME:
                serviceId = config.createControllerService(PHOENIX_DATABASE_ADAPTER_PROVIDER_CLASSNAME, emptyMap());
                break;
            case POSTGRESQL_DATABASE_ADAPTER_NAME:
                serviceId = config.createControllerService(POSTGRESQL_DATABASE_ADAPTER_PROVIDER_CLASSNAME, emptyMap());
                break;
            default:
                return; // fail migration, but let the user resolve it by hand.
        }

        config.setProperty(dbAdapterProviderProperty, serviceId);
        config.removeProperty(dbTypeProperty);
    }
}
