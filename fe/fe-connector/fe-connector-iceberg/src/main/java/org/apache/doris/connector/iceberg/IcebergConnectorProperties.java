// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.connector.iceberg;

/**
 * Property constants for Iceberg connector configuration.
 * Mirrors keys from fe-core's Iceberg property classes without taking
 * a compile-time dependency on fe-core.
 */
public final class IcebergConnectorProperties {

    private IcebergConnectorProperties() {
    }

    // -- Deployment-level settings, read from this plugin's own iceberg.conf (named after
    // ConnectorProvider.name()). Each falls back to the ENV_ name below it, which is the fe.conf key it
    // used to live under and still works.
    //
    // Both are shared with other connectors at the fe.conf end -- one jdbc_drivers_dir and one
    // hive_metastore_client_timeout_second serve jdbc, iceberg and paimon. A plugin conf cannot express
    // that, so a deployment that moves to these files sets the value in each plugin's own conf. That is
    // the accepted cost of a per-plugin file; the fe.conf keys stay as the shared fallback. --
    public static final String CONF_DRIVERS_DIR = "drivers_dir";
    public static final String CONF_METASTORE_CLIENT_TIMEOUT_SECOND = "metastore_client_timeout_second";

    /** The fe.conf name of {@link #CONF_DRIVERS_DIR}, forwarded through the engine environment. */
    public static final String ENV_JDBC_DRIVERS_DIR = "jdbc_drivers_dir";
    /** The fe.conf name of {@link #CONF_METASTORE_CLIENT_TIMEOUT_SECOND}. */
    public static final String ENV_HIVE_METASTORE_CLIENT_TIMEOUT_SECOND =
            "hive_metastore_client_timeout_second";
    /** Engine-wide, not this connector's: the FE install root. Stays in the engine environment. */
    public static final String ENV_DORIS_HOME = "doris_home";
    /** Legacy default when neither channel names a metastore client timeout. */
    public static final String DEFAULT_METASTORE_CLIENT_TIMEOUT_SECOND = "10";

    // -- Catalog type (second-level dispatch) --
    public static final String ICEBERG_CATALOG_TYPE = "iceberg.catalog.type";

    // -- Supported catalog type values --
    public static final String TYPE_REST = "rest";
    public static final String TYPE_HMS = "hms";
    public static final String TYPE_GLUE = "glue";
    public static final String TYPE_JDBC = "jdbc";
    public static final String TYPE_HADOOP = "hadoop";
    public static final String TYPE_S3_TABLES = "s3tables";

    // -- Warehouse --
    public static final String WAREHOUSE = "warehouse";

    // -- Type mapping options --
    // Dotted keys matching CatalogProperty.ENABLE_MAPPING_* — the exact spelling that real catalog
    // property maps carry. The underscore spelling never matches a live catalog map and reads
    // default-false (silent loss of the BINARY->VARBINARY / TIMESTAMP_TZ->TIMESTAMPTZ mapping).
    public static final String ENABLE_MAPPING_VARBINARY = "enable.mapping.varbinary";
    public static final String ENABLE_MAPPING_TIMESTAMP_TZ = "enable.mapping.timestamp_tz";

    // -- REST per-user session (OIDC delegated credential; #63068 re-migration) --
    // iceberg.rest.session = none (default, one shared catalog identity) | user (project the querying user's
    // delegated credential onto a per-request Iceberg REST SessionCatalog; requires security.type=oauth2 and
    // gates the SUPPORTS_USER_SESSION capability). Every other REST key -- including the rest of this one's
    // family -- is declared by IcebergRestMetaStoreProperties; these two remain only because test fixtures
    // spell the session mode out when building a catalog property map.
    public static final String REST_SESSION = "iceberg.rest.session";
    public static final String SESSION_USER = "user";

    // -- Namespace hierarchy (REST 3-level <catalog>.<db>.<table>) --
    // Mirrors legacy IcebergExternalCatalog.EXTERNAL_CATALOG_NAME: when present, this catalog level is
    // appended to every namespace and roots database listing.
    public static final String EXTERNAL_CATALOG_NAME = "external_catalog.name";

    // -- Cache configuration --
    public static final String TABLE_CACHE_ENABLE = "meta.cache.iceberg.table.enable";
    public static final String TABLE_CACHE_TTL = "meta.cache.iceberg.table.ttl-second";
    public static final String TABLE_CACHE_CAPACITY = "meta.cache.iceberg.table.capacity";
    public static final String MANIFEST_CACHE_ENABLE = "meta.cache.iceberg.manifest.enable";
    public static final String MANIFEST_CACHE_TTL = "meta.cache.iceberg.manifest.ttl-second";
    public static final String MANIFEST_CACHE_CAPACITY = "meta.cache.iceberg.manifest.capacity";

    // =====================================================================
    // Per-flavor INPUT alias keys + non-SDK literal EMITTED keys (T05).
    // Mirror the legacy fe-core Iceberg*MetaStoreProperties @ConnectorProperty aliases and the
    // literal catalog-option keys they emit. Keys that ARE iceberg-SDK constants
    // (CatalogProperties / S3FileIOProperties / AwsProperties / AwsClientProperties / OAuth2Properties)
    // are referenced via the SDK in IcebergCatalogFactory, not duplicated here.
    // =====================================================================

    // -- REST input alias (the rest of the family lives on IcebergRestMetaStoreProperties) --
    public static final String REST_VENDED_CREDENTIALS_ENABLED = "iceberg.rest.vended-credentials-enabled";

    // -- REST emitted literal keys / values (non-SDK) --
    public static final String REST_PREFIX_KEY = "prefix";
    public static final String REST_VENDED_CREDENTIALS_HEADER = "header.X-Iceberg-Access-Delegation";
    public static final String REST_VENDED_CREDENTIALS_VALUE = "vended-credentials";
    public static final String REST_CONNECTION_TIMEOUT_MS_KEY = "rest.client.connection-timeout-ms";
    public static final String REST_SOCKET_TIMEOUT_MS_KEY = "rest.client.socket-timeout-ms";
    public static final String REST_SIGNING_NAME_KEY = "rest.signing-name";
    public static final String REST_SIGV4_ENABLED_KEY = "rest.sigv4-enabled";
    public static final String REST_SIGNING_REGION_KEY = "rest.signing-region";
    public static final String SECURITY_TYPE_OAUTH2 = "oauth2";
    public static final String SIGNING_NAME_GLUE = "glue";
    public static final String SIGNING_NAME_S3TABLES = "s3tables";

    // -- GLUE emitted literal keys / values / defaults (non-SDK; the input aliases are declared by
    // IcebergGlueMetaStoreProperties) --
    public static final String GLUE_CREDENTIALS_PROVIDER_KEY = "client.credentials-provider";
    public static final String GLUE_CREDENTIALS_PROVIDER_2X =
            "org.apache.doris.connector.iceberg.glue.ConfigurationAWSCredentialsProvider2x";
    public static final String GLUE_CREDENTIALS_PROVIDER_ACCESS_KEY = "client.credentials-provider.glue.access_key";
    public static final String GLUE_CREDENTIALS_PROVIDER_SECRET_KEY = "client.credentials-provider.glue.secret_key";
    public static final String GLUE_CREDENTIALS_PROVIDER_SESSION_TOKEN =
            "client.credentials-provider.glue.session_token";
    public static final String AWS_REGION_KEY = "aws.region";
    public static final String GLUE_CHECKED_WAREHOUSE = "s3://doris";
    public static final String GLUE_DEFAULT_REGION = "us-east-1";

    // -- JDBC input alias (the rest of the family lives on IcebergJdbcMetaStoreProperties; this one is the
    // positional catalog name, which the assembly REMOVES from the options map rather than reads) --
    public static final String JDBC_CATALOG_NAME = "iceberg.jdbc.catalog_name";

    // -- JDBC emitted literal keys (non-SDK) --
    public static final String JDBC_USER_KEY = "jdbc.user";
    public static final String JDBC_PASSWORD_KEY = "jdbc.password";
    public static final String JDBC_INIT_CATALOG_TABLES_KEY = "jdbc.init-catalog-tables";
    public static final String JDBC_SCHEMA_VERSION_KEY = "jdbc.schema-version";
    public static final String JDBC_STRICT_MODE_KEY = "jdbc.strict-mode";
}
