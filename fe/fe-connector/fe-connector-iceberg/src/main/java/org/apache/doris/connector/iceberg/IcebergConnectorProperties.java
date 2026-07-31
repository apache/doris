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

    // -- REST catalog options --
    public static final String REST_NESTED_NAMESPACE_ENABLED = "iceberg.rest.nested-namespace-enabled";
    public static final String REST_VIEW_ENABLED = "iceberg.rest.view-enabled";

    // -- REST per-user session (OIDC delegated credential; #63068 re-migration) --
    // iceberg.rest.session = none (default, one shared catalog identity) | user (project the querying user's
    // delegated credential onto a per-request Iceberg REST SessionCatalog; requires security.type=oauth2 and
    // gates the SUPPORTS_USER_SESSION capability). delegated-token-mode picks how the token is attached:
    // access_token = verbatim OAuth2 bearer; token_exchange = typed token key so the REST server exchanges it.
    public static final String REST_SESSION = "iceberg.rest.session";
    public static final String SESSION_NONE = "none";
    public static final String SESSION_USER = "user";
    public static final String REST_DELEGATED_TOKEN_MODE = "iceberg.rest.oauth2.delegated-token-mode";
    public static final String DELEGATED_TOKEN_MODE_ACCESS_TOKEN = "access_token";
    public static final String DELEGATED_TOKEN_MODE_TOKEN_EXCHANGE = "token_exchange";
    // Optional per-session OAuth2 AuthSession timeout (maps to CatalogProperties.AUTH_SESSION_TIMEOUT_MS).
    public static final String REST_SESSION_TIMEOUT = "iceberg.rest.session-timeout";

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

    // -- REST input aliases (legacy IcebergRestProperties @ConnectorProperty names) --
    public static final String REST_URI = "iceberg.rest.uri";
    public static final String URI = "uri";
    public static final String REST_PREFIX = "iceberg.rest.prefix";
    public static final String REST_SECURITY_TYPE = "iceberg.rest.security.type";
    public static final String REST_OAUTH2_TOKEN = "iceberg.rest.oauth2.token";
    public static final String REST_OAUTH2_CREDENTIAL = "iceberg.rest.oauth2.credential";
    public static final String REST_OAUTH2_SCOPE = "iceberg.rest.oauth2.scope";
    public static final String REST_OAUTH2_SERVER_URI = "iceberg.rest.oauth2.server-uri";
    public static final String REST_OAUTH2_TOKEN_REFRESH_ENABLED = "iceberg.rest.oauth2.token-refresh-enabled";
    public static final String REST_VENDED_CREDENTIALS_ENABLED = "iceberg.rest.vended-credentials-enabled";
    public static final String REST_SIGV4_ENABLED = "iceberg.rest.sigv4-enabled";
    public static final String REST_SIGNING_NAME = "iceberg.rest.signing-name";
    public static final String REST_SIGNING_REGION = "iceberg.rest.signing-region";
    public static final String REST_ACCESS_KEY_ID = "iceberg.rest.access-key-id";
    public static final String REST_SECRET_ACCESS_KEY = "iceberg.rest.secret-access-key";
    public static final String REST_SESSION_TOKEN = "iceberg.rest.session-token";
    public static final String REST_CREDENTIALS_PROVIDER_TYPE = "iceberg.rest.credentials_provider_type";
    public static final String REST_CONNECTION_TIMEOUT_MS = "iceberg.rest.connection-timeout-ms";
    public static final String REST_SOCKET_TIMEOUT_MS = "iceberg.rest.socket-timeout-ms";

    // -- REST emitted literal keys / values / defaults (non-SDK) --
    public static final String REST_PREFIX_KEY = "prefix";
    public static final String REST_VENDED_CREDENTIALS_HEADER = "header.X-Iceberg-Access-Delegation";
    public static final String REST_VENDED_CREDENTIALS_VALUE = "vended-credentials";
    public static final String REST_CONNECTION_TIMEOUT_MS_KEY = "rest.client.connection-timeout-ms";
    public static final String REST_SOCKET_TIMEOUT_MS_KEY = "rest.client.socket-timeout-ms";
    public static final String REST_SIGNING_NAME_KEY = "rest.signing-name";
    public static final String REST_SIGV4_ENABLED_KEY = "rest.sigv4-enabled";
    public static final String REST_SIGNING_REGION_KEY = "rest.signing-region";
    public static final String DEFAULT_REST_CONNECTION_TIMEOUT_MS = "10000";
    public static final String DEFAULT_REST_SOCKET_TIMEOUT_MS = "60000";
    public static final String SECURITY_TYPE_OAUTH2 = "oauth2";
    public static final String SIGNING_NAME_GLUE = "glue";
    public static final String SIGNING_NAME_S3TABLES = "s3tables";
    public static final String PROVIDER_MODE_DEFAULT = "DEFAULT";

    // -- GLUE input alias arrays (legacy AWSGlueMetaStoreBaseProperties / IcebergGlueMetaStoreProperties) --
    public static final String[] GLUE_ENDPOINT = {"glue.endpoint", "aws.endpoint", "aws.glue.endpoint"};
    public static final String[] GLUE_REGION = {"glue.region", "aws.region", "aws.glue.region"};
    public static final String[] GLUE_ACCESS_KEY = {
            "glue.access_key", "aws.glue.access-key", "client.credentials-provider.glue.access_key"};
    public static final String[] GLUE_SECRET_KEY = {
            "glue.secret_key", "aws.glue.secret-key", "client.credentials-provider.glue.secret_key"};
    public static final String[] GLUE_SESSION_TOKEN = {"aws.glue.session-token"};
    public static final String[] GLUE_IAM_ROLE = {"glue.role_arn"};
    public static final String[] GLUE_EXTERNAL_ID = {"glue.external_id"};

    // -- GLUE emitted literal keys / values / defaults (non-SDK) --
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

    // -- JDBC input aliases (legacy IcebergJdbcMetaStoreProperties @ConnectorProperty names) --
    public static final String[] JDBC_URI = {"uri", "iceberg.jdbc.uri"};
    public static final String JDBC_USER = "iceberg.jdbc.user";
    public static final String JDBC_PASSWORD = "iceberg.jdbc.password";
    public static final String JDBC_INIT_CATALOG_TABLES = "iceberg.jdbc.init-catalog-tables";
    public static final String JDBC_SCHEMA_VERSION = "iceberg.jdbc.schema-version";
    public static final String JDBC_STRICT_MODE = "iceberg.jdbc.strict-mode";
    public static final String JDBC_CATALOG_NAME = "iceberg.jdbc.catalog_name";
    public static final String JDBC_DRIVER_URL = "iceberg.jdbc.driver_url";
    public static final String JDBC_DRIVER_CLASS = "iceberg.jdbc.driver_class";

    // -- JDBC emitted literal keys (non-SDK) --
    public static final String JDBC_PREFIX = "jdbc.";
    public static final String JDBC_USER_KEY = "jdbc.user";
    public static final String JDBC_PASSWORD_KEY = "jdbc.password";
    public static final String JDBC_INIT_CATALOG_TABLES_KEY = "jdbc.init-catalog-tables";
    public static final String JDBC_SCHEMA_VERSION_KEY = "jdbc.schema-version";
    public static final String JDBC_STRICT_MODE_KEY = "jdbc.strict-mode";
}
