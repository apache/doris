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

package org.apache.doris.datasource.property.metastore;

import org.apache.doris.datasource.property.ConnectorProperty;

import com.amazonaws.ClientConfiguration;
import org.apache.hadoop.hive.conf.HiveConf;

import java.util.Map;

public class HMSGlueMetaStoreProperties extends AbstractHMSProperties {

    // ========== Constants ==========
    public static final String AWS_GLUE_SECRET_KEY_KEY = "aws.glue.secret-key";
    public static final String AWS_GLUE_ACCESS_KEY_KEY = "aws.glue.access-key";
    public static final String AWS_GLUE_ENDPOINT_KEY = "aws.glue.endpoint";
    public static final String AWS_REGION_KEY = "aws.region";
    public static final String AWS_GLUE_SESSION_TOKEN_KEY = "aws.glue.session-token";
    public static final String AWS_GLUE_CATALOG_SEPARATOR_KEY = "aws.glue.catalog.separator";
    public static final String AWS_GLUE_CONNECTION_TIMEOUT_KEY = "aws.glue.connection-timeout";
    public static final int DEFAULT_CONNECTION_TIMEOUT = ClientConfiguration.DEFAULT_CONNECTION_TIMEOUT;
    public static final String AWS_GLUE_MAX_CONNECTIONS_KEY = "aws.glue.max-connections";
    public static final int DEFAULT_MAX_CONNECTIONS = ClientConfiguration.DEFAULT_MAX_CONNECTIONS;
    public static final String AWS_GLUE_MAX_RETRY_KEY = "aws.glue.max-error-retries";
    public static final int DEFAULT_MAX_RETRY = 5;
    public static final String AWS_GLUE_SOCKET_TIMEOUT_KEY = "aws.glue.socket-timeout";
    public static final int DEFAULT_SOCKET_TIMEOUT = ClientConfiguration.DEFAULT_SOCKET_TIMEOUT;

    // ========== Fields ==========
    private AWSGlueMetaStoreBaseProperties baseProperties;

    @ConnectorProperty(names = {AWS_GLUE_MAX_RETRY_KEY},
            required = false,
            description = "Maximum number of retry attempts for AWS Glue errors.")
    protected int awsGlueMaxErrorRetries = DEFAULT_MAX_RETRY;

    @ConnectorProperty(names = {AWS_GLUE_MAX_CONNECTIONS_KEY},
            required = false,
            description = "Maximum allowed connections for AWS Glue.")
    protected int awsGlueMaxConnections = DEFAULT_MAX_CONNECTIONS;

    @ConnectorProperty(names = {AWS_GLUE_CONNECTION_TIMEOUT_KEY},
            required = false,
            description = "Connection timeout duration (in milliseconds) for AWS Glue.")
    protected int awsGlueConnectionTimeout = DEFAULT_CONNECTION_TIMEOUT;

    @ConnectorProperty(names = {AWS_GLUE_SOCKET_TIMEOUT_KEY},
            required = false,
            description = "Socket timeout duration (in milliseconds) for AWS Glue.")
    protected int awsGlueSocketTimeout = DEFAULT_SOCKET_TIMEOUT;

    @ConnectorProperty(names = {AWS_GLUE_CATALOG_SEPARATOR_KEY},
            required = false,
            description = "Catalog separator character for AWS Glue.")
    protected String awsGlueCatalogSeparator = "";

    // ========== Constructor ==========
    /**
     * Constructs an instance with the given metastore type and original properties.
     *
     * @param type      The metastore type.
     * @param origProps The original configuration properties.
     */
    protected HMSGlueMetaStoreProperties(Type type, Map<String, String> origProps) {
        super(type, origProps);
    }

    // ========== Initialization Methods ==========
    @Override
    public void initNormalizeAndCheckProps() {
        super.initNormalizeAndCheckProps();
        baseProperties = AWSGlueMetaStoreBaseProperties.of(origProps);
        initHiveConf();
    }

    /**
     * Initializes the HiveConf object with AWS Glue related properties.
     */
    private void initHiveConf() {
        hiveConf = new HiveConf();
        hiveConf.set(AWS_GLUE_ENDPOINT_KEY, baseProperties.glueEndpoint);
        hiveConf.set(AWS_REGION_KEY, baseProperties.glueRegion);
        hiveConf.set(AWS_GLUE_SESSION_TOKEN_KEY, baseProperties.glueSessionToken);
        hiveConf.set(AWS_GLUE_ACCESS_KEY_KEY, baseProperties.glueAccessKey);
        hiveConf.set(AWS_GLUE_SECRET_KEY_KEY, baseProperties.glueSecretKey);
        hiveConf.set(AWS_GLUE_MAX_RETRY_KEY, String.valueOf(awsGlueMaxErrorRetries));
        hiveConf.set(AWS_GLUE_MAX_CONNECTIONS_KEY, String.valueOf(awsGlueMaxConnections));
        hiveConf.set(AWS_GLUE_CONNECTION_TIMEOUT_KEY, String.valueOf(awsGlueConnectionTimeout));
        hiveConf.set(AWS_GLUE_SOCKET_TIMEOUT_KEY, String.valueOf(awsGlueSocketTimeout));
        hiveConf.set(AWS_GLUE_CATALOG_SEPARATOR_KEY, awsGlueCatalogSeparator);
        hiveConf.set("hive.metastore.type", "glue");
    }

    public HMSGlueMetaStoreProperties(Map<String, String> origProps) {
        super(Type.GLUE, origProps);
    }
}
