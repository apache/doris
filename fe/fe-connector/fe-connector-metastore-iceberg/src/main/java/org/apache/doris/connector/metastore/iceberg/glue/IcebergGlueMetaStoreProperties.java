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

package org.apache.doris.connector.metastore.iceberg.glue;

import org.apache.doris.connector.metastore.spi.AbstractMetaStoreProperties;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;
import org.apache.doris.foundation.property.ConnectorProperty;
import org.apache.doris.foundation.property.ParamRules;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * Iceberg AWS Glue catalog metastore backend — validation only (the Glue/S3 catalog conf is connector-side
 * in {@code IcebergCatalogFactory}). Ports the legacy {@code AWSGlueMetaStoreBaseProperties}'
 * {@code buildRules()} + {@code requireExplicitGlueCredentials()} verbatim (§4 of the P6-T10 design), in
 * fire order: AK/SK-together, endpoint-required, endpoint-https, then at-least-one-credential. No
 * warehouse requirement.
 */
public final class IcebergGlueMetaStoreProperties extends AbstractMetaStoreProperties {

    @ConnectorProperty(names = {"glue.access_key", "aws.glue.access-key",
            "client.credentials-provider.glue.access_key"},
            required = false, description = "The access key of the AWS Glue.")
    private String glueAccessKey = "";

    @ConnectorProperty(names = {"glue.secret_key", "aws.glue.secret-key",
            "client.credentials-provider.glue.secret_key"},
            required = false, sensitive = true, description = "The secret key of the AWS Glue.")
    private String glueSecretKey = "";

    @ConnectorProperty(names = {"glue.endpoint", "aws.endpoint", "aws.glue.endpoint"},
            required = false, description = "The endpoint of the AWS Glue.")
    private String glueEndpoint = "";

    @ConnectorProperty(names = {"glue.role_arn"}, required = false,
            description = "The IAM role of the AWS Glue.")
    private String glueIamRole = "";

    @ConnectorProperty(names = {"glue.region", "aws.region", "aws.glue.region"},
            required = false, description = "The region of the AWS Glue.")
    private String glueRegion = "";

    @ConnectorProperty(names = {"aws.glue.session-token"}, required = false, sensitive = true,
            description = "The session token accompanying the AWS Glue access key pair.")
    private String glueSessionToken = "";

    @ConnectorProperty(names = {"glue.external_id"}, required = false,
            description = "The external id required when assuming the AWS Glue IAM role.")
    private String glueExternalId = "";

    private IcebergGlueMetaStoreProperties(Map<String, String> raw) {
        super(raw);
    }

    public static IcebergGlueMetaStoreProperties of(Map<String, String> raw) {
        IcebergGlueMetaStoreProperties props = new IcebergGlueMetaStoreProperties(raw);
        ConnectorPropertiesUtils.bindConnectorProperties(props, raw);
        return props;
    }

    @Override
    public String providerName() {
        return "GLUE";
    }

    // ---------------------------------------------------------------------
    // Assembly surface: the connector builds the glue catalog options from these, so the alias sets declared
    // above are the single place a glue key name lives (it used to keep a second copy of them to scan with).
    // ---------------------------------------------------------------------

    public String getGlueAccessKey() {
        return glueAccessKey;
    }

    public String getGlueSecretKey() {
        return glueSecretKey;
    }

    public String getGlueEndpoint() {
        return glueEndpoint;
    }

    public String getGlueIamRole() {
        return glueIamRole;
    }

    /** Blank when the catalog names no region — the connector then derives one from the endpoint. */
    public String getGlueRegion() {
        return glueRegion;
    }

    public String getGlueSessionToken() {
        return glueSessionToken;
    }

    public String getGlueExternalId() {
        return glueExternalId;
    }

    @Override
    public void validate() {
        // Legacy AWSGlueMetaStoreBaseProperties.checkAndInit -> buildRules().validate() (rules run in
        // registration order), then IcebergGlueMetaStoreProperties.initNormalizeAndCheckProps ->
        // requireExplicitGlueCredentials().
        new ParamRules()
                .requireTogether(new String[] {glueAccessKey, glueSecretKey},
                        "glue.access_key and glue.secret_key must be set together")
                .require(glueEndpoint, "glue.endpoint must be set")
                .check(() -> StringUtils.isNotBlank(glueEndpoint) && !glueEndpoint.startsWith("https://"),
                        "glue.endpoint must use https protocol,please set glue.endpoint to https://...")
                .validate();
        // requireExplicitGlueCredentials: at least one of an access key or an IAM role must be explicit
        // (iceberg cannot use the default credential chain).
        if (StringUtils.isNotBlank(glueAccessKey) || StringUtils.isNotBlank(glueIamRole)) {
            return;
        }
        throw new IllegalArgumentException("At least one of glue.access_key or glue.role_arn must be set");
    }
}
