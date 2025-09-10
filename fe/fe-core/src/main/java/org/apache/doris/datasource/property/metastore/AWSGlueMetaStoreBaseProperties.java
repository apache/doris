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

import org.apache.doris.datasource.property.ConnectorPropertiesUtils;
import org.apache.doris.datasource.property.ConnectorProperty;
import org.apache.doris.datasource.property.ParamRules;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AWSGlueMetaStoreBaseProperties {
    @ConnectorProperty(names = {"glue.endpoint", "aws.endpoint", "aws.glue.endpoint"},
            description = "The endpoint of the AWS Glue.")
    protected String glueEndpoint = "";

    @ConnectorProperty(names = {"glue.region", "aws.region", "aws.glue.region"},
            description = "The region of the AWS Glue. "
                    + "If not set, it will use the default region configured in the AWS SDK or environment variables."
    )
    protected String glueRegion = "";

    /**
     * AWS credentials.
     */
    @ConnectorProperty(names = {"client.credentials-provider"},
            description = "The class name of the credentials provider for AWS Glue.",
            supported = false)
    protected String credentialsProviderClass = "com.amazonaws.auth.DefaultAWSCredentialsProviderChain";

    @ConnectorProperty(names = {"glue.access_key",
            "aws.glue.access-key", "client.credentials-provider.glue.access_key"},
            description = "The access key of the AWS Glue.")
    protected String glueAccessKey = "";

    @ConnectorProperty(names = {"glue.secret_key",
            "aws.glue.secret-key", "client.credentials-provider.glue.secret_key"},
            sensitive = true,
            description = "The secret key of the AWS Glue.")
    protected String glueSecretKey = "";

    @ConnectorProperty(names = {"aws.glue.session-token"},
            description = "The session token of the AWS Glue.")
    protected String glueSessionToken = "";

    @ConnectorProperty(names = {"glue.role_arn"},
            description = "The IAM role the AWS Glue.",
            supported = false)
    protected String glueIAMRole = "";

    @ConnectorProperty(names = {"glue.external_id"},
            description = "The external id of the AWS Glue.",
            supported = false)
    protected String glueExternalId = "";

    public static AWSGlueMetaStoreBaseProperties of(Map<String, String> properties) {
        AWSGlueMetaStoreBaseProperties propertiesObj = new AWSGlueMetaStoreBaseProperties();
        ConnectorPropertiesUtils.bindConnectorProperties(propertiesObj, properties);
        propertiesObj.checkAndInit();
        return propertiesObj;
    }

    /**
     * The pattern of the AWS Glue endpoint.
     * FYI: https://docs.aws.amazon.com/general/latest/gr/glue.html#glue_region
     * eg:
     * glue.us-east-1.amazonaws.com↳
     * <p>
     * glue-fips.us-east-1.api.aws
     * <p>
     * glue-fips.us-east-1.amazonaws.com
     * <p>
     * glue.us-east-1.api.aws
     */
    private static final Pattern ENDPOINT_PATTERN = Pattern.compile(
            "^(?:https?://)?(?:glue|glue-fips)\\.([a-z0-9-]+)\\.(?:api\\.aws|amazonaws\\.com)$"
    );

    private ParamRules buildRules() {

        return new ParamRules()
                .require(glueAccessKey,
                        "glue.access_key or aws.glue.access-key or client.credentials-provider.glue.access_key")
                .require(glueSecretKey,
                        "glue.secret_key or aws.glue.secret-key or client.credentials-provider.glue.secret_key")
                .require(glueEndpoint, "glue.endpoint or aws.endpoint or aws.glue.endpoint is required");
    }

    private void checkAndInit() {
        buildRules().validate();

        Matcher matcher = ENDPOINT_PATTERN.matcher(glueEndpoint.toLowerCase());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid AWS Glue endpoint: " + glueEndpoint);
        }

        if (StringUtils.isBlank(glueRegion)) {
            this.glueRegion = extractRegionFromEndpoint(matcher);
        }
    }

    private String extractRegionFromEndpoint(Matcher matcher) {
        for (int i = 1; i <= matcher.groupCount(); i++) {
            String group = matcher.group(i);
            if (StringUtils.isNotBlank(group)) {
                return group;
            }
        }
        throw new IllegalArgumentException("Could not extract region from endpoint: " + glueEndpoint);
    }
}


