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

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import lombok.Getter;

import java.util.Map;

public class AWSGlueProperties extends MetastoreProperties {

    @ConnectorProperty(names = {"glue.endpoint", "aws.endpoint"},
            description = "The endpoint of the AWS Glue.")
    private String glueEndpoint = "";

    @ConnectorProperty(names = {"glue.access_key",
            "aws.glue.access-key", "client.credentials-provider.glue.access_key"},
            description = "The access key of the AWS Glue.")
    private String glueAccessKey = "";

    @ConnectorProperty(names = {"glue.secret_key",
            "aws.glue.secret-key", "client.credentials-provider.glue.secret_key"},
            description = "The secret key of the AWS Glue.")
    private String glueSecretKey = "";

    @ConnectorProperty(names = {"glue.catalog_id"},
            description = "The catalog id of the AWS Glue.",
            supported = false)
    private String glueCatalogId = "";

    @ConnectorProperty(names = {"glue.iam_role"},
            description = "The IAM role the AWS Glue.",
            supported = false)
    private String glueIAMRole = "";

    @ConnectorProperty(names = {"glue.external_id"},
            description = "The external id of the AWS Glue.",
            supported = false)
    private String glueExternalId = "";

    public AWSGlueProperties(Map<String, String> origProps) {
        super(Type.GLUE, origProps);
    }

    @Override
    protected void checkRequiredProperties() {
        if (Strings.isNullOrEmpty(glueAccessKey)
                || Strings.isNullOrEmpty(glueSecretKey)
                || Strings.isNullOrEmpty(glueEndpoint)) {
            throw new IllegalArgumentException("AWS Glue properties(glue.access_key, glue.secret_key, glue.endpoint) "
                    + "are not set correctly.");
        }
    }

    public AWSCatalogMetastoreClientCredentials getAWSCatalogMetastoreClientCredentials() {
        return new AWSCatalogMetastoreClientCredentials(glueEndpoint, glueAccessKey, glueSecretKey);
    }

    public void toIcebergGlueCatalogProperties(Map<String, String> catalogProps) {
        // See AwsClientProperties.java for property keys
        catalogProps.put("client.credentials-provider",
                "com.amazonaws.glue.catalog.credentials.ConfigurationAWSCredentialsProvider2x");
        catalogProps.put("client.credentials-provider.glue.access_key", glueAccessKey);
        catalogProps.put("client.credentials-provider.glue.secret_key", glueSecretKey);
        catalogProps.put("client.region", getRegionFromGlueEndpoint());
    }

    private String getRegionFromGlueEndpoint() {
        // https://glue.ap-northeast-1.amazonaws.com
        // -> ap-northeast-1
        return glueEndpoint.split("\\.")[1];
    }

    @Getter
    public static class AWSCatalogMetastoreClientCredentials {
        private Map<String, String> credentials = Maps.newHashMap();

        // Used for AWSCatalogMetastoreClient
        // See AWSGlueClientFactory in AWSCatalogMetastoreClient.java
        public AWSCatalogMetastoreClientCredentials(String endpoint, String ak, String sk) {
            credentials.put("aws.catalog.credentials.provider.factory.class",
                    "com.amazonaws.glue.catalog.credentials.ConfigurationAWSCredentialsProviderFactory");
            credentials.put("aws.glue.access-key", ak);
            credentials.put("aws.glue.secret-key", sk);
            credentials.put("aws.glue.endpoint", endpoint);
        }
    }
}
