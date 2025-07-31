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

import com.google.common.collect.Maps;
import lombok.Getter;

import java.util.Map;

public class AWSGlueMetaStoreProperties extends MetastoreProperties {

    private AWSGlueMetaStoreBaseProperties baseProperties;

    @ConnectorProperty(names = {"glue.catalog_id"},
            description = "The catalog id of the AWS Glue.",
            supported = false)
    private String glueCatalogId = "";

    public AWSGlueMetaStoreProperties(Map<String, String> origProps) {
        super(Type.HMS, origProps);
    }


    @Override
    public void initNormalizeAndCheckProps() {
        super.initNormalizeAndCheckProps();
        baseProperties = AWSGlueMetaStoreBaseProperties.of(origProps);
        baseProperties.checkAndInit();
    }


    public AWSCatalogMetastoreClientCredentials getAWSCatalogMetastoreClientCredentials() {
        return new AWSCatalogMetastoreClientCredentials(baseProperties.glueEndpoint, baseProperties.glueAccessKey,
                baseProperties.glueSecretKey);
    }

    public void toIcebergGlueCatalogProperties(Map<String, String> catalogProps) {
        // See AwsClientProperties.java for property keys
        catalogProps.put("client.credentials-provider",
                "com.amazonaws.glue.catalog.credentials.ConfigurationAWSCredentialsProvider2x");
        catalogProps.put("client.credentials-provider.glue.access_key", baseProperties.glueAccessKey);
        catalogProps.put("client.credentials-provider.glue.secret_key", baseProperties.glueSecretKey);
        catalogProps.put("client.region", baseProperties.glueRegion);
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
