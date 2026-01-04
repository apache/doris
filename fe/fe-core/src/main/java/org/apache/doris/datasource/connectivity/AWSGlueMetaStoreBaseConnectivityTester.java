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

package org.apache.doris.datasource.connectivity;

import org.apache.doris.datasource.property.metastore.AWSGlueMetaStoreBaseProperties;

import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.GlueClientBuilder;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;

import java.net.URI;

public class AWSGlueMetaStoreBaseConnectivityTester implements MetaConnectivityTester {
    private final AWSGlueMetaStoreBaseProperties properties;

    public AWSGlueMetaStoreBaseConnectivityTester(AWSGlueMetaStoreBaseProperties properties) {
        this.properties = properties;
    }

    @Override
    public String getTestType() {
        return "AWS Glue";
    }

    @Override
    public void testConnection() throws Exception {
        GlueClientBuilder clientBuilder = GlueClient.builder();

        String glueRegion = properties.getGlueRegion();
        String glueEndpoint = properties.getGlueEndpoint();

        // Set region
        if (StringUtils.isNotBlank(glueRegion)) {
            clientBuilder.region(Region.of(glueRegion));
        }

        // Set endpoint if specified
        if (StringUtils.isNotBlank(glueEndpoint)) {
            clientBuilder.endpointOverride(URI.create(glueEndpoint));
        }

        // Set credentials using properties method
        clientBuilder.credentialsProvider(properties.getAwsCredentialsProvider());

        // Test connection by listing databases (lightweight operation)
        try (GlueClient glueClient = clientBuilder.build()) {
            GetDatabasesRequest request = GetDatabasesRequest.builder()
                    .maxResults(1)
                    .build();
            glueClient.getDatabases(request);
        }
    }
}
