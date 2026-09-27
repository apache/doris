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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.catalog.Resource.ResourceType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.fs.TestFileSystemPluginManagers;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CreateResourceInfoTest {

    private static final ImmutableMap<String, String> AZURE_SHAPED = ImmutableMap.of(
            "type", "s3",
            "s3.endpoint", "https://acct.blob.core.windows.net",
            "s3.access_key", "ak",
            "s3.secret_key", "sk");

    @Test
    public void testAzureShapedPropertiesResolveToAzure() throws Exception {
        CreateResourceInfo info = new CreateResourceInfo(false, false, "azure_res", AZURE_SHAPED);
        info.analyzeResourceType();
        Assertions.assertEquals(ResourceType.AZURE, info.getResourceType());
    }

    /**
     * The resource type is persisted and ALTER RESOURCE cannot change it, so with the Azure plugin
     * absent an Azure-shaped {@code type=s3} map must be refused - not created as an S3Resource that
     * stays one after the plugin is repaired. A real S3 map is unaffected by the absent plugin.
     */
    @Test
    public void testAzureShapedPropertiesAreRefusedWhileTheAzurePluginIsAbsent() throws Exception {
        StorageAdapter.initPluginManager(TestFileSystemPluginManagers.withoutProviders("AZURE"));
        try {
            CreateResourceInfo info = new CreateResourceInfo(false, false, "azure_res", AZURE_SHAPED);
            AnalysisException refused = Assertions.assertThrows(AnalysisException.class, info::analyzeResourceType);
            Assertions.assertTrue(refused.getMessage().contains("select Azure Blob storage"), refused.getMessage());
            Assertions.assertTrue(refused.getMessage().contains("'AZURE' is not available"), refused.getMessage());

            // The other leg of the provider's guess: provider=azure with an endpoint whose host carries
            // no recognised suffix (a private link, a proxy, the Azurite emulator).
            CreateResourceInfo byProvider = new CreateResourceInfo(false, false, "azure_res", ImmutableMap.of(
                    "type", "s3", "provider", "azure", "s3.endpoint", "https://storage.internal.example:10000",
                    "s3.access_key", "ak", "s3.secret_key", "sk"));
            AnalysisException refusedByProvider =
                    Assertions.assertThrows(AnalysisException.class, byProvider::analyzeResourceType);
            Assertions.assertTrue(refusedByProvider.getMessage().contains("'AZURE' is not available"),
                    refusedByProvider.getMessage());

            CreateResourceInfo s3 = new CreateResourceInfo(false, false, "s3_res", ImmutableMap.of(
                    "type", "s3", "s3.endpoint", "s3.us-east-1.amazonaws.com"));
            s3.analyzeResourceType();
            Assertions.assertEquals(ResourceType.S3, s3.getResourceType());
        } finally {
            StorageAdapter.initPluginManager(null);
        }
    }
}
