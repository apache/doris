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

package org.apache.doris.datasource.property.storage;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/** Phase A5 golden tests freezing current fe-core AzureProperties behaviour. */
public class AzurePropertiesParityTest {

    private static Map<String, String> sharedKeyProps() {
        Map<String, String> props = new HashMap<>();
        props.put("provider", "azure");
        props.put("azure.account_name", "myacct");
        props.put("azure.account_key", "mykey");
        props.put("container", "mycontainer");
        return props;
    }

    @Test
    public void testIdentity() {
        StorageProperties sp = StorageProperties.createPrimary(sharedKeyProps());
        Assertions.assertTrue(sp instanceof AzureProperties);
        Assertions.assertEquals(StorageProperties.Type.AZURE, sp.getType());
        Assertions.assertEquals("AZURE", sp.getStorageName());
        Assertions.assertEquals(ImmutableSet.of("wasb", "wasbs", "abfs", "abfss"),
                ((AzureProperties) sp).schemas());
    }

    @Test
    public void testSharedKeyBackendMapExact() {
        StorageProperties sp = StorageProperties.createPrimary(sharedKeyProps());
        // Endpoint is formatted from account name when absent.
        ParityAsserts.assertExactMap(ParityAsserts.map(
                "AWS_ENDPOINT", "https://myacct.blob.core.windows.net",
                "AWS_REGION", "dummy_region",
                "AWS_ACCESS_KEY", "myacct",
                "AWS_SECRET_KEY", "mykey",
                "AWS_NEED_OVERRIDE_ENDPOINT", "true",
                "provider", "azure",
                "use_path_style", "false"
        ), sp.getBackendConfigProperties());
    }

    @Test
    public void testSharedKeyHadoopConfig() {
        StorageProperties sp = StorageProperties.createPrimary(sharedKeyProps());
        Configuration conf = sp.getHadoopStorageConfig();
        ParityAsserts.assertConfContains(conf, ParityAsserts.map(
                "fs.azure.account.key", "mykey",
                // Config.azure_blob_host_suffixes defaults include .blob.core.windows.net
                "fs.azure.account.key.myacct.blob.core.windows.net", "mykey",
                "fs.abfs.impl.disable.cache", "true",
                "fs.abfss.impl.disable.cache", "true",
                "fs.wasb.impl.disable.cache", "true",
                "fs.wasbs.impl.disable.cache", "true"
        ));
    }

    @Test
    public void testOAuth2BackendDumpsWholeConf() {
        // 2.4-7: with OAuth2 the backend map is a dump of the entire Hadoop
        // Configuration — including hadoop defaults and user fs.* passthrough.
        Map<String, String> props = new HashMap<>();
        props.put("provider", "azure");
        props.put("azure.auth_type", "OAuth2");
        props.put("azure.oauth2_account_host", "myacct.dfs.core.windows.net");
        props.put("azure.oauth2_client_id", "cid");
        props.put("azure.oauth2_client_secret", "csec");
        props.put("azure.oauth2_server_uri", "https://login.microsoftonline.com/tid/oauth2/token");
        props.put("iceberg.catalog.type", "rest");
        props.put("fs.custom.flag", "v1");
        StorageProperties sp = StorageProperties.createPrimary(props);
        Map<String, String> backend = sp.getBackendConfigProperties();
        Assertions.assertEquals("OAuth",
                backend.get("fs.azure.account.auth.type.myacct.dfs.core.windows.net"));
        Assertions.assertEquals("org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                backend.get("fs.azure.account.oauth.provider.type.myacct.dfs.core.windows.net"));
        Assertions.assertEquals("cid",
                backend.get("fs.azure.account.oauth2.client.id.myacct.dfs.core.windows.net"));
        Assertions.assertEquals("csec",
                backend.get("fs.azure.account.oauth2.client.secret.myacct.dfs.core.windows.net"));
        Assertions.assertEquals("https://login.microsoftonline.com/tid/oauth2/token",
                backend.get("fs.azure.account.oauth2.client.endpoint.myacct.dfs.core.windows.net"));
        // user fs.* passthrough lands in the dump
        Assertions.assertEquals("v1", backend.get("fs.custom.flag"));
        // hadoop defaults prove it dumps the whole Configuration
        Assertions.assertNotNull(backend.get("io.file.buffer.size"));
    }

    @Test
    public void testOAuth2OutsideIcebergRestThrows() {
        Map<String, String> props = new HashMap<>();
        props.put("provider", "azure");
        props.put("azure.auth_type", "OAuth2");
        props.put("azure.oauth2_account_host", "myacct.dfs.core.windows.net");
        props.put("azure.oauth2_client_id", "cid");
        props.put("azure.oauth2_client_secret", "csec");
        props.put("azure.oauth2_server_uri", "https://login.microsoftonline.com/tid/oauth2/token");
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> StorageProperties.createPrimary(props));
    }
}
