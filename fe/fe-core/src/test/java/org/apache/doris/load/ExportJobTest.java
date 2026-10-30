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

package org.apache.doris.load;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.property.fileformat.ParquetFileFormatProperties;
import org.apache.doris.datasource.storage.StorageTypeId;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class ExportJobTest {

    @Test
    public void testEnableInt96TimestampsOutfileProperty() {
        ExportJob exportJob = new ExportJob(1);
        exportJob.setFormat("parquet");
        exportJob.setMaxFileSize("");
        exportJob.setBrokerDesc(new BrokerDesc("local"));

        Map<String, String> outfileProperties =
                Deencapsulation.invoke(exportJob, "convertOutfileProperties");
        Assertions.assertFalse(outfileProperties.containsKey(
                ParquetFileFormatProperties.ENABLE_INT96_TIMESTAMPS));

        exportJob.setEnableInt96Timestamps("false");
        outfileProperties = Deencapsulation.invoke(exportJob, "convertOutfileProperties");
        Assertions.assertEquals("false", outfileProperties.get(
                ParquetFileFormatProperties.ENABLE_INT96_TIMESTAMPS));
    }

    @Test
    public void testAzureOauth2OutfilePropertiesResolveToAzure() {
        // EXPORT ... WITH S3 with Azure OAuth2. OUTFILE resolves its storage from the handed-over
        // properties again; the backend map of OAuth2 resolved to HDFS.
        Map<String, String> props = new HashMap<>();
        props.put("provider", "azure");
        props.put("azure.auth_type", "OAuth2");
        props.put("azure.oauth2_account_host", "acct.dfs.core.windows.net");
        props.put("azure.oauth2_client_id", "client");
        props.put("azure.oauth2_client_secret", "secret");
        props.put("azure.oauth2_server_uri", "https://login.microsoftonline.com/tenant-1/oauth2/token");
        ExportJob exportJob = new ExportJob(1);
        exportJob.setFormat("parquet");
        exportJob.setMaxFileSize("");
        exportJob.setBrokerDesc(new BrokerDesc("S3", StorageBackend.StorageType.S3, props));

        Map<String, String> outfileProperties =
                Deencapsulation.invoke(exportJob, "convertOutfileProperties");

        Assertions.assertEquals("OAuth2", outfileProperties.get("azure.auth_type"));
        Assertions.assertEquals(StorageTypeId.AZURE,
                new BrokerDesc(null, outfileProperties).getStorageAdapter().getType());
    }
}
