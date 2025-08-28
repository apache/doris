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

import org.apache.doris.datasource.property.storage.StorageProperties;

import org.apache.paimon.catalog.Catalog;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Disabled("only used for your local test")
public class PaimonCatalogTest {
    @Test
    public void testNameSpace() throws Exception {
        Map<String, String> pa = new HashMap<>();
        pa.put("type", "paimon");
        pa.put("paimon.catalog.type", "hms");
        pa.put("hive.metastore.uris", "thrift://172.20.48.119:9383");
        pa.put("warehouse", "s3a://doris/paimon_warehouse");
        pa.put("s3.region", "ap-east-1");

        // User must provide real Access Key / Secret Key to enable initialization
        pa.put("s3.access_key", "");
        pa.put("s3.secret_key", "");
        pa.put("s3.endpoint", "s3.ap-east-1.amazonaws.com");

        Catalog catalog = initCatalog(pa);
        if (catalog != null) {
            catalog.listDatabases().forEach(System.out::println);
        }
    }

    /**
     * Initializes a Paimon HMS Catalog.
     * <p>
     * Initialization is skipped by default. Users must provide valid S3
     * Access Key and Secret Key in the configuration map to enable it.
     * <p>
     * Steps:
     * 1. Validate that credentials are provided.
     * 2. Normalize and check metastore properties.
     * 3. Create storage properties.
     * 4. Initialize and return the Catalog instance.
     *
     * @param params A map containing the configuration parameters.
     * @return Catalog instance if initialized, or {@code null} if skipped.
     * @throws Exception If initialization fails.
     */
    private Catalog initCatalog(Map<String, String> params) throws Exception {
        if (isDisabled(params)) {
            System.out.println("Catalog initialization skipped: Missing valid S3 Access Key/Secret Key.");
            return null;
        }

        AbstractPaimonProperties metaStoreProps =
                (AbstractPaimonProperties) MetastoreProperties.create(params);

        metaStoreProps.initNormalizeAndCheckProps();

        List<StorageProperties> storageProps = StorageProperties.createAll(params);

        return metaStoreProps.initializeCatalog("paimon_catalog", storageProps);
    }

    /**
     * Checks if initialization should be skipped due to missing credentials.
     *
     * @param params The configuration parameters.
     * @return {@code true} if missing AK/SK, {@code false} otherwise.
     */
    private boolean isDisabled(Map<String, String> params) {
        String ak = params.get("s3.access_key");
        String sk = params.get("s3.secret_key");
        return ak == null || ak.isEmpty() || sk == null || sk.isEmpty();
    }
}
