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

package org.apache.doris.datasource.paimon;

import org.apache.doris.datasource.property.constants.PaimonProperties;

import com.aliyun.datalake.metastore.hive2.ProxyMetaStoreClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class PaimonDLFExternalCatalog extends PaimonExternalCatalog {
    private static final Logger LOG = LogManager.getLogger(PaimonDLFExternalCatalog.class);

    public PaimonDLFExternalCatalog(long catalogId, String name, String resource,
                                    Map<String, String> props, String comment) {
        super(catalogId, name, resource, props, comment);
    }

    @Override
    protected void initLocalObjectsImpl() {
        super.initLocalObjectsImpl();
        catalogType = PAIMON_DLF;
        catalog = createCatalog();
    }

    @Override
    protected void setPaimonCatalogOptions(Map<String, String> properties, Map<String, String> options) {
        options.put(PaimonProperties.PAIMON_CATALOG_TYPE, PaimonProperties.PAIMON_HMS_CATALOG);
        options.put(PaimonProperties.PAIMON_METASTORE_CLIENT, ProxyMetaStoreClient.class.getName());
        options.put(PaimonProperties.PAIMON_OSS_ENDPOINT,
                properties.get(PaimonProperties.PAIMON_OSS_ENDPOINT));
        options.put(PaimonProperties.PAIMON_OSS_ACCESS_KEY,
                properties.get(PaimonProperties.PAIMON_OSS_ACCESS_KEY));
        options.put(PaimonProperties.PAIMON_OSS_SECRET_KEY,
                properties.get(PaimonProperties.PAIMON_OSS_SECRET_KEY));
    }
}
