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

import org.apache.doris.common.security.authentication.HadoopExecutionAuthenticator;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.datasource.property.storage.HdfsProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;

import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.FileSystemCatalogFactory;

import java.util.List;
import java.util.Map;

public class PaimonFileSystemMetaStoreProperties extends AbstractPaimonProperties {
    protected PaimonFileSystemMetaStoreProperties(Map<String, String> props) {
        super(props);
    }

    @Override
    public Catalog initializeCatalog(String catalogName, List<StorageProperties> storagePropertiesList) {
        buildCatalogOptions(storagePropertiesList);
        Configuration conf = new Configuration(false);
        storagePropertiesList.forEach(storageProperties -> {
            for (Map.Entry<String, String> entry : storageProperties.getHadoopStorageConfig()) {
                catalogOptions.set(entry.getKey(), entry.getValue());
            }
            conf.addResource(storageProperties.getHadoopStorageConfig());
            if (storageProperties.getType().equals(StorageProperties.Type.HDFS)) {
                this.executionAuthenticator = new HadoopExecutionAuthenticator(((HdfsProperties) storageProperties)
                        .getHadoopAuthenticator());
            }
        });

        CatalogContext catalogContext = CatalogContext.create(catalogOptions, conf);
        try {
            return this.executionAuthenticator.execute(() -> CatalogFactory.createCatalog(catalogContext));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void appendCustomCatalogOptions() {
        //nothing need to do
    }

    @Override
    protected String getMetastoreType() {
        return FileSystemCatalogFactory.IDENTIFIER;
    }

    @Override
    public String getPaimonCatalogType() {
        return PaimonExternalCatalog.PAIMON_FILESYSTEM;
    }
}
