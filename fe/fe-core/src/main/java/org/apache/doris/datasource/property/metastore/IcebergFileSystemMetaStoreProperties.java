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
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.property.storage.HdfsProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IcebergFileSystemMetaStoreProperties extends AbstractIcebergProperties {

    @Override
    public String getIcebergCatalogType() {
        return IcebergExternalCatalog.ICEBERG_HADOOP;
    }

    public IcebergFileSystemMetaStoreProperties(Map<String, String> props) {
        super(props);
    }

    @Override
    public Catalog initializeCatalog(String catalogName, List<StorageProperties> storagePropertiesList) {
        Configuration configuration = buildConfiguration(storagePropertiesList);
        Map<String, String> catalogProps = buildCatalogProps(storagePropertiesList);

        HadoopCatalog catalog = new HadoopCatalog();
        catalog.setConf(configuration);
        try {
            this.executionAuthenticator.execute(() -> {
                catalog.initialize(catalogName, catalogProps);
                return null;
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize iceberg filesystem catalog: "
                    + ExceptionUtils.getRootCauseMessage(e), e);
        }

        return catalog;
    }

    private Configuration buildConfiguration(List<StorageProperties> storagePropertiesList) {
        Configuration configuration = new Configuration();
        for (StorageProperties sp : storagePropertiesList) {
            if (sp.getHadoopStorageConfig() != null) {
                configuration.addResource(sp.getHadoopStorageConfig());
            }
        }
        return configuration;
    }

    private Map<String, String> buildCatalogProps(List<StorageProperties> storagePropertiesList) {
        Map<String, String> props = new HashMap<>(origProps);

        if (storagePropertiesList.size() == 1 && storagePropertiesList.get(0) instanceof HdfsProperties) {
            HdfsProperties hdfsProps = (HdfsProperties) storagePropertiesList.get(0);
            if (hdfsProps.isKerberos()) {
                props.put(CatalogProperties.FILE_IO_IMPL,
                        "org.apache.doris.datasource.iceberg.fileio.DelegateFileIO");
                this.executionAuthenticator = new HadoopExecutionAuthenticator(hdfsProps.getHadoopAuthenticator());
            }
        }

        props.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
        return props;
    }

}
