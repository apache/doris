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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.iceberg.dlf.DLFCatalog;
import org.apache.doris.datasource.property.PropertyConverter;
import org.apache.doris.datasource.property.constants.HMSProperties;

import com.aliyun.datalake.metastore.common.DataLakeConfig;

import java.util.Map;

public class IcebergDLFExternalCatalog extends IcebergExternalCatalog {

    public IcebergDLFExternalCatalog(long catalogId, String name, String resource, Map<String, String> props,
            String comment) {
        super(catalogId, name, comment);
        props.put(HMSProperties.HIVE_METASTORE_TYPE, "dlf");
        props = PropertyConverter.convertToMetaProperties(props);
        catalogProperty = new CatalogProperty(resource, props);
    }

    @Override
    protected void initCatalog() {
        icebergCatalogType = ICEBERG_DLF;
        DLFCatalog dlfCatalog = new DLFCatalog();
        dlfCatalog.setConf(getConfiguration());
        // initialize catalog
        Map<String, String> catalogProperties = catalogProperty.getHadoopProperties();
        String dlfUid = catalogProperties.get(DataLakeConfig.CATALOG_USER_ID);
        dlfCatalog.initialize(dlfUid, catalogProperties);
        catalog = dlfCatalog;
    }
}
