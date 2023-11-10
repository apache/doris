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

import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.property.constants.HMSProperties;
import org.apache.doris.datasource.property.constants.PaimonProperties;

import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

public class PaimonHMSExternalCatalog extends PaimonExternalCatalog {
    private static final Logger LOG = LogManager.getLogger(PaimonHMSExternalCatalog.class);
    private static final List<String> REQUIRED_PROPERTIES = ImmutableList.of(
            HMSProperties.HIVE_METASTORE_URIS
    );

    public PaimonHMSExternalCatalog(long catalogId, String name, String resource,
            Map<String, String> props, String comment) {
        super(catalogId, name, comment);
        catalogProperty = new CatalogProperty(resource, props);
    }

    @Override
    protected void initLocalObjectsImpl() {
        catalogType = PAIMON_HMS;
        catalog = createCatalog();
    }

    @Override
    protected void setPaimonCatalogOptions(Map<String, String> properties, Map<String, String> options) {
        options.put(PaimonProperties.PAIMON_CATALOG_TYPE, getPaimonCatalogType(catalogType));
        options.put(PaimonProperties.HIVE_METASTORE_URIS, properties.get(HMSProperties.HIVE_METASTORE_URIS));
    }

    @Override
    public void checkProperties() throws DdlException {
        super.checkProperties();
        for (String requiredProperty : REQUIRED_PROPERTIES) {
            if (!catalogProperty.getProperties().containsKey(requiredProperty)) {
                throw new DdlException("Required property '" + requiredProperty + "' is missing");
            }
        }
    }
}
