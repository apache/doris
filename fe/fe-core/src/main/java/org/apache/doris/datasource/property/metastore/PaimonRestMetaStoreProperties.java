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

import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.datasource.property.ConnectorProperty;
import org.apache.doris.datasource.property.ParamRules;
import org.apache.doris.datasource.property.storage.StorageProperties;

import lombok.Getter;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;

import java.util.List;
import java.util.Map;

public class PaimonRestMetaStoreProperties extends AbstractPaimonProperties {

    private static final String PAIMON_REST_PROPERTY_PREFIX = "paimon.rest.";

    @ConnectorProperty(names = {"paimon.rest.uri", "uri"},
            description = "The uri of the Paimon rest catalog service.")
    private String paimonRestUri = "";

    @Getter
    @ConnectorProperty(
            names = {"paimon.rest.token.provider"},
            description = "the token provider for Paimon REST metastore, e.g., 'dlf' for Aliyun DLF."
    )
    protected String tokenProvider = "";

    // The following properties are specific to DLF rest catalog
    @ConnectorProperty(
            names = {"paimon.rest.dlf.access-key-id"},
            required = false,
            description = "The access key ID for DLF, required when using DLF as token provider."
    )
    protected String paimonRestDlfAccessKey = "";

    @ConnectorProperty(
            names = {"paimon.rest.dlf.access-key-secret"},
            required = false,
            description = "The secret key secret for DLF, required when using DLF as token provider."
    )
    protected String paimonRestDlfSecretKey = "";

    protected PaimonRestMetaStoreProperties(Map<String, String> props) {
        super(props);
    }

    @Override
    public void initNormalizeAndCheckProps() {
        super.initNormalizeAndCheckProps();
        buildRules().validate();
    }

    @Override
    public String getPaimonCatalogType() {
        return PaimonExternalCatalog.PAIMON_REST;
    }

    @Override
    public Catalog initializeCatalog(String catalogName, List<StorageProperties> storagePropertiesList) {
        buildCatalogOptions(storagePropertiesList);
        CatalogContext catalogContext = CatalogContext.create(catalogOptions);
        return CatalogFactory.createCatalog(catalogContext);
    }

    @Override
    protected void appendCustomCatalogOptions() {
        catalogOptions.set("uri", paimonRestUri);
        for (Map.Entry<String, String> entry : origProps.entrySet()) {
            if (entry.getKey().startsWith(PAIMON_REST_PROPERTY_PREFIX)) {
                String key = entry.getKey().substring(PAIMON_REST_PROPERTY_PREFIX.length());
                catalogOptions.set(key, entry.getValue());
            }
        }
    }

    @Override
    protected String getMetastoreType() {
        return "rest";
    }

    private ParamRules buildRules() {
        ParamRules rules = new ParamRules();
        // Check for dlf rest catalog
        rules.requireIf(tokenProvider, "dlf",
                new String[] {paimonRestDlfAccessKey,
                        paimonRestDlfSecretKey},
                "DLF token provider requires 'paimon.rest.dlf.access-key-id' "
                        + "and 'paimon.rest.dlf.access-key-secret'");
        return rules;
    }
}
