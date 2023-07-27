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
import org.apache.doris.datasource.ExternalCatalog;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;

public class PaimonExternalCatalogFactory {

    public static ExternalCatalog createCatalog(long catalogId, String name, String resource, Map<String, String> props,
            String comment) throws DdlException {
        String metastoreType = props.get(PaimonExternalCatalog.PAIMON_CATALOG_TYPE);
        if (StringUtils.isEmpty(metastoreType)) {
            metastoreType = PaimonExternalCatalog.PAIMON_FILESYSTEM;
        }
        metastoreType = metastoreType.toLowerCase();
        switch (metastoreType) {
            case PaimonExternalCatalog.PAIMON_HMS:
                return new PaimonHMSExternalCatalog(catalogId, name, resource, props, comment);
            case PaimonExternalCatalog.PAIMON_FILESYSTEM:
                return new PaimonFileExternalCatalog(catalogId, name, resource, props, comment);
            default:
                throw new DdlException("Unknown " + PaimonExternalCatalog.PAIMON_CATALOG_TYPE
                        + " value: " + metastoreType);
        }
    }
}
