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

import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.CatalogIf;

import java.util.Map;

public class IcebergExternalCatalogFactory {

    public static CatalogIf createCatalog(long catalogId, String name, String resource, Map<String, String> props)
            throws DdlException {
        String catalogType = props.get(IcebergExternalCatalog.ICEBERG_CATALOG_TYPE);
        if (catalogType == null) {
            throw new DdlException("Missing " + IcebergExternalCatalog.ICEBERG_CATALOG_TYPE + " property");
        }
        switch (catalogType) {
            case IcebergExternalCatalog.ICEBERG_REST:
                return new IcebergRestExternalCatalog(catalogId, name, resource, catalogType, props);
            case IcebergExternalCatalog.ICEBERG_HMS:
                return new IcebergHMSExternalCatalog(catalogId, name, resource, catalogType, props);
            default:
                throw new DdlException("Unknown " + IcebergExternalCatalog.ICEBERG_CATALOG_TYPE
                    + " value: " + catalogType);
        }
    }
}
