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

package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.metastore.FileSystemMetaStoreProperties;
import org.apache.doris.connector.metastore.MetaStoreProperties;
import org.apache.doris.connector.metastore.deltalake.glue.GlueCatalogProperties;
import org.apache.doris.connector.metastore.deltalake.uc.UnityCatalogProperties;
import org.apache.doris.connector.spi.ConnectorContext;

import org.apache.hadoop.conf.Configuration;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

enum DeltaLakeCatalogType {
    FILESYSTEM((props, io, storage, context) ->
            new DeltaLakeFileSystemTableResolver(((FileSystemMetaStoreProperties) props).getWarehouse(), io)),
    UC((props, io, storage, context) -> new DeltaLakeUnityCatalog((UnityCatalogProperties) props, storage, context)),
    GLUE((props, io, storage, context) -> new DeltaLakeGlueCatalog((GlueCatalogProperties) props));

    private final Factory factory;
    private final String token = name().toLowerCase(Locale.ROOT);

    DeltaLakeCatalogType(Factory factory) {
        this.factory = factory;
    }

    String token() {
        return token;
    }

    static DeltaLakeCatalogType from(String value) {
        for (DeltaLakeCatalogType type : values()) {
            if (type.token.equalsIgnoreCase(value)) {
                return type;
            }
        }
        String supported = Arrays.stream(values()).map(DeltaLakeCatalogType::token).collect(Collectors.joining(", "));
        throw new IllegalArgumentException("Unsupported " + DeltaLakeCatalogProperties.CATALOG_TYPE + ": " + value
                + "; expected one of " + supported);
    }

    DeltaLakeTableResolver create(MetaStoreProperties props, DeltaLakeFileIO fileIO, Configuration storage,
            ConnectorContext context) {
        return factory.create(props, fileIO, storage, context);
    }

    private interface Factory {
        DeltaLakeTableResolver create(MetaStoreProperties props, DeltaLakeFileIO fileIO, Configuration storage,
                ConnectorContext context);
    }
}
