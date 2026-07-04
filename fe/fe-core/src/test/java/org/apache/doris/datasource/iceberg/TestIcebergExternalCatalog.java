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

import java.util.Map;

/**
 * Minimal concrete {@link IcebergExternalCatalog} for tests.
 *
 * <p>The concrete iceberg catalog flavor subclasses
 * ({@code Iceberg{HMS,Glue,Hadoop,Jdbc,S3Tables,DLF,Rest}ExternalCatalog}) were removed as dead code
 * after the iceberg SPI flip routed native iceberg catalogs through {@code PluginDrivenExternalCatalog}.
 * The abstract base class stays (still live via the HMS-iceberg path) but cannot be instantiated
 * directly, so tests that need a concrete {@code IcebergExternalCatalog} fixture use this trivial
 * subclass. It reproduces the deleted flavors' empty constructor: populate {@code catalogProperty}
 * from {@code resource} + {@code props} and nothing else.</p>
 */
public class TestIcebergExternalCatalog extends IcebergExternalCatalog {

    public TestIcebergExternalCatalog(long catalogId, String name, String resource, Map<String, String> props,
            String comment) {
        super(catalogId, name, comment);
        catalogProperty = new CatalogProperty(resource, props);
    }
}
