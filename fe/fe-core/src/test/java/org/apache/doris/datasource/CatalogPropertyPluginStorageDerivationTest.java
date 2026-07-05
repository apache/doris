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

package org.apache.doris.datasource;

import org.apache.doris.datasource.property.storage.HdfsProperties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Design S8: for a plugin catalog the connector owns storage-property derivation, so {@link CatalogProperty}
 * folds the connector-supplied defaults (via {@link CatalogProperty#setPluginDerivedStorageDefaultsSupplier})
 * into BOTH the raw fe-filesystem bind map ({@link CatalogProperty#getEffectiveRawStorageProperties}) and the
 * typed BE storage map ({@link CatalogProperty#getStoragePropertiesMap}) WITHOUT parsing fe-core
 * {@code MetastoreProperties}. This is what lets the fe-core Iceberg/Paimon MetastoreProperties cluster be
 * retired (their factory is un-registered, so any getMetastoreProperties() on the plugin path would throw).
 */
public class CatalogPropertyPluginStorageDerivationTest {

    private static CatalogProperty hadoopIcebergCatalog() {
        Map<String, String> props = new HashMap<>();
        props.put("type", "iceberg");
        props.put("iceberg.catalog.type", "hadoop");
        props.put(HdfsProperties.FS_HDFS_SUPPORT, "true");
        props.put("warehouse", "hdfs://realns/wh");
        return new CatalogProperty(null, props);
    }

    @Test
    public void pluginSupplierFoldsDerivedDefaultsIntoBothMaps() {
        CatalogProperty cp = hadoopIcebergCatalog();
        // A value the fe-core metastore path would NOT produce (that path derives hdfs://realns from warehouse):
        // asserting hdfs://from-connector proves the plugin path uses the connector supplier and does not
        // re-derive from MetastoreProperties. MUTATION: route resolveDerivedStorageDefaults back through
        // getMetastoreProperties() -> value becomes hdfs://realns -> red.
        cp.setPluginDerivedStorageDefaultsSupplier(
                () -> Collections.singletonMap("fs.defaultFS", "hdfs://from-connector"));
        // Raw supplier (fe-filesystem bind path).
        Assertions.assertEquals("hdfs://from-connector",
                cp.getEffectiveRawStorageProperties().get("fs.defaultFS"));
        // Typed supplier (BE storage map / URI normalization path): same folded default.
        Assertions.assertEquals("hdfs://from-connector",
                cp.getStoragePropertiesMap().values().iterator().next().getOrigProps().get("fs.defaultFS"));
    }

    @Test
    public void pluginSupplierEmptyYieldsNoDerivedFs() {
        // A rest/vended catalog: the connector derives nothing, so the raw map carries the user props unchanged
        // and no synthesized fs.defaultFS. MUTATION: fall back to a warehouse bridge -> red.
        Map<String, String> props = new HashMap<>();
        props.put("type", "iceberg");
        props.put("iceberg.catalog.type", "rest");
        props.put("iceberg.rest.uri", "http://localhost:8181");
        CatalogProperty cp = new CatalogProperty(null, props);
        cp.setPluginDerivedStorageDefaultsSupplier(Collections::emptyMap);
        Map<String, String> raw = cp.getEffectiveRawStorageProperties();
        Assertions.assertFalse(raw.containsKey("fs.defaultFS"));
        Assertions.assertEquals("http://localhost:8181", raw.get("iceberg.rest.uri"));
    }

    @Test
    public void derivedDefaultsNeverMutatePersistedProps() {
        CatalogProperty cp = hadoopIcebergCatalog();
        cp.setPluginDerivedStorageDefaultsSupplier(
                () -> Collections.singletonMap("fs.defaultFS", "hdfs://from-connector"));
        cp.getEffectiveRawStorageProperties();
        Assertions.assertFalse(cp.getProperties().containsKey("fs.defaultFS"),
                "persisted props must not gain the derived fs.defaultFS");
    }
}
