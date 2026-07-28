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


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Design S8: the connector owns storage-property derivation, so {@link CatalogProperty} folds the
 * connector-supplied defaults (via {@link CatalogProperty#setPluginDerivedStorageDefaultsSupplier})
 * into BOTH the raw fe-filesystem bind map ({@link CatalogProperty#getEffectiveRawStorageProperties}) and the
 * typed BE storage map ({@link CatalogProperty#getStorageAdaptersMap}). This is the sole guard on that path
 * now that the fe-core metastore-property cluster is retired: fe-core has no second way to derive storage
 * defaults, so an unwired supplier must fail loud rather than silently derive nothing.
 */
public class CatalogPropertyPluginStorageDerivationTest {

    private static CatalogProperty hadoopIcebergCatalog() {
        Map<String, String> props = new HashMap<>();
        props.put("type", "iceberg");
        props.put("iceberg.catalog.type", "hadoop");
        // literal, matching upstream #66004 (the fe-core HdfsProperties constant went with the typed hierarchy)
        props.put("fs.hdfs.support", "true");
        props.put("warehouse", "hdfs://realns/wh");
        return new CatalogProperty(null, props);
    }

    @Test
    public void pluginSupplierFoldsDerivedDefaultsIntoBothMaps() {
        CatalogProperty cp = hadoopIcebergCatalog();
        // hdfs://from-connector is deliberately NOT derivable from any property below (a warehouse bridge
        // would yield hdfs://realns), so asserting it proves the value travelled through the connector
        // supplier. MUTATION: drop the derived-defaults fold in mergeDerivedStorageDefaults (or have
        // resolveDerivedStorageDefaults ignore the supplier) -> both assertions go null -> red.
        cp.setPluginDerivedStorageDefaultsSupplier(
                () -> Collections.singletonMap("fs.defaultFS", "hdfs://from-connector"));
        // Raw supplier (fe-filesystem bind path).
        Assertions.assertEquals("hdfs://from-connector",
                cp.getEffectiveRawStorageProperties().get("fs.defaultFS"));
        // Typed supplier (BE storage map / URI normalization path): same folded default.
        Assertions.assertEquals("hdfs://from-connector",
                cp.getStorageAdaptersMap().values().iterator().next().getOrigProps().get("fs.defaultFS"));
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

    @Test
    public void unwiredSupplierFailsLoudInsteadOfDerivingNothing() {
        // Retiring the fe-core metastore parse left the connector supplier as the ONLY derivation source, so
        // reading storage before it is wired can no longer fall back to anything. It must throw: silently
        // deriving nothing would drop the warehouse -> fs.defaultFS bridge AND cache the under-derived
        // StorageBindings for good, because setPluginDerivedStorageDefaultsSupplier deliberately does not
        // reset caches -- a later correct wiring would never repair it. No production path reaches this today
        // (every catalog on the storage path is plugin-driven and no connector touches storage while being
        // constructed); this pins that a future one fails visibly.
        // MUTATION: return Collections.emptyMap() instead of throwing -> red.
        CatalogProperty cp = hadoopIcebergCatalog();
        Assertions.assertThrows(IllegalStateException.class, cp::getEffectiveRawStorageProperties);
        Assertions.assertThrows(IllegalStateException.class, cp::getStorageAdaptersMap);
    }
}
