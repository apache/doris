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

import java.util.HashMap;
import java.util.Map;

/**
 * Design S2: unit tests for {@link CatalogProperty#getEffectiveRawStorageProperties()} — the raw storage map a
 * plugin catalog hands fe-filesystem to bind directly (no fe-core {@code StorageProperties.createAll}
 * round-trip). The invariant that de-risks the whole cut: this map is byte-identical to what the fe-core parse
 * path exposes via {@code getStoragePropertiesMap().values().iterator().next().getOrigProps()}, so binding
 * either yields the same typed storage and the same BE {@code location.*} map. Also pins that the derived
 * warehouse -> fs.defaultFS defaults survive and (design S4) that the removed vended gate no longer empties
 * the map for a vended catalog.
 */
public class CatalogPropertyEffectiveRawStoragePropsTest {

    /** A hadoop-flavored native iceberg catalog (real MetastoreProperties + HDFS storage, all built offline). */
    private static CatalogProperty hadoopIceberg(String warehouse) {
        Map<String, String> props = new HashMap<>();
        props.put("type", "iceberg");
        props.put("iceberg.catalog.type", "hadoop");
        props.put(HdfsProperties.FS_HDFS_SUPPORT, "true");
        if (warehouse != null) {
            props.put("warehouse", warehouse);
        }
        return new CatalogProperty(null, props);
    }

    @Test
    public void effectiveRawEqualsGetOrigProps() {
        // The fe-filesystem bind path (getEffectiveRawStorageProperties) and the fe-core parse path
        // (getStoragePropertiesMap -> getOrigProps) must feed byte-identical raw maps, so binding either yields
        // the same typed storage / BE location.* map. MUTATION: skip the derived merge or the vended gate in
        // either path -> the maps diverge -> red.
        CatalogProperty cp = hadoopIceberg("hdfs://nsbridge/wh");
        Map<String, String> viaBind = cp.getEffectiveRawStorageProperties();
        Map<String, String> viaOrigProps =
                cp.getStoragePropertiesMap().values().iterator().next().getOrigProps();
        Assertions.assertEquals(viaOrigProps, viaBind);
    }

    @Test
    public void effectiveRawCarriesDerivedDefaultFs() {
        // A hadoop catalog with ONLY warehouse (no inline fs.defaultFS): the derived warehouse -> fs.defaultFS
        // default must be present in the raw map handed to fe-filesystem, else HA-nameservice / warehouse-only
        // catalogs regress. MUTATION: drop mergeDerivedStorageDefaults -> fs.defaultFS absent -> red.
        CatalogProperty cp = hadoopIceberg("hdfs://nsbridge/wh");
        Assertions.assertEquals("hdfs://nsbridge",
                cp.getEffectiveRawStorageProperties().get("fs.defaultFS"));
    }

    @Test
    public void effectiveRawDoesNotMutatePersistedProps() {
        // Derived defaults are merged into a copy; the persisted catalog map is never mutated.
        CatalogProperty cp = hadoopIceberg("hdfs://nsbridge/wh");
        cp.getEffectiveRawStorageProperties();
        Assertions.assertFalse(cp.getProperties().containsKey("fs.defaultFS"),
                "persisted props must not gain the derived fs.defaultFS");
    }

    @Test
    public void vendedCatalogRawMapNoLongerGated() {
        // Design S4: the former vended gate is removed — fe-core hands the connector the raw storage map
        // unconditionally (the connector owns static+vended precedence, overlaying vended per-table). A vended
        // REST catalog is no longer emptied by fe-core; it carries no static object-store keys, so a downstream
        // fe-filesystem bind still yields no static storage, but the map itself is un-gated. MUTATION: re-add a
        // vended gate returning empty -> the raw props vanish -> red.
        Map<String, String> props = new HashMap<>();
        props.put("type", "iceberg");
        props.put("iceberg.catalog.type", "rest");
        props.put("iceberg.rest.uri", "http://localhost:8181");
        props.put("iceberg.rest.vended-credentials-enabled", "true");
        CatalogProperty cp = new CatalogProperty(null, props);
        Map<String, String> raw = cp.getEffectiveRawStorageProperties();
        Assertions.assertFalse(raw.isEmpty(), "S4: vended no longer gates the raw storage map to empty");
        Assertions.assertEquals("http://localhost:8181", raw.get("iceberg.rest.uri"),
                "the raw catalog props are handed over un-gated");
    }
}
