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

package org.apache.doris.connector.iceberg;

import org.apache.iceberg.ManifestContent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link IcebergManifestEntryKey} (T08). The key is path + content; two manifests that share a
 * path (across tables) hit the same cache entry, which is what lets a REFRESH TABLE keep the manifest cache.
 */
public class IcebergManifestEntryKeyTest {

    @Test
    public void samePathSameContentAreEqual() {
        IcebergManifestEntryKey a = new IcebergManifestEntryKey("/m/shared.avro", ManifestContent.DATA);
        IcebergManifestEntryKey b = new IcebergManifestEntryKey("/m/shared.avro", ManifestContent.DATA);
        // WHY: the path-keyed cache must treat two references to the same manifest path as one entry (cross-table
        // sharing). MUTATION: keying on identity instead of (path, content) -> not equal -> red.
        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    public void differentContentSamePathAreNotEqual() {
        IcebergManifestEntryKey data = new IcebergManifestEntryKey("/m/x.avro", ManifestContent.DATA);
        IcebergManifestEntryKey deletes = new IcebergManifestEntryKey("/m/x.avro", ManifestContent.DELETES);
        // A data manifest and a delete manifest are distinct cache entries even at the same path. MUTATION:
        // ignoring content -> equal -> red.
        Assertions.assertNotEquals(data, deletes);
    }

    @Test
    public void differentPathAreNotEqual() {
        Assertions.assertNotEquals(
                new IcebergManifestEntryKey("/m/a.avro", ManifestContent.DATA),
                new IcebergManifestEntryKey("/m/b.avro", ManifestContent.DATA));
    }
}
