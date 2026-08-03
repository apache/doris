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

package org.apache.doris.connector.metastore.spi;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Pins the shared storage-overlay + alias helpers. {@code applyStorageConfig} is the most
 * parity-fragile code in the module (the {@code paimon.s3.}/{@code fs.oss.} -> {@code fs.s3a.} re-key)
 * and is exercised here directly so a regression cannot slip through the backend tests.
 */
public class MetaStoreParseUtilsTest {

    private static Map<String, String> raw(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    private static Map<String, String> overlay(Map<String, String> storage, Map<String, String> props) {
        Map<String, String> out = new LinkedHashMap<>();
        MetaStoreParseUtils.applyStorageConfig(storage, props, out::put);
        return out;
    }

    @Test
    public void reKeysPaimonStoragePrefixesToFsS3a() {
        Map<String, String> out = overlay(new HashMap<>(), raw(
                "paimon.s3.access-key", "ak",
                "paimon.s3a.path.style.access", "true",
                "paimon.fs.s3.region", "us-east-1",
                "paimon.fs.oss.endpoint", "oss-ep"));
        Assertions.assertEquals("ak", out.get("fs.s3a.access-key"));
        Assertions.assertEquals("true", out.get("fs.s3a.path.style.access"));
        Assertions.assertEquals("us-east-1", out.get("fs.s3a.region"));
        Assertions.assertEquals("oss-ep", out.get("fs.s3a.endpoint"));
        // the original prefixed keys are NOT carried verbatim
        Assertions.assertFalse(out.containsKey("paimon.s3.access-key"));
    }

    @Test
    public void passesThroughHadoopFsDfsAndDropsUnrelatedKeys() {
        Map<String, String> out = overlay(new HashMap<>(), raw(
                "fs.defaultFS", "hdfs://nn",
                "dfs.nameservices", "ns",
                "hadoop.security.authentication", "kerberos",
                "warehouse", "oss://b/wh",
                "some.random.key", "x"));
        Assertions.assertEquals("hdfs://nn", out.get("fs.defaultFS"));
        Assertions.assertEquals("ns", out.get("dfs.nameservices"));
        Assertions.assertEquals("kerberos", out.get("hadoop.security.authentication"));
        // non-storage keys are dropped
        Assertions.assertFalse(out.containsKey("warehouse"));
        Assertions.assertFalse(out.containsKey("some.random.key"));
    }

    @Test
    public void storageConfigIsOverlaidFirstThenPropsWin() {
        Map<String, String> storage = raw("fs.s3a.endpoint", "from-storage", "fs.s3a.region", "us-west-2");
        // explicit fs.s3a.endpoint in props (last-write-wins) overrides the canonical storage value
        Map<String, String> out = overlay(storage, raw("fs.s3a.endpoint", "from-props"));
        Assertions.assertEquals("from-props", out.get("fs.s3a.endpoint"));
        Assertions.assertEquals("us-west-2", out.get("fs.s3a.region"));
    }

    @Test
    public void firstNonBlankSkipsBlanksAndHonoursAliasOrder() {
        Assertions.assertEquals("x", MetaStoreParseUtils.firstNonBlank(raw("a", "  ", "b", "x"), "a", "b"));
        Assertions.assertEquals("y", MetaStoreParseUtils.firstNonBlank(raw("a", "y", "b", "x"), "a", "b"));
        Assertions.assertNull(MetaStoreParseUtils.firstNonBlank(raw(), "a", "b"));
    }
}
