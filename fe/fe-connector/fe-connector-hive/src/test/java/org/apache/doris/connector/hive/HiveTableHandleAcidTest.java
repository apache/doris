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

package org.apache.doris.connector.hive;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests the ACID classification derived from a {@link HiveTableHandle}'s metastore parameters.
 *
 * <p>WHY: the scan path branches on these flags. {@code isTransactional} decides whether the ACID
 * descent runs at all; {@code isFullAcid} (transactional AND not insert-only) decides whether delete
 * deltas and the bucket_ file filter apply. Getting insert-only vs full-ACID wrong either skips real
 * row deletes or mis-filters data files.</p>
 */
public class HiveTableHandleAcidTest {

    private HiveTableHandle handleWithParams(Map<String, String> params) {
        return new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .inputFormat("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat")
                .tableParameters(params)
                .build();
    }

    @Test
    public void testNonTransactionalByDefault() {
        HiveTableHandle handle = handleWithParams(new HashMap<>());
        Assertions.assertFalse(handle.isTransactional());
        Assertions.assertFalse(handle.isFullAcid());
    }

    @Test
    public void testFullAcidWhenTransactionalAndNotInsertOnly() {
        Map<String, String> params = new HashMap<>();
        params.put("transactional", "true");
        HiveTableHandle handle = handleWithParams(params);
        Assertions.assertTrue(handle.isTransactional());
        Assertions.assertTrue(handle.isFullAcid());
    }

    @Test
    public void testInsertOnlyIsTransactionalButNotFullAcid() {
        Map<String, String> params = new HashMap<>();
        params.put("transactional", "true");
        params.put("transactional_properties", "insert_only");
        HiveTableHandle handle = handleWithParams(params);
        Assertions.assertTrue(handle.isTransactional());
        Assertions.assertFalse(handle.isFullAcid(),
                "insert_only ACID tables have no row-level deletes");
    }

    @Test
    public void testInsertOnlyDetectionIsCaseInsensitive() {
        Map<String, String> params = new HashMap<>();
        params.put("transactional", "true");
        params.put("transactional_properties", "INSERT_ONLY");
        Assertions.assertFalse(handleWithParams(params).isFullAcid(),
                "insert_only detection must be case-insensitive");
    }

    @Test
    public void testTransactionalTrueIsCaseInsensitive() {
        Map<String, String> params = new HashMap<>();
        params.put("transactional", "TRUE");
        Assertions.assertTrue(handleWithParams(params).isTransactional());

        Map<String, String> upperKey = new HashMap<>();
        upperKey.put("TRANSACTIONAL", "true");
        Assertions.assertTrue(handleWithParams(upperKey).isTransactional(),
                "the upper-cased parameter key is accepted as a fallback");
    }
}
