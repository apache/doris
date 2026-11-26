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

package org.apache.doris.datasource.scan;

import org.apache.doris.common.Config;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TLanceBlobResource;
import org.apache.doris.thrift.TLanceScanParams;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

class LanceBlobResourcePlannerTest {
    private static TLanceScanParams params(String uri, long version, String... columns) {
        TLanceScanParams params = new TLanceScanParams().setDatasetUri(uri).setVersion(version);
        Map<String, Integer> ids = new TreeMap<>();
        for (String column : columns) {
            ids.put(column, 0);
        }
        params.setBlobReadSourceIds(ids);
        return params;
    }

    @Test
    void assignsDistinctStableSourcesAcrossScansAndCopiesAccessOptions() {
        TLanceScanParams first = params("s3://bucket/a.lance", 7, "image", "audio");
        first.setLanceStorageOptions(new TreeMap<>(Collections.singletonMap("aws_endpoint", "http://minio:9000")));
        TLanceScanParams second = params("s3://bucket/a.lance", 8, "image");
        Map<Integer, TLanceBlobResource> resources = LanceBlobResourcePlanner.planParams(Arrays.asList(
                new TFileScanRangeParams().setLanceScanParams(first), new TFileScanRangeParams(),
                new TFileScanRangeParams().setLanceScanParams(second)), Config.LANCE_BLOB_MIN_BE_EXEC_VERSION);
        Assertions.assertEquals(3, resources.size());
        Assertions.assertEquals(Integer.valueOf(1), first.getBlobReadSourceIds().get("audio"));
        Assertions.assertEquals(Integer.valueOf(2), first.getBlobReadSourceIds().get("image"));
        Assertions.assertEquals(Integer.valueOf(3), second.getBlobReadSourceIds().get("image"));
        Assertions.assertEquals(7, resources.get(2).getVersion());
        Assertions.assertEquals(8, resources.get(3).getVersion());
        Assertions.assertEquals("image", resources.get(2).getColumnName());
        first.getLanceStorageOptions().clear();
        Assertions.assertEquals("http://minio:9000", resources.get(2).getStorageOptions().get("aws_endpoint"));
        Assertions.assertEquals(resources.keySet(), LanceBlobResourcePlanner.planParams(Arrays.asList(
                new TFileScanRangeParams().setLanceScanParams(first),
                new TFileScanRangeParams().setLanceScanParams(second)), Config.LANCE_BLOB_MIN_BE_EXEC_VERSION).keySet());
    }

    @Test
    void rejectsLocalUnpinnedAndOldBackendBlobPlans() {
        for (String uri : Arrays.asList("/tmp/a.lance", "file:///tmp/a.lance", "memory://a.lance")) {
            Assertions.assertThrows(IllegalStateException.class,
                    () -> LanceBlobResourcePlanner.assign(params(uri, 1, "image"),
                            Collections.singletonList("image"), 1));
        }
        Assertions.assertThrows(IllegalStateException.class,
                () -> LanceBlobResourcePlanner.assign(params("s3://bucket/a.lance", 0, "image"),
                        Collections.singletonList("image"), 1));
        Assertions.assertThrows(IllegalStateException.class,
                () -> LanceBlobResourcePlanner.planParams(Collections.singletonList(new TFileScanRangeParams()
                        .setLanceScanParams(params("s3://bucket/a.lance", 1, "image"))),
                        Config.LANCE_BLOB_MIN_BE_EXEC_VERSION - 1));
        Assertions.assertTrue(LanceBlobResourcePlanner.planParams(
                Collections.singletonList(new TFileScanRangeParams()), 19).isEmpty());
    }
}
