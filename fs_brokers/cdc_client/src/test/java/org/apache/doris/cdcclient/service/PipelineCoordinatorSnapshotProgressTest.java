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

package org.apache.doris.cdcclient.service;

import org.apache.doris.cdcclient.source.reader.mysql.MySqlSourceReader;
import org.apache.doris.cdcclient.source.reader.postgres.PostgresSourceReader;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PipelineCoordinatorSnapshotProgressTest {
    @Test
    void putsPostgresMinimumFirstAndPreservesAllSplitIds() {
        List<Map<String, String>> offsets = new ArrayList<>(List.of(pg(0, "100"), pg(1, "9"), pg(2, "20")));
        PipelineCoordinator.putMinimumWatermarkFirst(new PostgresSourceReader(), offsets);
        assertEquals(List.of(compact(1, "9"), pg(0, "100"), pg(2, "20")), offsets);
    }

    @Test
    void acceptsBatchWithNoncontiguousIdsWithoutInferringProgress() {
        List<Map<String, String>> offsets = new ArrayList<>(List.of(pg(81, "200"), pg(0, "100")));
        PipelineCoordinator.putMinimumWatermarkFirst(new PostgresSourceReader(), offsets);
        assertEquals(List.of(compact(0, "100"), pg(81, "200")), offsets);
    }

    @Test
    void comparesMysqlPositionNumericallyAndKeepsCompleteOffset() {
        List<Map<String, String>> offsets = new ArrayList<>(List.of(mysql(0, "100"), mysql(1, "9")));
        PipelineCoordinator.putMinimumWatermarkFirst(new MySqlSourceReader(), offsets);
        assertEquals("9", offsets.get(0).get("pos"));
        assertEquals("public.orders:1", offsets.get(0).get("splitId"));
        assertEquals("1", offsets.get(0).get("_compact"));
        assertEquals("mysql-bin.000001", offsets.get(0).get("file"));
        assertEquals("SPECIFIC", offsets.get(0).get("kind"));
        assertEquals(mysql(0, "100"), offsets.get(1));
    }

    private static Map<String, String> compact(int id, String lsn) {
        return Map.of("splitId", "public.orders:" + id, "lsn", lsn, "_compact", "1");
    }

    private static Map<String, String> pg(int id, String lsn) {
        return Map.of("splitId", "public.orders:" + id, "lsn", lsn);
    }

    private static Map<String, String> mysql(int id, String pos) {
        return Map.of("splitId", "public.orders:" + id, "file", "mysql-bin.000001", "pos", pos,
                "event", "0", "row", "0", "kind", "SPECIFIC", "ts_sec", "0");
    }
}
