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

import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;
import org.apache.doris.thrift.TTransactionalHiveDeleteDeltaDesc;
import org.apache.doris.thrift.TTransactionalHiveDesc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests that {@link HiveScanRange} carries ACID delete-delta info to the BE.
 *
 * <p>WHY: for transactional Hive tables the BE applies row-level deletes by reading the
 * delete-delta files listed per partition. The delta descriptor needs BOTH the directory
 * AND the file names within it. If the file names are dropped, the BE cannot locate the
 * delete records and silently under-deletes — returning rows that were logically deleted.
 * These tests pin the "dir|file1,file2" encode/decode round-trip end to end.</p>
 */
public class HiveScanRangeAcidTest {

    @Test
    public void testDeleteDeltaCarriesDirectoryAndFileNames() {
        HiveScanRange range = HiveScanRange.builder()
                .path("/tbl/delta_0000005_0000005/bucket_00000")
                .acidInfo("/tbl/p=1", Arrays.asList(
                        "/tbl/p=1/delete_delta_0000003_0000003|bucket_00000,bucket_00001",
                        "/tbl/p=1/delete_delta_0000004_0000004|bucket_00000"))
                .build();

        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
        range.populateRangeParams(formatDesc, new TFileRangeDesc());

        Assertions.assertTrue(formatDesc.isSetTransactionalHiveParams(),
                "transactional_hive range must emit transactional params");
        TTransactionalHiveDesc txnDesc = formatDesc.getTransactionalHiveParams();
        Assertions.assertEquals("/tbl/p=1", txnDesc.getPartition());

        List<TTransactionalHiveDeleteDeltaDesc> deltas = txnDesc.getDeleteDeltas();
        Assertions.assertEquals(2, deltas.size());

        TTransactionalHiveDeleteDeltaDesc first = deltas.get(0);
        Assertions.assertEquals("/tbl/p=1/delete_delta_0000003_0000003",
                first.getDirectoryLocation());
        // The regression: file names must survive the "dir|file1,file2" round-trip.
        Assertions.assertEquals(Arrays.asList("bucket_00000", "bucket_00001"),
                first.getFileNames());

        TTransactionalHiveDeleteDeltaDesc second = deltas.get(1);
        Assertions.assertEquals("/tbl/p=1/delete_delta_0000004_0000004",
                second.getDirectoryLocation());
        Assertions.assertEquals(Arrays.asList("bucket_00000"), second.getFileNames());
    }

    @Test
    public void testDeleteDeltaWithoutFileNamesLeavesFileNamesUnset() {
        // A directory-only encoding (no '|') must still set the directory and simply
        // carry no file names, rather than mis-parsing the whole string as a directory
        // with a bogus trailing file name.
        HiveScanRange range = HiveScanRange.builder()
                .path("/tbl/base_0000005/bucket_00000")
                .acidInfo("/tbl/p=1", Arrays.asList("/tbl/p=1/delete_delta_dir_only"))
                .build();

        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
        range.populateRangeParams(formatDesc, new TFileRangeDesc());

        TTransactionalHiveDeleteDeltaDesc delta =
                formatDesc.getTransactionalHiveParams().getDeleteDeltas().get(0);
        Assertions.assertEquals("/tbl/p=1/delete_delta_dir_only", delta.getDirectoryLocation());
        Assertions.assertFalse(delta.isSetFileNames());
    }

    @Test
    public void testNonTransactionalRangeEmitsNoTransactionalParams() {
        HiveScanRange range = HiveScanRange.builder()
                .path("/tbl/000000_0")
                .fileFormat("parquet")
                .build();

        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
        range.populateRangeParams(formatDesc, new TFileRangeDesc());

        Assertions.assertFalse(formatDesc.isSetTransactionalHiveParams());
    }
}
