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

package org.apache.doris.datasource.hive;

import org.apache.doris.common.UserException;
import org.apache.doris.filesystem.FileSystem;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link HiveUtil}, focusing on isSplittable() behavior
 * for LZO compressed text InputFormats.
 */
public class HiveUtilTest {

    private static final FileSystem MOCK_FS = Mockito.mock(FileSystem.class);
    private static final String DUMMY_LOCATION = "hdfs://namenode/warehouse/test.lzo";

    // -------------------------------------------------------------------------
    // LZO InputFormat variants: must NOT be splittable
    // -------------------------------------------------------------------------

    @Test
    public void testIsSplittable_CompressionLzoTextInputFormat_ReturnsFalse() throws UserException {
        // twitter hadoop-lzo: com.hadoop.compression.lzo.LzoTextInputFormat
        // LZO files have no global index by default, so they cannot be split.
        boolean result = HiveUtil.isSplittable(MOCK_FS,
                "com.hadoop.compression.lzo.LzoTextInputFormat", DUMMY_LOCATION);
        Assertions.assertFalse(result,
                "com.hadoop.compression.lzo.LzoTextInputFormat should not be splittable");
    }

    @Test
    public void testIsSplittable_MapreduceLzoTextInputFormat_ReturnsFalse() throws UserException {
        // lzo-hadoop (org.anarres) mapreduce API: com.hadoop.mapreduce.LzoTextInputFormat
        // LZO files have no global index by default, so they cannot be split.
        boolean result = HiveUtil.isSplittable(MOCK_FS,
                "com.hadoop.mapreduce.LzoTextInputFormat", DUMMY_LOCATION);
        Assertions.assertFalse(result,
                "com.hadoop.mapreduce.LzoTextInputFormat should not be splittable");
    }

    @Test
    public void testIsSplittable_DeprecatedLzoTextInputFormat_ReturnsFalse() throws UserException {
        // lzo-hadoop (org.anarres) legacy mapred API: com.hadoop.mapred.DeprecatedLzoTextInputFormat
        // It produces the same .lzo file format and is equally non-splittable.
        boolean result = HiveUtil.isSplittable(MOCK_FS,
                "com.hadoop.mapred.DeprecatedLzoTextInputFormat", DUMMY_LOCATION);
        Assertions.assertFalse(result,
                "com.hadoop.mapred.DeprecatedLzoTextInputFormat should not be splittable");
    }

    // -------------------------------------------------------------------------
    // Standard splittable formats: must still be splittable
    // -------------------------------------------------------------------------

    @Test
    public void testIsSplittable_TextInputFormat_ReturnsTrue() throws UserException {
        boolean result = HiveUtil.isSplittable(MOCK_FS,
                "org.apache.hadoop.mapred.TextInputFormat", DUMMY_LOCATION);
        Assertions.assertTrue(result,
                "org.apache.hadoop.mapred.TextInputFormat should be splittable");
    }

    @Test
    public void testIsSplittable_ParquetInputFormat_ReturnsTrue() throws UserException {
        boolean result = HiveUtil.isSplittable(MOCK_FS,
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat", DUMMY_LOCATION);
        Assertions.assertTrue(result,
                "MapredParquetInputFormat should be splittable");
    }

    @Test
    public void testIsSplittable_OrcInputFormat_ReturnsTrue() throws UserException {
        boolean result = HiveUtil.isSplittable(MOCK_FS,
                "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat", DUMMY_LOCATION);
        Assertions.assertTrue(result,
                "OrcInputFormat should be splittable");
    }

    // -------------------------------------------------------------------------
    // Unsupported format: must return false (not in whitelist)
    // -------------------------------------------------------------------------

    @Test
    public void testIsSplittable_UnsupportedFormat_ReturnsFalse() throws UserException {
        boolean result = HiveUtil.isSplittable(MOCK_FS,
                "org.apache.hadoop.mapred.SequenceFileInputFormat", DUMMY_LOCATION);
        Assertions.assertFalse(result,
                "Unsupported input format should not be splittable");
    }

    // -------------------------------------------------------------------------
    // isLzoInputFormat: class-name detection
    // -------------------------------------------------------------------------

    @Test
    public void testIsLzoInputFormat_CompressionVariant() {
        Assertions.assertTrue(HiveUtil.isLzoInputFormat("com.hadoop.compression.lzo.LzoTextInputFormat"));
    }

    @Test
    public void testIsLzoInputFormat_MapreduceVariant() {
        Assertions.assertTrue(HiveUtil.isLzoInputFormat("com.hadoop.mapreduce.LzoTextInputFormat"));
    }

    @Test
    public void testIsLzoInputFormat_DeprecatedVariant() {
        Assertions.assertTrue(HiveUtil.isLzoInputFormat("com.hadoop.mapred.DeprecatedLzoTextInputFormat"));
    }

    @Test
    public void testIsLzoInputFormat_NonLzo() {
        Assertions.assertFalse(HiveUtil.isLzoInputFormat("org.apache.hadoop.mapred.TextInputFormat"));
        Assertions.assertFalse(HiveUtil.isLzoInputFormat("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"));
    }

    @Test
    public void testIsLzoInputFormat_Null_ReturnsFalse() {
        // Null inputFormat (e.g. damaged HMS metadata) must not throw NPE; treat as non-LZO.
        Assertions.assertFalse(HiveUtil.isLzoInputFormat(null));
    }

    // -------------------------------------------------------------------------
    // isLzoDataFile: sidecar filter
    // -------------------------------------------------------------------------

    @Test
    public void testIsLzoDataFile_DataFile() {
        // Real LZO data files must be included
        Assertions.assertTrue(HiveUtil.isLzoDataFile("hdfs://namenode/warehouse/part-m-00000.lzo"));
        Assertions.assertTrue(HiveUtil.isLzoDataFile("/data/part-00001.LZO")); // case-insensitive
    }

    @Test
    public void testIsLzoDataFile_IndexSidecar_Excluded() {
        // .lzo.index sidecar files must be excluded
        Assertions.assertFalse(HiveUtil.isLzoDataFile("hdfs://namenode/warehouse/part-m-00000.lzo.index"));
        Assertions.assertFalse(HiveUtil.isLzoDataFile("hdfs://namenode/warehouse/part-m-00000.LZO.INDEX"));
    }

    @Test
    public void testIsLzoDataFile_OtherExtensions_Excluded() {
        // Other files in the partition directory must also be excluded for LZO InputFormats
        Assertions.assertFalse(HiveUtil.isLzoDataFile("hdfs://namenode/warehouse/part-m-00000"));
        Assertions.assertFalse(HiveUtil.isLzoDataFile("hdfs://namenode/warehouse/part-m-00000.gz"));
        Assertions.assertFalse(HiveUtil.isLzoDataFile("hdfs://namenode/warehouse/part-m-00000.orc"));
    }

    @Test
    public void testIsLzoDataFile_QueryStringStripped() {
        // Paths with query strings should still be recognised correctly
        Assertions.assertTrue(
                HiveUtil.isLzoDataFile("hdfs://namenode/warehouse/part-m-00000.lzo?auth=token"));
        Assertions.assertFalse(
                HiveUtil.isLzoDataFile("hdfs://namenode/warehouse/part-m-00000.lzo.index?auth=token"));
    }
}
