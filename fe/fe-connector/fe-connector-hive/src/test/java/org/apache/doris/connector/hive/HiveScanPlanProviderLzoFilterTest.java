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

/**
 * Tests the LZO file-filtering helpers on {@link HiveScanPlanProvider}. These reproduce legacy
 * {@code HiveExternalMetaCache}'s {@code HiveUtil.isLzoInputFormat}/{@code isLzoDataFile} filtering, which was
 * lost at the SPI cutover: {@link HiveFileListingCache} only strips {@code _}/{@code .}-prefixed hidden files,
 * so for an LZO text table the {@code *.lzo.index} sidecar (which starts with neither) would otherwise be read
 * as an extra text row — the exact failure {@code test_hive_lzo_text_format} caught (count 6 instead of 5).
 */
public class HiveScanPlanProviderLzoFilterTest {

    @Test
    public void testIsLzoInputFormatRecognizesAllVariants() {
        // The three hadoop-lzo text InputFormat variants must all be recognized.
        Assertions.assertTrue(
                HiveScanPlanProvider.isLzoInputFormat("com.hadoop.compression.lzo.LzoTextInputFormat"));
        Assertions.assertTrue(
                HiveScanPlanProvider.isLzoInputFormat("com.hadoop.mapreduce.LzoTextInputFormat"));
        Assertions.assertTrue(
                HiveScanPlanProvider.isLzoInputFormat("com.hadoop.mapred.DeprecatedLzoTextInputFormat"));
    }

    @Test
    public void testIsLzoInputFormatRejectsNonLzo() {
        // Plain text / parquet / orc tables are not LZO and must NOT trigger the sidecar filter.
        Assertions.assertFalse(
                HiveScanPlanProvider.isLzoInputFormat("org.apache.hadoop.mapred.TextInputFormat"));
        Assertions.assertFalse(HiveScanPlanProvider.isLzoInputFormat(
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"));
        Assertions.assertFalse(HiveScanPlanProvider.isLzoInputFormat(null));
    }

    @Test
    public void testIsLzoDataFileExcludesIndexSidecar() {
        // Only *.lzo is data; the *.lzo.index sidecar (and any other extension) must be excluded.
        Assertions.assertTrue(HiveScanPlanProvider.isLzoDataFile(
                "hdfs://ns/user/doris/preinstalled_data/text_lzo/part-m-00000.lzo"));
        Assertions.assertFalse(HiveScanPlanProvider.isLzoDataFile(
                "hdfs://ns/user/doris/preinstalled_data/text_lzo/part-m-00000.lzo.index"));
        // Case-insensitive extension match.
        Assertions.assertTrue(HiveScanPlanProvider.isLzoDataFile("hdfs://ns/dir/part-m-00000.LZO"));
        Assertions.assertFalse(HiveScanPlanProvider.isLzoDataFile("hdfs://ns/dir/part-m-00000.txt"));
    }

    @Test
    public void testIsLzoDataFileStripsObjectStoreQueryString() {
        // An object-store URI may carry a signed query string; the extension match must ignore it.
        Assertions.assertTrue(HiveScanPlanProvider.isLzoDataFile("s3://bucket/dir/part.lzo?sig=abc&exp=123"));
        Assertions.assertFalse(
                HiveScanPlanProvider.isLzoDataFile("s3://bucket/dir/part.lzo.index?sig=abc&exp=123"));
    }

    @Test
    public void testLzoTextDetectsAsSplittableTextSoMustBeMasked() {
        // A LZO text table's InputFormat resolves to the TEXT format, whose isSplittable() is true — the enum
        // alone would (wrongly) split a .lzo stream at byte boundaries, which cannot be decompressed from an
        // arbitrary offset. This is exactly why planScan masks splittable with !isLzoInputFormat(...): without
        // the mask, a large LZO file would be split and produce garbage.
        HiveFileFormat fmt = HiveFileFormat.detect(
                "com.hadoop.mapreduce.LzoTextInputFormat",
                "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                false, false);
        Assertions.assertEquals(HiveFileFormat.TEXT, fmt);
        Assertions.assertTrue(fmt.isSplittable());
        Assertions.assertTrue(HiveScanPlanProvider.isLzoInputFormat("com.hadoop.mapreduce.LzoTextInputFormat"));
    }
}
