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
 * Tests {@link HiveFileFormat} detection (first test for fe-connector-hive; P3-T07 batch C).
 *
 * <p>WHY: the detected format selects which BE file reader runs (parquet/orc/text/json
 * scanner). Misdetection causes read failures or silent corruption. Detection is a
 * case-insensitive substring match on the InputFormat class name with a SerDe-library
 * fallback — these tests pin that contract, the inputFormat-wins precedence, and the
 * splittability of each format.</p>
 */
public class HiveFileFormatTest {

    @Test
    public void testFromInputFormatDetectsByContent() {
        Assertions.assertEquals(HiveFileFormat.PARQUET,
                HiveFileFormat.fromInputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"));
        Assertions.assertEquals(HiveFileFormat.ORC,
                HiveFileFormat.fromInputFormat("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"));
        Assertions.assertEquals(HiveFileFormat.TEXT,
                HiveFileFormat.fromInputFormat("org.apache.hadoop.mapred.TextInputFormat"));
        Assertions.assertEquals(HiveFileFormat.JSON,
                HiveFileFormat.fromInputFormat("org.apache.hadoop.hive.json.JsonInputFormat"));
    }

    @Test
    public void testFromInputFormatUnknownAndNull() {
        Assertions.assertEquals(HiveFileFormat.UNKNOWN, HiveFileFormat.fromInputFormat(null));
        Assertions.assertEquals(HiveFileFormat.UNKNOWN,
                HiveFileFormat.fromInputFormat("com.example.CustomInputFormat"));
    }

    @Test
    public void testFromSerDeLib() {
        Assertions.assertEquals(HiveFileFormat.PARQUET,
                HiveFileFormat.fromSerDeLib("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"));
        Assertions.assertEquals(HiveFileFormat.ORC,
                HiveFileFormat.fromSerDeLib("org.apache.hadoop.hive.ql.io.orc.OrcSerde"));
        Assertions.assertEquals(HiveFileFormat.TEXT,
                HiveFileFormat.fromSerDeLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"));
        Assertions.assertEquals(HiveFileFormat.TEXT,
                HiveFileFormat.fromSerDeLib("org.apache.hadoop.hive.serde2.OpenCSVSerde"));
        Assertions.assertEquals(HiveFileFormat.JSON,
                HiveFileFormat.fromSerDeLib("org.apache.hive.hcatalog.data.JsonSerDe"));
        Assertions.assertEquals(HiveFileFormat.UNKNOWN, HiveFileFormat.fromSerDeLib(null));
    }

    @Test
    public void testDetectPrefersInputFormatThenFallsBackToSerDe() {
        // inputFormat wins when recognized (even if the SerDe says otherwise)...
        Assertions.assertEquals(HiveFileFormat.PARQUET,
                HiveFileFormat.detect(
                        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                        "org.apache.hadoop.hive.ql.io.orc.OrcSerde"));
        // ...and the SerDe is the fallback when the inputFormat is unrecognized.
        Assertions.assertEquals(HiveFileFormat.TEXT,
                HiveFileFormat.detect("com.example.CustomInputFormat",
                        "org.apache.hadoop.hive.serde2.OpenCSVSerde"));
    }

    @Test
    public void testIsSplittable() {
        Assertions.assertTrue(HiveFileFormat.PARQUET.isSplittable());
        Assertions.assertTrue(HiveFileFormat.ORC.isSplittable());
        Assertions.assertTrue(HiveFileFormat.TEXT.isSplittable());
        Assertions.assertFalse(HiveFileFormat.JSON.isSplittable());
        Assertions.assertFalse(HiveFileFormat.UNKNOWN.isSplittable());
    }

    @Test
    public void testFormatName() {
        Assertions.assertEquals("parquet", HiveFileFormat.PARQUET.getFormatName());
        Assertions.assertEquals("orc", HiveFileFormat.ORC.getFormatName());
        Assertions.assertEquals("text", HiveFileFormat.TEXT.getFormatName());
        Assertions.assertEquals("json", HiveFileFormat.JSON.getFormatName());
    }
}
