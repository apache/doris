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

import org.apache.doris.connector.api.DorisConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link HiveFileFormat} read-format detection, pinned to the legacy
 * {@code HMSExternalTable.getFileFormatType} truth table.
 *
 * <p>WHY these assertions matter:
 * <ul>
 *   <li>Detection is TWO-STAGE and serde-authoritative for the text family. A standard hive JSON table has
 *       {@code inputFormat=TextInputFormat} (its JSON-ness is only in the serde); an input-format-first
 *       classifier mis-read it as text/CSV — the headline read-correctness bug this fixes.</li>
 *   <li>{@code LazySimpleSerDe}/{@code MultiDelimitSerDe} -> TEXT (BE FORMAT_TEXT), {@code OpenCSVSerde} -> CSV
 *       (BE FORMAT_CSV_PLAIN): these are DISTINCT BE readers (the text reader honors hive collection/map
 *       delimiters, {@code \\N} nulls, hive escaping), so they must not collapse.</li>
 *   <li>Unknown inputFormat/serde fails loud (legacy parity), rather than silently degrading to a JNI read.</li>
 * </ul>
 */
public class HiveFileFormatTest {

    private static final String TEXT_INPUT_FORMAT = "org.apache.hadoop.mapred.TextInputFormat";
    private static final String PARQUET_INPUT_FORMAT =
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
    private static final String ORC_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";

    private static final String LAZY_SIMPLE_SERDE = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
    private static final String MULTI_DELIMIT_SERDE = "org.apache.hadoop.hive.serde2.MultiDelimitSerDe";
    private static final String CONTRIB_MULTI_DELIMIT_SERDE =
            "org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe";
    private static final String OPEN_CSV_SERDE = "org.apache.hadoop.hive.serde2.OpenCSVSerde";
    private static final String HCATALOG_JSON_SERDE = "org.apache.hive.hcatalog.data.JsonSerDe";
    private static final String LEGACY_JSON_SERDE = "org.apache.hadoop.hive.serde2.JsonSerDe";
    private static final String OPENX_JSON_SERDE = "org.openx.data.jsonserde.JsonSerDe";
    private static final String ORC_SERDE = "org.apache.hadoop.hive.ql.io.orc.OrcSerde";

    private static HiveFileFormat detect(String inputFormat, String serDeLib) {
        return HiveFileFormat.detect(inputFormat, serDeLib, false, false);
    }

    @Test
    public void inputFormatPicksParquetAndOrcRegardlessOfSerde() {
        Assertions.assertEquals(HiveFileFormat.PARQUET, detect(PARQUET_INPUT_FORMAT, LAZY_SIMPLE_SERDE));
        Assertions.assertEquals(HiveFileFormat.ORC, detect(ORC_INPUT_FORMAT, ORC_SERDE));
    }

    @Test
    public void textFamilySerdeDecidesTextVsCsvVsJson() {
        // The DEFAULT hive text serde -> FORMAT_TEXT (not CSV): the regression that mis-read hive-text tables.
        Assertions.assertEquals(HiveFileFormat.TEXT, detect(TEXT_INPUT_FORMAT, LAZY_SIMPLE_SERDE));
        // MultiDelimit under BOTH package names -> TEXT.
        Assertions.assertEquals(HiveFileFormat.TEXT, detect(TEXT_INPUT_FORMAT, MULTI_DELIMIT_SERDE));
        Assertions.assertEquals(HiveFileFormat.TEXT, detect(TEXT_INPUT_FORMAT, CONTRIB_MULTI_DELIMIT_SERDE));
        // OpenCSV -> CSV (FORMAT_CSV_PLAIN).
        Assertions.assertEquals(HiveFileFormat.CSV, detect(TEXT_INPUT_FORMAT, OPEN_CSV_SERDE));
    }

    @Test
    public void jsonTableWithTextInputFormatResolvesToJsonNotText() {
        // THE HEADLINE BUG: a JSON table's inputFormat is TextInputFormat; its JSON-ness lives only in the
        // serde. Detection must consult the serde and return JSON, not text/CSV.
        Assertions.assertEquals(HiveFileFormat.JSON, detect(TEXT_INPUT_FORMAT, HCATALOG_JSON_SERDE));
        Assertions.assertEquals(HiveFileFormat.JSON, detect(TEXT_INPUT_FORMAT, LEGACY_JSON_SERDE));
        // OpenX JSON with the one-column mode OFF (default) -> JSON.
        Assertions.assertEquals(HiveFileFormat.JSON, detect(TEXT_INPUT_FORMAT, OPENX_JSON_SERDE));
    }

    @Test
    public void openxJsonOneColumnModeReadsAsCsvWhenFirstColumnIsString() {
        // read_hive_json_in_one_column=true + first column is string -> the whole row is read as one CSV column.
        Assertions.assertEquals(HiveFileFormat.CSV,
                HiveFileFormat.detect(TEXT_INPUT_FORMAT, OPENX_JSON_SERDE, true, true));
        // Non-OpenX json serdes ignore the flag (stay JSON).
        Assertions.assertEquals(HiveFileFormat.JSON,
                HiveFileFormat.detect(TEXT_INPUT_FORMAT, HCATALOG_JSON_SERDE, true, true));
    }

    @Test
    public void openxJsonOneColumnModeFailsLoudWhenFirstColumnNotString() {
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> HiveFileFormat.detect(TEXT_INPUT_FORMAT, OPENX_JSON_SERDE, true, false));
        Assertions.assertTrue(e.getMessage().contains("read_hive_json_in_one_column"),
                "message should name the offending session flag");
    }

    @Test
    public void unknownSerdeInTextFamilyFailsLoud() {
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> detect(TEXT_INPUT_FORMAT, "com.example.WeirdSerDe"));
        Assertions.assertTrue(e.getMessage().contains("Unsupported hive table serde"),
                "unknown serde must fail loud (legacy parity), not silently degrade to JNI");
    }

    @Test
    public void unknownInputFormatFailsLoud() {
        Assertions.assertThrows(DorisConnectorException.class,
                () -> detect("com.example.CustomInputFormat", LAZY_SIMPLE_SERDE));
        Assertions.assertThrows(DorisConnectorException.class,
                () -> detect(null, LAZY_SIMPLE_SERDE));
    }

    @Test
    public void formatNameMatchesGenericVocabularyToken() {
        Assertions.assertEquals("parquet", HiveFileFormat.PARQUET.getFormatName());
        Assertions.assertEquals("orc", HiveFileFormat.ORC.getFormatName());
        Assertions.assertEquals("text", HiveFileFormat.TEXT.getFormatName());
        Assertions.assertEquals("csv", HiveFileFormat.CSV.getFormatName());
        Assertions.assertEquals("json", HiveFileFormat.JSON.getFormatName());
    }

    @Test
    public void splittability() {
        Assertions.assertTrue(HiveFileFormat.PARQUET.isSplittable());
        Assertions.assertTrue(HiveFileFormat.ORC.isSplittable());
        Assertions.assertTrue(HiveFileFormat.TEXT.isSplittable());
        Assertions.assertTrue(HiveFileFormat.CSV.isSplittable());
        Assertions.assertFalse(HiveFileFormat.JSON.isSplittable());
    }
}
