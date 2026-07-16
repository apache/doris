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

import java.util.Locale;

/**
 * Resolves a Hive table's read file format, a faithful port of legacy
 * {@code HMSExternalTable.getFileFormatType} (+ {@code HiveMetaStoreClientHelper.HiveFileFormat.getFormat}).
 *
 * <p>The algorithm is TWO-STAGE and <b>serde-authoritative for the text family</b>: the {@code inputFormat}
 * class name picks the coarse family (parquet / orc / text) by substring, and for the text family the
 * <b>SerDe</b> picks the fine format. This is why detection must NOT be input-format-first: a standard Hive
 * JSON table has {@code inputFormat=org.apache.hadoop.mapred.TextInputFormat} — its JSON-ness lives ONLY in
 * the JsonSerDe, so an input-format-first classifier would mis-read it as text/CSV.</p>
 *
 * <p>The five values map 1:1 to the BE {@code TFileFormatType} the generic
 * {@code PluginDrivenScanNode.mapFileFormatType} resolves the emitted {@link #getFormatName() token} to:
 * {@code parquet}->FORMAT_PARQUET, {@code orc}->FORMAT_ORC, {@code text}->FORMAT_TEXT, {@code csv}->
 * FORMAT_CSV_PLAIN, {@code json}->FORMAT_JSON. {@code FORMAT_TEXT} (hive {@code LazySimpleSerDe} /
 * {@code MultiDelimitSerDe}) is a DISTINCT BE reader from {@code FORMAT_CSV_PLAIN} (hive {@code OpenCSVSerde}):
 * the text reader honors hive collection/map delimiters, {@code \\N} nulls and hive escaping, so the two must
 * not be collapsed.</p>
 */
public enum HiveFileFormat {

    PARQUET("parquet"),
    ORC("orc"),
    // Hive text family (LazySimpleSerDe / MultiDelimitSerDe) -> BE FORMAT_TEXT.
    TEXT("text"),
    // Hive OpenCSVSerde (and OpenX-JSON read in one column) -> BE FORMAT_CSV_PLAIN.
    CSV("csv"),
    // Hive JSON serdes -> BE FORMAT_JSON.
    JSON("json");

    // Coarse inputFormat families, mirroring legacy HiveMetaStoreClientHelper.HiveFileFormat descs. Detection
    // is a lowercase SUBSTRING match in THIS order (text first), first match wins; the LZO text input formats
    // (com.hadoop...LzoTextInputFormat / DeprecatedLzoTextInputFormat) contain "text" and so land on text.
    private static final String FAMILY_TEXT = "text";
    private static final String FAMILY_PARQUET = "parquet";
    private static final String FAMILY_ORC = "orc";

    // SerDe class names, mirroring the legacy constants in HiveMetaStoreClientHelper (the connector cannot
    // import fe-core). MultiDelimitSerDe is matched under BOTH its modern (serde2, the legacy Doris constant)
    // and historical (contrib.serde2) package names — both are valid Hive classes and both read as FORMAT_TEXT;
    // recognizing both is a small, safe superset of legacy's serde2-only match (it only makes more tables
    // readable, never breaks one).
    private static final String LAZY_SIMPLE_SERDE = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
    private static final String MULTI_DELIMIT_SERDE = "org.apache.hadoop.hive.serde2.MultiDelimitSerDe";
    private static final String CONTRIB_MULTI_DELIMIT_SERDE =
            "org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe";
    private static final String OPEN_CSV_SERDE = "org.apache.hadoop.hive.serde2.OpenCSVSerde";
    private static final String HIVE_JSON_SERDE = "org.apache.hive.hcatalog.data.JsonSerDe";
    private static final String LEGACY_HIVE_JSON_SERDE = "org.apache.hadoop.hive.serde2.JsonSerDe";
    private static final String OPENX_JSON_SERDE = "org.openx.data.jsonserde.JsonSerDe";

    private final String formatName;

    HiveFileFormat(String formatName) {
        this.formatName = formatName;
    }

    /** The neutral format token emitted to fe-core (mapped to a BE TFileFormatType there). */
    public String getFormatName() {
        return formatName;
    }

    /**
     * Resolves the read format of a hive table, reproducing legacy {@code HMSExternalTable.getFileFormatType}.
     * The {@code inputFormat} chooses parquet / orc / text-file; for the text-file family the {@code serDeLib}
     * chooses json / csv / hive-text. The two extra flags reproduce the legacy OpenX-JSON special case: with
     * {@code readHiveJsonInOneColumn} an OpenX-JSON table is read as a single CSV column (only when its first
     * column is a string). Fails loud on an unrecognized inputFormat or serde, matching legacy (which threw
     * rather than silently degrading).
     *
     * @param inputFormat the HMS {@code StorageDescriptor.inputFormat} class name
     * @param serDeLib the HMS {@code SerDeInfo.serializationLib} class name
     * @param readHiveJsonInOneColumn the session {@code read_hive_json_in_one_column} flag
     * @param firstColumnIsString whether the table's first column is a {@code STRING} (OpenX one-column gate)
     * @throws DorisConnectorException if the inputFormat/serde is unsupported, or the OpenX one-column mode is
     *     requested on a table whose first column is not a string
     */
    public static HiveFileFormat detect(String inputFormat, String serDeLib,
            boolean readHiveJsonInOneColumn, boolean firstColumnIsString) {
        switch (detectFamily(inputFormat)) {
            case FAMILY_PARQUET:
                return PARQUET;
            case FAMILY_ORC:
                return ORC;
            default:
                // text family: the serde decides json / csv / hive-text.
                return detectTextFamily(serDeLib, readHiveJsonInOneColumn, firstColumnIsString);
        }
    }

    /**
     * Maps an inputFormat class name to its coarse family token ({@link #FAMILY_TEXT}/{@link #FAMILY_PARQUET}/
     * {@link #FAMILY_ORC}) by lowercase substring, first match wins in text->parquet->orc order (legacy
     * {@code HiveMetaStoreClientHelper.HiveFileFormat.getFormat}). Throws on an unrecognized inputFormat,
     * matching legacy's {@code "Not supported Hive file format"}.
     */
    private static String detectFamily(String inputFormat) {
        if (inputFormat == null) {
            throw new DorisConnectorException("Not supported Hive file format: null inputFormat");
        }
        String lower = inputFormat.toLowerCase(Locale.ROOT);
        if (lower.contains(FAMILY_TEXT)) {
            return FAMILY_TEXT;
        }
        if (lower.contains(FAMILY_PARQUET)) {
            return FAMILY_PARQUET;
        }
        if (lower.contains(FAMILY_ORC)) {
            return FAMILY_ORC;
        }
        throw new DorisConnectorException("Not supported Hive file format: " + inputFormat);
    }

    /**
     * Chooses the fine text-family format from the serde, reproducing the legacy serde switch verbatim,
     * including the OpenX-JSON {@code read_hive_json_in_one_column} branch and the unsupported-serde throw.
     */
    private static HiveFileFormat detectTextFamily(String serDeLib,
            boolean readHiveJsonInOneColumn, boolean firstColumnIsString) {
        if (HIVE_JSON_SERDE.equals(serDeLib) || LEGACY_HIVE_JSON_SERDE.equals(serDeLib)) {
            return JSON;
        }
        if (OPENX_JSON_SERDE.equals(serDeLib)) {
            if (!readHiveJsonInOneColumn) {
                return JSON;
            }
            if (firstColumnIsString) {
                return CSV;
            }
            throw new DorisConnectorException("read_hive_json_in_one_column = true, but the first column of "
                    + "the hive table is not a string column.");
        }
        if (LAZY_SIMPLE_SERDE.equals(serDeLib)
                || MULTI_DELIMIT_SERDE.equals(serDeLib)
                || CONTRIB_MULTI_DELIMIT_SERDE.equals(serDeLib)) {
            return TEXT;
        }
        if (OPEN_CSV_SERDE.equals(serDeLib)) {
            return CSV;
        }
        throw new DorisConnectorException("Unsupported hive table serde: " + serDeLib);
    }

    /**
     * Whether this format supports splitting a file at byte boundaries. Parquet/ORC have internal block
     * structure; text/csv are line-splittable. JSON is read whole (not split), matching the legacy behavior.
     */
    public boolean isSplittable() {
        return this == PARQUET || this == ORC || this == TEXT || this == CSV;
    }
}
