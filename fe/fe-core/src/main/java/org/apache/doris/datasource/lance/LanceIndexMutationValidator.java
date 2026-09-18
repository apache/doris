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

package org.apache.doris.datasource.lance;

import org.apache.doris.analysis.IndexDef;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.datasource.lance.job.LanceIndexNameNormalizer;
import org.apache.doris.nereids.trees.plans.commands.info.IndexDefinition;

import com.google.common.collect.ImmutableSet;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Static FE-side validation for CREATE/DROP INDEX statements targeting Lance catalog tables.
 *
 * <p>These are the FE static bounds of the Lance index lifecycle design (section 2.4). Exact
 * Arrow-level revalidation — fixed-size-list-ness, vector dimension, float16 vs float32, and
 * num_sub_vectors divisibility — is deferred to the isolated index-build worker per design
 * sections 2.4/4.2, because {@link LanceTypeConverter} erases those facts when mapping Arrow
 * types to Doris types (LanceTypeConverter.java:101-106).
 */
public final class LanceIndexMutationValidator {
    private static final int MAX_INDEX_NAME_BYTES = 64;
    /**
     * System index entries such as {@code __lance_frag_reuse} and {@code __lance_mem_wal} are
     * filtered out of every metadata read, so a user index under this prefix would be invisible
     * to SHOW INDEX and impossible to drop through Doris. It is reserved for all three
     * mutations (design sections 3.4/4.1).
     */
    private static final String RESERVED_INDEX_NAME_PREFIX = "__lance_";
    private static final Set<String> ANN_PROPERTY_KEYS = ImmutableSet.of(
            "index_type", "metric", "num_partitions", "num_sub_vectors", "num_bits");
    private static final Set<String> ANN_METRICS = ImmutableSet.of("l2", "cosine", "dot");

    private LanceIndexMutationValidator() {
    }

    /**
     * Validates a top-level CREATE [OR REPLACE] INDEX statement against a Lance catalog table.
     * Returns normally when the statement is statically valid; the caller then decides whether
     * the statement is admitted.
     */
    public static void validateCreateIndex(LanceExternalCatalog catalog, LanceExternalTable table,
            IndexDefinition def) throws AnalysisException {
        validateCreateIndexCatalog(catalog, def);
        String lanceType = def.getLanceIndexType();
        if (lanceType == null) {
            lanceType = def.getIndexType() == IndexDef.IndexType.ANN ? "ANN" : null;
        }
        if (lanceType == null) {
            rejectInvalidDefinition("Lance catalog tables only support USING ANN, BTREE, or BITMAP");
        }
        if (def.getCols() == null || def.getCols().size() != 1) {
            rejectInvalidDefinition("Lance index must be built on exactly one column");
        }
        validateIndexName(def.getIndexName());
        String columnName = def.getCols().get(0);
        Column column = table.getColumn(columnName);
        if (column == null) {
            rejectInvalidDefinition("Index column '" + columnName + "' does not exist");
        }
        if (column.isAllowNull()) {
            rejectInvalidDefinition(lanceType + " index must be built on a column that is not nullable");
        }
        switch (lanceType) {
            case "ANN":
                validateAnnIndex(column, def.getProperties());
                break;
            case "BTREE":
                validateBtreeIndex(column, def.getProperties());
                break;
            case "BITMAP":
                validateBitmapIndex(column, def.getProperties());
                break;
            default:
                rejectInvalidDefinition("Lance catalog tables only support USING ANN, BTREE, or BITMAP");
        }
    }

    /**
     * Rejects CREATE INDEX against a Lance REST catalog without resolving its database or table.
     */
    public static void validateCreateIndexCatalog(LanceExternalCatalog catalog, IndexDefinition def)
            throws AnalysisException {
        if (catalog.isRestCatalogConfigured()) {
            rejectUnsupportedOperation(def.isOrReplace() ? "CREATE OR REPLACE INDEX" : "CREATE INDEX",
                    "REST catalogs");
        }
    }

    /**
     * Validates a top-level DROP INDEX statement targeting a Lance catalog table. The REST
     * rejection keeps failing fast; Directory catalogs then get the same index-name bounds as
     * the CREATE path.
     */
    public static void validateDropIndex(LanceExternalCatalog catalog, String indexName)
            throws AnalysisException {
        if (catalog.isRestCatalogConfigured()) {
            rejectUnsupportedOperation("DROP INDEX", "REST catalogs");
        }
        validateIndexName(indexName);
    }

    /**
     * Shared Lance index-name bounds for the CREATE and DROP paths: the name becomes the durable
     * logical identity that an admitted job and its same-name fence key are built on, so
     * null/empty names are rejected here instead of being masked as unsupported operations.
     */
    private static void validateIndexName(String indexName) throws AnalysisException {
        if (indexName == null || indexName.isEmpty()) {
            rejectInvalidDefinition("index name cannot be empty");
        }
        if (indexName.getBytes(StandardCharsets.UTF_8).length > MAX_INDEX_NAME_BYTES) {
            rejectInvalidDefinition("index name too long, the index name length at most is 64.");
        }
        rejectIfReservedIndexName(indexName);
    }

    /**
     * Reserved-name rejection shared with admission (same package): judged on the normalized
     * name so case variants of the system prefix are covered, and applied to CREATE, CREATE OR
     * REPLACE and DROP alike — DROP included, so a reserved name reports "reserved" instead of a
     * misleading "not found" for an index Doris can never drop.
     */
    static void rejectIfReservedIndexName(String indexName) throws AnalysisException {
        if (LanceIndexNameNormalizer.normalize(indexName).startsWith(RESERVED_INDEX_NAME_PREFIX)) {
            rejectInvalidDefinition("index name '" + indexName
                    + "' uses the reserved '__lance_' prefix of Lance system indexes");
        }
    }

    private static void validateAnnIndex(Column column, Map<String, String> properties)
            throws AnalysisException {
        Type columnType = column.getType();
        if (!(columnType instanceof ArrayType)) {
            rejectInvalidDefinition("ANN index column must be array type");
        }
        Type itemType = ((ArrayType) columnType).getItemType();
        if (!itemType.isScalarType(PrimitiveType.FLOAT)) {
            rejectInvalidDefinition("ANN index column item type must be float type");
        }
        // Keys match case-insensitively; the normalized view below is validation-local only.
        // Persisting normalized keys/values into the admitted job spec is owned by admission.
        Map<String, String> lowerCaseProperties = new HashMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey().toLowerCase(Locale.ROOT);
            if (!ANN_PROPERTY_KEYS.contains(key)) {
                rejectInvalidDefinition("Unknown property '" + entry.getKey() + "' for Lance ANN index");
            }
            if (lowerCaseProperties.put(key, entry.getValue()) != null) {
                rejectInvalidDefinition("Duplicate property '" + entry.getKey() + "' for Lance ANN index");
            }
        }
        String indexType = lowerCaseProperties.get("index_type");
        if (indexType == null || !indexType.equalsIgnoreCase("IVF_PQ")) {
            rejectInvalidDefinition("Lance ANN index requires property \"index_type\" = \"IVF_PQ\"");
        }
        String metric = lowerCaseProperties.get("metric");
        if (metric != null && !ANN_METRICS.contains(metric.toLowerCase(Locale.ROOT))) {
            rejectInvalidDefinition("metric must be one of l2, cosine, dot");
        }
        checkRequiredPositiveInt(lowerCaseProperties, "num_partitions");
        checkRequiredPositiveInt(lowerCaseProperties, "num_sub_vectors");
        // Design section 2.4 "configured static bounds apply": the positive checks above
        // guarantee parseable ints here. The bounds are read at the validation point (the
        // Env.java lower_case_table_names precedent), so a mutable override takes effect
        // without a restart.
        if (parsePositiveInt(lowerCaseProperties.get("num_partitions"))
                > Config.lance_index_max_num_partitions) {
            rejectInvalidDefinition("num_partitions must not exceed "
                    + Config.lance_index_max_num_partitions + " (lance_index_max_num_partitions)");
        }
        if (parsePositiveInt(lowerCaseProperties.get("num_sub_vectors"))
                > Config.lance_index_max_num_sub_vectors) {
            rejectInvalidDefinition("num_sub_vectors must not exceed "
                    + Config.lance_index_max_num_sub_vectors + " (lance_index_max_num_sub_vectors)");
        }
        String numBits = lowerCaseProperties.get("num_bits");
        if (numBits != null && parsePositiveInt(numBits) != 8) {
            rejectInvalidDefinition("num_bits must be 8");
        }
    }

    private static void validateBtreeIndex(Column column, Map<String, String> properties)
            throws AnalysisException {
        if (!properties.isEmpty()) {
            rejectInvalidDefinition("BTREE indexes do not support properties");
        }
        Type columnType = column.getType();
        // LARGEINT (Arrow uint64) and TIMESTAMPTZ are included deliberately.
        if (!columnType.isIntegerType() && !columnType.isLargeIntType()
                && !columnType.isFloatingPointType() && !columnType.isDecimalV3()
                && !columnType.isStringType() && !columnType.isDateV2()
                && !columnType.isDatetimeV2() && !columnType.isTimeStampTz()) {
            rejectInvalidDefinition("BTREE index does not support column type " + columnType);
        }
    }

    private static void validateBitmapIndex(Column column, Map<String, String> properties)
            throws AnalysisException {
        if (!properties.isEmpty()) {
            rejectInvalidDefinition("BITMAP indexes do not support properties");
        }
        Type columnType = column.getType();
        // LARGEINT (Arrow uint64) is integral, included here exactly as in the BTREE matrix.
        if (!columnType.isBoolean() && !columnType.isIntegerType() && !columnType.isLargeIntType()
                && !columnType.isStringType() && !columnType.isDateV2()) {
            rejectInvalidDefinition("BITMAP index does not support column type " + columnType);
        }
    }

    private static void checkRequiredPositiveInt(Map<String, String> properties, String key)
            throws AnalysisException {
        String value = properties.get(key);
        if (value == null || parsePositiveInt(value) <= 0) {
            rejectInvalidDefinition(key + " must be a positive integer");
        }
    }

    /**
     * Rejects a Lance index operation with a stable client-visible error code.
     */
    public static void rejectUnsupportedOperation(String operation, String target)
            throws AnalysisException {
        ErrorReport.reportAnalysisException(ErrorCode.ERR_LANCE_INDEX_OPERATION_NOT_SUPPORTED,
                operation, target);
    }

    /**
     * Rejects a Lance index mutation while the admission gate stays off (the default). The
     * rejection fires from validate() after static validation, before any metadata read or id
     * allocation; admission itself never runs with the gate off.
     */
    public static void rejectMutationDisabled(String operation) throws AnalysisException {
        ErrorReport.reportAnalysisException(ErrorCode.ERR_LANCE_INDEX_MUTATION_DISABLED, operation);
    }

    private static void rejectInvalidDefinition(String detail) throws AnalysisException {
        ErrorReport.reportAnalysisException(ErrorCode.ERR_LANCE_INDEX_INVALID, detail);
    }

    private static int parsePositiveInt(String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return -1;
        }
    }
}
