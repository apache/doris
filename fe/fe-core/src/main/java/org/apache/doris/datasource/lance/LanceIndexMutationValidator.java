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
        if (catalog.isRestCatalogConfigured()) {
            throw new AnalysisException((def.isOrReplace() ? "CREATE OR REPLACE INDEX" : "CREATE INDEX")
                    + " is not supported for Lance REST catalogs");
        }
        String lanceType = def.getLanceIndexType();
        if (lanceType == null) {
            lanceType = def.getIndexType() == IndexDef.IndexType.ANN ? "ANN" : null;
        }
        if (lanceType == null) {
            throw new AnalysisException("Lance catalog tables only support USING ANN, BTREE, or BITMAP");
        }
        if (def.getCols() == null || def.getCols().size() != 1) {
            throw new AnalysisException("Lance index must be built on exactly one column");
        }
        if (def.getIndexName().getBytes(StandardCharsets.UTF_8).length > MAX_INDEX_NAME_BYTES) {
            throw new AnalysisException("index name too long, the index name length at most is 64.");
        }
        String columnName = def.getCols().get(0);
        Column column = table.getColumn(columnName);
        if (column == null) {
            throw new AnalysisException("Index column '" + columnName + "' does not exist");
        }
        if (column.isAllowNull()) {
            throw new AnalysisException(lanceType + " index must be built on a column that is not nullable");
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
                throw new AnalysisException("Lance catalog tables only support USING ANN, BTREE, or BITMAP");
        }
    }

    /**
     * Validates a top-level DROP INDEX statement targeting a Lance catalog table.
     */
    public static void validateDropIndex(LanceExternalCatalog catalog) throws AnalysisException {
        if (catalog.isRestCatalogConfigured()) {
            throw new AnalysisException("DROP INDEX is not supported for Lance REST catalogs");
        }
    }

    private static void validateAnnIndex(Column column, Map<String, String> properties)
            throws AnalysisException {
        Type columnType = column.getType();
        if (!(columnType instanceof ArrayType)) {
            throw new AnalysisException("ANN index column must be array type");
        }
        Type itemType = ((ArrayType) columnType).getItemType();
        if (!itemType.isScalarType(PrimitiveType.FLOAT)) {
            throw new AnalysisException("ANN index column item type must be float type");
        }
        // Keys match case-insensitively; the normalized view below is validation-local only.
        // Persisting normalized keys/values into the admitted job spec is owned by admission.
        Map<String, String> lowerCaseProperties = new HashMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey().toLowerCase(Locale.ROOT);
            if (!ANN_PROPERTY_KEYS.contains(key)) {
                throw new AnalysisException("Unknown property '" + entry.getKey() + "' for Lance ANN index");
            }
            if (lowerCaseProperties.put(key, entry.getValue()) != null) {
                throw new AnalysisException("Duplicate property '" + entry.getKey() + "' for Lance ANN index");
            }
        }
        String indexType = lowerCaseProperties.get("index_type");
        if (indexType == null || !indexType.equalsIgnoreCase("IVF_PQ")) {
            throw new AnalysisException("Lance ANN index requires property \"index_type\" = \"IVF_PQ\"");
        }
        String metric = lowerCaseProperties.get("metric");
        if (metric != null && !ANN_METRICS.contains(metric.toLowerCase(Locale.ROOT))) {
            throw new AnalysisException("metric must be one of l2, cosine, dot");
        }
        checkRequiredPositiveInt(lowerCaseProperties, "num_partitions");
        checkRequiredPositiveInt(lowerCaseProperties, "num_sub_vectors");
        String numBits = lowerCaseProperties.get("num_bits");
        if (numBits != null && parsePositiveInt(numBits) != 8) {
            throw new AnalysisException("num_bits must be 8");
        }
    }

    private static void validateBtreeIndex(Column column, Map<String, String> properties)
            throws AnalysisException {
        if (!properties.isEmpty()) {
            throw new AnalysisException("BTREE indexes do not support properties");
        }
        Type columnType = column.getType();
        // LARGEINT (Arrow uint64) and TIMESTAMPTZ are included deliberately.
        if (!columnType.isIntegerType() && !columnType.isLargeIntType()
                && !columnType.isFloatingPointType() && !columnType.isDecimalV3()
                && !columnType.isStringType() && !columnType.isDateV2()
                && !columnType.isDatetimeV2() && !columnType.isTimeStampTz()) {
            throw new AnalysisException("BTREE index does not support column type " + columnType);
        }
    }

    private static void validateBitmapIndex(Column column, Map<String, String> properties)
            throws AnalysisException {
        if (!properties.isEmpty()) {
            throw new AnalysisException("BITMAP indexes do not support properties");
        }
        Type columnType = column.getType();
        // LARGEINT (Arrow uint64) is integral, included here exactly as in the BTREE matrix.
        if (!columnType.isBoolean() && !columnType.isIntegerType() && !columnType.isLargeIntType()
                && !columnType.isStringType() && !columnType.isDateV2()) {
            throw new AnalysisException("BITMAP index does not support column type " + columnType);
        }
    }

    private static void checkRequiredPositiveInt(Map<String, String> properties, String key)
            throws AnalysisException {
        String value = properties.get(key);
        if (value == null || parsePositiveInt(value) <= 0) {
            throw new AnalysisException(key + " must be a positive integer");
        }
    }

    private static int parsePositiveInt(String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return -1;
        }
    }
}
