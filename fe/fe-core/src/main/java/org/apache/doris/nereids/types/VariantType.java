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

package org.apache.doris.nereids.types;

import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.types.coercion.PrimitiveType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Variant type in Nereids.
 * Why Variant is not complex type? Since it's nested structure is not pre-defined, then using
 * primitive type will be easy to handle meta info in FE.
 * Also, could predefine some fields of nested columns.
 * Example: VARIANT <`a.b`:INT, `a.c`:DATETIMEV2>
 *
 */
public class VariantType extends PrimitiveType {

    public static final VariantType INSTANCE = new VariantType(0);
    public static final VariantType COMPUTE_V2_INSTANCE = new VariantType(0, true);

    public static final int WIDTH = 24;

    public static final String UNSUPPORTED_ORDERING_COMPARISON_MESSAGE =
            "Variant does not support ordering/comparison, CAST to a concrete type first";

    private final int variantMaxSubcolumnsCount;

    private final boolean enableTypedPathsToSparse;

    private final int variantMaxSparseColumnStatisticsSize;

    private final List<VariantField> predefinedFields;
    private final int variantSparseHashShardCount;

    private final boolean enableVariantDocMode;
    private final long variantDocMaterializationMinRows;
    private final int variantDocShardCount;
    private final boolean enableNestedGroup;
    private final boolean computeV2;

    /**
     * Creates a Variant type without predefined fields and only configures the max subcolumn limit.
     *
     * @param variantMaxSubcolumnsCount max number of subcolumns allowed (0 means unlimited)
     */
    public VariantType(int variantMaxSubcolumnsCount) {
        this(variantMaxSubcolumnsCount, false);
    }

    private VariantType(int variantMaxSubcolumnsCount, boolean computeV2) {
        this.variantMaxSubcolumnsCount = variantMaxSubcolumnsCount;
        this.predefinedFields = Lists.newArrayList();
        this.enableTypedPathsToSparse = false;
        this.variantMaxSparseColumnStatisticsSize = 10000;
        this.variantSparseHashShardCount = 0;
        this.enableVariantDocMode = false;
        this.variantDocMaterializationMinRows = 0L;
        this.variantDocShardCount = 64;
        this.enableNestedGroup = false;
        this.computeV2 = computeV2;
    }

    /**
     *   Contains predefined fields like struct
     */
    public VariantType(List<VariantField> fields) {
        this.predefinedFields = ImmutableList.copyOf(Objects.requireNonNull(fields, "fields should not be null"));
        this.variantMaxSubcolumnsCount = 0;
        this.enableTypedPathsToSparse = false;
        this.variantMaxSparseColumnStatisticsSize = 10000;
        this.variantSparseHashShardCount = 0;
        this.enableVariantDocMode = false;
        this.variantDocMaterializationMinRows = 0L;
        this.variantDocShardCount = 64;
        this.enableNestedGroup = false;
        this.computeV2 = false;
    }

    /**
     * Creates a Variant type with predefined fields and advanced optional properties.
     *
     * @param fields predefined variant path fields
     * @param variantMaxSubcolumnsCount max number of subcolumns allowed
     * @param enableTypedPathsToSparse whether typed paths should be materialized as sparse columns
     * @param variantMaxSparseColumnStatisticsSize upper bound of sparse path statistics entries
     * @param variantSparseHashShardCount hash buckets count when writing sparse shards
     * @param enableVariantDocMode whether to enable variant doc snapshot writing mode
     * @param variantDocMaterializationMinRows minimum rows to generate doc snapshot columns
     */
    public VariantType(List<VariantField> fields, int variantMaxSubcolumnsCount,
            boolean enableTypedPathsToSparse, int variantMaxSparseColumnStatisticsSize,
            int variantSparseHashShardCount, boolean enableVariantDocMode,
            long variantDocMaterializationMinRows, int variantDocShardCount,
            boolean enableNestedGroup) {
        this(fields, variantMaxSubcolumnsCount, enableTypedPathsToSparse,
                variantMaxSparseColumnStatisticsSize, variantSparseHashShardCount,
                enableVariantDocMode, variantDocMaterializationMinRows, variantDocShardCount,
                enableNestedGroup, false);
    }

    /**
     * Creates a Variant type and selects its compute-only physical representation.
     */
    public VariantType(List<VariantField> fields, int variantMaxSubcolumnsCount,
            boolean enableTypedPathsToSparse, int variantMaxSparseColumnStatisticsSize,
            int variantSparseHashShardCount, boolean enableVariantDocMode,
            long variantDocMaterializationMinRows, int variantDocShardCount,
            boolean enableNestedGroup, boolean computeV2) {
        this.predefinedFields = ImmutableList.copyOf(Objects.requireNonNull(fields, "fields should not be null"));
        this.variantMaxSubcolumnsCount = variantMaxSubcolumnsCount;
        this.enableTypedPathsToSparse = enableTypedPathsToSparse;
        this.variantMaxSparseColumnStatisticsSize = variantMaxSparseColumnStatisticsSize;
        this.variantSparseHashShardCount = variantSparseHashShardCount;
        this.enableVariantDocMode = enableVariantDocMode;
        this.variantDocMaterializationMinRows = variantDocMaterializationMinRows;
        this.variantDocShardCount = variantDocShardCount;
        this.enableNestedGroup = enableNestedGroup;
        this.computeV2 = computeV2;
    }

    @Override
    public boolean isInjectiveCastTo(DataType target) {
        return target.equals(this) || target instanceof VariantType;
    }

    @Override
    public DataType conversion() {
        return new VariantType(predefinedFields.stream().map(VariantField::conversion)
                                .collect(Collectors.toList()), variantMaxSubcolumnsCount, enableTypedPathsToSparse,
                                    variantMaxSparseColumnStatisticsSize, variantSparseHashShardCount,
                                    enableVariantDocMode, variantDocMaterializationMinRows,
                                    variantDocShardCount, enableNestedGroup, computeV2);
    }

    @Override
    public Type toCatalogDataType() {
        org.apache.doris.catalog.VariantType type = new org.apache.doris.catalog.VariantType(predefinedFields.stream()
                .map(VariantField::toCatalogDataType)
                .collect(Collectors.toCollection(ArrayList::new)), variantMaxSubcolumnsCount, enableTypedPathsToSparse,
                     variantMaxSparseColumnStatisticsSize, variantSparseHashShardCount, enableVariantDocMode,
                     variantDocMaterializationMinRows, variantDocShardCount, enableNestedGroup, computeV2);
        return type;
    }

    @Override
    public boolean acceptsType(DataType other) {
        return other instanceof VariantType;
    }

    @Override
    public boolean isAssignableFrom(DataType targetDataType) {
        // Any VariantType is assignable to any other VariantType,
        // regardless of property differences (maxSubcolumns, etc.)
        if (targetDataType instanceof VariantType) {
            return true;
        }
        return super.isAssignableFrom(targetDataType);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("variant");
        sb.append("<");
        if (!predefinedFields.isEmpty()) {
            sb.append(predefinedFields.stream().map(VariantField::toSql).collect(Collectors.joining(",")));
            sb.append(",");
        }

        sb.append("PROPERTIES (");
        if (enableVariantDocMode) {
            sb.append("\"variant_enable_doc_mode\" = \"")
                                    .append(String.valueOf(enableVariantDocMode)).append("\"");
            sb.append(",");
            sb.append("\"variant_doc_materialization_min_rows\" = \"")
                                    .append(String.valueOf(variantDocMaterializationMinRows)).append("\"");
            sb.append(",");
            sb.append("\"variant_doc_hash_shard_count\" = \"")
                                    .append(String.valueOf(variantDocShardCount)).append("\"");
        } else {
            sb.append("\"variant_max_subcolumns_count\" = \"")
                                    .append(String.valueOf(variantMaxSubcolumnsCount)).append("\"");
            sb.append(",");
            sb.append("\"variant_enable_typed_paths_to_sparse\" = \"")
                                    .append(String.valueOf(enableTypedPathsToSparse)).append("\"");
            sb.append(",");
            sb.append("\"variant_max_sparse_column_statistics_size\" = \"")
                                    .append(String.valueOf(variantMaxSparseColumnStatisticsSize))
                                    .append("\"");
            sb.append(",");
            // Output at least 1 for backward compatibility: old data without this parameter defaults to 0
            sb.append("\"variant_sparse_hash_shard_count\" = \"")
                                    .append(String.valueOf(Math.max(1, variantSparseHashShardCount)))
                                    .append("\"");
        }
        if (enableNestedGroup) {
            sb.append(",");
            sb.append("\"variant_enable_nested_group\" = \"")
                    .append(String.valueOf(enableNestedGroup)).append("\"");
        }
        sb.append(")>");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VariantType other = (VariantType) o;
        return this.variantMaxSubcolumnsCount == other.variantMaxSubcolumnsCount
                    && this.enableTypedPathsToSparse == other.enableTypedPathsToSparse
                    && this.enableVariantDocMode == other.enableVariantDocMode
                    && this.variantDocMaterializationMinRows == other.variantDocMaterializationMinRows
                    && this.computeV2 == other.computeV2
                    && Objects.equals(predefinedFields, other.predefinedFields);
    }

    @Override
    public boolean equalsForRecursiveCte(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VariantType other = (VariantType) o;
        if (computeV2 != other.computeV2) {
            return false;
        }
        if (predefinedFields.size() != other.predefinedFields.size()) {
            return false;
        }
        for (int i = 0; i < predefinedFields.size(); ++i) {
            if (!predefinedFields.get(i).getDataType()
                    .equalsForRecursiveCte(other.predefinedFields.get(i).getDataType())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), variantMaxSubcolumnsCount, enableTypedPathsToSparse,
                            variantMaxSparseColumnStatisticsSize, variantSparseHashShardCount,
                            enableVariantDocMode, variantDocMaterializationMinRows, variantDocShardCount,
                            predefinedFields, computeV2);
    }

    @Override
    public int width() {
        return WIDTH;
    }

    @Override
    public String toString() {
        return toSql();
    }

    public List<VariantField> getPredefinedFields() {
        return predefinedFields;
    }

    public int getVariantMaxSubcolumnsCount() {
        return variantMaxSubcolumnsCount;
    }

    public int getVariantMaxSparseColumnStatisticsSize() {
        return variantMaxSparseColumnStatisticsSize;
    }

    public int getVariantSparseHashShardCount() {
        return variantSparseHashShardCount;
    }

    public boolean getEnableVariantDocMode() {
        return enableVariantDocMode;
    }

    public long getvariantDocMaterializationMinRows() {
        return variantDocMaterializationMinRows;
    }

    public int getVariantDocShardCount() {
        return variantDocShardCount;
    }

    public boolean getEnableNestedGroup() {
        return enableNestedGroup;
    }

    public boolean isComputeV2() {
        return computeV2;
    }

    /** Whether the type or any nested complex type contains Variant. */
    public static boolean containsVariant(DataType dataType) {
        if (dataType instanceof VariantType) {
            return true;
        } else if (dataType instanceof ArrayType) {
            return containsVariant(((ArrayType) dataType).getItemType());
        } else if (dataType instanceof MapType) {
            MapType mapType = (MapType) dataType;
            return containsVariant(mapType.getKeyType()) || containsVariant(mapType.getValueType());
        } else if (dataType instanceof StructType) {
            return ((StructType) dataType).getFields().stream()
                    .anyMatch(field -> containsVariant(field.getDataType()));
        }
        return false;
    }

    /** Whether this is a legacy Variant leaf rather than the compute-only V2 representation. */
    public static boolean isLegacyVariant(DataType dataType) {
        return dataType instanceof VariantType && !((VariantType) dataType).isComputeV2();
    }

    /**
     * Whether the Variant V2 execution kernel can convert this source type.
     *
     * <p>This mirrors the BE {@code execute_to_variant} contract: encoded JSON, nested arrays,
     * compute V2 values, and the scalar types supported by the typed Variant representation.
     * MAP, STRUCT, TIMEV2 and DECIMAL256 are intentionally excluded until their BE conversions
     * are implemented.</p>
     */
    public static boolean isSupportedComputeV2CastSource(DataType dataType) {
        if (dataType.isNullType() || dataType.isJsonType()) {
            return true;
        }
        if (dataType instanceof VariantType) {
            return ((VariantType) dataType).isComputeV2();
        }
        if (dataType instanceof ArrayType) {
            return isSupportedComputeV2CastSource(((ArrayType) dataType).getItemType());
        }
        if (dataType instanceof DecimalV3Type) {
            return ((DecimalV3Type) dataType).getPrecision()
                    <= DecimalV3Type.MAX_DECIMAL128_PRECISION;
        }
        return dataType.isBooleanType()
                || dataType.isIntegralType()
                || dataType.isFloatLikeType()
                || dataType.isDecimalV2Type()
                || dataType.isDateLikeType()
                || dataType.isStringLikeType()
                || dataType.isIPType();
    }

    /**
     * Whether converting between two types requires no execution-time cast.
     *
     * <p>Variant V2 layout properties describe storage/materialization behavior, but every
     * compute-only V2 value is the same value/metadata pair. Legacy Variant values still require
     * exact type equality because their execution layout depends on those properties. Complex
     * containers recurse so nested V2 leaves get the same treatment.</p>
     */
    public static boolean isNoOpCastCompatible(DataType left, DataType right) {
        if (left.equals(right)) {
            return true;
        }
        if (left instanceof VariantType && right instanceof VariantType) {
            return ((VariantType) left).isCastCompatibleWith((VariantType) right);
        }
        if (left instanceof ArrayType && right instanceof ArrayType) {
            return isNoOpCastCompatible(
                    ((ArrayType) left).getItemType(), ((ArrayType) right).getItemType());
        }
        if (left instanceof MapType && right instanceof MapType) {
            MapType leftMap = (MapType) left;
            MapType rightMap = (MapType) right;
            return isNoOpCastCompatible(leftMap.getKeyType(), rightMap.getKeyType())
                    && isNoOpCastCompatible(leftMap.getValueType(), rightMap.getValueType());
        }
        if (left instanceof StructType && right instanceof StructType) {
            List<StructField> leftFields = ((StructType) left).getFields();
            List<StructField> rightFields = ((StructType) right).getFields();
            if (leftFields.size() != rightFields.size()) {
                return false;
            }
            for (int i = 0; i < leftFields.size(); i++) {
                StructField leftField = leftFields.get(i);
                StructField rightField = rightFields.get(i);
                if (leftField.isNullable() != rightField.isNullable()
                        || !leftField.getName().equals(rightField.getName())
                        || !isNoOpCastCompatible(
                                leftField.getDataType(), rightField.getDataType())) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /** Selects the compute-only Variant representation in a possibly nested type. */
    public static DataType toComputeV2(DataType dataType) {
        if (dataType instanceof VariantType) {
            return COMPUTE_V2_INSTANCE;
        } else if (dataType instanceof ArrayType) {
            return ArrayType.of(toComputeV2(((ArrayType) dataType).getItemType()));
        } else if (dataType instanceof MapType) {
            MapType mapType = (MapType) dataType;
            return MapType.of(toComputeV2(mapType.getKeyType()), toComputeV2(mapType.getValueType()));
        } else if (dataType instanceof StructType) {
            return new StructType(((StructType) dataType).getFields().stream()
                    .map(field -> field.withDataType(toComputeV2(field.getDataType())))
                    .collect(Collectors.toList()));
        }
        return dataType;
    }

    /**
     * Whether two Variant values use an execution-compatible physical representation.
     *
     * <p>Legacy Variant values retain their existing common-type behavior. Compute-only Variant
     * V2 values share one physical representation, independent of source layout properties.</p>
     */
    public boolean hasCommonExecutionTypeWith(VariantType other) {
        return computeV2 == other.computeV2;
    }

    /**
     * Whether a cast between two Variant types is safe.
     *
     * <p>Variant V1 embeds layout properties in its execution type, so V1 casts still require
     * exact type equality. All compute-only V2 types share the same physical value/metadata
     * representation, therefore layout-property differences do not require conversion.</p>
     */
    public boolean isCastCompatibleWith(VariantType other) {
        return (computeV2 && other.computeV2) || equals(other);
    }

    /** Returns this Variant type with the requested compute-only physical representation. */
    public VariantType withComputeV2(boolean enabled) {
        if (computeV2 == enabled) {
            return this;
        }
        return new VariantType(predefinedFields, variantMaxSubcolumnsCount, enableTypedPathsToSparse,
                variantMaxSparseColumnStatisticsSize, variantSparseHashShardCount, enableVariantDocMode,
                variantDocMaterializationMinRows, variantDocShardCount, enableNestedGroup, enabled);
    }
}
