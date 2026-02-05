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

package org.apache.doris.catalog;

import org.apache.doris.thrift.TTypeDesc;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class VariantType extends ScalarType {
    private static final Logger LOG = LogManager.getLogger(VariantType.class);
    @SerializedName(value = "fieldMap")
    private final HashMap<String, VariantField> fieldMap = Maps.newHashMap();

    @SerializedName(value = "fields")
    private final ArrayList<VariantField> predefinedFields;

    @SerializedName(value = "variantMaxSubcolumnsCount")
    private final int variantMaxSubcolumnsCount;

    @SerializedName(value = "enableTypedPathsToSparse")
    private final boolean enableTypedPathsToSparse;

    @SerializedName(value = "variantMaxSparseColumnStatisticsSize")
    private final int variantMaxSparseColumnStatisticsSize;

    @SerializedName(value = "variantSparseHashShardCount")
    private final int variantSparseHashShardCount;

    @SerializedName(value = "enableVariantDocMode")
    private final boolean enableVariantDocMode;

    @SerializedName(value = "variantDocMaterializationMinRows")
    private final long variantDocMaterializationMinRows;

    @SerializedName(value = "variantDocShardCount")
    private final int variantDocShardCount;

    private Map<String, String> properties = Maps.newHashMap();

    public VariantType() {
        super(PrimitiveType.VARIANT);
        this.predefinedFields = Lists.newArrayList();
        this.variantMaxSubcolumnsCount = 0;
        this.enableTypedPathsToSparse = false;
        this.variantMaxSparseColumnStatisticsSize = 10000;
        this.variantSparseHashShardCount = 0;
        this.enableVariantDocMode = false;
        this.variantDocMaterializationMinRows = 0L;
        this.variantDocShardCount = 64;
    }

    public VariantType(ArrayList<VariantField> fields) {
        super(PrimitiveType.VARIANT);
        Preconditions.checkNotNull(fields);
        this.predefinedFields = fields;
        for (VariantField predefinedField : this.predefinedFields) {
            fieldMap.put(predefinedField.getPattern(), predefinedField);
        }
        this.variantMaxSubcolumnsCount = 0;
        this.enableTypedPathsToSparse = false;
        this.variantMaxSparseColumnStatisticsSize = 10000;
        this.variantSparseHashShardCount = 0;
        this.enableVariantDocMode = false;
        this.variantDocMaterializationMinRows = 0L;
        this.variantDocShardCount = 64;
    }

    public VariantType(Map<String, String> properties) {
        super(PrimitiveType.VARIANT);
        this.predefinedFields = Lists.newArrayList();
        this.properties = properties;
        this.variantMaxSubcolumnsCount = 0;
        this.enableTypedPathsToSparse = false;
        this.variantMaxSparseColumnStatisticsSize = 10000;
        this.variantSparseHashShardCount = 0;
        this.enableVariantDocMode = false;
        this.variantDocMaterializationMinRows = 0L;
        this.variantDocShardCount = 64;
    }

    public VariantType(ArrayList<VariantField> fields, Map<String, String> properties) {
        super(PrimitiveType.VARIANT);
        Preconditions.checkNotNull(fields);
        this.predefinedFields = fields;
        for (VariantField predefinedField : this.predefinedFields) {
            fieldMap.put(predefinedField.getPattern(), predefinedField);
        }
        this.properties = properties;
        this.variantMaxSubcolumnsCount = 0;
        this.enableTypedPathsToSparse = false;
        this.variantMaxSparseColumnStatisticsSize = 10000;
        this.variantSparseHashShardCount = 0;
        this.enableVariantDocMode = false;
        this.variantDocMaterializationMinRows = 0L;
        this.variantDocShardCount = 64;
    }

    public VariantType(ArrayList<VariantField> fields, int variantMaxSubcolumnsCount,
                                                        boolean enableTypedPathsToSparse,
                                                        int variantMaxSparseColumnStatisticsSize,
                                                        int variantSparseHashShardCount,
                                                        boolean enableVariantDocMode,
                                                        long variantDocMaterializationMinRows,
                                                        int variantDocShardCount) {
        super(PrimitiveType.VARIANT);
        Preconditions.checkNotNull(fields);
        this.predefinedFields = fields;
        for (VariantField predefinedField : this.predefinedFields) {
            fieldMap.put(predefinedField.getPattern(), predefinedField);
        }
        this.variantMaxSubcolumnsCount = variantMaxSubcolumnsCount;
        this.enableTypedPathsToSparse = enableTypedPathsToSparse;
        this.variantMaxSparseColumnStatisticsSize = variantMaxSparseColumnStatisticsSize;
        this.variantSparseHashShardCount = variantSparseHashShardCount;
        this.enableVariantDocMode = enableVariantDocMode;
        this.variantDocMaterializationMinRows = variantDocMaterializationMinRows;
        this.variantDocShardCount = variantDocShardCount;
    }

    @Override
    public String toSql(int depth) {
        StringBuilder sb = new StringBuilder();
        sb.append("variant");
        sb.append("<");
        if (!predefinedFields.isEmpty()) {
            sb.append(predefinedFields.stream()
                                .map(variantField -> variantField.toSql(depth)).collect(Collectors.joining(",")));
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
                                        .append(String.valueOf(variantMaxSparseColumnStatisticsSize)).append("\"");
            sb.append(",");
            sb.append("\"variant_sparse_hash_shard_count\" = \"")
                                        .append(String.valueOf(variantSparseHashShardCount)).append("\"");
        }
        sb.append(")>");
        return sb.toString();
    }

    public ArrayList<VariantField> getPredefinedFields() {
        return predefinedFields;
    }

    @Override
    public void toThrift(TTypeDesc container) {
        super.toThrift(container);
        // set the count
        container.getTypes().get(container.getTypes().size() - 1)
                .scalar_type.setVariantMaxSubcolumnsCount(variantMaxSubcolumnsCount);
    }

    @Override
    public boolean supportSubType(Type subType) {
        for (Type supportedType : Type.getVariantSubTypes()) {
            // Only one level of array is supported
            if (subType.getPrimitiveType() == PrimitiveType.ARRAY
                    && ((ArrayType) subType).getItemType().getPrimitiveType() != PrimitiveType.ARRAY) {
                return supportSubType(((ArrayType) subType).getItemType());
            }
            if (subType.getPrimitiveType() == supportedType.getPrimitiveType()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof VariantType)) {
            return false;
        }
        VariantType otherVariantType = (VariantType) other;
        return Objects.equals(otherVariantType.getPredefinedFields(), predefinedFields)
                && variantMaxSubcolumnsCount == otherVariantType.variantMaxSubcolumnsCount
                && enableTypedPathsToSparse == otherVariantType.enableTypedPathsToSparse
                && enableVariantDocMode == otherVariantType.enableVariantDocMode
                && variantDocMaterializationMinRows == otherVariantType.variantDocMaterializationMinRows;
    }

    @Override
    public boolean matchesType(Type type) {
        return type.isVariantType();
    }

    public int getVariantMaxSubcolumnsCount() {
        return variantMaxSubcolumnsCount;
    }

    public boolean getEnableTypedPathsToSparse() {
        return enableTypedPathsToSparse;
    }

    public Map<String, String> getProperties() {
        return properties;
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
}
