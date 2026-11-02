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

import org.apache.doris.common.DdlException;

import org.apache.commons.collections4.CollectionUtils;

import java.util.Objects;
import java.util.function.Function;

/** Statement-time validation of Variant write-property changes; not used during metadata replay. */
public final class VariantPropertyChangePolicy {
    enum ChangeKind {
        LIGHT, IMMUTABLE, TEMPLATE
    }

    // Classify persisted VariantType fields only. The unpersisted raw properties map is not policy state.
    // Declaration order preserves master's diagnostic precedence for immutable differences.
    enum FieldPolicy {
        MAX_SUBCOLUMNS_COUNT("variantMaxSubcolumnsCount", Column::getVariantMaxSubcolumnsCount,
                "Can not change variant max subcolumns count"),
        TYPED_PATHS_TO_SPARSE("enableTypedPathsToSparse", Column::getVariantEnableTypedPathsToSparse,
                "Can not change variant enable typed paths to sparse"),
        SPARSE_STATISTICS_SIZE("variantMaxSparseColumnStatisticsSize", Column::getVariantMaxSparseColumnStatisticsSize,
                "Can not change variant max sparse column statistics size"),
        SPARSE_SHARD_COUNT("variantSparseHashShardCount", Column::getVariantSparseHashShardCount,
                "Can not change variant sparse bucket num"),
        DOC_MODE("enableVariantDocMode", Column::getVariantEnableDocMode,
                "Can not change variant enable doc snapshot mode"),
        DOC_SHARD_COUNT("variantDocShardCount", Column::getVariantDocShardCount,
                "Can not change variant doc snapshot shard count"),
        NESTED_GROUP("enableNestedGroup", Column::getVariantEnableNestedGroup,
                "Can not change variant enable nested group"),
        DOC_MATERIALIZATION_MIN_ROWS("variantDocMaterializationMinRows", ChangeKind.LIGHT),
        FIELDS("fields", ChangeKind.TEMPLATE),
        FIELD_MAP("fieldMap", ChangeKind.TEMPLATE);

        final String serializedName;
        final ChangeKind kind;
        private final Function<Column, ?> value;
        private final String rejectionMessage;

        FieldPolicy(String serializedName, Function<Column, ?> value, String rejectionMessage) {
            this.serializedName = serializedName;
            this.kind = ChangeKind.IMMUTABLE;
            this.value = value;
            this.rejectionMessage = rejectionMessage;
        }

        FieldPolicy(String serializedName, ChangeKind kind) {
            this.serializedName = serializedName;
            this.kind = kind;
            this.value = null;
            this.rejectionMessage = null;
        }
    }

    private VariantPropertyChangePolicy() {
    }

    public static void checkColumnLevel(Column source, Column target) throws DdlException {
        for (FieldPolicy field : FieldPolicy.values()) {
            if (field.kind == ChangeKind.IMMUTABLE
                    && !Objects.equals(field.value.apply(source), field.value.apply(target))) {
                throw new DdlException(field.rejectionMessage);
            }
        }
        // Both persisted template fields follow master's children rule, even for unchanged templates.
        if (CollectionUtils.isNotEmpty(source.getChildren()) || CollectionUtils.isNotEmpty(target.getChildren())) {
            throw new DdlException("Can not change variant schema templates");
        }
    }
}
