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

package org.apache.doris.nereids.properties;

import org.apache.doris.nereids.trees.expressions.ExprId;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/**
 * Merge-style distribution: route insert and delete/update rows using different partition keys.
 */
public class DistributionSpecMerge extends DistributionSpec {
    /**
     * Iceberg partition field metadata for merge insert routing.
     */
    public static class IcebergPartitionField {
        private final String transform;
        private final ExprId sourceExprId;
        private final Integer param;
        private final String name;
        private final Integer sourceId;

        /**
         * Create a partition field mapping for merge insert routing.
         */
        public IcebergPartitionField(String transform, ExprId sourceExprId, Integer param,
                String name, Integer sourceId) {
            this.transform = Objects.requireNonNull(transform, "transform should not be null");
            this.sourceExprId = Objects.requireNonNull(sourceExprId, "sourceExprId should not be null");
            this.param = param;
            this.name = name;
            this.sourceId = sourceId;
        }

        public String getTransform() {
            return transform;
        }

        public ExprId getSourceExprId() {
            return sourceExprId;
        }

        public Integer getParam() {
            return param;
        }

        public String getName() {
            return name;
        }

        public Integer getSourceId() {
            return sourceId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            IcebergPartitionField that = (IcebergPartitionField) o;
            return transform.equals(that.transform)
                    && sourceExprId.equals(that.sourceExprId)
                    && Objects.equals(param, that.param)
                    && Objects.equals(name, that.name)
                    && Objects.equals(sourceId, that.sourceId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(transform, sourceExprId, param, name, sourceId);
        }
    }

    private final ExprId operationExprId;
    private final ImmutableList<ExprId> insertPartitionExprIds;
    private final ImmutableList<ExprId> deletePartitionExprIds;
    private final boolean insertRandom;
    private final ImmutableList<IcebergPartitionField> insertPartitionFields;
    private final Integer partitionSpecId;

    /**
     * Create merge distribution spec for Iceberg DML routing.
     */
    public DistributionSpecMerge(ExprId operationExprId, List<ExprId> insertPartitionExprIds,
            List<ExprId> deletePartitionExprIds, boolean insertRandom,
            List<IcebergPartitionField> insertPartitionFields, Integer partitionSpecId) {
        this.operationExprId = Objects.requireNonNull(operationExprId, "operationExprId should not be null");
        this.insertPartitionExprIds = ImmutableList.copyOf(
                Objects.requireNonNull(insertPartitionExprIds, "insertPartitionExprIds should not be null"));
        this.deletePartitionExprIds = ImmutableList.copyOf(
                Objects.requireNonNull(deletePartitionExprIds, "deletePartitionExprIds should not be null"));
        this.insertRandom = insertRandom;
        this.insertPartitionFields = ImmutableList.copyOf(
                Objects.requireNonNull(insertPartitionFields, "insertPartitionFields should not be null"));
        this.partitionSpecId = partitionSpecId;
        Preconditions.checkState(!deletePartitionExprIds.isEmpty(), "deletePartitionExprIds should not be empty");
    }

    public ExprId getOperationExprId() {
        return operationExprId;
    }

    public List<ExprId> getInsertPartitionExprIds() {
        return insertPartitionExprIds;
    }

    public List<ExprId> getDeletePartitionExprIds() {
        return deletePartitionExprIds;
    }

    public boolean isInsertRandom() {
        return insertRandom;
    }

    public List<IcebergPartitionField> getInsertPartitionFields() {
        return insertPartitionFields;
    }

    public Integer getPartitionSpecId() {
        return partitionSpecId;
    }

    @Override
    public boolean satisfy(DistributionSpec required) {
        if (required instanceof DistributionSpecAny) {
            return true;
        }
        if (!(required instanceof DistributionSpecMerge)) {
            return false;
        }
        DistributionSpecMerge other = (DistributionSpecMerge) required;
        return insertRandom == other.insertRandom
                && operationExprId.equals(other.operationExprId)
                && insertPartitionExprIds.equals(other.insertPartitionExprIds)
                && deletePartitionExprIds.equals(other.deletePartitionExprIds)
                && insertPartitionFields.equals(other.insertPartitionFields)
                && Objects.equals(partitionSpecId, other.partitionSpecId);
    }

    @Override
    public String shapeInfo() {
        return "DistributionSpecMerge";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DistributionSpecMerge that = (DistributionSpecMerge) o;
        return insertRandom == that.insertRandom
                && operationExprId.equals(that.operationExprId)
                && insertPartitionExprIds.equals(that.insertPartitionExprIds)
                && deletePartitionExprIds.equals(that.deletePartitionExprIds)
                && insertPartitionFields.equals(that.insertPartitionFields)
                && Objects.equals(partitionSpecId, that.partitionSpecId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operationExprId, insertPartitionExprIds, deletePartitionExprIds,
                insertRandom, insertPartitionFields, partitionSpecId);
    }
}
