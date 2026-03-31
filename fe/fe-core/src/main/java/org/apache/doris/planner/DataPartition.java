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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/DataPartition.java
// and modified by Doris

package org.apache.doris.planner;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprToSqlVisitor;
import org.apache.doris.analysis.ExprToThriftVisitor;
import org.apache.doris.analysis.ToSqlParams;
import org.apache.doris.thrift.TDataPartition;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TIcebergPartitionField;
import org.apache.doris.thrift.TMergePartitionInfo;
import org.apache.doris.thrift.TPartitionType;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Specification of the partition of a single stream of data.
 * Examples of those streams of data are: the scan of a table; the output
 * of a plan fragment; etc. (ie, this is not restricted to direct exchanges
 * between two fragments, which in the backend is facilitated by the classes
 * DataStreamSender/DataStreamMgr/DataStreamRecvr).
 * TODO: better name? just Partitioning?
 */
public class DataPartition {

    public static final DataPartition UNPARTITIONED = new DataPartition(TPartitionType.UNPARTITIONED);
    public static final DataPartition RANDOM = new DataPartition(TPartitionType.RANDOM);
    public static final DataPartition TABLET_ID = new DataPartition(TPartitionType.OLAP_TABLE_SINK_HASH_PARTITIONED);

    private final TPartitionType type;
    // for hash partition: exprs used to compute hash value
    private ImmutableList<Expr> partitionExprs;
    private MergePartitionInfo mergePartitionInfo;

    public DataPartition(TPartitionType type, List<Expr> exprs) {
        Preconditions.checkNotNull(exprs);
        Preconditions.checkState(!exprs.isEmpty());
        Preconditions.checkState(type == TPartitionType.HASH_PARTITIONED
                || type == TPartitionType.RANGE_PARTITIONED
                || type == TPartitionType.HIVE_TABLE_SINK_HASH_PARTITIONED
                || type == TPartitionType.BUCKET_SHFFULE_HASH_PARTITIONED);
        this.type = type;
        this.partitionExprs = ImmutableList.copyOf(exprs);
    }

    public DataPartition(TPartitionType type) {
        Preconditions.checkState(type == TPartitionType.UNPARTITIONED
                || type == TPartitionType.RANDOM
                || type == TPartitionType.HIVE_TABLE_SINK_UNPARTITIONED
                || type == TPartitionType.OLAP_TABLE_SINK_HASH_PARTITIONED);
        this.type = type;
        this.partitionExprs = ImmutableList.of();
    }

    public DataPartition(TPartitionType type, Expr operationExpr, List<Expr> insertPartitionExprs,
            List<Expr> deletePartitionExprs, boolean insertRandom,
            List<IcebergPartitionField> insertPartitionFields, Integer partitionSpecId) {
        Preconditions.checkState(type == TPartitionType.MERGE_PARTITIONED);
        this.type = type;
        this.partitionExprs = ImmutableList.of();
        this.mergePartitionInfo = new MergePartitionInfo(operationExpr, insertPartitionExprs,
                deletePartitionExprs, insertRandom, insertPartitionFields, partitionSpecId);
    }

    public boolean isPartitioned() {
        return type != TPartitionType.UNPARTITIONED;
    }

    public boolean isBucketShuffleHashPartition() {
        return type == TPartitionType.BUCKET_SHFFULE_HASH_PARTITIONED;
    }

    public TPartitionType getType() {
        return type;
    }

    public List<Expr> getPartitionExprs() {
        return partitionExprs;
    }

    public TDataPartition toThrift() {
        TDataPartition result = new TDataPartition(type);
        if (partitionExprs != null) {
            result.setPartitionExprs(ExprToThriftVisitor.treesToThrift(partitionExprs));
        }
        if (mergePartitionInfo != null) {
            result.setMergePartitionInfo(mergePartitionInfo.toThrift());
        }
        return result;
    }

    public String getExplainString(TExplainLevel explainLevel) {
        StringBuilder str = new StringBuilder();
        str.append(type.toString());
        if (explainLevel == TExplainLevel.BRIEF) {
            return str.toString();
        }
        if (mergePartitionInfo != null) {
            str.append(": op=").append(mergePartitionInfo.operationExpr
                    .accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE));
            if (mergePartitionInfo.insertRandom) {
                str.append(", insert=RR");
            } else if (!mergePartitionInfo.insertPartitionExprs.isEmpty()) {
                List<String> insertExprs = Lists.newArrayList();
                for (Expr expr : mergePartitionInfo.insertPartitionExprs) {
                    insertExprs.add(expr.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE));
                }
                str.append(", insert=").append(Joiner.on(", ").join(insertExprs));
            } else if (!mergePartitionInfo.insertPartitionFields.isEmpty()) {
                List<String> insertFields = Lists.newArrayList();
                for (IcebergPartitionField field : mergePartitionInfo.insertPartitionFields) {
                    insertFields.add(field.toSql());
                }
                str.append(", insert=").append(Joiner.on(", ").join(insertFields));
            }
            if (!mergePartitionInfo.deletePartitionExprs.isEmpty()) {
                List<String> deleteExprs = Lists.newArrayList();
                for (Expr expr : mergePartitionInfo.deletePartitionExprs) {
                    deleteExprs.add(expr.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE));
                }
                str.append(", delete=").append(Joiner.on(", ").join(deleteExprs));
            }
        } else if (!partitionExprs.isEmpty()) {
            List<String> strings = Lists.newArrayList();
            for (Expr expr : partitionExprs) {
                strings.add(expr.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE));
            }
            str.append(": ").append(Joiner.on(", ").join(strings));
        }
        str.append("\n");
        return str.toString();
    }

    public static class IcebergPartitionField {
        private final Expr sourceExpr;
        private final String transform;
        private final Integer param;
        private final String name;
        private final Integer sourceId;

        public IcebergPartitionField(Expr sourceExpr, String transform, Integer param,
                String name, Integer sourceId) {
            this.sourceExpr = Preconditions.checkNotNull(sourceExpr, "sourceExpr should not be null");
            this.transform = Preconditions.checkNotNull(transform, "transform should not be null");
            this.param = param;
            this.name = name;
            this.sourceId = sourceId;
        }

        public TIcebergPartitionField toThrift() {
            TIcebergPartitionField field = new TIcebergPartitionField();
            field.setTransform(transform);
            field.setSourceExpr(ExprToThriftVisitor.treeToThrift(sourceExpr));
            if (param != null) {
                field.setParam(param);
            }
            if (name != null) {
                field.setName(name);
            }
            if (sourceId != null) {
                field.setSourceId(sourceId);
            }
            return field;
        }

        public String toSql() {
            return transform + "(" + sourceExpr.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE) + ")";
        }
    }

    private static class MergePartitionInfo {
        private final Expr operationExpr;
        private final ImmutableList<Expr> insertPartitionExprs;
        private final ImmutableList<Expr> deletePartitionExprs;
        private final boolean insertRandom;
        private final ImmutableList<IcebergPartitionField> insertPartitionFields;
        private final Integer partitionSpecId;

        private MergePartitionInfo(Expr operationExpr, List<Expr> insertPartitionExprs,
                List<Expr> deletePartitionExprs, boolean insertRandom,
                List<IcebergPartitionField> insertPartitionFields, Integer partitionSpecId) {
            this.operationExpr = Preconditions.checkNotNull(operationExpr, "operationExpr should not be null");
            this.insertPartitionExprs = ImmutableList.copyOf(
                    Preconditions.checkNotNull(insertPartitionExprs, "insertPartitionExprs should not be null"));
            this.deletePartitionExprs = ImmutableList.copyOf(
                    Preconditions.checkNotNull(deletePartitionExprs, "deletePartitionExprs should not be null"));
            this.insertRandom = insertRandom;
            this.insertPartitionFields = ImmutableList.copyOf(
                    Preconditions.checkNotNull(insertPartitionFields, "insertPartitionFields should not be null"));
            this.partitionSpecId = partitionSpecId;
        }

        private TMergePartitionInfo toThrift() {
            TMergePartitionInfo info = new TMergePartitionInfo();
            info.setOperationExpr(ExprToThriftVisitor.treeToThrift(operationExpr));
            if (!insertPartitionExprs.isEmpty()) {
                info.setInsertPartitionExprs(ExprToThriftVisitor.treesToThrift(insertPartitionExprs));
            }
            if (!deletePartitionExprs.isEmpty()) {
                info.setDeletePartitionExprs(ExprToThriftVisitor.treesToThrift(deletePartitionExprs));
            }
            info.setInsertRandom(insertRandom);
            if (!insertPartitionFields.isEmpty()) {
                List<TIcebergPartitionField> fields = Lists.newArrayList();
                for (IcebergPartitionField field : insertPartitionFields) {
                    fields.add(field.toThrift());
                }
                info.setInsertPartitionFields(fields);
            }
            if (partitionSpecId != null) {
                info.setPartitionSpecId(partitionSpecId);
            }
            return info;
        }
    }
}
