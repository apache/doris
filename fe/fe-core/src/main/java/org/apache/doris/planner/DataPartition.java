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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprSubstitutionMap;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TDataPartition;
import org.apache.doris.thrift.TExplainLevel;
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
    public static final DataPartition TABLET_ID = new DataPartition(TPartitionType.TABLET_SINK_SHUFFLE_PARTITIONED);

    private final TPartitionType type;
    // for hash partition: exprs used to compute hash value
    private ImmutableList<Expr> partitionExprs;

    public DataPartition(TPartitionType type, List<Expr> exprs) {
        Preconditions.checkNotNull(exprs);
        Preconditions.checkState(!exprs.isEmpty());
        Preconditions.checkState(type == TPartitionType.HASH_PARTITIONED
                || type == TPartitionType.RANGE_PARTITIONED
                || type == TPartitionType.TABLE_SINK_HASH_PARTITIONED
                || type == TPartitionType.BUCKET_SHFFULE_HASH_PARTITIONED);
        this.type = type;
        this.partitionExprs = ImmutableList.copyOf(exprs);
    }

    public DataPartition(TPartitionType type) {
        Preconditions.checkState(type == TPartitionType.UNPARTITIONED
                || type == TPartitionType.RANDOM
                || type == TPartitionType.TABLE_SINK_RANDOM_PARTITIONED
                || type == TPartitionType.TABLET_SINK_SHUFFLE_PARTITIONED);
        this.type = type;
        this.partitionExprs = ImmutableList.of();
    }

    public static DataPartition hashPartitioned(List<Expr> exprs) {
        return new DataPartition(TPartitionType.HASH_PARTITIONED, exprs);
    }

    public void substitute(ExprSubstitutionMap smap, Analyzer analyzer) throws AnalysisException {
        List<Expr> list = Expr.trySubstituteList(partitionExprs, smap, analyzer, false);
        partitionExprs = ImmutableList.copyOf(list);
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
            result.setPartitionExprs(Expr.treesToThrift(partitionExprs));
        }
        return result;
    }

    public String getExplainString(TExplainLevel explainLevel) {
        StringBuilder str = new StringBuilder();
        str.append(type.toString());
        if (explainLevel == TExplainLevel.BRIEF) {
            return str.toString();
        }
        if (!partitionExprs.isEmpty()) {
            List<String> strings = Lists.newArrayList();
            for (Expr expr : partitionExprs) {
                strings.add(expr.toSql());
            }
            str.append(": ").append(Joiner.on(", ").join(strings));
        }
        str.append("\n");
        return str.toString();
    }
}
