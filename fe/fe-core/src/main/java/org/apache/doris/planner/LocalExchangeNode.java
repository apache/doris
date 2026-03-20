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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/ExchangeNode.java
// and modified by Doris

package org.apache.doris.planner;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TLocalExchangeNode;
import org.apache.doris.thrift.TLocalPartitionType;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** LocalExchangeNode */
public class LocalExchangeNode extends PlanNode {
    public static final String EXCHANGE_NODE = "LOCAL-EXCHANGE";

    private LocalExchangeType exchangeType;

    /**
     * use for Nereids only.
     */
    public LocalExchangeNode(PlanNodeId id, PlanNode inputNode, LocalExchangeType exchangeType) {
        this(id, inputNode, exchangeType, null);
    }

    public LocalExchangeNode(PlanNodeId id, PlanNode inputNode, LocalExchangeType exchangeType,
            List<Expr> distributeExprs) {
        super(id, inputNode, EXCHANGE_NODE);
        this.offset = 0;
        this.limit = -1;
        this.conjuncts = Collections.emptyList();
        this.children.add(inputNode);
        this.exchangeType = exchangeType;
        this.fragment = inputNode.getFragment();

        List<Expr> hashExprs = distributeExprs;
        boolean isHashShuffle = (exchangeType == LocalExchangeType.BUCKET_HASH_SHUFFLE
                || exchangeType == LocalExchangeType.LOCAL_EXECUTION_HASH_SHUFFLE
                || exchangeType == LocalExchangeType.GLOBAL_EXECUTION_HASH_SHUFFLE);
        if (isHashShuffle && hashExprs != null && !hashExprs.isEmpty()) {
            setDistributeExprLists(hashExprs);
        }
        TupleDescriptor outputTupleDesc = inputNode.getOutputTupleDesc();
        updateTupleIds(outputTupleDesc);
    }

    public void updateTupleIds(TupleDescriptor outputTupleDesc) {
        if (outputTupleDesc != null) {
            clearTupleIds();
            tupleIds.add(outputTupleDesc.getId());
        } else {
            clearTupleIds();
            tupleIds.addAll(getChild(0).getOutputTupleIds());
        }
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        // FE-planned LocalExchangeNode itself must stay non-serial. In the BE-planned path,
        // the serial semantics belong to the upstream scan/exchange pipeline rather than the
        // downstream LocalExchangeSource pipeline. Marking LocalExchangeNode as serial would
        // incorrectly reduce the downstream pipeline's task count to 1.
        msg.setIsSerialOperator(false);

        msg.node_type = TPlanNodeType.LOCAL_EXCHANGE_NODE;
        msg.local_exchange_node = new TLocalExchangeNode(exchangeType.toThrift());

        if (exchangeType.isHashShuffle()) {
            List<TExpr> thriftDistributeExprLists = new ArrayList<>();
            for (Expr expr : distributeExprLists()) {
                thriftDistributeExprLists.add(expr.treeToThrift());
            }
            msg.local_exchange_node.setDistributeExprLists(thriftDistributeExprLists);
        }
    }

    private List<Expr> distributeExprLists() {
        if (distributeExprLists == null) {
            return Collections.emptyList();
        }
        return distributeExprLists;
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        return prefix + "type: " + exchangeType.name() + "\n";
    }

    public LocalExchangeType getExchangeType() {
        return exchangeType;
    }

    @Override
    protected boolean shouldResetSerialFlagForChild(int childIndex) {
        return true;
    }

    /** LocalExchangeTypeRequire */
    public interface LocalExchangeTypeRequire {
        boolean satisfy(LocalExchangeType provide);

        LocalExchangeType preferType();

        default LocalExchangeTypeRequire autoRequireHash() {
            return RequireHash.INSTANCE;
        }

        static NoRequire noRequire() {
            return NoRequire.INSTANCE;
        }

        static RequireHash requireHash() {
            return RequireHash.INSTANCE;
        }

        static RequireSpecific requirePassthrough() {
            return requireSpecific(LocalExchangeType.PASSTHROUGH);
        }

        static RequireSpecific requirePassToOne() {
            return requireSpecific(LocalExchangeType.PASS_TO_ONE);
        }

        static RequireSpecific requireBroadcast() {
            return requireSpecific(LocalExchangeType.BROADCAST);
        }

        static RequireSpecific requireAdaptivePassthrough() {
            return requireSpecific(LocalExchangeType.ADAPTIVE_PASSTHROUGH);
        }

        static RequireSpecific requireBucketHash() {
            return requireSpecific(LocalExchangeType.BUCKET_HASH_SHUFFLE);
        }

        static RequireSpecific requireGlobalExecutionHash() {
            return requireSpecific(LocalExchangeType.GLOBAL_EXECUTION_HASH_SHUFFLE);
        }

        static RequireSpecific requireSpecific(LocalExchangeType require) {
            return new RequireSpecific(require);
        }

        default LocalExchangeType noopTo(LocalExchangeType defaultType) {
            LocalExchangeType preferType = preferType();
            return (preferType == LocalExchangeType.NOOP) ? defaultType : preferType;
        }
    }

    /** NoRequire */
    public static class NoRequire implements LocalExchangeTypeRequire {
        public static final NoRequire INSTANCE = new NoRequire();

        @Override
        public boolean satisfy(LocalExchangeType provide) {
            return true;
        }

        @Override
        public LocalExchangeType preferType() {
            return LocalExchangeType.NOOP;
        }
    }

    /** RequireHash */
    public static class RequireHash implements LocalExchangeTypeRequire {
        public static final RequireHash INSTANCE = new RequireHash();

        @Override
        public boolean satisfy(LocalExchangeType provide) {
            switch (provide) {
                case GLOBAL_EXECUTION_HASH_SHUFFLE:
                case LOCAL_EXECUTION_HASH_SHUFFLE:
                case BUCKET_HASH_SHUFFLE:
                    return true;
                default:
                    return false;
            }
        }

        @Override
        public LocalExchangeType preferType() {
            return LocalExchangeType.GLOBAL_EXECUTION_HASH_SHUFFLE;
        }

        @Override
        public LocalExchangeTypeRequire autoRequireHash() {
            return this;
        }
    }

    public static class RequireSpecific implements LocalExchangeTypeRequire {
        LocalExchangeType requireType;

        public RequireSpecific(LocalExchangeType requireType) {
            this.requireType = requireType;
        }

        @Override
        public boolean satisfy(LocalExchangeType provide) {
            return requireType == provide;
        }

        @Override
        public LocalExchangeType preferType() {
            return requireType;
        }

        @Override
        public LocalExchangeTypeRequire autoRequireHash() {
            if (requireType == LocalExchangeType.GLOBAL_EXECUTION_HASH_SHUFFLE
                    || requireType == LocalExchangeType.BUCKET_HASH_SHUFFLE) {
                return this;
            }
            return RequireHash.INSTANCE;
        }
    }

    public enum LocalExchangeType {
        NOOP,
        GLOBAL_EXECUTION_HASH_SHUFFLE,
        LOCAL_EXECUTION_HASH_SHUFFLE,
        BUCKET_HASH_SHUFFLE,
        PASSTHROUGH,
        ADAPTIVE_PASSTHROUGH,
        BROADCAST,
        PASS_TO_ONE,
        LOCAL_MERGE_SORT;

        public boolean isHashShuffle() {
            switch (this) {
                case GLOBAL_EXECUTION_HASH_SHUFFLE:
                case LOCAL_EXECUTION_HASH_SHUFFLE:
                case BUCKET_HASH_SHUFFLE:
                    return true;
                default:
                    return false;
            }
        }

        public TLocalPartitionType toThrift() {
            switch (this) {
                case GLOBAL_EXECUTION_HASH_SHUFFLE:
                    return TLocalPartitionType.GLOBAL_EXECUTION_HASH_SHUFFLE;
                case LOCAL_EXECUTION_HASH_SHUFFLE:
                    return TLocalPartitionType.LOCAL_EXECUTION_HASH_SHUFFLE;
                case BUCKET_HASH_SHUFFLE:
                    return TLocalPartitionType.BUCKET_HASH_SHUFFLE;
                case PASSTHROUGH:
                    return TLocalPartitionType.PASSTHROUGH;
                case ADAPTIVE_PASSTHROUGH:
                    return TLocalPartitionType.ADAPTIVE_PASSTHROUGH;
                case BROADCAST:
                    return TLocalPartitionType.BROADCAST;
                case PASS_TO_ONE:
                    return TLocalPartitionType.PASS_TO_ONE;
                case LOCAL_MERGE_SORT:
                    return TLocalPartitionType.LOCAL_MERGE_SORT;
                default: {
                    throw new IllegalStateException("Unsupported LocalExchangeType: " + this);
                }
            }
        }
    }
}
