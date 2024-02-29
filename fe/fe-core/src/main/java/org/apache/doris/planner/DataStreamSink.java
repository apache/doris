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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/DataStreamSink.java
// and modified by Doris

package org.apache.doris.planner;

import org.apache.doris.analysis.BitmapFilterPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TDataStreamSink;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TOlapTableLocationParam;
import org.apache.doris.thrift.TOlapTablePartitionParam;
import org.apache.doris.thrift.TOlapTableSchemaParam;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Data sink that forwards data to an exchange node.
 */
public class DataStreamSink extends DataSink {

    private PlanNodeId exchNodeId;

    private DataPartition outputPartition;

    protected TupleDescriptor outputTupleDesc;

    protected List<Expr> projections;

    protected List<Expr> conjuncts = Lists.newArrayList();

    protected List<RuntimeFilter> runtimeFilters = Lists.newArrayList();

    // use for tablet id shuffle sink only
    protected TOlapTableSchemaParam tabletSinkSchemaParam = null;
    protected TOlapTablePartitionParam tabletSinkPartitionParam = null;
    protected TOlapTableLocationParam tabletSinkLocationParam = null;
    protected TupleDescriptor tabletSinkTupleDesc = null;
    protected long tabletSinkTxnId = -1;

    public DataStreamSink() {

    }

    public DataStreamSink(PlanNodeId exchNodeId) {
        this.exchNodeId = exchNodeId;
    }

    @Override
    public PlanNodeId getExchNodeId() {
        return exchNodeId;
    }

    public void setExchNodeId(PlanNodeId exchNodeId) {
        this.exchNodeId = exchNodeId;
    }

    @Override
    public DataPartition getOutputPartition() {
        return outputPartition;
    }

    public void setOutputPartition(DataPartition outputPartition) {
        this.outputPartition = outputPartition;
    }

    public TupleDescriptor getOutputTupleDesc() {
        return outputTupleDesc;
    }

    public void setOutputTupleDesc(TupleDescriptor outputTupleDesc) {
        this.outputTupleDesc = outputTupleDesc;
    }

    public List<Expr> getProjections() {
        return projections;
    }

    public void setProjections(List<Expr> projections) {
        this.projections = projections;
    }

    public List<Expr> getConjuncts() {
        return conjuncts;
    }

    public void setConjuncts(List<Expr> conjuncts) {
        this.conjuncts = conjuncts;
    }

    public void addConjunct(Expr conjunct) {
        this.conjuncts.add(conjunct);
    }

    public List<RuntimeFilter> getRuntimeFilters() {
        return runtimeFilters;
    }

    public void setRuntimeFilters(List<RuntimeFilter> runtimeFilters) {
        this.runtimeFilters = runtimeFilters;
    }

    public void addRuntimeFilter(RuntimeFilter runtimeFilter) {
        this.runtimeFilters.add(runtimeFilter);
    }

    public void setTabletSinkSchemaParam(TOlapTableSchemaParam schemaParam) {
        this.tabletSinkSchemaParam = schemaParam;
    }

    public void setTabletSinkPartitionParam(TOlapTablePartitionParam partitionParam) {
        this.tabletSinkPartitionParam = partitionParam;
    }

    public void setTabletSinkTupleDesc(TupleDescriptor tupleDesc) {
        this.tabletSinkTupleDesc = tupleDesc;
    }

    public void setTabletSinkLocationParam(TOlapTableLocationParam locationParam) {
        this.tabletSinkLocationParam = locationParam;
    }

    public void setTabletSinkTxnId(long txnId) {
        this.tabletSinkTxnId = txnId;
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix).append("STREAM DATA SINK\n");
        strBuilder.append(prefix).append("  EXCHANGE ID: ").append(exchNodeId);
        if (outputPartition != null) {
            strBuilder.append("\n").append(prefix).append("  ").append(outputPartition.getExplainString(explainLevel));
        }
        if (!conjuncts.isEmpty()) {
            Expr expr = PlanNode.convertConjunctsToAndCompoundPredicate(conjuncts);
            strBuilder.append(prefix).append("  CONJUNCTS: ").append(expr.toSql()).append("\n");
        }
        if (!runtimeFilters.isEmpty()) {
            strBuilder.append(prefix).append("  runtime filters: ");
            strBuilder.append(getRuntimeFilterExplainString(false, false));
        }
        if (!CollectionUtils.isEmpty(projections)) {
            strBuilder.append(prefix).append("  PROJECTIONS: ")
                    .append(PlanNode.getExplainString(projections)).append("\n");
            strBuilder.append(prefix).append("  PROJECTION TUPLE: ").append(outputTupleDesc.getId());
            strBuilder.append("\n");
        }

        return strBuilder.toString();
    }

    protected String getRuntimeFilterExplainString(boolean isBuildNode, boolean isBrief) {
        if (runtimeFilters.isEmpty()) {
            return "";
        }
        List<String> filtersStr = new ArrayList<>();
        for (RuntimeFilter filter : runtimeFilters) {
            filtersStr.add(filter.getExplainString(isBuildNode, isBrief, getExchNodeId()));
        }
        return Joiner.on(", ").join(filtersStr) + "\n";
    }

    @Override
    protected TDataSink toThrift() {
        TDataSink result = new TDataSink(TDataSinkType.DATA_STREAM_SINK);
        TDataStreamSink tStreamSink =
                new TDataStreamSink(exchNodeId.asInt(), outputPartition.toThrift());
        for (Expr e : conjuncts) {
            if  (!(e instanceof BitmapFilterPredicate)) {
                tStreamSink.addToConjuncts(e.treeToThrift());
            }
        }
        if (projections != null) {
            for (Expr expr : projections) {
                tStreamSink.addToOutputExprs(expr.treeToThrift());
            }
        }
        if (outputTupleDesc != null) {
            tStreamSink.setOutputTupleId(outputTupleDesc.getId().asInt());
        }

        if (runtimeFilters != null) {
            for (RuntimeFilter rf : runtimeFilters) {
                tStreamSink.addToRuntimeFilters(rf.toThrift());
            }
        }
        Preconditions.checkState((tabletSinkSchemaParam != null) == (tabletSinkPartitionParam != null),
                "schemaParam and partitionParam should be set together.");
        if (tabletSinkSchemaParam != null) {
            tStreamSink.setTabletSinkSchema(tabletSinkSchemaParam);
        }
        if (tabletSinkPartitionParam != null) {
            tStreamSink.setTabletSinkPartition(tabletSinkPartitionParam);
        }
        if (tabletSinkTupleDesc != null) {
            tStreamSink.setTabletSinkTupleId(tabletSinkTupleDesc.getId().asInt());
        }
        if (tabletSinkLocationParam != null) {
            tStreamSink.setTabletSinkLocation(tabletSinkLocationParam);
        }
        tStreamSink.setTabletSinkTxnId(tabletSinkTxnId);
        result.setStreamSink(tStreamSink);
        return result;
    }
}
