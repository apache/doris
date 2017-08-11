// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

package com.baidu.palo.planner;

import com.baidu.palo.analysis.Analyzer;
import com.baidu.palo.analysis.Expr;
import com.baidu.palo.analysis.ExprSubstitutionMap;
import com.baidu.palo.analysis.InsertStmt;
import com.baidu.palo.analysis.TupleDescriptor;
import com.baidu.palo.catalog.Column;
import com.baidu.palo.catalog.Table;
import com.baidu.palo.thrift.TExplainLevel;
import com.baidu.palo.thrift.TOlapRewriteNode;
import com.baidu.palo.thrift.TPlanNode;
import com.baidu.palo.thrift.TPlanNodeType;
import com.baidu.palo.common.InternalException;

import com.google.common.collect.Lists;
import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.List;

// Used to convert column to valid OLAP table
public class OlapRewriteNode extends PlanNode {
    private static final Logger LOG = LogManager.getLogger(OlapRewriteNode.class);

    private InsertStmt insertStmt;

    private Table table;
    private TupleDescriptor tupleDescriptor;
    private List<Expr> newResultExprs;

    public OlapRewriteNode(PlanNodeId id, PlanNode child, InsertStmt insertStmt) {
        super(id, insertStmt.getOlapTuple().getId().asList(), "OLAP REWRITE NODE");
        addChild(child);

        this.table = insertStmt.getTargetTable();
        this.tupleDescriptor = insertStmt.getOlapTuple();
        this.insertStmt = insertStmt;
    }

    public OlapRewriteNode(PlanNodeId id, PlanNode child,
                           Table table,
                           TupleDescriptor tupleDescriptor,
                           List<Expr> slotRefs) {
        super(id, child.getTupleIds(), "OLAP REWRITE NODE");
        addChild(child);
        this.table = table;
        this.tupleDescriptor = tupleDescriptor;
        this.newResultExprs = slotRefs;
    }

    @Override
    public void init(Analyzer analyzer) throws InternalException {
        assignConjuncts(analyzer);
      
        // Set smap to the combined childrens' smaps and apply that to all conjuncts_.
        createDefaultSmap(analyzer);

        computeStats(analyzer);
        // assignedConjuncts = analyzr.getAssignedConjuncts();

        if (insertStmt != null) {
            ExprSubstitutionMap combinedChildSmap = getCombinedChildSmap();
            newResultExprs = Lists.newArrayList();
            for (Expr expr : insertStmt.getResultExprs()) {
                newResultExprs.add(expr.clone(combinedChildSmap));
            }
        }
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.OLAP_REWRITE_NODE;
        TOlapRewriteNode tnode = new TOlapRewriteNode();
        for (Column column : table.getBaseSchema()) {
            tnode.addToColumn_types(column.getColumnType().toThrift());
        }
        for (Expr expr : newResultExprs) {
            tnode.addToColumns(expr.treeToThrift());
        }
        tnode.setOutput_tuple_id(tupleDescriptor.getId().asInt());
        msg.setOlap_rewrite_node(tnode);
    }

    @Override
    public void computeStats(Analyzer analyzer) {
        super.computeStats(analyzer);
        if (getChild(0).cardinality == -1) {
            cardinality = -1;
        } else {
            cardinality = Math.round(((double) getChild(0).cardinality) * computeSelectivity());
            Preconditions.checkState(cardinality >= 0);
        }
        LOG.info("stats Select: cardinality=" + Long.toString(cardinality));
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        if (!conjuncts.isEmpty()) {
            output.append(prefix + "predicates: " + getExplainString(conjuncts) + "\n");
        }
        return output.toString();
    }

    @Override
    public int getNumInstances() {
        return children.get(0).getNumInstances();
    }
}
