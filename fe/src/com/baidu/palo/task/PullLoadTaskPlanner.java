// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.task;

import com.baidu.palo.analysis.Analyzer;
import com.baidu.palo.analysis.DescriptorTable;
import com.baidu.palo.analysis.Expr;
import com.baidu.palo.analysis.SlotDescriptor;
import com.baidu.palo.analysis.SlotRef;
import com.baidu.palo.analysis.TupleDescriptor;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Column;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.catalog.OlapTable;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.InternalException;
import com.baidu.palo.common.NotImplementedException;
import com.baidu.palo.planner.BrokerScanNode;
import com.baidu.palo.planner.DataPartition;
import com.baidu.palo.planner.DataSplitSink;
import com.baidu.palo.planner.ExchangeNode;
import com.baidu.palo.planner.OlapRewriteNode;
import com.baidu.palo.planner.PlanFragment;
import com.baidu.palo.planner.PlanFragmentId;
import com.baidu.palo.planner.PlanNodeId;
import com.baidu.palo.planner.ScanNode;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;

// Planner used to generate a plan for pull load ETL work
public class PullLoadTaskPlanner {
    private static final Logger LOG = LogManager.getLogger(PullLoadTaskPlanner.class);

    // Input param
    private final PullLoadTask task;

    // Something useful
    private final Analyzer analyzer;
    private final DescriptorTable descTable;

    // Output param
    private final List<PlanFragment> fragments;
    private final List<ScanNode> scanNodes;

    private int nextNodeId = 0;

    public PullLoadTaskPlanner(PullLoadTask task) {
        this.task = task;
        this.analyzer = new Analyzer(Catalog.getInstance(), null);
        this.descTable = analyzer.getDescTbl();
        this.fragments = Lists.newArrayList();
        this.scanNodes = Lists.newArrayList();
    }

    // NOTE: DB lock need hold when call this function.
    public void plan() throws InternalException {
        // Tuple descriptor used for all nodes in plan.
        OlapTable table = task.table;

        // Generate tuple descriptor
        List<Expr> slotRefs = Lists.newArrayList();
        TupleDescriptor tupleDesc = descTable.createTupleDescriptor();
        for (Column col : table.getBaseSchema()) {
            SlotDescriptor slotDesc = descTable.addSlotDescriptor(tupleDesc);
            slotDesc.setIsMaterialized(true);
            slotDesc.setColumn(col);
            if (col.isAllowNull()) {
                slotDesc.setIsNullable(true);
            } else {
                slotDesc.setIsNullable(false);
            }
            slotRefs.add(new SlotRef(slotDesc));
        }

        // Generate plan tree
        // 1. first Scan node
        BrokerScanNode scanNode = new BrokerScanNode(new PlanNodeId(nextNodeId++), tupleDesc, "BrokerScanNode");
        scanNode.setLoadInfo(table, task.brokerDesc, task.fileGroups);
        scanNode.init(analyzer);
        scanNodes.add(scanNode);

        // rewrite node
        OlapRewriteNode rewriteNode = new OlapRewriteNode(
                new PlanNodeId(nextNodeId++), scanNode, table, tupleDesc, slotRefs);
        rewriteNode.init(analyzer);

        descTable.computeMemLayout();
        rewriteNode.finalize(analyzer);

        PlanFragment scanFragment = new PlanFragment(new PlanFragmentId(0), rewriteNode, DataPartition.RANDOM);
        scanNode.setFragmentId(scanFragment.getFragmentId());
        scanNode.setFragment(scanFragment);
        fragments.add(scanFragment);

        // exchange node
        ExchangeNode exchangeNode = new ExchangeNode(new PlanNodeId(nextNodeId++), rewriteNode, false);
        exchangeNode.init(analyzer);

        // Create data sink
        DataSplitSink splitSink = null;
        try {
            splitSink = new DataSplitSink(table, tupleDesc);
        } catch (AnalysisException e) {
            LOG.info("New DataSplitSink failed.{}", e);
            throw new InternalException(e.getMessage());
        }
        PlanFragment sinkFragment = new PlanFragment(new PlanFragmentId(1), exchangeNode, splitSink.getOutputPartition());
        scanFragment.setDestination(exchangeNode);
        scanFragment.setOutputPartition(splitSink.getOutputPartition());
        sinkFragment.setSink(splitSink);

        fragments.add(sinkFragment);

        // Get partition
        for (PlanFragment fragment : fragments) {
            try {
                fragment.finalize(analyzer, false);
            } catch (NotImplementedException e) {
                LOG.info("Fragment finalize failed.{}", e);
                throw new InternalException("Fragment finalize failed.");
            }
        }
        Collections.reverse(fragments);
    }

    public DescriptorTable getDescTable() {
        return descTable;
    }

    public List<PlanFragment> getFragments() {
        return fragments;
    }

    public List<ScanNode> getScanNodes() {
        return scanNodes;
    }
}
