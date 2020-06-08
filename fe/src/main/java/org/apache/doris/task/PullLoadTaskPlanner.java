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

package org.apache.doris.task;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.NotImplementedException;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.BrokerScanNode;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.DataSplitSink;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.OlapRewriteNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.thrift.TBrokerFileStatus;

import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;

// Planner used to generate a plan for pull load ETL work
@Deprecated
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
        this.analyzer = new Analyzer(Catalog.getCurrentCatalog(), null);
        this.descTable = analyzer.getDescTbl();
        this.fragments = Lists.newArrayList();
        this.scanNodes = Lists.newArrayList();
    }

    // NOTE: DB lock need hold when call this function.
    public void plan(List<List<TBrokerFileStatus>> fileStatusesList, int filesAdded) throws UserException {
        // Tuple descriptor used for all nodes in plan.
        OlapTable table = task.table;

        // Generate tuple descriptor
        List<Expr> slotRefs = Lists.newArrayList();
        TupleDescriptor tupleDesc = descTable.createTupleDescriptor();
        for (Column col : table.getFullSchema()) {
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
        BrokerScanNode scanNode = new BrokerScanNode(new PlanNodeId(nextNodeId++), tupleDesc, "BrokerScanNode",
                                                     fileStatusesList, filesAdded);
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
            throw new UserException(e.getMessage());
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
                throw new UserException("Fragment finalize failed.");
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
