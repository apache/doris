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

package org.apache.doris.planner;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.DistributionInfo.DistributionInfoType;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.common.Config;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/*
 * Prune conjuncts according to the tablets to be scanned by OlapScanNode. It only support prune InPredicate.
 *
 * For example:
 *      table_a's distribution column is k1, bucket num is 2. tablet_0 contains data with k1 in (0, 2),
 *      tablet_1 contains data with k1 in (1, 3).
 *      when executing the following query: 'select * from table_a where k1 in (0, 1, 2, 3)'.
 *      Suppose there are two OlapScanNode in the execution plan, scan_node0 and scan_node1 are responsible for scanning
 *      tablet_0 and tablet_1 respectively. Then each sacn_node's conjuncts are 'k1 in (0, 1, 2, 3)'.
 *      However, considering the situation of the tablets scanned by OlapScanNode, it is actually possible to prune the
 *      InPredicate of the scan_node0 to 'k1 in (0, 2)' and the InPredicate of the scan_node1 to 'k1 in (1, 3)'.
 *
 * By prune InPredicate, we can reduce the number of ScanKeys in OlapScanNode and improve the performance of data scanning.
 */
public class OlapScanNodeConjunctsPrunner {
    private final static Logger LOG = LogManager.getLogger(OlapScanNodeConjunctsPrunner.class);

    private TupleDescriptor tupleDescriptor;

    private Map<Long /* tablet id */, Integer /* bucket */> tabletBucket = new HashMap<>();
    private Map<Long /* partition id */, List<Long /* tablet id */>> selectedTablets = new HashMap<>();
    private Map<Long /* partition id */, DistributionInfo> partitionDis = new HashMap<>();

    public OlapScanNodeConjunctsPrunner(TupleDescriptor tupleDescriptor) {
        this.tupleDescriptor = tupleDescriptor;
    }

    public void addSelectedTablets(long partitionId, DistributionInfo distributionInfo, List<Long> tabletsInOrder,
                                   List<Long> selectedTablets) {
        partitionDis.put(partitionId, distributionInfo);
        for (int i = 0; i < tabletsInOrder.size(); i++) {
            tabletBucket.put(tabletsInOrder.get(i), i);
        }
        this.selectedTablets.put(partitionId, selectedTablets);
    }

    public List<Expr> pruneConjuncts(List<Expr> conjuncts, Set<Long> tabletIds) {
        Preconditions.checkState(selectedTablets.size() > 0);

        if (!partitionDis.values().stream().allMatch(dis -> dis.getType() == DistributionInfoType.HASH)) {
            return conjuncts;
        }

        List<List<Column>> distributionColumns = partitionDis.values().stream()
                .map(dis -> (HashDistributionInfo) dis)
                .map(hashDis -> hashDis.getDistributionColumns())
                .collect(Collectors.toList());

        // Sanity check: All partition's DistributionInfo must have the same Distribution Columns.
        boolean hasDifferentDistributionColumns = false;
        for (int i = 1; i < distributionColumns.size(); i++) {
            if (!distributionColumns.get(i - 1).equals(distributionColumns.get(i))) {
                hasDifferentDistributionColumns = true;
                break;
            }
        }
        Preconditions.checkState(!hasDifferentDistributionColumns,
                "Invalidate state: Partitions have diffrent distribution columns.");

        List<Column> columns = distributionColumns.get(0);
        // Only deal with scenes with only one distribution column.
        if (columns.size() > 1) {
            return conjuncts;
        }

        Column distributionColumn = columns.get(0);
        SlotDescriptor disSlotDesc = tupleDescriptor.getColumnSlot(distributionColumn.getName());
        if (disSlotDesc == null) {
            // There is no conjunct bound to distribution column.
            return conjuncts;
        }

        Map<Long /* partition id */, Set<Integer /* bucket */>> candidateBuckets = new HashMap<>();
        partitionDis.entrySet().forEach(entry -> {
            selectedTablets.get(entry.getKey()).forEach(tabletId -> {
                if (tabletIds.contains(tabletId)) {
                    candidateBuckets.computeIfAbsent(entry.getKey(), key -> new HashSet<>())
                            .add(tabletBucket.get(tabletId));
                }
            });
        });

        List<Expr> rets = Lists.newArrayList();
        for (Expr expr : conjuncts) {
            // Only InPredicate can be prunned.
            if (!(expr instanceof InPredicate)) {
                rets.add(expr);
                continue;
            }

            InPredicate inPredicate = (InPredicate) expr;
            // InPredicate not bound by distribution column.
            if (!inPredicate.isBound(disSlotDesc.getId())) {
                rets.add(expr);
                continue;
            }

            if (!inPredicate.isLiteralChildren() || inPredicate.isNotIn()) {
                rets.add(expr);
                continue;
            }

            if (!(inPredicate.getChild(0) instanceof SlotRef)) {
                rets.add(expr);
                continue;
            }

            PartitionKey partitionKey = new PartitionKey();
            List<Boolean> contained = new ArrayList<>(inPredicate.getInElementNum());
            for (int i = 0; i < inPredicate.getInElementNum(); i++) {
                contained.add(false);
            }

            for (int i = 0; i < inPredicate.getInElementNum(); i++) {
                LiteralExpr element = (LiteralExpr) inPredicate.getChild(1 + i);
                partitionKey.pushColumn(element, distributionColumn.getDataType());
                long hash = partitionKey.getHashValue();
                for (Map.Entry<Long, Set<Integer>> candidate : candidateBuckets.entrySet()) {
                    DistributionInfo dis = partitionDis.get(candidate.getKey());
                    if (candidate.getValue().contains((int) ((hash & 0xffffffff) % dis.getBucketNum()))) {
                        contained.set(i, true);
                        break;
                    }
                }
                partitionKey.popColumn();
            }

            Preconditions.checkState(contained.stream().anyMatch(c -> c),
                    "Invalide state, InPredicate's elements is empty after prunned.");
            if (contained.stream().allMatch(c -> c)) {
                rets.add(expr);
            } else {
                InPredicate newIn = (InPredicate) inPredicate.clone();
                newIn.clearChildren();
                newIn.addChild(inPredicate.getChild(0));

                for (int i = 0; i < inPredicate.getInElementNum(); i++) {
                    if (contained.get(i)) {
                        newIn.addChild(inPredicate.getChild(1 + i));
                    }
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("InPredicate prunned: original size: {}, new size: {}",
                            inPredicate.getInElementNum(), newIn.getInElementNum());
                    List<String> originalElements = inPredicate.getChildren().subList(1, inPredicate.getInElementNum() + 1)
                            .stream()
                            .map(in -> (LiteralExpr) in)
                            .map(li -> li.getStringValue())
                            .collect(Collectors.toList());
                    List<String> newElements = newIn.getChildren().subList(1, newIn.getInElementNum() + 1).stream()
                            .map(in -> (LiteralExpr) in)
                            .map(li -> li.getStringValue())
                            .collect(Collectors.toList());
                    LOG.debug("InPredicate prunned: original: {}, new: {}", originalElements, newElements);
                }

                rets.add(newIn);
            }
        }

        return rets;
    }
}
