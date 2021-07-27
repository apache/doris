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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.UserException;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * Representation of the common elements of all scan nodes.
 */
abstract public class ScanNode extends PlanNode {
    private final static Logger LOG = LogManager.getLogger(ScanNode.class);
    protected final TupleDescriptor desc;
    protected Map<String, PartitionColumnFilter> columnFilters = Maps.newHashMap();
    protected String sortColumn = null;

    public ScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        super(id, desc.getId().asList(), planNodeName);
        this.desc = desc;
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
        // materialize conjuncts in where
        analyzer.materializeSlots(conjuncts);
    }

    /**
     * Helper function to parse a "host:port" address string into TNetworkAddress
     * This is called with ipaddress:port when doing scan range assigment.
     */
    protected static TNetworkAddress addressToTNetworkAddress(String address) {
        TNetworkAddress result = new TNetworkAddress();
        String[] hostPort = address.split(":");
        result.hostname = hostPort[0];
        result.port = Integer.parseInt(hostPort[1]);
        return result;
    }

    public TupleDescriptor getTupleDesc() { return desc; }

    public void setSortColumn(String column) {
        sortColumn = column;
    }

    /**
     * cast expr to SlotDescriptor type
     */
    protected Expr castToSlot(SlotDescriptor slotDesc, Expr expr) throws UserException {
        PrimitiveType dstType = slotDesc.getType().getPrimitiveType();
        PrimitiveType srcType = expr.getType().getPrimitiveType();
        if (dstType != srcType) {
            return expr.castTo(slotDesc.getType());
        } else {
            return expr;
        }
    }

    /**
     * Returns all scan ranges plus their locations. Needs to be preceded by a call to
     * finalize().
     *
     * @param maxScanRangeLength The maximum number of bytes each scan range should scan;
     *                           only applicable to HDFS; less than or equal to zero means no
     *                           maximum.
     */
    abstract public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength);

    // TODO(ML): move it into PrunerOptimizer
    public void computeColumnFilter() {
        for (Column column : desc.getTable().getBaseSchema()) {
            SlotDescriptor slotDesc = desc.getColumnSlot(column.getName());
            if (null == slotDesc) {
                continue;
            }
            PartitionColumnFilter keyFilter = createPartitionFilter(slotDesc, conjuncts);
            if (null != keyFilter) {
                columnFilters.put(column.getName(), keyFilter);
            }
        }
    }

    private PartitionColumnFilter createPartitionFilter(SlotDescriptor desc, List<Expr> conjuncts) {
        PartitionColumnFilter partitionColumnFilter = null;
        for (Expr expr : conjuncts) {
            if (!expr.isBound(desc.getId())) {
                continue;
            }
            if (expr instanceof BinaryPredicate) {
                BinaryPredicate binPredicate = (BinaryPredicate) expr;
                Expr slotBinding = binPredicate.getSlotBinding(desc.getId());
                if (slotBinding == null || !slotBinding.isConstant()) {
                    continue;
                }
                if (binPredicate.getOp() == BinaryPredicate.Operator.NE
                        || !(slotBinding instanceof LiteralExpr)) {
                    continue;
                }

                if (null == partitionColumnFilter) {
                    partitionColumnFilter = new PartitionColumnFilter();
                }
                LiteralExpr literal = (LiteralExpr) slotBinding;
                BinaryPredicate.Operator op = binPredicate.getOp();
                if (!binPredicate.slotIsLeft()) {
                    op = op.commutative();
                }
                switch (op) {
                    case EQ:
                        partitionColumnFilter.setLowerBound(literal, true);
                        partitionColumnFilter.setUpperBound(literal, true);
                        break;
                    case LE:
                        partitionColumnFilter.setUpperBound(literal, true);
                        partitionColumnFilter.lowerBoundInclusive = true;
                        break;
                    case LT:
                        partitionColumnFilter.setUpperBound(literal, false);
                        partitionColumnFilter.lowerBoundInclusive = true;
                        break;
                    case GE:
                        partitionColumnFilter.setLowerBound(literal, true);
                        break;
                    case GT:
                        partitionColumnFilter.setLowerBound(literal, false);
                        break;
                    default:
                        break;
                }
            } else if (expr instanceof InPredicate) {
                InPredicate inPredicate = (InPredicate) expr;
                if (!inPredicate.isLiteralChildren() || inPredicate.isNotIn()) {
                    continue;
                }
                if (!(inPredicate.getChild(0).unwrapExpr(false) instanceof SlotRef)) {
                    // If child(0) of the in predicate is not a SlotRef,
                    // then other children of in predicate should not be used as a condition for partition prune.
                    continue;
                }
                if (null == partitionColumnFilter) {
                    partitionColumnFilter = new PartitionColumnFilter();
                }
                partitionColumnFilter.setInPredicate(inPredicate);
            } else if (expr instanceof IsNullPredicate) {
                IsNullPredicate isNullPredicate = (IsNullPredicate) expr;
                if (!isNullPredicate.isSlotRefChildren() || isNullPredicate.isNotNull()) {
                    continue;
                }

                // If we meet a IsNull predicate on partition column, then other predicates are useless
                // eg: (xxxx) and (col is null), only the IsNull predicate has an effect on partition pruning.
                partitionColumnFilter = new PartitionColumnFilter();
                NullLiteral nullLiteral = new NullLiteral();
                partitionColumnFilter.setLowerBound(nullLiteral, true);
                partitionColumnFilter.setUpperBound(nullLiteral, true);
                break;
            }
        }
        LOG.debug("partitionColumnFilter: {}", partitionColumnFilter);
        return partitionColumnFilter;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("tid", desc.getId().asInt()).add("tblName",
                desc.getTable().getName()).add("keyRanges", "").addValue(
                super.debugString()).toString();
    }
}
