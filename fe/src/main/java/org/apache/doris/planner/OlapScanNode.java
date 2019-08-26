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

import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TOlapScanNode;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
//import java.util.stream.Collectors;

/**
 * Full scan of an Olap table.
 */
public class OlapScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(OlapScanNode.class);

    private boolean isPreAggregation = false;
    private String reasonOfPreAggregation = null;
    private boolean canTurnOnPreAggr = true;
    private ArrayList<String> tupleColumns = new ArrayList<String>();
    private HashSet<String> predicateColumns = new HashSet<String>();
    private OlapTable olapTable = null;
    private PartitionSelector.SelectedPartitionRange selectedPartitionRange;
    private boolean isFinalized;

    /**
     * Constructs node to scan given data files of table 'tbl'.
     */
    public OlapScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        super(id, desc, planNodeName);
        olapTable = (OlapTable) desc.getTable();
        selectedPartitionRange = new PartitionSelector.SelectedPartitionRange();
    }

    public void setIsPreAggregation(boolean isPreAggregation, String reason) {
        this.isPreAggregation = isPreAggregation;
        this.reasonOfPreAggregation = reason;
    }


    public boolean isPreAggregation() {
        return isPreAggregation;
    }

    public boolean getCanTurnOnPreAggr() {
        return canTurnOnPreAggr;
    }

    public void setCanTurnOnPreAggr(boolean canChangePreAggr) {
        this.canTurnOnPreAggr = canChangePreAggr;
    }

    public OlapTable getOlapTable() {
        return olapTable;
    }

    @Override
    protected String debugString() {
        ToStringHelper helper = Objects.toStringHelper(this);
        helper.addValue(super.debugString());
        helper.addValue("olapTable=" + olapTable.getName());
        return helper.toString();
    }

    @Override
    public void finalize(Analyzer analyzer) throws UserException {
        if (isFinalized) {
            return;
        }

        LOG.debug("OlapScanNode finalize. Tuple: {}", desc);
        try {

            if (olapTable.getKeysType() == KeysType.DUP_KEYS) {
                isPreAggregation = true;
            }
            final PartitionSelector selector =
                    new PartitionSelector(olapTable, desc, conjuncts, analyzer);
            selectedPartitionRange = selector.selectBestPartitionRange(analyzer, isPreAggregation);
        } catch (AnalysisException e) {
            throw new UserException(e.getMessage());
        }

        computeStats(analyzer);
        isFinalized = true;
    }

    @Override
    public void computeStats(Analyzer analyzer) {
        if (cardinality > 0) {
            avgRowSize = selectedPartitionRange.getTotalBytes() / (float) cardinality;
            if (hasLimit()) {
                cardinality = Math.min(cardinality, limit);
            }
            numNodes = selectedPartitionRange.getScanBackendIds().size();
        }
    }

    /**
     * We query Palo Meta to get request's data location
     * extra result info will pass to backend ScanNode
     */
    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return selectedPartitionRange.getResult();
    }


    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();

        output.append(prefix).append("TABLE: ").append(olapTable.getName()).append("\n");

        if (null != sortColumn) {
            output.append(prefix).append("SORT COLUMN: ").append(sortColumn).append("\n");
        }
        if (isPreAggregation) {
            output.append(prefix).append("PREAGGREGATION: ON").append("\n");
        } else {
            output.append(prefix).append("PREAGGREGATION: OFF. Reason: ").append(reasonOfPreAggregation).append("\n");
        }
        if (!conjuncts.isEmpty()) {
            output.append(prefix).append("PREDICATES: ").append(
                    getExplainString(conjuncts)).append("\n");
        }

        output.append(prefix).append(String.format(
                "partitions=%s/%s",
                selectedPartitionRange.getSelectedPartitionNum(),
                olapTable.getPartitions().size()));
        output.append("\n");

        if (selectedPartitionRange.getSelectedIndexId() != null) {
            String indexName = olapTable.getIndexNameById(
                    selectedPartitionRange.getSelectedIndexId().getSelectedRollupId());
            output.append(prefix).append(String.format("rollup: %s", indexName));
            output.append("\n");

            if (selectedPartitionRange.getSelectedIndexId().getMatchedPrefixIndex().size() > 0) {
                output.append(prefix).append("Matched prefix index:");
                for (Column column : selectedPartitionRange.getSelectedIndexId().getMatchedPrefixIndex()) {
                    output.append(" ").append(column.getName());
                }
                output.append("\n");
            }
        }

        output.append(prefix).append(String.format(
                "buckets=%s/%s", selectedPartitionRange.getSelectedTabletsNum(),
                selectedPartitionRange.getTotalTabletsNum()));
        output.append("\n");

        output.append(prefix).append(String.format(
                "cardinality=%s", cardinality));
        output.append("\n");

        output.append(prefix).append(String.format(
                "avgRowSize=%s", avgRowSize));
        output.append("\n");

        output.append(prefix).append(String.format(
                "numNodes=%s", numNodes));
        output.append("\n");

        return output.toString();
    }

    @Override
    public int getNumInstances() {
        return selectedPartitionRange.getResult().size();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        List<String> keyColumnNames = new ArrayList<String>();
        List<TPrimitiveType> keyColumnTypes = new ArrayList<TPrimitiveType>();
        final long selectIndexId = selectedPartitionRange.getSelectedIndexId().getSelectedRollupId();
        if (selectIndexId != -1) {
            for (Column col : olapTable.getSchemaByIndexId(selectIndexId)) {
                if (!col.isKey()) {
                    break;
                }
                keyColumnNames.add(col.getName());
                keyColumnTypes.add(col.getDataType().toThrift());
            }
        }
        msg.node_type = TPlanNodeType.OLAP_SCAN_NODE;
        msg.olap_scan_node =
                new TOlapScanNode(desc.getId().asInt(), keyColumnNames, keyColumnTypes, isPreAggregation);
        if (null != sortColumn) {
            msg.olap_scan_node.setSort_column(sortColumn);
        }
    }

    // export some tablets
    public static OlapScanNode createOlapScanNodeByLocation(
            PlanNodeId id, TupleDescriptor desc, String planNodeName, List<TScanRangeLocations> locationsList) {
        OlapScanNode olapScanNode = new OlapScanNode(id, desc, planNodeName);
        olapScanNode.numInstances = 1;

        ArrayList<Partition> partitions = Lists.newArrayList(olapScanNode.olapTable.getPartitions());
        Preconditions.checkState(!partitions.isEmpty());
        if (!partitions.isEmpty()) {
            olapScanNode.selectedPartitionRange.getSelectedIndexId()
                    .setSelectedRollupId(partitions.get(0).getBaseIndex().getId());
        }

        olapScanNode.selectedPartitionRange.setSelectedPartitionNum(1);
        olapScanNode.selectedPartitionRange.setSelectedTabletsNum(1);
        olapScanNode.selectedPartitionRange.setTotalTabletsNum(1);
        olapScanNode.isPreAggregation = false;
        olapScanNode.isFinalized = true;

        olapScanNode.selectedPartitionRange.getResult().addAll(locationsList);
        return olapScanNode;
    }

    public ArrayListMultimap<Integer, TScanRangeLocations> getBucketSeq2locations() {
        return selectedPartitionRange.getBucketSeq2locations();
    }
}
