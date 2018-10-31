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

package org.apache.doris.planner;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.UserException;
import org.apache.doris.thrift.PaloInternalServiceVersion;
import org.apache.doris.thrift.TExecPlanFragmentParams;
import org.apache.doris.thrift.TPlanFragmentExecParams;
import org.apache.doris.thrift.TQueryGlobals;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TQueryType;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TScanRangeParams;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

// Used to generate a plan fragment for a streaming load.
// we only support OlapTable now.
// TODO(zc): support other type table
public class StreamLoadPlanner {
    private static final Logger LOG = LogManager.getLogger(StreamLoadPlanner.class);
    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    // destination Db and table get from request
    // Data will load to this table
    private Database db;
    private OlapTable destTable;
    private TStreamLoadPutRequest request;

    private Analyzer analyzer;
    private DescriptorTable descTable;

    public StreamLoadPlanner(Database db, OlapTable destTable, TStreamLoadPutRequest request) {
        this.db = db;
        this.destTable = destTable;
        this.request = request;

        analyzer = new Analyzer(Catalog.getInstance(), null);
        descTable = analyzer.getDescTbl();
    }

    public TExecPlanFragmentParams plan() throws UserException {
        // construct tuple descriptor, used for scanNode and dataSink
        TupleDescriptor tupleDesc = descTable.createTupleDescriptor("DstTableTuple");
        for (Column col : destTable.getBaseSchema()) {
            SlotDescriptor slotDesc = descTable.addSlotDescriptor(tupleDesc);
            slotDesc.setIsMaterialized(true);
            slotDesc.setColumn(col);
            if (col.isAllowNull()) {
                slotDesc.setIsNullable(true);
            } else {
                slotDesc.setIsNullable(false);
            }
        }

        // create scan node
        StreamLoadScanNode scanNode = new StreamLoadScanNode(new PlanNodeId(0), tupleDesc, destTable, request);
        scanNode.init(analyzer);
        descTable.computeMemLayout();
        scanNode.finalize(analyzer);

        // create dest sink
        OlapTableSink olapTableSink = new OlapTableSink(destTable, tupleDesc, request.getPartitions());
        olapTableSink.init(request.getLoadId(), request.getTxnId(), db.getId());
        olapTableSink.finalize();

        // for stream load, we only need one fragment, ScanNode -> DataSink.
        // OlapTableSink can dispatch data to corresponding node.
        PlanFragment fragment = new PlanFragment(new PlanFragmentId(0), scanNode, DataPartition.UNPARTITIONED);
        fragment.setSink(olapTableSink);

        fragment.finalize(null, false);

        TExecPlanFragmentParams params = new TExecPlanFragmentParams();
        params.setProtocol_version(PaloInternalServiceVersion.V1);
        params.setFragment(fragment.toThrift());

        params.setDesc_tbl(analyzer.getDescTbl().toThrift());

        TPlanFragmentExecParams execParams = new TPlanFragmentExecParams();
        // Only use fragment id
        UUID uuid = UUID.randomUUID();
        TUniqueId queryId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        execParams.setQuery_id(queryId);
        execParams.setFragment_instance_id(new TUniqueId(queryId.hi, queryId.lo + 1));
        execParams.per_exch_num_senders = Maps.newHashMap();
        execParams.destinations = Lists.newArrayList();
        Map<Integer, List<TScanRangeParams>> perNodeScanRange = Maps.newHashMap();
        List<TScanRangeParams> scanRangeParams = Lists.newArrayList();
        for (TScanRangeLocations locations : scanNode.getScanRangeLocations(0)) {
            scanRangeParams.add(new TScanRangeParams(locations.getScan_range()));
        }
        // For stream load, only one sender
        execParams.setSender_id(0);
        execParams.setNum_senders(1);
        perNodeScanRange.put(scanNode.getId().asInt(), scanRangeParams);
        execParams.setPer_node_scan_ranges(perNodeScanRange);
        params.setParams(execParams);
        TQueryOptions queryOptions = new TQueryOptions();
        queryOptions.setQuery_type(TQueryType.LOAD);
        params.setQuery_options(queryOptions);
        TQueryGlobals queryGlobals = new TQueryGlobals();
        queryGlobals.setNow_string(DATE_FORMAT.format(new Date()));
        params.setQuery_globals(queryGlobals);

        LOG.info("params is {}", params);
        return params;
    }
}
