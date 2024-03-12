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

package org.apache.doris.planner.external.lakesoul;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dmetasoul.lakesoul.meta.LakeSoulOptions;
import com.dmetasoul.lakesoul.meta.jnr.NativeMetadataJavaClient;
import com.dmetasoul.lakesoul.meta.jnr.SplitDesc;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.external.LakeSoulExternalTable;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.external.FileQueryScanNode;
import org.apache.doris.planner.external.TableFormatType;
import org.apache.doris.planner.external.hudi.HudiSplit;
import org.apache.doris.spi.Split;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TLakeSoulFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import com.dmetasoul.lakesoul.meta.DBUtil;
import com.dmetasoul.lakesoul.meta.DataFileInfo;
import com.dmetasoul.lakesoul.meta.DataOperation;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import lombok.Setter;
import org.apache.hadoop.fs.Path;
import shade.doris.hive.com.google.common.collect.Lists;

import java.util.*;
import java.util.stream.Collectors;

public class LakeSoulScanNode extends FileQueryScanNode {

    protected final LakeSoulExternalTable lakeSoulExternalTable;

    protected final TableInfo table;

    @Setter
    private LogicalFileScan.SelectedPartitions selectedPartitions = null;

    public LakeSoulScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv) {
        super(id, desc, "planNodeName", StatisticalType.LAKESOUL_SCAN_NODE, needCheckColumnPriv);
        lakeSoulExternalTable = (LakeSoulExternalTable) desc.getTable();
        table = lakeSoulExternalTable.getLakeSoulTableInfo();
    }

    @Override
    protected TFileType getLocationType() throws UserException {
        String location = table.getTablePath();
        return getLocationType(location);
    }

    @Override
    protected TFileType getLocationType(String location) throws UserException {
        return Optional.ofNullable(LocationPath.getTFileType(location)).orElseThrow(() ->
            new DdlException("Unknown file location " + location + " for lakesoul table "));
    }

    @Override
    protected TFileFormatType getFileFormatType() throws UserException {
        return TFileFormatType.FORMAT_JNI;
    }

    @Override
    protected List<String> getPathPartitionKeys() throws UserException {
        return new ArrayList<>(DBUtil.parseTableInfoPartitions(table.getPartitions()).rangeKeys);
    }

    @Override
    protected TableIf getTargetTable() throws UserException {
        return lakeSoulExternalTable;
    }

    @Override
    protected Map<String, String> getLocationProperties() throws UserException {
        return lakeSoulExternalTable.getHadoopProperties();
    }

    @Override
    protected void setScanParams(TFileRangeDesc rangeDesc, Split split) {
        if (split instanceof LakeSoulSplit) {
            setLakeSoulParams(rangeDesc, (LakeSoulSplit) split);
        }
    }

    public void setLakeSoulParams(TFileRangeDesc rangeDesc, LakeSoulSplit lakeSoulSplit) {
        TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
        tableFormatFileDesc.setTableFormatType(lakeSoulSplit.getTableFormatType().value());
        TLakeSoulFileDesc fileDesc = new TLakeSoulFileDesc();
        fileDesc.setFilePaths(lakeSoulSplit.getPaths());
        fileDesc.setPrimaryKeys(lakeSoulSplit.getPrimaryKeys());
        fileDesc.setTableSchema(lakeSoulSplit.getTableSchema());
        fileDesc.setPartitionDescs(lakeSoulSplit.getPartitionDesc()
            .entrySet().stream().map(entry ->
                String.format("%s=%s", entry.getKey(), entry.getValue())).collect(Collectors.toList()));
        tableFormatFileDesc.setLakesoulParams(fileDesc);
        rangeDesc.setTableFormatParams(tableFormatFileDesc);
    }

    protected List<Split> getSplits() throws UserException {
        List<Split> splits = new ArrayList<>();
        List<SplitDesc> splitDescs = NativeMetadataJavaClient.getInstance().createSplitDescArray(this.table.getTableName(), this.table.getTableNamespace());
        for (SplitDesc sd : splitDescs) {
            LakeSoulSplit lakeSoulSplit =  new LakeSoulSplit(
                sd, 0, 0, 0, new String[0], null
            );
            lakeSoulSplit.setTableFormatType(TableFormatType.LAKESOUL);
            splits.add(lakeSoulSplit);
        }
        return splits;
    }

}

