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

package org.apache.doris.datasource.lakesoul.source;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.TableFormatType;
import org.apache.doris.datasource.lakesoul.LakeSoulExternalTable;
import org.apache.doris.planner.PlanNodeId;
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
import com.dmetasoul.lakesoul.meta.LakeSoulOptions;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import com.google.common.collect.Lists;
import com.lakesoul.shaded.com.alibaba.fastjson.JSON;
import com.lakesoul.shaded.com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class LakeSoulScanNode extends FileQueryScanNode {

    protected final LakeSoulExternalTable lakeSoulExternalTable;

    protected final TableInfo table;

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
        return Optional.ofNullable(LocationPath.getTFileTypeForBE(location)).orElseThrow(() ->
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

    public static boolean isExistHashPartition(TableInfo tif) {
        JSONObject tableProperties = JSON.parseObject(tif.getProperties());
        if (tableProperties.containsKey(LakeSoulOptions.HASH_BUCKET_NUM())
                && tableProperties.getString(LakeSoulOptions.HASH_BUCKET_NUM()).equals("-1")) {
            return false;
        } else {
            return tableProperties.containsKey(LakeSoulOptions.HASH_BUCKET_NUM());
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

    public List<Split> getSplits() throws UserException {
        List<Split> splits = new ArrayList<>();
        Map<String, Map<Integer, List<String>>> splitByRangeAndHashPartition = new LinkedHashMap<>();
        TableInfo tif = table;
        DataFileInfo[] dfinfos = DataOperation.getTableDataInfo(table.getTableId());
        for (DataFileInfo pif : dfinfos) {
            if (isExistHashPartition(tif) && pif.file_bucket_id() != -1) {
                splitByRangeAndHashPartition.computeIfAbsent(pif.range_partitions(), k -> new LinkedHashMap<>())
                        .computeIfAbsent(pif.file_bucket_id(), v -> new ArrayList<>())
                        .add(pif.path());
            } else {
                splitByRangeAndHashPartition.computeIfAbsent(pif.range_partitions(), k -> new LinkedHashMap<>())
                        .computeIfAbsent(-1, v -> new ArrayList<>())
                        .add(pif.path());
            }
        }
        List<String> pkKeys = null;
        if (!table.getPartitions().equals(";")) {
            pkKeys = Lists.newArrayList(table.getPartitions().split(";")[1].split(","));
        }

        for (Map.Entry<String, Map<Integer, List<String>>> entry : splitByRangeAndHashPartition.entrySet()) {
            String rangeKey = entry.getKey();
            LinkedHashMap<String, String> rangeDesc = new LinkedHashMap<>();
            if (!rangeKey.equals("-5")) {
                String[] keys = rangeKey.split(",");
                for (String item : keys) {
                    String[] kv = item.split("=");
                    rangeDesc.put(kv[0], kv[1]);
                }
            }
            for (Map.Entry<Integer, List<String>> split : entry.getValue().entrySet()) {
                LakeSoulSplit lakeSoulSplit = new LakeSoulSplit(
                        split.getValue(),
                        pkKeys,
                        rangeDesc,
                        table.getTableSchema(),
                        0, 0, 0,
                        new String[0], null);
                lakeSoulSplit.setTableFormatType(TableFormatType.LAKESOUL);
                splits.add(lakeSoulSplit);
            }
        }
        return splits;

    }

}

