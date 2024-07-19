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
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.TableFormatType;
import org.apache.doris.datasource.lakesoul.LakeSoulExternalTable;
import org.apache.doris.datasource.lakesoul.LakeSoulUtils;
import org.apache.doris.datasource.property.constants.S3Properties;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.spi.Split;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TLakeSoulFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import com.dmetasoul.lakesoul.lakesoul.io.substrait.SubstraitUtil;
import com.dmetasoul.lakesoul.meta.DBUtil;
import com.dmetasoul.lakesoul.meta.DataFileInfo;
import com.dmetasoul.lakesoul.meta.DataOperation;
import com.dmetasoul.lakesoul.meta.LakeSoulOptions;
import com.dmetasoul.lakesoul.meta.entity.PartitionInfo;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import com.google.common.collect.Lists;
import com.lakesoul.shaded.com.alibaba.fastjson.JSON;
import com.lakesoul.shaded.com.alibaba.fastjson.JSONObject;
import com.lakesoul.shaded.com.fasterxml.jackson.core.type.TypeReference;
import com.lakesoul.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import com.lakesoul.shaded.org.apache.arrow.vector.types.pojo.Field;
import com.lakesoul.shaded.org.apache.arrow.vector.types.pojo.Schema;
import io.substrait.proto.Plan;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class LakeSoulScanNode extends FileQueryScanNode {

    private static final Logger LOG = LogManager.getLogger(LakeSoulScanNode.class);
    protected LakeSoulExternalTable lakeSoulExternalTable;

    String tableName;

    String location;

    String partitions;

    Schema tableArrowSchema;

    Schema partitionArrowSchema;
    private Map<String, String> tableProperties;

    String readType;

    public LakeSoulScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv) {
        super(id, desc, "planNodeName", StatisticalType.LAKESOUL_SCAN_NODE, needCheckColumnPriv);
    }

    @Override
    protected void doInitialize() throws UserException {
        super.doInitialize();
        lakeSoulExternalTable = (LakeSoulExternalTable) desc.getTable();
        TableInfo tableInfo = lakeSoulExternalTable.getLakeSoulTableInfo();
        location = tableInfo.getTablePath();
        tableName = tableInfo.getTableName();
        partitions = tableInfo.getPartitions();
        readType = LakeSoulOptions.ReadType$.MODULE$.FULL_READ();
        try {
            tableProperties = new ObjectMapper().readValue(
                tableInfo.getProperties(),
                new TypeReference<Map<String, String>>() {}
            );
            tableArrowSchema = Schema.fromJSON(tableInfo.getTableSchema());
            List<Field> partitionFields =
                    DBUtil.parseTableInfoPartitions(partitions)
                        .rangeKeys
                        .stream()
                        .map(tableArrowSchema::findField).collect(Collectors.toList());
            partitionArrowSchema = new Schema(partitionFields);
        } catch (IOException e) {
            throw new UserException(e);
        }
    }

    @Override
    protected TFileType getLocationType() throws UserException {
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
        return new ArrayList<>(DBUtil.parseTableInfoPartitions(partitions).rangeKeys);
    }

    @Override
    protected TableIf getTargetTable() throws UserException {
        return desc.getTable();
    }

    @Override
    protected Map<String, String> getLocationProperties() throws UserException {
        return lakeSoulExternalTable.getHadoopProperties();
    }

    @SneakyThrows
    @Override
    protected void setScanParams(TFileRangeDesc rangeDesc, Split split) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("{}", rangeDesc);
        }
        if (split instanceof LakeSoulSplit) {
            setLakeSoulParams(rangeDesc, (LakeSoulSplit) split);
        }
    }

    public ExternalCatalog getCatalog() {
        return lakeSoulExternalTable.getCatalog();
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

    public void setLakeSoulParams(TFileRangeDesc rangeDesc, LakeSoulSplit lakeSoulSplit) throws IOException {
        TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
        tableFormatFileDesc.setTableFormatType(lakeSoulSplit.getTableFormatType().value());
        TLakeSoulFileDesc fileDesc = new TLakeSoulFileDesc();
        fileDesc.setFilePaths(lakeSoulSplit.getPaths());
        fileDesc.setPrimaryKeys(lakeSoulSplit.getPrimaryKeys());
        fileDesc.setTableSchema(lakeSoulSplit.getTableSchema());


        JSONObject options = new JSONObject();
        Plan predicate = LakeSoulUtils.getPushPredicate(
                conjuncts,
                tableName,
                tableArrowSchema,
                partitionArrowSchema,
                tableProperties,
                readType.equals(LakeSoulOptions.ReadType$.MODULE$.INCREMENTAL_READ()));
        if (predicate != null) {
            options.put(LakeSoulUtils.SUBSTRAIT_PREDICATE, SubstraitUtil.encodeBase64String(predicate));
        }
        Map<String, String> catalogProps = getCatalog().getProperties();
        if (LOG.isDebugEnabled()) {
            LOG.debug("{}", catalogProps);
        }

        if (catalogProps.get(S3Properties.Env.ENDPOINT) != null) {
            options.put(LakeSoulUtils.FS_S3A_ENDPOINT, catalogProps.get(S3Properties.Env.ENDPOINT));
            options.put(LakeSoulUtils.FS_S3A_PATH_STYLE_ACCESS, "true");
            if (catalogProps.get(S3Properties.Env.ACCESS_KEY) != null) {
                options.put(LakeSoulUtils.FS_S3A_ACCESS_KEY, catalogProps.get(S3Properties.Env.ACCESS_KEY));
            }
            if (catalogProps.get(S3Properties.Env.SECRET_KEY) != null) {
                options.put(LakeSoulUtils.FS_S3A_SECRET_KEY, catalogProps.get(S3Properties.Env.SECRET_KEY));
            }
            if (catalogProps.get(S3Properties.Env.REGION) != null) {
                options.put(LakeSoulUtils.FS_S3A_REGION, catalogProps.get(S3Properties.Env.REGION));
            }
        }

        fileDesc.setOptions(JSON.toJSONString(options));

        fileDesc.setPartitionDescs(lakeSoulSplit.getPartitionDesc()
                .entrySet().stream().map(entry ->
                        String.format("%s=%s", entry.getKey(), entry.getValue())).collect(Collectors.toList()));
        tableFormatFileDesc.setLakesoulParams(fileDesc);
        rangeDesc.setTableFormatParams(tableFormatFileDesc);
    }

    public List<Split> getSplits() throws UserException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("getSplits with columnFilters={}", columnFilters);
            LOG.debug("getSplits with columnNameToRange={}", columnNameToRange);
            LOG.debug("getSplits with conjuncts={}", conjuncts);
        }

        List<PartitionInfo> allPartitionInfo = lakeSoulExternalTable.listPartitionInfo();
        if (LOG.isDebugEnabled()) {
            LOG.debug("allPartitionInfo={}", allPartitionInfo);
        }
        List<PartitionInfo> filteredPartitionInfo = allPartitionInfo;
        try {
            filteredPartitionInfo =
                    LakeSoulUtils.applyPartitionFilters(
                        allPartitionInfo,
                        tableName,
                        partitionArrowSchema,
                        columnNameToRange
                    );
        } catch (IOException e) {
            throw new UserException(e);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("filteredPartitionInfo={}", filteredPartitionInfo);
        }
        DataFileInfo[] dataFileInfos = DataOperation.getTableDataInfo(filteredPartitionInfo);

        List<Split> splits = new ArrayList<>();
        Map<String, Map<Integer, List<String>>> splitByRangeAndHashPartition = new LinkedHashMap<>();
        TableInfo tableInfo = lakeSoulExternalTable.getLakeSoulTableInfo();

        for (DataFileInfo fileInfo : dataFileInfos) {
            if (isExistHashPartition(tableInfo) && fileInfo.file_bucket_id() != -1) {
                splitByRangeAndHashPartition.computeIfAbsent(fileInfo.range_partitions(), k -> new LinkedHashMap<>())
                        .computeIfAbsent(fileInfo.file_bucket_id(), v -> new ArrayList<>())
                        .add(fileInfo.path());
            } else {
                splitByRangeAndHashPartition.computeIfAbsent(fileInfo.range_partitions(), k -> new LinkedHashMap<>())
                        .computeIfAbsent(-1, v -> new ArrayList<>())
                        .add(fileInfo.path());
            }
        }
        List<String> pkKeys = null;
        if (!tableInfo.getPartitions().equals(";")) {
            pkKeys = Lists.newArrayList(tableInfo.getPartitions().split(";")[1].split(","));
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
                        tableInfo.getTableSchema(),
                        0, 0, 0,
                        new String[0], null);
                lakeSoulSplit.setTableFormatType(TableFormatType.LAKESOUL);
                splits.add(lakeSoulSplit);
            }
        }
        return splits;

    }

}

