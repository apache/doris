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
import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.ImportColumnDesc;
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.HMSResource;
import org.apache.doris.catalog.HiveMetaStoreClientHelper;
import org.apache.doris.catalog.HiveTable;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TExplainLevel;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HiveScanNode extends BrokerScanNode {
    private static final Logger LOG = LogManager.getLogger(HiveScanNode.class);

    private static final String HIVE_DEFAULT_COLUMN_SEPARATOR = "\001";
    private static final String HIVE_DEFAULT_LINE_DELIMITER = "\n";

    private HiveTable hiveTable;
    // partition column predicates of hive table
    private ExprNodeGenericFuncDesc hivePartitionPredicate;
    private List<ImportColumnDesc> parsedColumnExprList = new ArrayList<>();
    private String hdfsUri;

    private Table remoteHiveTable;

    /* hive table properties */
    private String columnSeparator;
    private String lineDelimiter;
    private String fileFormat;
    private String path;
    private List<String> partitionKeys = new ArrayList<>();
    private StorageBackend.StorageType storageType;
    /* hive table properties */

    public String getHostUri() {
        return hdfsUri;
    }

    public List<ImportColumnDesc> getParsedColumnExprList() {
        return parsedColumnExprList;
    }

    public String getColumnSeparator() {
        return columnSeparator;
    }

    public String getLineDelimiter() {
        return lineDelimiter;
    }

    public String getFileFormat() {
        return fileFormat;
    }

    public String getPath() {
        return path;
    }

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    public HiveScanNode(PlanNodeId id, TupleDescriptor destTupleDesc, String planNodeName,
                        List<List<TBrokerFileStatus>> fileStatusesList, int filesAdded) {
        super(id, destTupleDesc, planNodeName, fileStatusesList, filesAdded, StatisticalType.HIVE_SCAN_NODE);
        this.hiveTable = (HiveTable) destTupleDesc.getTable();
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
    }

    @Override
    protected void initFileGroup() throws UserException {
        initHiveTblProperties();
        analyzeColumnFromPath();

        HiveTable hiveTable = (HiveTable) desc.getTable();
        fileGroups = Lists.newArrayList(
                new BrokerFileGroup(hiveTable.getId(),
                        getColumnSeparator(),
                        getLineDelimiter(),
                        getPath(),
                        getFileFormat(),
                        getPartitionKeys(),
                        getParsedColumnExprList()));
        brokerDesc = new BrokerDesc("HiveTableDesc", storageType, hiveTable.getHiveProperties());
        targetTable = hiveTable;
    }

    private void setStorageType(String location) throws UserException {
        String[] strings = StringUtils.split(location, "/");
        String storagePrefix = strings[0].split(":")[0];
        if (Util.isS3CompatibleStorageSchema(storagePrefix)) {
            this.storageType = StorageBackend.StorageType.S3;
        } else if (storagePrefix.equalsIgnoreCase("hdfs")) {
            this.storageType = StorageBackend.StorageType.HDFS;
        } else {
            throw new UserException("Not supported storage type: " + storagePrefix);
        }
    }

    private void initHiveTblProperties() throws UserException {
        this.remoteHiveTable = HiveMetaStoreClientHelper.getTable(hiveTable);
        this.fileFormat = HiveMetaStoreClientHelper.HiveFileFormat.getFormat(remoteHiveTable.getSd().getInputFormat());
        this.setStorageType(remoteHiveTable.getSd().getLocation());

        Map<String, String> serDeInfoParams = remoteHiveTable.getSd().getSerdeInfo().getParameters();
        this.columnSeparator = Strings.isNullOrEmpty(serDeInfoParams.get("field.delim"))
                ? HIVE_DEFAULT_COLUMN_SEPARATOR : serDeInfoParams.get("field.delim");
        this.lineDelimiter = Strings.isNullOrEmpty(serDeInfoParams.get("line.delim"))
                ? HIVE_DEFAULT_LINE_DELIMITER : serDeInfoParams.get("line.delim");
        this.path = remoteHiveTable.getSd().getLocation();
        for (FieldSchema fieldSchema : remoteHiveTable.getPartitionKeys()) {
            this.partitionKeys.add(fieldSchema.getName());
        }
    }

    @Override
    protected void getFileStatus() throws UserException {
        if (partitionKeys.size() > 0) {
            hivePartitionPredicate = HiveMetaStoreClientHelper.convertToHivePartitionExpr(
                    conjuncts, partitionKeys, hiveTable.getName());
        }
        List<TBrokerFileStatus> fileStatuses = new ArrayList<>();
        this.hdfsUri = HiveMetaStoreClientHelper.getHiveDataFiles(hiveTable, hivePartitionPredicate,
                fileStatuses, remoteHiveTable, storageType);
        fileStatusesList.add(fileStatuses);
        filesAdded += fileStatuses.size();
        for (TBrokerFileStatus fstatus : fileStatuses) {
            LOG.debug("Add file status is {}", fstatus);
        }
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        if (!isLoad()) {
            output.append(prefix).append("TABLE: ").append(hiveTable.getName()).append("\n");
            output.append(prefix).append("PATH: ")
                    .append(hiveTable.getHiveProperties().get(HMSResource.HIVE_METASTORE_URIS)).append("\n");
        }
        return output.toString();
    }

    /**
     * Analyze columns from path, the partition columns
     */
    private void analyzeColumnFromPath() {
        for (String colName : partitionKeys) {
            ImportColumnDesc importColumnDesc = new ImportColumnDesc(colName, null);
            parsedColumnExprList.add(importColumnDesc);
        }
    }
}
