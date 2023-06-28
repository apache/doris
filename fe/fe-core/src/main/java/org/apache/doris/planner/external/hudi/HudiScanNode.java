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

package org.apache.doris.planner.external.hudi;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.HiveMetaStoreClientHelper;
import org.apache.doris.catalog.HudiUtils;
import org.apache.doris.catalog.external.ExternalTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.hive.HivePartition;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.external.FileSplit;
import org.apache.doris.planner.external.HiveScanNode;
import org.apache.doris.planner.external.TableFormatType;
import org.apache.doris.spi.Split;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.THudiFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class HudiScanNode extends HiveScanNode {

    private static final Logger LOG = LogManager.getLogger(HudiScanNode.class);

    private final boolean isCowTable;

    private long noLogsSplitNum = 0;

    /**
     * External file scan node for Query Hudi table
     * needCheckColumnPriv: Some of ExternalFileScanNode do not need to check column priv
     * eg: s3 tvf
     * These scan nodes do not have corresponding catalog/database/table info, so no need to do priv check
     */
    public HudiScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv) {
        super(id, desc, "HUDI_SCAN_NODE", StatisticalType.HUDI_SCAN_NODE, needCheckColumnPriv);
        isCowTable = hmsTable.isHoodieCowTable();
        if (isCowTable) {
            LOG.debug("Hudi table {} can read as cow table", hmsTable.getName());
        } else {
            LOG.debug("Hudi table {} is a mor table, and will use JNI to read data in BE", hmsTable.getName());
        }
    }

    @Override
    public TFileFormatType getFileFormatType() throws UserException {
        if (isCowTable) {
            return super.getFileFormatType();
        } else {
            // Use jni to read hudi table in BE
            return TFileFormatType.FORMAT_JNI;
        }
    }

    @Override
    protected void doInitialize() throws UserException {
        ExternalTable table = (ExternalTable) desc.getTable();
        if (table.isView()) {
            throw new AnalysisException(
                    String.format("Querying external view '%s.%s' is not supported", table.getDbName(),
                            table.getName()));
        }
        computeColumnFilter();
        initBackendPolicy();
        initSchemaParams();
    }

    @Override
    protected Map<String, String> getLocationProperties() throws UserException {
        if (isCowTable) {
            return super.getLocationProperties();
        } else {
            // HudiJniScanner uses hadoop client to read data.
            return hmsTable.getHadoopProperties();
        }
    }

    public static void setHudiParams(TFileRangeDesc rangeDesc, HudiSplit hudiSplit) {
        TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
        tableFormatFileDesc.setTableFormatType(hudiSplit.getTableFormatType().value());
        THudiFileDesc fileDesc = new THudiFileDesc();
        fileDesc.setInstantTime(hudiSplit.getInstantTime());
        fileDesc.setSerde(hudiSplit.getSerde());
        fileDesc.setInputFormat(hudiSplit.getInputFormat());
        fileDesc.setBasePath(hudiSplit.getBasePath());
        fileDesc.setDataFilePath(hudiSplit.getDataFilePath());
        fileDesc.setDataFileLength(hudiSplit.getFileLength());
        fileDesc.setDeltaLogs(hudiSplit.getHudiDeltaLogs());
        fileDesc.setColumnNames(hudiSplit.getHudiColumnNames());
        fileDesc.setColumnTypes(hudiSplit.getHudiColumnTypes());
        // TODO(gaoxin): support complex types
        // fileDesc.setNestedFields(hudiSplit.getNestedFields());
        tableFormatFileDesc.setHudiParams(fileDesc);
        rangeDesc.setTableFormatParams(tableFormatFileDesc);
    }

    @Override
    public List<Split> getSplits() throws UserException {
        if (isCowTable) {
            // skip hidden files start with "."
            List<Split> cowSplits = super.getSplits().stream()
                    .filter(split -> !((FileSplit) split).getPath().getName().startsWith("."))
                    .collect(Collectors.toList());
            noLogsSplitNum = cowSplits.size();
            return cowSplits;
        }

        HoodieTableMetaClient hudiClient = HiveMetaStoreClientHelper.getHudiClient(hmsTable);
        hudiClient.reloadActiveTimeline();
        String basePath = hmsTable.getRemoteTable().getSd().getLocation();
        String inputFormat = hmsTable.getRemoteTable().getSd().getInputFormat();
        String serdeLib = hmsTable.getRemoteTable().getSd().getSerdeInfo().getSerializationLib();

        TableSchemaResolver schemaUtil = new TableSchemaResolver(hudiClient);
        Schema hudiSchema;
        try {
            hudiSchema = HoodieAvroUtils.createHoodieWriteSchema(schemaUtil.getTableAvroSchema());
        } catch (Exception e) {
            throw new RuntimeException("Cannot get hudi table schema.");
        }

        List<String> columnNames = new ArrayList<>();
        List<String> columnTypes = new ArrayList<>();
        List<FieldSchema> allFields = hmsTable.getRemoteTable().getSd().getCols();
        allFields.addAll(hmsTable.getRemoteTable().getPartitionKeys());

        List<Split> splits = new ArrayList<>();
        for (Schema.Field hudiField : hudiSchema.getFields()) {
            String columnName = hudiField.name().toLowerCase(Locale.ROOT);
            // keep hive metastore column in hudi avro schema.
            Optional<FieldSchema> field = allFields.stream().filter(f -> f.getName().equals(columnName)).findFirst();
            if (!field.isPresent()) {
                String errorMsg = String.format("Hudi column %s not exists in hive metastore.", hudiField.name());
                throw new IllegalArgumentException(errorMsg);
            }
            columnNames.add(columnName);
            String columnType = HudiUtils.fromAvroHudiTypeToHiveTypeString(hudiField.schema());
            columnTypes.add(columnType);
        }

        HoodieTimeline timeline = hudiClient.getCommitsAndCompactionTimeline().filterCompletedInstants();
        Option<HoodieInstant> latestInstant = timeline.lastInstant();
        if (!latestInstant.isPresent()) {
            return new ArrayList<>();
        }
        String queryInstant = latestInstant.get().getTimestamp();
        // Non partition table will get one dummy partition
        List<HivePartition> partitions = getPartitions();
        try {
            for (HivePartition partition : partitions) {
                String globPath;
                String partitionName = "";
                if (partition.isDummyPartition()) {
                    globPath = hudiClient.getBasePathV2().toString() + "/*";
                } else {
                    partitionName = FSUtils.getRelativePartitionPath(hudiClient.getBasePathV2(),
                            new Path(partition.getPath()));
                    globPath = String.format("%s/%s/*", hudiClient.getBasePathV2().toString(), partitionName);
                }
                List<FileStatus> statuses = FSUtils.getGlobStatusExcludingMetaFolder(hudiClient.getRawFs(),
                        new Path(globPath));
                HoodieTableFileSystemView fileSystemView = new HoodieTableFileSystemView(hudiClient,
                        timeline, statuses.toArray(new FileStatus[0]));

                Iterator<FileSlice> hoodieFileSliceIterator = fileSystemView
                        .getLatestMergedFileSlicesBeforeOrOn(partitionName, queryInstant).iterator();
                while (hoodieFileSliceIterator.hasNext()) {
                    FileSlice fileSlice = hoodieFileSliceIterator.next();
                    Optional<HoodieBaseFile> baseFile = fileSlice.getBaseFile().toJavaOptional();
                    String filePath = baseFile.map(BaseFile::getPath).orElse("");
                    long fileSize = baseFile.map(BaseFile::getFileSize).orElse(0L);

                    List<String> logs = fileSlice.getLogFiles().map(HoodieLogFile::getPath).map(Path::toString)
                            .collect(Collectors.toList());
                    if (logs.isEmpty()) {
                        noLogsSplitNum++;
                    }

                    HudiSplit split = new HudiSplit(new Path(filePath), 0, fileSize, fileSize, new String[0],
                            partition.getPartitionValues());
                    split.setTableFormatType(TableFormatType.HUDI);
                    split.setDataFilePath(filePath);
                    split.setHudiDeltaLogs(logs);
                    split.setInputFormat(inputFormat);
                    split.setSerde(serdeLib);
                    split.setBasePath(basePath);
                    split.setHudiColumnNames(columnNames);
                    split.setHudiColumnTypes(columnTypes);
                    split.setInstantTime(queryInstant);
                    splits.add(split);
                }
            }
        } catch (Exception e) {
            String errorMsg = String.format("Failed to get hudi info on basePath: %s", basePath);
            LOG.error(errorMsg, e);
            throw new IllegalArgumentException(errorMsg, e);
        }
        return splits;
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        return super.getNodeExplainString(prefix, detailLevel)
                + String.format("%shudiNativeReadSplits=%d/%d\n", prefix, noLogsSplitNum, inputSplitsNum);
    }
}
