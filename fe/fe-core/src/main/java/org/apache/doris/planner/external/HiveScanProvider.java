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

package org.apache.doris.planner.external;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.HiveBucketUtil;
import org.apache.doris.catalog.HiveMetaStoreClientHelper;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.external.hive.util.HiveUtil;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.planner.external.ExternalFileScanNode.ParamCreateContext;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TFileScanSlotInfo;
import org.apache.doris.thrift.TFileTextScanRangeParams;
import org.apache.doris.thrift.TFileType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A HiveScanProvider to get information for scan node.
 */
public class HiveScanProvider extends HMSTableScanProvider {
    private static final Logger LOG = LogManager.getLogger(HiveScanProvider.class);

    private static final String PROP_FIELD_DELIMITER = "field.delim";
    private static final String DEFAULT_FIELD_DELIMITER = "\1"; // "\x01"
    private static final String DEFAULT_LINE_DELIMITER = "\n";

    protected HMSExternalTable hmsTable;

    protected final TupleDescriptor desc;

    public HiveScanProvider(HMSExternalTable hmsTable, TupleDescriptor desc) {
        this.hmsTable = hmsTable;
        this.desc = desc;
    }

    @Override
    public TableIf getTargetTable() {
        return hmsTable;
    }

    @Override
    public TFileFormatType getFileFormatType() throws DdlException, MetaNotFoundException {
        TFileFormatType type = null;
        String inputFormatName = getRemoteHiveTable().getSd().getInputFormat();
        String hiveFormat = HiveMetaStoreClientHelper.HiveFileFormat.getFormat(inputFormatName);
        if (hiveFormat.equals(HiveMetaStoreClientHelper.HiveFileFormat.PARQUET.getDesc())) {
            type = TFileFormatType.FORMAT_PARQUET;
        } else if (hiveFormat.equals(HiveMetaStoreClientHelper.HiveFileFormat.ORC.getDesc())) {
            type = TFileFormatType.FORMAT_ORC;
        } else if (hiveFormat.equals(HiveMetaStoreClientHelper.HiveFileFormat.TEXT_FILE.getDesc())) {
            type = TFileFormatType.FORMAT_CSV_PLAIN;
        }
        return type;
    }

    @Override
    public TFileType getLocationType() throws DdlException, MetaNotFoundException {
        String location = hmsTable.getRemoteTable().getSd().getLocation();
        if (location != null && !location.isEmpty()) {
            if (location.startsWith(FeConstants.FS_PREFIX_S3)
                    || location.startsWith(FeConstants.FS_PREFIX_S3A)
                    || location.startsWith(FeConstants.FS_PREFIX_S3N)
                    || location.startsWith(FeConstants.FS_PREFIX_BOS)
                    || location.startsWith(FeConstants.FS_PREFIX_COS)
                    || location.startsWith(FeConstants.FS_PREFIX_OSS)
                    || location.startsWith(FeConstants.FS_PREFIX_OBS)) {
                return TFileType.FILE_S3;
            } else if (location.startsWith(FeConstants.FS_PREFIX_HDFS)) {
                return TFileType.FILE_HDFS;
            } else if (location.startsWith(FeConstants.FS_PREFIX_FILE)) {
                return TFileType.FILE_LOCAL;
            }
        }
        throw new DdlException("Unknown file location " + location + " for hms table " + hmsTable.getName());
    }

    @Override
    public String getMetaStoreUrl() {
        return hmsTable.getMetastoreUri();
    }

    @Override
    public List<InputSplit> getSplits(List<Expr> exprs) throws IOException, UserException {
        // eg:
        // oss://buckts/data_dir
        // hdfs://hosts/data_dir
        String location = getRemoteHiveTable().getSd().getLocation();
        List<String> partitionKeys = getRemoteHiveTable().getPartitionKeys().stream().map(FieldSchema::getName)
                .collect(Collectors.toList());
        List<Partition> hivePartitions = new ArrayList<>();

        if (partitionKeys.size() > 0) {
            ExprNodeGenericFuncDesc hivePartitionPredicate = HiveMetaStoreClientHelper.convertToHivePartitionExpr(exprs,
                    partitionKeys, hmsTable.getName());
            hivePartitions.addAll(hmsTable.getHivePartitions(hivePartitionPredicate));
        }

        String inputFormatName = getRemoteHiveTable().getSd().getInputFormat();

        Configuration configuration = setConfiguration();
        InputFormat<?, ?> inputFormat = HiveUtil.getInputFormat(configuration, inputFormatName, false);
        List<InputSplit> splits;
        if (!hivePartitions.isEmpty()) {
            try {
                splits = hivePartitions.stream().flatMap(x -> {
                    try {
                        return getSplitsByPath(inputFormat, configuration, x.getSd().getLocation()).stream();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }).collect(Collectors.toList());
            } catch (RuntimeException e) {
                throw new IOException(e);
            }
        } else {
            splits = getSplitsByPath(inputFormat, configuration, location);
        }
        return HiveBucketUtil.getPrunedSplitsByBuckets(splits, hmsTable.getName(), exprs,
                getRemoteHiveTable().getSd().getBucketCols(), getRemoteHiveTable().getSd().getNumBuckets(),
                getRemoteHiveTable().getParameters());
    }

    private List<InputSplit> getSplitsByPath(InputFormat<?, ?> inputFormat, Configuration configuration,
            String location) throws IOException {
        String finalLocation = convertToS3IfNecessary(location);
        JobConf jobConf = new JobConf(configuration);
        // For Tez engine, it may generate subdirectoies for "union" query.
        // So there may be files and directories in the table directory at the same time. eg:
        //      /user/hive/warehouse/region_tmp_union_all2/000000_0
        //      /user/hive/warehouse/region_tmp_union_all2/1
        //      /user/hive/warehouse/region_tmp_union_all2/2
        // So we need to set this config to support visit dir recursively.
        // Otherwise, getSplits() may throw exception: "Not a file xxx"
        // https://blog.actorsfit.com/a?ID=00550-ce56ec63-1bff-4b0c-a6f7-447b93efaa31
        jobConf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true");
        FileInputFormat.setInputPaths(jobConf, finalLocation);
        InputSplit[] splits = inputFormat.getSplits(jobConf, 0);
        return Lists.newArrayList(splits);
    }

    // convert oss:// to s3://
    private String convertToS3IfNecessary(String location) throws IOException {
        LOG.debug("try convert location to s3 prefix: " + location);
        if (location.startsWith(FeConstants.FS_PREFIX_COS)
                || location.startsWith(FeConstants.FS_PREFIX_BOS)
                || location.startsWith(FeConstants.FS_PREFIX_BOS)
                || location.startsWith(FeConstants.FS_PREFIX_OSS)
                || location.startsWith(FeConstants.FS_PREFIX_S3A)
                || location.startsWith(FeConstants.FS_PREFIX_S3N)) {
            int pos = location.indexOf("://");
            if (pos == -1) {
                throw new IOException("No '://' found in location: " + location);
            }
            return "s3" + location.substring(pos);
        }
        return location;
    }

    protected Configuration setConfiguration() {
        Configuration conf = new HdfsConfiguration();
        for (Map.Entry<String, String> entry : hmsTable.getCatalog().getCatalogProperty().getProperties().entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }
        Map<String, String> s3Properties = hmsTable.getS3Properties();
        for (Map.Entry<String, String> entry : s3Properties.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }
        return conf;
    }

    @Override
    public Table getRemoteHiveTable() throws DdlException, MetaNotFoundException {
        return hmsTable.getRemoteTable();
    }

    @Override
    public Map<String, String> getTableProperties() throws MetaNotFoundException {
        // TODO: implement it when we really properties from remote table.
        return Maps.newHashMap();
    }

    @Override
    public Map<String, String> getLocationProperties() throws MetaNotFoundException, DdlException {
        TFileType locationType = getLocationType();
        if (locationType == TFileType.FILE_S3) {
            return hmsTable.getS3Properties();
        } else if (locationType == TFileType.FILE_HDFS) {
            return hmsTable.getDfsProperties();
        } else {
            return Maps.newHashMap();
        }
    }

    @Override
    public List<String> getPathPartitionKeys() throws DdlException, MetaNotFoundException {
        return getRemoteHiveTable().getPartitionKeys().stream().map(FieldSchema::getName).collect(Collectors.toList());
    }

    @Override
    public ParamCreateContext createContext(Analyzer analyzer) throws UserException {
        ParamCreateContext context = new ParamCreateContext();
        context.params = new TFileScanRangeParams();
        context.destTupleDescriptor = desc;
        context.params.setDestTupleId(desc.getId().asInt());
        context.fileGroup = new BrokerFileGroup(hmsTable.getId(),
                hmsTable.getRemoteTable().getSd().getLocation(), hmsTable.getRemoteTable().getSd().getInputFormat());


        // Hive table must extract partition value from path and hudi/iceberg table keep
        // partition field in file.
        List<String> partitionKeys = getPathPartitionKeys();
        List<Column> columns = hmsTable.getBaseSchema(false);
        context.params.setNumOfColumnsFromFile(columns.size() - partitionKeys.size());
        for (SlotDescriptor slot : desc.getSlots()) {
            if (!slot.isMaterialized()) {
                continue;
            }

            TFileScanSlotInfo slotInfo = new TFileScanSlotInfo();
            slotInfo.setSlotId(slot.getId().asInt());
            slotInfo.setIsFileSlot(!partitionKeys.contains(slot.getColumn().getName()));
            context.params.addToRequiredSlots(slotInfo);
        }
        return context;
    }

    @Override
    public TFileAttributes getFileAttributes() throws UserException {
        TFileTextScanRangeParams textParams = new TFileTextScanRangeParams();
        textParams.setColumnSeparator(hmsTable.getRemoteTable().getSd().getSerdeInfo().getParameters()
                .getOrDefault(PROP_FIELD_DELIMITER, DEFAULT_FIELD_DELIMITER));
        textParams.setLineDelimiter(DEFAULT_LINE_DELIMITER);
        TFileAttributes fileAttributes = new TFileAttributes();
        fileAttributes.setTextParams(textParams);
        fileAttributes.setHeaderType("");
        return fileAttributes;
    }
}


