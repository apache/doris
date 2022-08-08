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

import org.apache.doris.analysis.Expr;
import org.apache.doris.catalog.HiveBucketUtil;
import org.apache.doris.catalog.HiveMetaStoreClientHelper;
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.external.hive.util.HiveUtil;
import org.apache.doris.thrift.TFileFormatType;
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
public class ExternalHiveScanProvider implements ExternalFileScanProvider {
    private static final Logger LOG = LogManager.getLogger(ExternalHiveScanProvider.class);

    protected HMSExternalTable hmsTable;

    public ExternalHiveScanProvider(HMSExternalTable hmsTable) {
        this.hmsTable = hmsTable;
    }

    @Override
    public TFileFormatType getTableFormatType() throws DdlException, MetaNotFoundException {
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
    public TFileType getTableFileType() throws DdlException, MetaNotFoundException {
        String location = hmsTable.getRemoteTable().getSd().getLocation();
        if (location != null && !location.isEmpty()) {
            if (location.startsWith("s3a") || location.startsWith("s3n")) {
                return TFileType.FILE_S3;
            } else if (location.startsWith("hdfs:")) {
                return TFileType.FILE_HDFS;
            }
        }
        throw new DdlException("Unknown file type for hms table.");
    }

    @Override
    public String getMetaStoreUrl() {
        return hmsTable.getMetastoreUri();
    }

    @Override
    public List<InputSplit> getSplits(List<Expr> exprs)
            throws IOException, UserException {
        String splitsPath = getRemoteHiveTable().getSd().getLocation();
        List<String> partitionKeys = getRemoteHiveTable().getPartitionKeys()
                .stream().map(FieldSchema::getName).collect(Collectors.toList());
        List<Partition> hivePartitions = new ArrayList<>();

        if (partitionKeys.size() > 0) {
            ExprNodeGenericFuncDesc hivePartitionPredicate = HiveMetaStoreClientHelper.convertToHivePartitionExpr(
                    exprs, partitionKeys, hmsTable.getName());

            String metaStoreUris = getMetaStoreUrl();
            hivePartitions.addAll(HiveMetaStoreClientHelper.getHivePartitions(
                    metaStoreUris,  getRemoteHiveTable(), hivePartitionPredicate));
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
            splits = getSplitsByPath(inputFormat, configuration, splitsPath);
        }
        return HiveBucketUtil.getPrunedSplitsByBuckets(splits, hmsTable.getName(), exprs,
                getRemoteHiveTable().getSd().getBucketCols(), getRemoteHiveTable().getSd().getNumBuckets(),
                getRemoteHiveTable().getParameters());
    }

    private List<InputSplit> getSplitsByPath(InputFormat<?, ?> inputFormat, Configuration configuration,
            String splitsPath) throws IOException {
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
        FileInputFormat.setInputPaths(jobConf, splitsPath);
        InputSplit[] splits = inputFormat.getSplits(jobConf, 0);
        return Lists.newArrayList(splits);
    }


    protected Configuration setConfiguration() {
        Configuration conf = new HdfsConfiguration();
        Map<String, String> dfsProperties = hmsTable.getDfsProperties();
        for (Map.Entry<String, String> entry : dfsProperties.entrySet()) {
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
        Map<String, String> properteis = Maps.newHashMap(hmsTable.getRemoteTable().getParameters());
        properteis.putAll(hmsTable.getDfsProperties());
        return properteis;
    }

    @Override
    public List<String> getPathPartitionKeys() throws DdlException, MetaNotFoundException {
        return getRemoteHiveTable().getPartitionKeys().stream().map(FieldSchema::getName).collect(Collectors.toList());
    }
}
