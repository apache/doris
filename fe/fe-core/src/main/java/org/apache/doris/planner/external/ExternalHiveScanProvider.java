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
import org.apache.doris.catalog.HiveMetaStoreClientHelper;
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.external.hive.util.HiveUtil;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A HiveScanProvider to get information for scan node.
 */
public class ExternalHiveScanProvider implements ExternalFileScanProvider {
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
    public TFileType getTableFileType() {
        return TFileType.FILE_HDFS;
    }

    @Override
    public String getMetaStoreUrl() {
        return hmsTable.getMetastoreUri();
    }

    @Override
    public InputSplit[] getSplits(List<Expr> exprs)
            throws IOException, UserException {
        String splitsPath = getRemoteHiveTable().getSd().getLocation();
        List<String> partitionKeys = getRemoteHiveTable().getPartitionKeys()
                .stream().map(FieldSchema::getName).collect(Collectors.toList());

        if (partitionKeys.size() > 0) {
            ExprNodeGenericFuncDesc hivePartitionPredicate = HiveMetaStoreClientHelper.convertToHivePartitionExpr(
                    exprs, partitionKeys, hmsTable.getName());

            String metaStoreUris = getMetaStoreUrl();
            List<Partition> hivePartitions = HiveMetaStoreClientHelper.getHivePartitions(
                    metaStoreUris,  getRemoteHiveTable(), hivePartitionPredicate);
            if (!hivePartitions.isEmpty()) {
                splitsPath = hivePartitions.stream().map(x -> x.getSd().getLocation())
                        .collect(Collectors.joining(","));
            }
        }

        String inputFormatName = getRemoteHiveTable().getSd().getInputFormat();

        Configuration configuration = setConfiguration();
        InputFormat<?, ?> inputFormat = HiveUtil.getInputFormat(configuration, inputFormatName, false);
        JobConf jobConf = new JobConf(configuration);
        FileInputFormat.setInputPaths(jobConf, splitsPath);
        return inputFormat.getSplits(jobConf, 0);
    }

    private Configuration setConfiguration() {
        Configuration conf = new Configuration();
        Map<String, String> dfsProperties = hmsTable.getDfsProperties();
        for (Map.Entry<String, String> entry : dfsProperties.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }
        Map<String, String> s3Properties = hmsTable.getDfsProperties();
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
