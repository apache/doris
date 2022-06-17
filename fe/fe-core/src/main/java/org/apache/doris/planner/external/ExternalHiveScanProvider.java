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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.ArrayList;
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
    public String getMetaStoreUrl() throws MetaNotFoundException {
        return getTableProperties().get(HiveConf.ConfVars.METASTOREURIS.name());
    }

    @Override
    public InputSplit[] getSplits(List<Expr> exprs)
            throws IOException, UserException {
        String splitsPath = getRemoteHiveTable().getSd().getLocation();
        List<String> partitionKeys = getRemoteHiveTable().getPartitionKeys()
                .stream().map(FieldSchema::getName).collect(Collectors.toList());

        if (partitionKeys.size() > 0) {
            ExprNodeGenericFuncDesc hivePartitionPredicate = extractHivePartitionPredicate(exprs, partitionKeys);

            String metaStoreUris = getMetaStoreUrl();
            List<Partition> hivePartitions = HiveMetaStoreClientHelper.getHivePartitions(
                    metaStoreUris,  getRemoteHiveTable(), hivePartitionPredicate);
            if (!hivePartitions.isEmpty()) {
                splitsPath = hivePartitions.stream().map(x -> x.getSd().getLocation())
                        .collect(Collectors.joining(","));
            }
        }

        String inputFormatName = getRemoteHiveTable().getSd().getInputFormat();

        Configuration configuration = new Configuration();
        InputFormat<?, ?> inputFormat = HiveUtil.getInputFormat(configuration, inputFormatName, false);
        JobConf jobConf = new JobConf(configuration);
        FileInputFormat.setInputPaths(jobConf, splitsPath);
        return inputFormat.getSplits(jobConf, 0);
    }


    private ExprNodeGenericFuncDesc extractHivePartitionPredicate(List<Expr> conjuncts, List<String> partitionKeys)
            throws DdlException {
        ExprNodeGenericFuncDesc hivePartitionPredicate;
        List<ExprNodeDesc> exprNodeDescs = new ArrayList<>();
        for (Expr conjunct : conjuncts) {
            ExprNodeGenericFuncDesc hiveExpr = HiveMetaStoreClientHelper.convertToHivePartitionExpr(
                    conjunct, partitionKeys, hmsTable.getName());
            if (hiveExpr != null) {
                exprNodeDescs.add(hiveExpr);
            }
        }
        int count = exprNodeDescs.size();

        if (count >= 2) {
            hivePartitionPredicate = HiveMetaStoreClientHelper.getCompoundExpr(exprNodeDescs, "and");
        } else if (count == 1) {
            hivePartitionPredicate = (ExprNodeGenericFuncDesc) exprNodeDescs.get(0);
        } else {
            HiveMetaStoreClientHelper.ExprBuilder exprBuilder =
                    new HiveMetaStoreClientHelper.ExprBuilder(hmsTable.getName());
            hivePartitionPredicate = exprBuilder.val(TypeInfoFactory.intTypeInfo, 1)
                    .val(TypeInfoFactory.intTypeInfo, 1)
                    .pred("=", 2).build();
        }
        return hivePartitionPredicate;
    }

    @Override
    public Table getRemoteHiveTable() throws DdlException, MetaNotFoundException {
        return hmsTable.getRemoteTable();
    }

    @Override
    public Map<String, String> getTableProperties() throws MetaNotFoundException {
        return hmsTable.getRemoteTable().getParameters();
    }
}
