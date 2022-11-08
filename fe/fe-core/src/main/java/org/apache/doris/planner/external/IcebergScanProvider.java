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
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.external.iceberg.util.IcebergUtils;
import org.apache.doris.thrift.TFileFormatType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A file scan provider for iceberg.
 */
public class IcebergScanProvider extends HiveScanProvider {

    public IcebergScanProvider(HMSExternalTable hmsTable, TupleDescriptor desc) {
        super(hmsTable, desc);
    }

    @Override
    public TFileFormatType getFileFormatType() throws DdlException, MetaNotFoundException {
        TFileFormatType type;

        String icebergFormat = getRemoteHiveTable().getParameters()
                .getOrDefault(TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
        if (icebergFormat.equals("parquet")) {
            type = TFileFormatType.FORMAT_PARQUET;
        } else if (icebergFormat.equals("orc")) {
            type = TFileFormatType.FORMAT_ORC;
        } else {
            throw new DdlException(String.format("Unsupported format name: %s for iceberg table.", icebergFormat));
        }
        return type;
    }

    @Override
    public List<InputSplit> getSplits(List<Expr> exprs) throws IOException, UserException {
        List<Expression> expressions = new ArrayList<>();
        for (Expr conjunct : exprs) {
            Expression expression = IcebergUtils.convertToIcebergExpr(conjunct);
            if (expression != null) {
                expressions.add(expression);
            }
        }

        org.apache.iceberg.Table table = getIcebergTable();
        TableScan scan = table.newScan();
        for (Expression predicate : expressions) {
            scan = scan.filter(predicate);
        }
        List<InputSplit> splits = new ArrayList<>();

        for (FileScanTask task : scan.planFiles()) {
            for (FileScanTask spitTask : task.split(128 * 1024 * 1024)) {
                splits.add(new FileSplit(new Path(spitTask.file().path().toString()),
                        spitTask.start(), spitTask.length(), new String[0]));
            }
        }
        return splits;
    }

    private org.apache.iceberg.Table getIcebergTable() throws MetaNotFoundException {
        org.apache.iceberg.hive.HiveCatalog hiveCatalog = new org.apache.iceberg.hive.HiveCatalog();
        Configuration conf = setConfiguration();
        hiveCatalog.setConf(conf);
        // initialize hive catalog
        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put("hive.metastore.uris", getMetaStoreUrl());
        catalogProperties.put("uri", getMetaStoreUrl());
        hiveCatalog.initialize("hive", catalogProperties);

        return hiveCatalog.loadTable(TableIdentifier.of(hmsTable.getDbName(), hmsTable.getName()));
    }

    @Override
    public List<String> getPathPartitionKeys() throws DdlException, MetaNotFoundException {
        return Collections.emptyList();
    }
}
