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
import org.apache.doris.catalog.HiveTable;
import org.apache.doris.catalog.IcebergProperty;
import org.apache.doris.catalog.IcebergTable;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.external.iceberg.util.IcebergUtils;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ExternalIcebergScanProvider implements ExternalFileScanProvider {
    private final org.apache.doris.catalog.Table catalogTable;
    public ExternalIcebergScanProvider(org.apache.doris.catalog.Table catalogTable) {
        this.catalogTable = catalogTable;
    }
    @Override
    public TFileFormatType getTableFormatType() throws DdlException {
        TFileFormatType type = null;

        String iceberg_format  = getRemoteHiveTable().getParameters()
                .getOrDefault(TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
        if (iceberg_format.equals("parquet")) {
            type = TFileFormatType.FORMAT_PARQUET;
        } else if (iceberg_format.equals("orc")) {
            type = TFileFormatType.FORMAT_ORC;
        } else {
            throw new DdlException(String.format("Unsupported format name: %s for iceberg table.", iceberg_format));
        }
        return type;
    }

    @Override
    public TFileType getTableFileType() {
        return TFileType.FILE_HDFS;
    }

    @Override
    public String getMetaStoreUrl() {
        return getTableProperties().get(IcebergProperty.ICEBERG_HIVE_METASTORE_URIS);
    }

    @Override
    public InputSplit[] getSplits(List<Expr> exprs)
            throws IOException, UserException {
        List<Expression> expressions = new ArrayList<>();
        for (Expr conjunct : exprs) {
            Expression expression = IcebergUtils.convertToIcebergExpr(conjunct);
            if (expression != null) {
                expressions.add(expression);
            }
        }

        org.apache.iceberg.Table table = ((IcebergTable)catalogTable).getTable();
        TableScan scan = table.newScan();
        for (Expression predicate : expressions) {
            scan = scan.filter(predicate);
        }
        List<FileSplit> splits = new ArrayList<>();

        for (FileScanTask task : scan.planFiles()) {
            for (FileScanTask spitTask: task.split(128 * 1024 * 1024)) {
                splits.add(new FileSplit(new Path(spitTask.file().path().toString()), spitTask.start(), spitTask.length(), new String[0]));
            }
        }
        return splits.toArray(new InputSplit[0]);
    }

    @Override
    public Table getRemoteHiveTable() throws DdlException {
        String dbName =((IcebergTable) catalogTable).getIcebergDb();
        String tableName =((IcebergTable) catalogTable).getIcebergTbl();
        return HiveMetaStoreClientHelper.getTable(dbName, tableName, getMetaStoreUrl());
    }

    @Override
    public Map<String, String> getTableProperties() {
        return ((IcebergTable) catalogTable).getIcebergProperties();
    }
}
