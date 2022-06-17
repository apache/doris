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
import org.apache.doris.catalog.IcebergProperty;
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.external.iceberg.HiveCatalog;
import org.apache.doris.external.iceberg.util.IcebergUtils;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;

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
import java.util.List;

/**
 * A file scan provider for iceberg.
 */
public class ExternalIcebergScanProvider extends ExternalHiveScanProvider {

    public ExternalIcebergScanProvider(HMSExternalTable hmsTable) {
        super(hmsTable);
    }

    @Override
    public TFileFormatType getTableFormatType() throws DdlException, MetaNotFoundException {
        TFileFormatType type;

        String icebergFormat  = getRemoteHiveTable().getParameters()
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
    public TFileType getTableFileType() {
        return TFileType.FILE_HDFS;
    }

    @Override
    public InputSplit[] getSplits(List<Expr> exprs) throws IOException, UserException {
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
        List<FileSplit> splits = new ArrayList<>();

        for (FileScanTask task : scan.planFiles()) {
            for (FileScanTask spitTask : task.split(128 * 1024 * 1024)) {
                splits.add(new FileSplit(new Path(spitTask.file().path().toString()),
                        spitTask.start(), spitTask.length(), new String[0]));
            }
        }
        return splits.toArray(new InputSplit[0]);
    }

    private org.apache.iceberg.Table getIcebergTable() throws MetaNotFoundException {
        HiveCatalog hiveCatalog = new HiveCatalog();
        hiveCatalog.initialize(new IcebergProperty(getTableProperties()));
        return hiveCatalog.loadTable(TableIdentifier.of(hmsTable.getDbName(), hmsTable.getName()));
    }
}
