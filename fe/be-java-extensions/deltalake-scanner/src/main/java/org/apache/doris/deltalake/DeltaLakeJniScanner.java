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

package org.apache.doris.deltalake;

import org.apache.doris.common.jni.JniScanner;
import org.apache.doris.common.jni.vec.ColumnType;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.DataType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static org.apache.doris.deltalake.DeltaLakeScannerUtils.decodeStringToConf;

public class DeltaLakeJniScanner extends JniScanner {
    private static final Logger LOG = LoggerFactory.getLogger(DeltaLakeJniScanner.class);
    private final String dbName;
    private final String tblName;
    private final String path;
    private CloseableIterator<RowRecord> iter = null;
    private final Configuration conf;
    private final DeltaLakeColumnValue columnValue = new DeltaLakeColumnValue();

    public DeltaLakeJniScanner(int batchSize, Map<String, String> params) {
        LOG.debug("params:{}", params);
        conf =decodeStringToConf( params.get("conf"));
        dbName = params.get("db_name");
        tblName = params.get("table_name");
        path = params.get("path");
        super.batchSize = batchSize;
        super.fields = params.get("deltalake_column_names").split(",");
    }

    @Override
    public void open() throws IOException {
        initIter();
        parseRequiredTypes();
    }

    @Override
    public void close() throws IOException {
        iter.close();
    }

    @Override
    protected int getNext() {
        int rows = 0;
        RowRecord row;
        while (iter.hasNext()) {
            row = iter.next();
            columnValue.setRecord(row);
            for (int i = 0; i < types.length; i++) {
                columnValue.setfieldName(types[i].getName());
                appendData(i, columnValue);
            }
            rows++;
        }
        return rows;
    }

    private void initIter() {
        DeltaLog deltaLog = DeltaLog.forTable(conf, path);
        iter = deltaLog.snapshot().open();
    }

    private void parseRequiredTypes() {
        DeltaLog deltaLog = DeltaLog.forTable(conf, path);
        StructType schema = deltaLog.snapshot().getMetadata().getSchema();
        assert schema != null;
        ColumnType[] columnTypes = new ColumnType[schema.length()];
        int i = 0;
        for (StructField field : schema.getFields()) {
            String columnName = field.getName();
            DataType dataType = field.getDataType();
            columnTypes[i] = ColumnType.parseType(columnName, dataType.toString());
            i++;
        }
        super.types = columnTypes;
    }
}
