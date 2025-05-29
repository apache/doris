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

package org.apache.doris.tablefunction.iceberg;

import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TIcebergQueryType;

import com.google.common.collect.ImmutableList;

import java.util.List;

class IcebergEntriesTableValuedFunction extends IcebergTableValuedFunction {
    private static final ImmutableList<Column> SCHEMA = ImmutableList.of(
            new Column("status", ScalarType.INT),
            new Column("snapshot_id", ScalarType.BIGINT),
            new Column("data_file", ScalarType.STRING),
            new Column("file_sequence_number", ScalarType.BIGINT),
            new Column("data_sequence_number", ScalarType.BIGINT),
            new Column("partition", ScalarType.STRING),
            new Column("file_path", ScalarType.STRING),
            new Column("file_format", ScalarType.STRING),
            new Column("record_count", ScalarType.BIGINT),
            new Column("file_size_in_bytes", ScalarType.BIGINT)
    );

    public IcebergEntriesTableValuedFunction(TableName icebergTableName) throws AnalysisException {
        super(icebergTableName, TIcebergQueryType.ENTRIES);
    }

    @Override
    protected List<Column> getSchema() {
        return SCHEMA;
    }
}
