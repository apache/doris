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

class IcebergPartitionsTableValuedFunction extends IcebergTableValuedFunction {
    private static final ImmutableList<Column> SCHEMA = ImmutableList.of(
            new Column("partition", ScalarType.STRING),
            new Column("spec_id", ScalarType.INT),
            new Column("record_count", ScalarType.BIGINT),
            new Column("file_count", ScalarType.BIGINT),
            new Column("total_data_file_size_in_bytes", ScalarType.BIGINT),
            new Column("position_delete_record_count", ScalarType.BIGINT),
            new Column("position_delete_file_count", ScalarType.BIGINT),
            new Column("equality_delete_record_count", ScalarType.BIGINT),
            new Column("equality_delete_file_count", ScalarType.BIGINT),
            new Column("last_updated_at", ScalarType.BIGINT));

    public IcebergPartitionsTableValuedFunction(TableName icebergTableName) throws AnalysisException {
        super(icebergTableName, TIcebergQueryType.PARTITIONS);
    }

    @Override
    protected List<Column> getSchema() {
        return SCHEMA;
    }
}
