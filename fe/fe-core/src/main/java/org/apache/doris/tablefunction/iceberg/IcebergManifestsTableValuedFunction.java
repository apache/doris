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

class IcebergManifestsTableValuedFunction extends IcebergTableValuedFunction {
    private static final ImmutableList<Column> SCHEMA = ImmutableList.of(
            new Column("path", ScalarType.STRING),
            new Column("length", ScalarType.BIGINT),
            new Column("partition_spec_id", ScalarType.INT),
            new Column("added_snapshot_id", ScalarType.BIGINT),
            new Column("added_data_files_count", ScalarType.INT),
            new Column("existing_data_files_count", ScalarType.INT),
            new Column("deleted_data_files_count", ScalarType.INT),
            new Column("partition_summaries", ScalarType.STRING)
    );

    public IcebergManifestsTableValuedFunction(TableName icebergTableName) throws AnalysisException {
        super(icebergTableName, TIcebergQueryType.MANIFESTS);
    }

    @Override
    protected List<Column> getSchema() {
        return SCHEMA;
    }
}
