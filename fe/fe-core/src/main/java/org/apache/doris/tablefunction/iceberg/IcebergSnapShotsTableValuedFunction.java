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
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TIcebergQueryType;

import com.google.common.collect.ImmutableList;

import java.util.List;

class IcebergSnapShotsTableValuedFunction extends IcebergTableValuedFunction {
    private static final ImmutableList<Column> SCHEMA = ImmutableList.of(
            new Column("committed_at", ScalarType.DATETIMEV2),
            new Column("snapshot_id", ScalarType.BIGINT),
            new Column("parent_id", ScalarType.BIGINT),
            new Column("operation", ScalarType.STRING),
            new Column("manifest_list", ScalarType.STRING),
            new Column("summary", new MapType(ScalarType.STRING, ScalarType.STRING)));

    public IcebergSnapShotsTableValuedFunction(TableName icebergTableName) throws AnalysisException {
        super(icebergTableName, TIcebergQueryType.SNAPSHOTS);
    }

    @Override
    protected List<Column> getSchema() {
        return SCHEMA;
    }
}
