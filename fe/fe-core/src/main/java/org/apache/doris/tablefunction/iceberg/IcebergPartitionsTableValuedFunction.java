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
import org.apache.doris.datasource.iceberg.share.ManifestFileBean;
import org.apache.doris.thrift.TIcebergQueryType;

import com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.SerializationUtil;

import java.util.List;
import java.util.stream.Collectors;

class IcebergPartitionsTableValuedFunction extends IcebergTableValuedFunction {
    // TODO: support partitions and add comment
    private static final ImmutableList<Column> SCHEMA = ImmutableList.of(
            new Column("partition", ScalarType.STRING, true),
            new Column("partition_spec_id", ScalarType.INT, true),
            new Column("spec_id", ScalarType.INT, true),
            new Column("record_count", ScalarType.BIGINT, true),
            new Column("file_count", ScalarType.INT, true),
            new Column("total_data_file_size_in_bytes", ScalarType.BIGINT, true),
            new Column("position_delete_record_count", ScalarType.BIGINT, true),
            new Column("position_delete_file_count", ScalarType.INT, true),
            new Column("equality_delete_record_count", ScalarType.BIGINT, true),
            new Column("equality_delete_file_count", ScalarType.INT, true),
            new Column("last_updated_at", ScalarType.DATETIME, true),
            new Column("last_updated_snapshot_id", ScalarType.BIGINT, true));

    public IcebergPartitionsTableValuedFunction(TableName icebergTableName) throws AnalysisException {
        super(icebergTableName, TIcebergQueryType.PARTITIONS);
    }

    @Override
    protected List<String> getSplits() {
        if (table.currentSnapshot() == null) {
            return List.of();
        }
        return table.currentSnapshot().allManifests(table.io()).stream()
                .map(ManifestFileBean::fromManifest).map(SerializationUtil::serializeToBase64)
                .collect(Collectors.toList());
    }

    @Override
    protected List<Column> getSchema() {
        return SCHEMA;
    }
}
