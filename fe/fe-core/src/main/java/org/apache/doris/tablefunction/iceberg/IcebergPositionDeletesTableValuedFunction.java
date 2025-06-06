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
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.iceberg.share.ManifestFileBean;
import org.apache.doris.thrift.TIcebergQueryType;

import com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.SerializationUtil;

import java.util.List;
import java.util.stream.Collectors;

class IcebergPositionDeletesTableValuedFunction extends IcebergTableValuedFunction {
    // TODO: support partition
    private static final ImmutableList<Column> SCHEMA = ImmutableList.of(
            new Column("file_path", ScalarType.STRING, true, "Path of a file in which a deleted row is stored"),
            new Column("pos", ScalarType.BIGINT, true, "Ordinal position of a deleted row in the data file"),
            new Column("row", new StructType(
                    new StructField("id", ScalarType.INT),
                    new StructField("name", ScalarType.STRING)),
                    true, "Deleted row values"),
            new Column("spec_id", ScalarType.INT, true, "Spec ID used to track the file containing a row"),
            new Column("delete_file_path", ScalarType.STRING, true, "Path of the file in which a row is stored"));

    public IcebergPositionDeletesTableValuedFunction(TableName icebergTableName) throws AnalysisException {
        super(icebergTableName, TIcebergQueryType.POSITION_DELETES);
    }

    @Override
    protected List<String> getSplits() {
        if (table.currentSnapshot() == null) {
            return List.of();
        }
        return table.currentSnapshot().deleteManifests(table.io()).stream()
                .map(ManifestFileBean::fromManifest).map(SerializationUtil::serializeToBase64)
                .collect(Collectors.toList());
    }

    @Override
    protected List<Column> getSchema() {
        return SCHEMA;
    }
}
