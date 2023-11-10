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

package org.apache.doris.catalog;

import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.DdlStmt;
import org.apache.doris.catalog.TableIf.TableType;

import com.google.common.base.Preconditions;

import java.util.List;

public class OlapTableFactory {

    public static class BuildParams {
        public long tableId;
        public String tableName;
        public List<Column> schema;
        public KeysType keysType;
        public PartitionInfo partitionInfo;
        public DistributionInfo distributionInfo;
    }

    public static class OlapTableParams extends BuildParams {
        public TableIndexes indexes;
    }

    private BuildParams params;

    public static TableType getTableType(DdlStmt stmt) {
        if (stmt instanceof CreateTableStmt) {
            return TableType.OLAP;
        } else {
            throw new IllegalArgumentException("Invalid DDL statement: " + stmt.toSql());
        }
    }

    public OlapTableFactory init(TableType type) {
        params = new OlapTableParams();
        return this;
    }

    public Table build() {
        Preconditions.checkNotNull(params, "The factory isn't initialized.");
        OlapTableParams olapTableParams = (OlapTableParams) params;
        return new OlapTable(
                olapTableParams.tableId,
                olapTableParams.tableName,
                olapTableParams.schema,
                olapTableParams.keysType,
                olapTableParams.partitionInfo,
                olapTableParams.distributionInfo,
                olapTableParams.indexes
        );
    }

    public OlapTableFactory withTableId(long tableId) {
        params.tableId = tableId;
        return this;
    }

    public OlapTableFactory withTableName(String tableName) {
        params.tableName = tableName;
        return this;
    }

    public OlapTableFactory withSchema(List<Column> schema) {
        params.schema = schema;
        return this;
    }

    public OlapTableFactory withKeysType(KeysType keysType) {
        params.keysType = keysType;
        return this;
    }

    public OlapTableFactory withPartitionInfo(PartitionInfo partitionInfo) {
        params.partitionInfo = partitionInfo;
        return this;
    }

    public OlapTableFactory withDistributionInfo(DistributionInfo distributionInfo) {
        params.distributionInfo = distributionInfo;
        return this;
    }

    public OlapTableFactory withIndexes(TableIndexes indexes) {
        Preconditions.checkState(params instanceof OlapTableParams, "Invalid argument for "
                + params.getClass().getSimpleName());
        OlapTableParams olapTableParams = (OlapTableParams) params;
        olapTableParams.indexes = indexes;
        return this;
    }

    public OlapTableFactory withExtraParams(DdlStmt stmt) {
        CreateTableStmt createOlapTableStmt = (CreateTableStmt) stmt;
        return withIndexes(new TableIndexes(createOlapTableStmt.getIndexes()));
    }
}
