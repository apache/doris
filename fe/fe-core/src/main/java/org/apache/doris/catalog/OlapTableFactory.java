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

import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.mtmv.MTMVPartitionInfo;
import org.apache.doris.mtmv.MTMVRefreshInfo;
import org.apache.doris.mtmv.MTMVRelation;
import org.apache.doris.nereids.trees.plans.commands.info.CreateMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Map;

public class OlapTableFactory {

    public static class BuildParams {
        public long tableId;
        public String tableName;
        public boolean isTemporary;
        public List<Column> schema;
        public KeysType keysType;
        public PartitionInfo partitionInfo;
        public DistributionInfo distributionInfo;
    }

    public static class OlapTableParams extends BuildParams {
        public TableIndexes indexes;

        public OlapTableParams(boolean isTemporary) {
            this.isTemporary = isTemporary;
        }
    }

    public static class MTMVParams extends BuildParams {
        public MTMVRefreshInfo refreshInfo;
        public String querySql;
        public Map<String, String> mvProperties;
        public MTMVPartitionInfo mvPartitionInfo;
        public MTMVRelation relation;
        public Map<String, String> sessionVariables;
    }

    private BuildParams params;


    public static TableType getTableType(CreateTableInfo createTableInfo) {
        if (createTableInfo instanceof CreateMTMVInfo) {
            return TableType.MATERIALIZED_VIEW;
        } else if (createTableInfo instanceof CreateTableInfo) {
            return TableType.OLAP;
        } else {
            throw new IllegalArgumentException("Invalid DDL statement: " + createTableInfo.toSql());
        }
    }

    public OlapTableFactory init(TableType type, boolean isTemporary) {
        if (type == TableType.OLAP) {
            params = new OlapTableParams(isTemporary);
        } else {
            params = new MTMVParams();
        }
        return this;
    }

    public Table build() {
        Preconditions.checkNotNull(params, "The factory isn't initialized.");

        if (params instanceof OlapTableParams) {
            OlapTableParams olapTableParams = (OlapTableParams) params;
            return new OlapTable(
                    olapTableParams.tableId,
                    olapTableParams.tableName,
                    olapTableParams.isTemporary,
                    olapTableParams.schema,
                    olapTableParams.keysType,
                    olapTableParams.partitionInfo,
                    olapTableParams.distributionInfo,
                    olapTableParams.indexes
            );
        } else {
            MTMVParams mtmvParams = (MTMVParams) params;
            return new MTMV(mtmvParams);
        }
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

    public OlapTableFactory withQuerySql(String querySql) {
        Preconditions.checkState(params instanceof MTMVParams, "Invalid argument for "
                + params.getClass().getSimpleName());
        MTMVParams mtmvParams = (MTMVParams) params;
        mtmvParams.querySql = querySql;
        return this;
    }

    public OlapTableFactory withMvProperties(Map<String, String> mvProperties) {
        Preconditions.checkState(params instanceof MTMVParams, "Invalid argument for "
                + params.getClass().getSimpleName());
        MTMVParams mtmvParams = (MTMVParams) params;
        mtmvParams.mvProperties = mvProperties;
        return this;
    }

    private OlapTableFactory withRefreshInfo(MTMVRefreshInfo refreshInfo) {
        Preconditions.checkState(params instanceof MTMVParams, "Invalid argument for "
                + params.getClass().getSimpleName());
        MTMVParams mtmvParams = (MTMVParams) params;
        mtmvParams.refreshInfo = refreshInfo;
        return this;
    }

    private OlapTableFactory withMvPartitionInfo(MTMVPartitionInfo mvPartitionInfo) {
        Preconditions.checkState(params instanceof MTMVParams, "Invalid argument for "
                + params.getClass().getSimpleName());
        MTMVParams mtmvParams = (MTMVParams) params;
        mtmvParams.mvPartitionInfo = mvPartitionInfo;
        return this;
    }

    private OlapTableFactory withMvRelation(MTMVRelation relation) {
        Preconditions.checkState(params instanceof MTMVParams, "Invalid argument for "
                + params.getClass().getSimpleName());
        MTMVParams mtmvParams = (MTMVParams) params;
        mtmvParams.relation = relation;
        return this;
    }

    private OlapTableFactory withSessionVariables(Map<String, String> sessionVariables) {
        Preconditions.checkState(params instanceof MTMVParams, "Invalid argument for "
                + params.getClass().getSimpleName());
        MTMVParams mtmvParams = (MTMVParams) params;
        mtmvParams.sessionVariables = sessionVariables;
        return this;
    }

    public OlapTableFactory withExtraParams(CreateTableInfo createTableInfo) {
        boolean isMaterializedView = createTableInfo instanceof CreateMTMVInfo;
        if (!isMaterializedView) {
            return withIndexes(new TableIndexes(createTableInfo.getIndexes()));
        } else {
            CreateMTMVInfo createMTMVInfo = (CreateMTMVInfo) createTableInfo;
            return withRefreshInfo(createMTMVInfo.getRefreshInfo())
                .withQuerySql(createMTMVInfo.getQuerySql())
                .withMvProperties(createMTMVInfo.getMvProperties())
                .withMvPartitionInfo(createMTMVInfo.getMvPartitionInfo())
                .withSessionVariables(createMTMVInfo.getSessionVariables())
                .withMvRelation(createMTMVInfo.getRelation());
        }
    }
}
