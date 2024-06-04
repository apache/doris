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

package org.apache.doris.tablefunction;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TMaterializedViewsMetadataParams;
import org.apache.doris.thrift.TMetaScanRange;
import org.apache.doris.thrift.TMetadataType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * The Implement of table valued function
 * mv_infos("database" = "db1").
 */
public class MvInfosTableValuedFunction extends MetadataTableValuedFunction {
    private static final Logger LOG = LogManager.getLogger(MvInfosTableValuedFunction.class);

    public static final String NAME = "mv_infos";
    private static final String DB = "database";

    private static final ImmutableSet<String> PROPERTIES_SET = ImmutableSet.of(DB);

    private static final ImmutableList<Column> SCHEMA = ImmutableList.of(
            new Column("Id", ScalarType.createType(PrimitiveType.BIGINT)),
            new Column("Name", ScalarType.createStringType()),
            new Column("JobName", ScalarType.createStringType()),
            new Column("State", ScalarType.createStringType()),
            new Column("SchemaChangeDetail", ScalarType.createStringType()),
            new Column("RefreshState", ScalarType.createStringType()),
            new Column("RefreshInfo", ScalarType.createStringType()),
            new Column("QuerySql", ScalarType.createStringType()),
            new Column("EnvInfo", ScalarType.createStringType()),
            new Column("MvProperties", ScalarType.createStringType()),
            new Column("MvPartitionInfo", ScalarType.createStringType()),
            new Column("SyncWithBaseTables", ScalarType.createType(PrimitiveType.BOOLEAN)));

    private static final ImmutableMap<String, Integer> COLUMN_TO_INDEX;

    static {
        ImmutableMap.Builder<String, Integer> builder = new ImmutableMap.Builder();
        for (int i = 0; i < SCHEMA.size(); i++) {
            builder.put(SCHEMA.get(i).getName().toLowerCase(), i);
        }
        COLUMN_TO_INDEX = builder.build();
    }

    public static Integer getColumnIndexFromColumnName(String columnName) {
        return COLUMN_TO_INDEX.get(columnName.toLowerCase());
    }

    private final String databaseName;

    public MvInfosTableValuedFunction(Map<String, String> params) throws AnalysisException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("MvInfosTableValuedFunction() start");
        }
        Map<String, String> validParams = Maps.newHashMap();
        for (String key : params.keySet()) {
            if (!PROPERTIES_SET.contains(key.toLowerCase())) {
                throw new AnalysisException("'" + key + "' is invalid property");
            }
            // check ctl, db, tbl
            validParams.put(key.toLowerCase(), params.get(key));
        }
        String dbName = validParams.get(DB);
        if (dbName == null) {
            throw new AnalysisException("Invalid async materialized view metadata query");
        }
        this.databaseName = dbName;
        if (LOG.isDebugEnabled()) {
            LOG.debug("MvInfosTableValuedFunction() end");
        }
    }

    @Override
    public TMetadataType getMetadataType() {
        return TMetadataType.MATERIALIZED_VIEWS;
    }

    @Override
    public TMetaScanRange getMetaScanRange() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("getMetaScanRange() start");
        }
        TMetaScanRange metaScanRange = new TMetaScanRange();
        metaScanRange.setMetadataType(TMetadataType.MATERIALIZED_VIEWS);
        TMaterializedViewsMetadataParams mtmvParam = new TMaterializedViewsMetadataParams();
        mtmvParam.setDatabase(databaseName);
        mtmvParam.setCurrentUserIdent(ConnectContext.get().getCurrentUserIdentity().toThrift());
        metaScanRange.setMaterializedViewsParams(mtmvParam);
        if (LOG.isDebugEnabled()) {
            LOG.debug("getMetaScanRange() end");
        }
        return metaScanRange;
    }

    @Override
    public String getTableName() {
        return "MvInfosTableValuedFunction";
    }

    @Override
    public List<Column> getTableColumns() throws AnalysisException {
        return SCHEMA;
    }
}
