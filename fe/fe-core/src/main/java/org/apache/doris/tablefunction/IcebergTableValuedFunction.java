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

import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TIcebergMetadataParams;
import org.apache.doris.thrift.TIcebergQueryType;
import org.apache.doris.thrift.TMetaScanRange;
import org.apache.doris.thrift.TMetadataType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * The Implement of table valued function
 * iceberg_meta("table" = "ctl.db.tbl", "query_type" = "snapshots").
 */
public class IcebergTableValuedFunction extends MetadataTableValuedFunction {

    public static final String NAME = "iceberg_meta";
    private static final String TABLE = "table";
    private static final String QUERY_TYPE = "query_type";

    private static final ImmutableSet<String> PROPERTIES_SET = ImmutableSet.of(TABLE, QUERY_TYPE);

    private static final ImmutableList<Column> SCHEMA_SNAPSHOT = ImmutableList.of(
            new Column("committed_at", PrimitiveType.DATETIMEV2, false),
            new Column("snapshot_id", PrimitiveType.BIGINT, false),
            new Column("parent_id", PrimitiveType.BIGINT, false),
            new Column("operation", PrimitiveType.STRING, false),
            // todo: compress manifest_list string
            new Column("manifest_list", PrimitiveType.STRING, false),
            new Column("summary", PrimitiveType.STRING, false));


    private static final ImmutableMap<String, Integer> COLUMN_TO_INDEX;

    static {
        ImmutableMap.Builder<String, Integer> builder = new ImmutableMap.Builder();
        for (int i = 0; i < SCHEMA_SNAPSHOT.size(); i++) {
            builder.put(SCHEMA_SNAPSHOT.get(i).getName().toLowerCase(), i);
        }
        COLUMN_TO_INDEX = builder.build();
    }

    public static Integer getColumnIndexFromColumnName(String columnName) {
        return COLUMN_TO_INDEX.get(columnName.toLowerCase());
    }

    private TIcebergQueryType queryType;

    // here tableName represents the name of a table in Iceberg.
    private final TableName icebergTableName;

    public IcebergTableValuedFunction(Map<String, String> params) throws AnalysisException {
        Map<String, String> validParams = Maps.newHashMap();
        for (String key : params.keySet()) {
            if (!PROPERTIES_SET.contains(key.toLowerCase())) {
                throw new AnalysisException("'" + key + "' is invalid property");
            }
            // check ctl, db, tbl
            validParams.put(key.toLowerCase(), params.get(key));
        }
        String tableName = validParams.get(TABLE);
        String queryTypeString = validParams.get(QUERY_TYPE);
        if (tableName == null || queryTypeString == null) {
            throw new AnalysisException("Invalid iceberg metadata query");
        }
        String[] names = tableName.split("\\.");
        if (names.length != 3) {
            throw new AnalysisException("The iceberg table name contains the catalogName, databaseName, and tableName");
        }
        this.icebergTableName = new TableName(names[0], names[1], names[2]);
        // check auth
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), this.icebergTableName, PrivPredicate.SELECT)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SELECT",
                    ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                    this.icebergTableName.getDb() + ": " + this.icebergTableName.getTbl());
        }
        try {
            this.queryType = TIcebergQueryType.valueOf(queryTypeString.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new AnalysisException("Unsupported iceberg metadata query type: " + queryType);
        }
    }

    public TIcebergQueryType getIcebergQueryType() {
        return queryType;
    }

    @Override
    public TMetadataType getMetadataType() {
        return TMetadataType.ICEBERG;
    }

    @Override
    public TMetaScanRange getMetaScanRange() {
        TMetaScanRange metaScanRange = new TMetaScanRange();
        metaScanRange.setMetadataType(TMetadataType.ICEBERG);
        // set iceberg metadata params
        TIcebergMetadataParams icebergMetadataParams = new TIcebergMetadataParams();
        icebergMetadataParams.setIcebergQueryType(queryType);
        icebergMetadataParams.setCatalog(icebergTableName.getCtl());
        icebergMetadataParams.setDatabase(icebergTableName.getDb());
        icebergMetadataParams.setTable(icebergTableName.getTbl());
        metaScanRange.setIcebergParams(icebergMetadataParams);
        return metaScanRange;
    }

    @Override
    public String getTableName() {
        return "IcebergMetadataTableValuedFunction";
    }

    /**
     * The tvf can register columns of metadata table
     * The data is provided by getIcebergMetadataTable in FrontendService
     *
     * @return metadata columns
     * @see org.apache.doris.service.FrontendServiceImpl
     */
    @Override
    public List<Column> getTableColumns() {
        if (queryType == TIcebergQueryType.SNAPSHOTS) {
            return SCHEMA_SNAPSHOT;
        }
        return Lists.newArrayList();
    }
}
