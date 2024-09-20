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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ShowCacheHotSpotStmt extends ShowStmt implements NotFallbackInParser {
    public static final ShowResultSetMetaData[] RESULT_SET_META_DATAS = {
        ShowResultSetMetaData.builder()
            .addColumn(new Column("cluster_id", ScalarType.createType(PrimitiveType.BIGINT)))
            .addColumn(new Column("cluster_name", ScalarType.createVarchar(128)))
            .addColumn(new Column("table_id", ScalarType.createType(PrimitiveType.BIGINT)))
            .addColumn(new Column("table_name", ScalarType.createVarchar(128)))
            .build(),
        ShowResultSetMetaData.builder()
            .addColumn(new Column("table_id", ScalarType.createType(PrimitiveType.BIGINT)))
            .addColumn(new Column("table_name", ScalarType.createVarchar(128)))
            .addColumn(new Column("partition_id", ScalarType.createType(PrimitiveType.BIGINT)))
            .addColumn(new Column("partition_name", ScalarType.createVarchar(65535)))
            .build(),
        ShowResultSetMetaData.builder()
            .addColumn(new Column("partition_id", ScalarType.createType(PrimitiveType.BIGINT)))
            .addColumn(new Column("partition_name", ScalarType.createVarchar(65535)))
            .build()
    };
    private int metaDataPos;
    private static final Logger LOG = LogManager.getLogger(ShowCacheHotSpotStmt.class);
    private static final TableName TABLE_NAME = new TableName(InternalCatalog.INTERNAL_CATALOG_NAME,
            FeConstants.INTERNAL_DB_NAME, FeConstants.INTERNAL_FILE_CACHE_HOTSPOT_TABLE_NAME);
    private final String tablePath;
    private List<String> whereExprVariables = Arrays.asList("cluster_name", "table_name");
    private List<String> whereExprValues = new ArrayList<>();
    private List<String> whereExpr = new ArrayList<>();
    private SelectStmt selectStmt;

    public ShowCacheHotSpotStmt(String url) {
        tablePath = url;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        if (!Config.isCloudMode()) {
            throw new UserException("The sql is illegal in disk mode ");
        }
        super.analyze(analyzer);
        if (Strings.isNullOrEmpty(tablePath)) {
            return;
        }

        if (!tablePath.startsWith("/")) {
            throw new AnalysisException("Path must starts with '/'");
        }
        String[] parts = tablePath.split("/");
        if (parts.length > 3) {
            throw new AnalysisException("Path must in format '/cluster/db.table/'");
        }
        if (parts.length >= 2) {
            if (!((CloudSystemInfoService) Env.getCurrentSystemInfo()).containClusterName(parts[1])) {
                throw new AnalysisException("The cluster " + parts[1] + " doesn't exist");
            }
            if (parts.length == 3) {
                String[] dbAndTable = parts[2].split("\\.");
                if (dbAndTable.length != 2) {
                    throw new AnalysisException("The tableName must in format 'dbName.tableName'");
                }
                Database db = Env.getCurrentInternalCatalog().getDbNullable("default_cluster:" + dbAndTable[0]);
                if (db == null) {
                    throw new AnalysisException("The db " + dbAndTable[0] + " doesn't exist");
                }
                if (!db.isTableExist(dbAndTable[1])) {
                    throw new AnalysisException("The table " + dbAndTable[1] + " doesn't exist");
                }
            }
        }
        whereExprValues = Arrays.asList(parts);
        for (int i = 1; i < whereExprValues.size(); i++) {
            whereExpr.add(String.format("%s = '%s' ", whereExprVariables.get(i - 1), whereExprValues.get(i)));
        }
        metaDataPos = whereExpr.size();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return RESULT_SET_META_DATAS[metaDataPos];
    }

    private String generateQueryString() {
        StringBuilder query = null;
        if (metaDataPos == 0) {
            StringBuilder q1 = new StringBuilder("with t1 as (select cluster_id, "
                            + "cluster_name, table_id, table_name, insert_day, "
                            + "sum(query_per_day) as query_per_day_total, "
                            + "sum(query_per_week) as query_per_week_total "
                            + "FROM " + TABLE_NAME.toString()
                            + " group by cluster_id, cluster_name, table_id, table_name, insert_day) ");
            StringBuilder q2 = new StringBuilder("select cluster_id, cluster_name, "
                            + "table_id, table_name as hot_table_name from (select row_number() "
                            + "over (partition by cluster_id order by insert_day desc, "
                            + "query_per_day_total desc, query_per_week_total desc) as dr2, "
                            + "* from t1) t2 where dr2 = 1;");
            query = q1.append(q2);
        } else if (metaDataPos == 1) {
            StringBuilder q1 = new StringBuilder("with t1 as (select cluster_id, "
                            + "cluster_name, table_id, table_name, partition_id, "
                            + "partition_name, insert_day, sum(query_per_day) as query_per_day_total, "
                            + "sum(query_per_week) as query_per_week_total "
                            + "FROM " + TABLE_NAME.toString()
                            + " where " +  whereExpr.get(0)
                            + "group by cluster_id, cluster_name, table_id, "
                            + "table_name, partition_id, partition_name, insert_day)");
            StringBuilder q2 = new StringBuilder("select table_id, table_name, "
                            + "partition_id, partition_name as hot_partition_name from (select row_number() "
                            + "over (partition by cluster_id, table_id order by insert_day desc, "
                            + "query_per_day_total desc, query_per_week_total desc) as dr2, "
                            + "* from t1) t2 where dr2 = 1;");
            query = q1.append(q2);
        } else if (metaDataPos == 2) {
            query = new StringBuilder("select partition_id, partition_name "
            + "FROM " + TABLE_NAME.toString()
            + " where " +  whereExpr.get(0)
            + " and " + whereExpr.get(1)
            + "group by cluster_id, cluster_name, table_id, "
            + "table_name, partition_id, partition_name;");
        }
        Preconditions.checkState(query != null);
        return query.toString();
    }

    @Override
    public SelectStmt toSelectStmt(Analyzer analyzer) throws AnalysisException {
        if (selectStmt != null) {
            return selectStmt;
        }
        try {
            analyze(analyzer);
        } catch (UserException e) {
            throw new AnalysisException(e.toString(), e);
        }
        String query = generateQueryString();
        LOG.debug("show cache hot spot stmt is {}", query);
        SqlScanner input = new SqlScanner(new StringReader(query));
        SqlParser parser = new SqlParser(input);
        try {
            selectStmt = (SelectStmt ) ((List<StatementBase> ) parser.parse().value).get(0);
        } catch (Exception e) {
            throw new AnalysisException(e.toString(), e);
        }
        return selectStmt;
    }
}
