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
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.ArrayList;
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

    public static String SNAPSHOTS = "snapshots";

    private static final ImmutableSet<String> PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(TABLE)
            .add(QUERY_TYPE)
            .build();

    private static final ImmutableSet<String> SUPPORTED_QUERY_TYPES = new ImmutableSet.Builder<String>()
            .add(SNAPSHOTS)
            .build();

    private final String queryType;
    private final TableName tableName;

    public IcebergTableValuedFunction(Map<String, String> params) throws AnalysisException {
        super(MetaType.ICEBERG);
        Map<String, String> validParams = Maps.newHashMap();
        for (String key : params.keySet()) {
            if (!PROPERTIES_SET.contains(key.toLowerCase())) {
                throw new AnalysisException(key + " is invalid property");
            }
            // check ctl db tbl
            validParams.put(key.toLowerCase(), params.get(key));
        }
        String tableName = validParams.get(TABLE);
        String queryType = validParams.get(QUERY_TYPE);
        if (tableName == null || queryType == null) {
            throw new AnalysisException("Invalid iceberg metadata query");
        }
        if (SUPPORTED_QUERY_TYPES.contains(queryType)) {
            throw new AnalysisException("Unsupported iceberg metadata query type: " + queryType);
        }
        String[] names = tableName.split("\\.");
        if (names.length != 3) {
            throw new AnalysisException("The iceberg table name contains the catalogName, databaseName, and tableName");
        }
        this.tableName = new TableName(names[0], names[1], names[2]);
        this.queryType = queryType;
    }

    public TableName getIcebergTableName() {
        return tableName;
    }

    public String getMetaQueryType() {
        return queryType;
    }

    @Override
    public List<Column> getTableColumns() throws AnalysisException {
        List<Column> resColumns = new ArrayList<>();
        if (queryType.equals(SNAPSHOTS)) {
            resColumns.add(new Column("committed_at", PrimitiveType.STRING, false));
            resColumns.add(new Column("snapshot_id", PrimitiveType.BIGINT, false));
            resColumns.add(new Column("parent_id", PrimitiveType.BIGINT, false));
            resColumns.add(new Column("operation", PrimitiveType.STRING, false));
            // todo: compress manifest_list string
            resColumns.add(new Column("manifest_list", PrimitiveType.STRING, false));
            // resColumns.add(new Column("summary", PrimitiveType.MAP, false));
        }
        return resColumns;
    }
}
