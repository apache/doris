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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TMetaScanRange;
import org.apache.doris.thrift.TMetadataType;
import org.apache.doris.thrift.TTabletsMetadataParams;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The Implement of table valued function
 * tablets("database" = "xxx", "table" = "xx", "partitions" = "p1")
 * tablets("database" = "xxx", "table" = "xx", "partitions" = "p1,...,pN")
 */
public class TabletsTableValuedFunction extends MetadataTableValuedFunction {

    public static final String NAME = "tablets";
    private static final String DB = "database";
    private static final String TABLE = "table";
    private static final String PARTITIONS = "partitions";
    private static final ImmutableList<Column> SCHEMA;
    private static final ImmutableList<String> TITLE_NAMES;
    private static final ImmutableMap<String, Integer> COLUMN_TO_INDEX;
    private static final ImmutableSet<String> PROPERTIES_SET = ImmutableSet.of(DB, TABLE, PARTITIONS);
    private final String dbName;
    private final String tableName;
    private final String partitions;

    static {
        ImmutableList.Builder<Column> columnBuilder = new ImmutableList.Builder<Column>()
                .add(new Column("TabletId", ScalarType.createStringType()))
                .add(new Column("ReplicaId", ScalarType.createStringType()))
                .add(new Column("BackendId", ScalarType.createStringType()))
                .add(new Column("SchemaHash", ScalarType.createStringType()))
                .add(new Column("Version", ScalarType.createStringType()))
                .add(new Column("LstSuccessVersion", ScalarType.createStringType()))
                .add(new Column("LstFailedVersion", ScalarType.createStringType()))
                .add(new Column("LstFailedTime", ScalarType.createStringType()))
                .add(new Column("LocalDataSize", ScalarType.createStringType()))
                .add(new Column("RemoteDataSize", ScalarType.createStringType()))
                .add(new Column("RowCount", ScalarType.createStringType()))
                .add(new Column("State", ScalarType.createStringType()))
                .add(new Column("LstConsistencyCheckTime", ScalarType.createStringType()))
                .add(new Column("CheckVersion", ScalarType.createStringType()))
                .add(new Column("VisibleVersionCount", ScalarType.createStringType()))
                .add(new Column("VersionCount", ScalarType.createStringType()))
                .add(new Column("QueryHits", ScalarType.createStringType()))
                .add(new Column("PathHash", ScalarType.createStringType()))
                .add(new Column("Path", ScalarType.createStringType()))
                .add(new Column("MetaUrl", ScalarType.createStringType()))
                .add(new Column("CompactionStatus", ScalarType.createStringType()))
                .add(new Column("CooldownReplicaId", ScalarType.createStringType()))
                .add(new Column("CooldownMetaId", ScalarType.createStringType()));
        if (Config.isCloudMode()) {
            columnBuilder.add(new Column("PrimaryBackendId", ScalarType.createStringType()));
        }
        SCHEMA = columnBuilder.build();
        ImmutableList.Builder<String> immutableListBuilder = ImmutableList.builder();
        ImmutableMap.Builder<String, Integer> builder = new ImmutableMap.Builder<>();
        for (int i = 0; i < SCHEMA.size(); i++) {
            String columnName = SCHEMA.get(i).getName();
            builder.put(columnName, i);
            immutableListBuilder.add(columnName);
        }
        COLUMN_TO_INDEX = builder.build();
        TITLE_NAMES = immutableListBuilder.build();
    }

    public TabletsTableValuedFunction(String dbName, String tableName, String partitions) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.partitions = partitions;
    }

    public static Integer getColumnIndexFromColumnName(String columnName) {
        return COLUMN_TO_INDEX.get(columnName.toLowerCase());
    }

    /**
     * create TabletsTableValuedFunction with properties
     */
    public static TabletsTableValuedFunction create(Map<String, String> params) throws AnalysisException {
        if (params == null || params.isEmpty()) {
            throw new AnalysisException("tablets table-valued-function params must be not null or empty");
        }
        Map<String, String> validParams = Maps.newHashMap();
        for (String key : params.keySet()) {
            if (!PROPERTIES_SET.contains(key.toLowerCase())) {
                throw new AnalysisException("'" + key + "' is invalid property");
            }
            // check db, tbl, partitions
            validParams.put(key.toLowerCase(), params.get(key));
        }
        String dbName = validParams.get(DB);
        String tableName = validParams.get(TABLE);
        String partitionNames = validParams.get(PARTITIONS);
        if (StringUtils.isEmpty(dbName) || StringUtils.isBlank(dbName)
                || StringUtils.isEmpty(tableName) || StringUtils.isBlank(tableName)) {
            throw new AnalysisException("dbName and tableName must not be null or empty");
        }
        if (!Env.getCurrentEnv().getAccessManager().checkDbPriv(ConnectContext.get(),
                InternalCatalog.INTERNAL_CATALOG_NAME, InfoSchemaDb.DATABASE_NAME, PrivPredicate.SELECT)) {
            String message = ErrorCode.ERR_DB_ACCESS_DENIED_ERROR.formatErrorMsg(
                    PrivPredicate.SELECT.getPrivs().toString(), InfoSchemaDb.DATABASE_NAME);
            throw new org.apache.doris.nereids.exceptions.AnalysisException(message);
        }
        return new TabletsTableValuedFunction(dbName, tableName, partitionNames);
    }

    @Override
    public TMetadataType getMetadataType() {
        return TMetadataType.TABLETS;
    }

    @Override
    public TMetaScanRange getMetaScanRange(List<String> requiredFields) {
        TMetaScanRange metaScanRange = new TMetaScanRange();
        metaScanRange.setMetadataType(TMetadataType.TABLETS);
        TTabletsMetadataParams tabletsMetadataParams = new TTabletsMetadataParams();
        tabletsMetadataParams.setDatabaseName(this.dbName);
        tabletsMetadataParams.setTableName(this.tableName);
        if (StringUtils.isNotEmpty(this.partitions) && StringUtils.isNotBlank(partitions)) {
            List<String> partitionNames = Arrays.stream(this.partitions.split(","))
                    .filter(p -> StringUtils.isNotEmpty(p.trim())).collect(Collectors.toList());
            tabletsMetadataParams.setPartitionNames(partitionNames);
        }
        return metaScanRange;
    }

    @Override
    public String getTableName() {
        return "TabletsTableValuedFunction";
    }

    @Override
    public List<Column> getTableColumns() throws AnalysisException {
        return SCHEMA;
    }

    /**
     * get title names for tablets tvf and show tablets command
     */
    public static ImmutableList<String> getTabletsTitleNames() {
        return TITLE_NAMES;
    }
}
