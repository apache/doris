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
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TMetaScanRange;
import org.apache.doris.thrift.TMetadataType;
import org.apache.doris.thrift.TPartitionsMetadataParams;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * The Implement of table valued function
 * partitions("database" = "db1","table" = "table1").
 */
public class PartitionsTableValuedFunction extends MetadataTableValuedFunction {
    private static final Logger LOG = LogManager.getLogger(PartitionsTableValuedFunction.class);

    public static final String NAME = "partitions";
    private static final String DB = "database";
    private static final String TABLE = "table";

    private static final ImmutableSet<String> PROPERTIES_SET = ImmutableSet.of(DB, TABLE);

    private static final ImmutableList<Column> SCHEMA = ImmutableList.of(
            new Column("PartitionId", ScalarType.createType(PrimitiveType.BIGINT)),
            new Column("PartitionName", ScalarType.createStringType()),
            new Column("VisibleVersion", ScalarType.createType(PrimitiveType.BIGINT)),
            new Column("VisibleVersionTime", ScalarType.createStringType()),
            new Column("State", ScalarType.createStringType()),
            new Column("PartitionKey", ScalarType.createStringType()),
            new Column("Range", ScalarType.createStringType()),
            new Column("DistributionKey", ScalarType.createStringType()),
            new Column("Buckets", ScalarType.createType(PrimitiveType.INT)),
            new Column("ReplicationNum", ScalarType.createType(PrimitiveType.INT)),
            new Column("StorageMedium", ScalarType.createStringType()),
            new Column("CooldownTime", ScalarType.createStringType()),
            new Column("RemoteStoragePolicy", ScalarType.createStringType()),
            new Column("LastConsistencyCheckTime", ScalarType.createStringType()),
            new Column("DataSize", ScalarType.createStringType()),
            new Column("IsInMemory", ScalarType.createType(PrimitiveType.BOOLEAN)),
            new Column("ReplicaAllocation", ScalarType.createStringType()),
            new Column("IsMutable", ScalarType.createType(PrimitiveType.BOOLEAN)),
            new Column("SyncWithBaseTables", ScalarType.createType(PrimitiveType.BOOLEAN)),
            new Column("UnsyncTables", ScalarType.createStringType()));

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
    private final String tableName;

    public PartitionsTableValuedFunction(Map<String, String> params) throws AnalysisException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("PartitionsTableValuedFunction() start");
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
        String tableName = validParams.get(TABLE);
        if (StringUtils.isEmpty(dbName) || StringUtils.isEmpty(tableName)) {
            throw new AnalysisException("Invalid partitions metadata query");
        }
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, dbName,
                        tableName, PrivPredicate.SHOW)) {
            String message = ErrorCode.ERR_TABLEACCESS_DENIED_ERROR.formatErrorMsg("SHOW PARTITIONS",
                    ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                    dbName + ": " + tableName);
            throw new AnalysisException(message);
        }
        this.databaseName = dbName;
        this.tableName = tableName;
        if (LOG.isDebugEnabled()) {
            LOG.debug("PartitionsTableValuedFunction() end");
        }
    }

    @Override
    public TMetadataType getMetadataType() {
        return TMetadataType.PARTITIONS;
    }

    @Override
    public TMetaScanRange getMetaScanRange() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("getMetaScanRange() start");
        }
        TMetaScanRange metaScanRange = new TMetaScanRange();
        metaScanRange.setMetadataType(TMetadataType.PARTITIONS);
        TPartitionsMetadataParams partitionParam = new TPartitionsMetadataParams();
        partitionParam.setDatabase(databaseName);
        partitionParam.setTable(tableName);
        metaScanRange.setPartitionsParams(partitionParam);
        if (LOG.isDebugEnabled()) {
            LOG.debug("getMetaScanRange() end");
        }
        return metaScanRange;
    }

    @Override
    public String getTableName() {
        return "PartitionsTableValuedFunction";
    }

    @Override
    public List<Column> getTableColumns() throws AnalysisException {
        return SCHEMA;
    }
}
