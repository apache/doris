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
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TMetaScanRange;
import org.apache.doris.thrift.TMetadataType;
import org.apache.doris.thrift.TPartitionValuesMetadataParams;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The Implement of table valued function
 * partition_values("catalog"="ctl1", "database" = "db1","table" = "table1").
 */
public class PartitionValuesTableValuedFunction extends MetadataTableValuedFunction {
    private static final Logger LOG = LogManager.getLogger(PartitionValuesTableValuedFunction.class);

    public static final String NAME = "partition_values";

    public static final String CATALOG = "catalog";
    public static final String DB = "database";
    public static final String TABLE = "table";

    private static final ImmutableSet<String> PROPERTIES_SET = ImmutableSet.of(CATALOG, DB, TABLE);

    private final String catalogName;
    private final String databaseName;
    private final String tableName;
    private TableIf table;
    private List<Column> schema;

    public PartitionValuesTableValuedFunction(Map<String, String> params) throws AnalysisException {
        Map<String, String> validParams = Maps.newHashMap();
        for (String key : params.keySet()) {
            if (!PROPERTIES_SET.contains(key.toLowerCase())) {
                throw new AnalysisException("'" + key + "' is invalid property");
            }
            // check ctl, db, tbl
            validParams.put(key.toLowerCase(), params.get(key));
        }
        String catalogName = validParams.get(CATALOG);
        String dbName = validParams.get(DB);
        String tableName = validParams.get(TABLE);
        if (StringUtils.isEmpty(catalogName) || StringUtils.isEmpty(dbName) || StringUtils.isEmpty(tableName)) {
            throw new AnalysisException("catalog, database and table are required");
        }
        this.table = analyzeAndGetTable(catalogName, dbName, tableName, true);
        this.catalogName = catalogName;
        this.databaseName = dbName;
        this.tableName = tableName;
        if (LOG.isDebugEnabled()) {
            LOG.debug("PartitionsTableValuedFunction() end");
        }
    }

    public static TableIf analyzeAndGetTable(String catalogName, String dbName, String tableName, boolean checkAuth) {
        if (checkAuth) {
            // This method will be called at 2 places:
            // One is when planing the query, which should check the privilege of the user.
            // the other is when BE call FE to fetch partition values, which should not check the privilege.
            if (!Env.getCurrentEnv().getAccessManager()
                    .checkTblPriv(ConnectContext.get(), catalogName, dbName,
                            tableName, PrivPredicate.SHOW)) {
                String message = ErrorCode.ERR_TABLEACCESS_DENIED_ERROR.formatErrorMsg("SHOW PARTITIONS",
                        ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                        catalogName + ": " + dbName + ": " + tableName);
                throw new AnalysisException(message);
            }
        }
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogName);
        if (catalog == null) {
            throw new AnalysisException("can not find catalog: " + catalogName);
        }
        // disallow unsupported catalog
        if (!(catalog.isInternalCatalog() || catalog instanceof HMSExternalCatalog
                || catalog instanceof MaxComputeExternalCatalog)) {
            throw new AnalysisException(String.format("Catalog of type '%s' is not allowed in ShowPartitionsStmt",
                    catalog.getType()));
        }

        Optional<DatabaseIf> db = catalog.getDb(dbName);
        if (!db.isPresent()) {
            throw new AnalysisException("can not find database: " + dbName);
        }
        TableIf table;
        try {
            table = db.get().getTableOrMetaException(tableName, TableType.OLAP,
                    TableType.HMS_EXTERNAL_TABLE, TableType.MAX_COMPUTE_EXTERNAL_TABLE);
        } catch (MetaNotFoundException e) {
            throw new AnalysisException(e.getMessage(), e);
        }

        if (!(table instanceof HMSExternalTable)) {
            throw new AnalysisException("Currently only support hive table's partition values meta table");
        }
        HMSExternalTable hmsTable = (HMSExternalTable) table;
        if (!hmsTable.isPartitionedTable()) {
            throw new AnalysisException("Table " + tableName + " is not a partitioned table");
        }
        return table;
    }

    @Override
    public TMetadataType getMetadataType() {
        return TMetadataType.PARTITION_VALUES;
    }

    @Override
    public TMetaScanRange getMetaScanRange() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("getMetaScanRange() start");
        }
        TMetaScanRange metaScanRange = new TMetaScanRange();
        metaScanRange.setMetadataType(TMetadataType.PARTITION_VALUES);
        TPartitionValuesMetadataParams partitionParam = new TPartitionValuesMetadataParams();
        partitionParam.setCatalog(catalogName);
        partitionParam.setDatabase(databaseName);
        partitionParam.setTable(tableName);
        metaScanRange.setPartitionValuesParams(partitionParam);
        return metaScanRange;
    }

    @Override
    public String getTableName() {
        return "PartitionsValuesTableValuedFunction";
    }

    @Override
    public List<Column> getTableColumns() throws AnalysisException {
        Preconditions.checkNotNull(table);
        // TODO: support other type of sys tables
        if (schema == null) {
            List<Column> partitionColumns = ((HMSExternalTable) table).getPartitionColumns();
            schema = Lists.newArrayList();
            for (Column column : partitionColumns) {
                schema.add(new Column(column));
            }
        }
        return schema;
    }
}
