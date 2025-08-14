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
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TIcebergMetadataParams;
import org.apache.doris.thrift.TMetaScanRange;
import org.apache.doris.thrift.TMetadataType;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.util.SerializationUtil;

import java.util.List;
import java.util.Map;

/**
 * The class of table valued function for iceberg metadata.
 * iceberg_meta("table" = "ctl.db.tbl", "query_type" = "snapshots").
 */
public class IcebergTableValuedFunction extends MetadataTableValuedFunction {

    public static final String NAME = "iceberg_meta";
    public static final String TABLE = "table";
    public static final String QUERY_TYPE = "query_type";

    private static final ImmutableSet<String> PROPERTIES_SET = ImmutableSet.of(TABLE, QUERY_TYPE);

    private final String queryType;
    private final Table sysTable;
    private final List<Column> schema;
    private final Map<String, String> hadoopProps;
    private final ExecutionAuthenticator preExecutionAuthenticator;

    public static IcebergTableValuedFunction create(Map<String, String> params)
            throws AnalysisException {
        Map<String, String> validParams = Maps.newHashMap();
        for (String key : params.keySet()) {
            if (!PROPERTIES_SET.contains(key.toLowerCase())) {
                throw new AnalysisException("'" + key + "' is invalid property");
            }
            // check ctl, db, tbl
            validParams.put(key.toLowerCase(), params.get(key));
        }
        String tableName = validParams.get(TABLE);
        String queryType = validParams.get(QUERY_TYPE);
        if (tableName == null || queryType == null) {
            throw new AnalysisException("Invalid iceberg metadata query");
        }
        // TODO: support these system tables in future;
        if (queryType.equalsIgnoreCase("all_manifests") || queryType.equalsIgnoreCase("position_deletes")) {
            throw new AnalysisException("SysTable " + queryType + " is not supported yet");
        }
        String[] names = tableName.split("\\.");
        if (names.length != 3) {
            throw new AnalysisException("The iceberg table name contains the catalogName, databaseName, and tableName");
        }
        TableName icebergTableName = new TableName(names[0], names[1], names[2]);
        // check auth
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), icebergTableName, PrivPredicate.SELECT)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SELECT",
                    ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                    icebergTableName.getDb() + ": " + icebergTableName.getTbl());
        }
        return new IcebergTableValuedFunction(icebergTableName, queryType);
    }

    public IcebergTableValuedFunction(TableName icebergTableName, String queryType)
            throws AnalysisException {
        this.queryType = queryType;
        CatalogIf<?> catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(icebergTableName.getCtl());
        if (!(catalog instanceof ExternalCatalog)) {
            throw new AnalysisException("Catalog " + icebergTableName.getCtl() + " is not an external catalog");
        }
        ExternalCatalog externalCatalog = (ExternalCatalog) catalog;
        hadoopProps = externalCatalog.getCatalogProperty().getBackendStorageProperties();
        preExecutionAuthenticator = externalCatalog.getExecutionAuthenticator();

        TableIf dorisTable = externalCatalog.getDbOrAnalysisException(icebergTableName.getDb())
                .getTableOrAnalysisException(icebergTableName.getTbl());
        if (!(dorisTable instanceof ExternalTable)) {
            throw new AnalysisException("Table " + icebergTableName + " is not an iceberg table");
        }
        Table icebergTable = IcebergUtils.getIcebergTable((ExternalTable) dorisTable);
        if (icebergTable == null) {
            throw new AnalysisException("Iceberg table " + icebergTableName + " does not exist");
        }
        MetadataTableType tableType = MetadataTableType.from(queryType);
        if (tableType == null) {
            throw new AnalysisException("Unrecognized queryType for iceberg metadata: " + queryType);
        }
        this.sysTable = MetadataTableUtils.createMetadataTableInstance(icebergTable, tableType);
        this.schema = IcebergUtils.parseSchema(sysTable.schema());
    }

    @Override
    public TMetadataType getMetadataType() {
        return TMetadataType.ICEBERG;
    }

    @Override
    public List<TMetaScanRange> getMetaScanRanges(List<String> requiredFileds) {
        List<TMetaScanRange> scanRanges = Lists.newArrayList();
        CloseableIterable<FileScanTask> tasks;
        try {
            tasks = preExecutionAuthenticator.execute(() -> {
                return sysTable.newScan().select(requiredFileds).planFiles();
            });
        } catch (Exception e) {
            throw new RuntimeException(ExceptionUtils.getRootCauseMessage(e));
        }
        for (FileScanTask task : tasks) {
            TMetaScanRange metaScanRange = new TMetaScanRange();
            metaScanRange.setMetadataType(TMetadataType.ICEBERG);
            // set iceberg metadata params
            TIcebergMetadataParams icebergMetadataParams = new TIcebergMetadataParams();
            icebergMetadataParams.setHadoopProps(hadoopProps);
            icebergMetadataParams.setSerializedTask(SerializationUtil.serializeToBase64(task));
            metaScanRange.setIcebergParams(icebergMetadataParams);
            scanRanges.add(metaScanRange);
        }
        return scanRanges;
    }

    @Override
    public String getTableName() {
        return "IcebergTableValuedFunction<" + queryType + ">";
    }

    @Override
    public List<Column> getTableColumns() {
        return schema;
    }
}
