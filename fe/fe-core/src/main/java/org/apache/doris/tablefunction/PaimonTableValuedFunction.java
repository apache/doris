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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonUtil;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TMetaScanRange;
import org.apache.doris.thrift.TMetadataType;
import org.apache.doris.thrift.TPaimonMetadataParams;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.Split;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Table-valued function for querying Paimon system tables metadata.
 */
public class PaimonTableValuedFunction extends MetadataTableValuedFunction {
    public static final String NAME = "paimon_meta";
    public static final String TABLE = "table";
    public static final String QUERY_TYPE = "query_type";

    private static final ImmutableSet<String> PROPERTIES_SET = ImmutableSet.of(TABLE, QUERY_TYPE);

    private final String queryType;
    private final Table paimonSysTable;
    private final List<Column> schema;
    private final Map<String, String> hadoopProps;
    private final Map<String, String> paimonProps;
    private final ExecutionAuthenticator hadoopAuthenticator;
    private final TableName paimonTableName;
    private final long ctlId;
    private final long dbId;
    private final long tblId;

    /**
     * Creates a new Paimon table-valued function instance.
     *
     * @param paimonTableName the target Paimon table name
     * @param queryType the type of metadata query to perform
     * @throws AnalysisException if table validation or initialization fails
     */
    public PaimonTableValuedFunction(TableName paimonTableName, String queryType) throws AnalysisException {
        this.queryType = queryType;
        CatalogIf<?> dorisCatalog = Env.getCurrentEnv()
                .getCatalogMgr()
                .getCatalog(paimonTableName.getCtl());

        if (!(dorisCatalog instanceof PaimonExternalCatalog)) {
            throw new AnalysisException("Catalog " + paimonTableName.getCtl() + " is not an paimon catalog");
        }

        this.paimonTableName = paimonTableName;
        PaimonExternalCatalog paimonExternalCatalog = (PaimonExternalCatalog) dorisCatalog;
        this.hadoopProps = paimonExternalCatalog.getCatalogProperty().getHadoopProperties();
        this.paimonProps = paimonExternalCatalog.getPaimonOptionsMap();
        this.hadoopAuthenticator = paimonExternalCatalog.getExecutionAuthenticator();
        this.ctlId = paimonExternalCatalog.getId();

        ExternalDatabase<? extends ExternalTable> database = paimonExternalCatalog.getDb(paimonTableName.getDb())
                .orElseThrow(() -> new AnalysisException(
                        String.format("Paimon catalog database '%s' does not exist", paimonTableName.getDb())
                ));
        this.dbId = database.getId();

        ExternalTable externalTable = database.getTable(paimonTableName.getTbl())
                .orElseThrow(() -> new AnalysisException(
                        String.format("Paimon catalog table '%s.%s' does not exist",
                                paimonTableName.getDb(), paimonTableName.getTbl())
                ));
        NameMapping buildNameMapping = externalTable.getOrBuildNameMapping();
        this.tblId = externalTable.getId();

        this.paimonSysTable = paimonExternalCatalog.getPaimonTable(buildNameMapping,
                "main", queryType);
        this.schema = PaimonUtil.parseSchema(paimonSysTable);
    }

    public static PaimonTableValuedFunction create(Map<String, String> params) throws AnalysisException {
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
            throw new AnalysisException("Invalid paimon metadata query");
        }

        String[] names = tableName.split("\\.");
        if (names.length != 3) {
            throw new AnalysisException("The paimon table name contains the catalogName, databaseName, and tableName");
        }
        TableName paimonTableName = new TableName(names[0], names[1], names[2]);
        // check auth
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), paimonTableName, PrivPredicate.SELECT)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SELECT",
                    ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                    paimonTableName.getDb() + ": " + paimonTableName.getTbl());
        }
        return new PaimonTableValuedFunction(paimonTableName, queryType);
    }

    @Override
    public TMetadataType getMetadataType() {
        return TMetadataType.PAIMON;
    }

    @Override
    public List<TMetaScanRange> getMetaScanRanges(List<String> requiredFileds) {
        int[] projections = requiredFileds.stream().mapToInt(
                        field -> paimonSysTable.rowType().getFieldNames()
                                .stream()
                                .map(String::toLowerCase)
                                .collect(Collectors.toList())
                                .indexOf(field))
                .toArray();
        List<Split> splits;

        try {
            splits = hadoopAuthenticator.execute(
                    () -> paimonSysTable.newReadBuilder().withProjection(projections).newScan().plan().splits());
        } catch (Exception e) {
            throw new RuntimeException(ExceptionUtils.getRootCauseMessage(e));
        }

        return splits.stream().map(this::createMetaScanRange).collect(Collectors.toList());
    }

    @Override
    public String getTableName() {
        return "PaimonTableValuedFunction<" + queryType + ">";
    }

    @Override
    public List<Column> getTableColumns() throws AnalysisException {
        return schema;
    }

    private TMetaScanRange createMetaScanRange(Split split) {
        TMetaScanRange tMetaScanRange = new TMetaScanRange();
        tMetaScanRange.setMetadataType(TMetadataType.PAIMON);

        TPaimonMetadataParams tPaimonMetadataParams = new TPaimonMetadataParams();
        tPaimonMetadataParams.setCtlId(ctlId);
        tPaimonMetadataParams.setDbId(dbId);
        tPaimonMetadataParams.setTblId(tblId);
        tPaimonMetadataParams.setQueryType(queryType);
        tPaimonMetadataParams.setDbName(paimonTableName.getDb());
        tPaimonMetadataParams.setTblName(paimonTableName.getTbl());
        tPaimonMetadataParams.setHadoopProps(hadoopProps);
        tPaimonMetadataParams.setPaimonProps(paimonProps);
        tPaimonMetadataParams.setSerializedSplit(PaimonUtil.encodeObjectToString(split));

        tMetaScanRange.setPaimonParams(tPaimonMetadataParams);
        return tMetaScanRange;
    }
}
