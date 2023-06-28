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

package org.apache.doris.statistics.util;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.DecimalLiteral;
import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LargeIntLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Histogram;
import org.apache.doris.statistics.StatisticConstants;
import org.apache.doris.statistics.util.InternalQueryResult.ResultRow;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Preconditions;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.thrift.TException;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

public class StatisticsUtil {

    private static final String ID_DELIMITER = "-";
    private static final String VALUES_DELIMITER = ",";

    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public static List<ResultRow> executeQuery(String template, Map<String, String> params) {
        StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
        String sql = stringSubstitutor.replace(template);
        return execStatisticQuery(sql);
    }

    public static void execUpdate(String template, Map<String, String> params) throws Exception {
        StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
        String sql = stringSubstitutor.replace(template);
        execUpdate(sql);
    }

    public static List<ResultRow> execStatisticQuery(String sql) {
        try (AutoCloseConnectContext r = StatisticsUtil.buildConnectContext()) {
            StmtExecutor stmtExecutor = new StmtExecutor(r.connectContext, sql);
            r.connectContext.setExecutor(stmtExecutor);
            return stmtExecutor.executeInternalQuery();
        }
    }

    public static QueryState execUpdate(String sql) throws Exception {
        try (AutoCloseConnectContext r = StatisticsUtil.buildConnectContext()) {
            r.connectContext.getSessionVariable().disableNereidsPlannerOnce();
            StmtExecutor stmtExecutor = new StmtExecutor(r.connectContext, sql);
            r.connectContext.setExecutor(stmtExecutor);
            stmtExecutor.execute();
            return r.connectContext.getState();
        }
    }

    public static List<AnalysisInfo> deserializeToAnalysisJob(List<ResultRow> resultBatches)
            throws TException {
        if (CollectionUtils.isEmpty(resultBatches)) {
            return Collections.emptyList();
        }
        return resultBatches.stream()
                .map(AnalysisInfo::fromResultRow)
                .collect(Collectors.toList());
    }

    public static List<ColumnStatistic> deserializeToColumnStatistics(List<ResultRow> resultBatches)
            throws Exception {
        if (CollectionUtils.isEmpty(resultBatches)) {
            return Collections.emptyList();
        }
        return resultBatches.stream().map(ColumnStatistic::fromResultRow).collect(Collectors.toList());
    }

    public static List<Histogram> deserializeToHistogramStatistics(List<ResultRow> resultBatches)
            throws Exception {
        return resultBatches.stream().map(Histogram::fromResultRow).collect(Collectors.toList());
    }

    public static AutoCloseConnectContext buildConnectContext() {
        ConnectContext connectContext = new ConnectContext();
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        sessionVariable.internalSession = true;
        sessionVariable.setMaxExecMemByte(StatisticConstants.STATISTICS_MAX_MEM_PER_QUERY_IN_BYTES);
        sessionVariable.setEnableInsertStrict(true);
        sessionVariable.parallelExecInstanceNum = StatisticConstants.STATISTIC_PARALLEL_EXEC_INSTANCE_NUM;
        sessionVariable.parallelPipelineTaskNum = StatisticConstants.STATISTIC_PARALLEL_EXEC_INSTANCE_NUM;
        sessionVariable.setEnableNereidsPlanner(false);
        sessionVariable.enableProfile = false;
        connectContext.setEnv(Env.getCurrentEnv());
        connectContext.setDatabase(FeConstants.INTERNAL_DB_NAME);
        connectContext.setQualifiedUser(UserIdentity.ROOT.getQualifiedUser());
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        UUID uuid = UUID.randomUUID();
        TUniqueId queryId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        connectContext.setQueryId(queryId);
        connectContext.setStartTime();
        connectContext.setCluster(SystemInfoService.DEFAULT_CLUSTER);
        return new AutoCloseConnectContext(connectContext);
    }

    public static void analyze(StatementBase statementBase) throws UserException {
        try (AutoCloseConnectContext r = buildConnectContext()) {
            Analyzer analyzer = new Analyzer(Env.getCurrentEnv(), r.connectContext);
            statementBase.analyze(analyzer);
        }
    }

    public static LiteralExpr readableValue(Type type, String columnValue) throws AnalysisException {
        Preconditions.checkArgument(type.isScalarType());
        ScalarType scalarType = (ScalarType) type;

        // check if default value is valid.
        // if not, some literal constructor will throw AnalysisException
        PrimitiveType primitiveType = scalarType.getPrimitiveType();
        switch (primitiveType) {
            case BOOLEAN:
                return new BoolLiteral(columnValue);
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                return new IntLiteral(columnValue, type);
            case LARGEINT:
                return new LargeIntLiteral(columnValue);
            case FLOAT:
                // the min max value will loose precision when value type is double.
            case DOUBLE:
                return new FloatLiteral(columnValue);
            case DECIMALV2:
                //no need to check precision and scale, since V2 is fixed point
                return new DecimalLiteral(columnValue);
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                DecimalLiteral decimalLiteral = new DecimalLiteral(columnValue);
                decimalLiteral.checkPrecisionAndScale(scalarType.getScalarPrecision(), scalarType.getScalarScale());
                return decimalLiteral;
            case DATE:
            case DATETIME:
            case DATEV2:
            case DATETIMEV2:
                return new DateLiteral(columnValue, type);
            case CHAR:
            case VARCHAR:
            case STRING:
                return new StringLiteral(columnValue);
            case HLL:
            case BITMAP:
            case ARRAY:
            case MAP:
            case STRUCT:
            default:
                throw new AnalysisException("Unsupported setting this type: " + type + " of min max value");
        }
    }

    public static double convertToDouble(Type type, String columnValue) throws AnalysisException {
        Preconditions.checkArgument(type.isScalarType());
        try {
            ScalarType scalarType = (ScalarType) type;

            // check if default value is valid.
            // if not, some literal constructor will throw AnalysisException
            PrimitiveType primitiveType = scalarType.getPrimitiveType();
            switch (primitiveType) {
                case BOOLEAN:
                    return Boolean.parseBoolean(columnValue) ? 1.0 : 0.0;
                case TINYINT:
                case SMALLINT:
                case INT:
                case BIGINT:
                case LARGEINT:
                case FLOAT:
                    // the min max value will loose precision when value type is double.
                case DOUBLE:
                case DECIMALV2:
                case DECIMAL32:
                case DECIMAL64:
                case DECIMAL128:
                    return Double.parseDouble(columnValue);
                case DATE:
                case DATEV2:
                    org.apache.doris.nereids.trees.expressions.literal.DateLiteral literal =
                            new org.apache.doris.nereids.trees.expressions.literal.DateLiteral(columnValue);
                    return literal.getDouble();

                case DATETIMEV2:
                case DATETIME:
                    DateTimeLiteral dateTimeLiteral = new DateTimeLiteral(columnValue);
                    return dateTimeLiteral.getDouble();
                case CHAR:
                case VARCHAR:
                case STRING:
                    VarcharLiteral varchar = new VarcharLiteral(columnValue);
                    return varchar.getDouble();
                case HLL:
                case BITMAP:
                case ARRAY:
                case MAP:
                case STRUCT:
                default:
                    throw new AnalysisException("Unsupported setting this type: " + type + " of min max value");
            }
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage(), e);
        }

    }


    public static DBObjects convertTableNameToObjects(TableName tableName) {
        CatalogIf<DatabaseIf> catalogIf = Env.getCurrentEnv().getCatalogMgr().getCatalog(tableName.getCtl());
        if (catalogIf == null) {
            throw new IllegalStateException(String.format("Catalog:%s doesn't exist", tableName.getCtl()));
        }
        DatabaseIf<TableIf> databaseIf = catalogIf.getDbNullable(tableName.getDb());
        if (databaseIf == null) {
            throw new IllegalStateException(String.format("DB:%s doesn't exist", tableName.getDb()));
        }
        TableIf tableIf = databaseIf.getTableNullable(tableName.getTbl());
        if (tableIf == null) {
            throw new IllegalStateException(String.format("Table:%s doesn't exist", tableName.getTbl()));
        }
        return new DBObjects(catalogIf, databaseIf, tableIf);
    }

    public static Column findColumn(long catalogId, long dbId, long tblId, long idxId, String columnName) {
        CatalogIf<DatabaseIf<TableIf>> catalogIf = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogId);
        if (catalogIf == null) {
            return null;
        }
        DatabaseIf<TableIf> db = catalogIf.getDb(dbId).orElse(null);
        if (db == null) {
            return null;
        }
        TableIf tblIf = db.getTable(tblId).orElse(null);
        if (tblIf == null) {
            return null;
        }
        if (idxId != -1) {
            if (tblIf instanceof OlapTable) {
                OlapTable olapTable = (OlapTable) tblIf;
                return olapTable.getIndexIdToMeta().get(idxId).getColumnByName(columnName);
            }
        }
        return tblIf.getColumn(columnName);
    }

    @SuppressWarnings({"unchecked"})
    public static Column findColumn(String catalogName, String dbName, String tblName, String columnName)
            throws Throwable {
        TableIf tableIf = findTable(catalogName, dbName, tblName);
        return tableIf.getColumn(columnName);
    }

    /**
     * Throw RuntimeException if table not exists.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static TableIf findTable(String catalogName, String dbName, String tblName) {
        try {
            DatabaseIf db = findDatabase(catalogName, dbName);
            return db.getTableOrException(tblName,
                    t -> new RuntimeException("Table: " + t + " not exists"));
        } catch (Throwable t) {
            throw new RuntimeException("Table: `" + catalogName + "." + dbName + "." + tblName + "` not exists");
        }
    }

    /**
     * Throw RuntimeException if database not exists.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static DatabaseIf findDatabase(String catalogName, String dbName) throws Throwable {
        CatalogIf catalog = findCatalog(catalogName);
        return catalog.getDbOrException(dbName,
                d -> new RuntimeException("DB: " + d + " not exists"));
    }

    /**
     * Throw RuntimeException if catalog not exists.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static CatalogIf findCatalog(String catalogName) {
        return Env.getCurrentEnv().getCatalogMgr()
                .getCatalogOrException(catalogName, c -> new RuntimeException("Catalog: " + c + " not exists"));
    }

    public static boolean isNullOrEmpty(String str) {
        return Optional.ofNullable(str)
                .map(String::trim)
                .map(String::toLowerCase)
                .map(s -> "null".equalsIgnoreCase(s) || s.isEmpty())
                .orElse(true);
    }

    public static boolean statsTblAvailable() {
        String dbName = SystemInfoService.DEFAULT_CLUSTER + ":" + FeConstants.INTERNAL_DB_NAME;
        List<OlapTable> statsTbls = new ArrayList<>();
        try {
            statsTbls.add(
                    (OlapTable) StatisticsUtil
                            .findTable(InternalCatalog.INTERNAL_CATALOG_NAME,
                                    dbName,
                                    StatisticConstants.STATISTIC_TBL_NAME));
            statsTbls.add(
                    (OlapTable) StatisticsUtil
                            .findTable(InternalCatalog.INTERNAL_CATALOG_NAME,
                                    dbName,
                                    StatisticConstants.HISTOGRAM_TBL_NAME));
        } catch (Throwable t) {
            return false;
        }
        for (OlapTable table : statsTbls) {
            for (Partition partition : table.getPartitions()) {
                if (partition.getBaseIndex().getTablets().stream()
                        .anyMatch(t -> t.getNormalReplicaBackendIds().isEmpty())) {
                    return false;
                }
            }
        }
        return true;
    }

    public static Map<Long, Partition> getIdToPartition(TableIf table) {
        return table.getPartitionNames().stream()
                .map(table::getPartition)
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(
                        Partition::getId,
                        Function.identity()
                ));
    }

    public static Map<Long, String> getPartitionIdToName(TableIf table) {
        return table.getPartitionNames().stream()
                .map(table::getPartition)
                .collect(Collectors.toMap(
                        Partition::getId,
                        Partition::getName
                ));
    }

    public static <T> String joinElementsToString(Collection<T> values, String delimiter) {
        StringJoiner builder = new StringJoiner(delimiter);
        values.forEach(v -> builder.add(String.valueOf(v)));
        return builder.toString();
    }

    public static int convertStrToInt(String str) {
        return StringUtils.isNumeric(str) ? Integer.parseInt(str) : 0;
    }

    public static long convertStrToLong(String str) {
        return StringUtils.isNumeric(str) ? Long.parseLong(str) : 0;
    }

    public static String getReadableTime(long timeInMs) {
        if (timeInMs <= 0) {
            return "";
        }
        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        return format.format(new Date(timeInMs));
    }

    @SafeVarargs
    public static <T> String constructId(T... items) {
        if (items == null || items.length == 0) {
            return "";
        }
        List<String> idElements = Arrays.stream(items)
                .map(String::valueOf)
                .collect(Collectors.toList());
        return StatisticsUtil.joinElementsToString(idElements, ID_DELIMITER);
    }

    public static String replaceParams(String template, Map<String, String> params) {
        StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
        return stringSubstitutor.replace(template);
    }


    /**
     * The health of the table indicates the health of the table statistics.
     * When update_rows >= row_count, the health is 0;
     * when update_rows < row_count, the health degree is 100 (1 - update_rows row_count).
     *
     * @param updatedRows The number of rows updated by the table
     * @return Health, the value range is [0, 100], the larger the value,
     * @param totalRows The current number of rows in the table
     * the healthier the statistics of the table
     */
    public static int getTableHealth(long totalRows, long updatedRows) {
        if (updatedRows >= totalRows) {
            return 0;
        } else {
            double healthCoefficient = (double) (totalRows - updatedRows) / (double) totalRows;
            return (int) (healthCoefficient * 100.0);
        }
    }
}
