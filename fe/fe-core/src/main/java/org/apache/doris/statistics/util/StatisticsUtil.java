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
import org.apache.doris.analysis.CreateMaterializedViewStmt;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.DecimalLiteral;
import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LargeIntLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SetType;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.analysis.VariableExpr;
import org.apache.doris.catalog.AggStateType;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.TableAttributes;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.VariantType;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable.DLAType;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.qe.AuditLogHelper;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.GlobalVariable;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.AnalysisManager;
import org.apache.doris.statistics.ColStatsMeta;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.Histogram;
import org.apache.doris.statistics.PartitionColumnStatistic;
import org.apache.doris.statistics.ResultRow;
import org.apache.doris.statistics.StatisticConstants;
import org.apache.doris.statistics.TableStatsMeta;
import org.apache.doris.system.Frontend;

import com.google.common.base.Preconditions;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class StatisticsUtil {
    private static final Logger LOG = LogManager.getLogger(StatisticsUtil.class);

    private static final String ID_DELIMITER = "-";

    private static final String TOTAL_SIZE = "totalSize";
    private static final String NUM_ROWS = "numRows";

    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final int UPDATED_PARTITION_THRESHOLD = 3;

    public static List<ResultRow> executeQuery(String template, Map<String, String> params) {
        StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
        String sql = stringSubstitutor.replace(template);
        return execStatisticQuery(sql, true);
    }

    public static void execUpdate(String template, Map<String, String> params) throws Exception {
        StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
        String sql = stringSubstitutor.replace(template);
        execUpdate(sql);
    }

    public static List<ResultRow> execStatisticQuery(String sql) {
        return execStatisticQuery(sql, false);
    }

    public static List<ResultRow> execStatisticQuery(String sql, boolean enableFileCache) {
        if (!FeConstants.enableInternalSchemaDb) {
            return Collections.emptyList();
        }
        boolean useFileCacheForStat = (enableFileCache && Config.allow_analyze_statistics_info_polluting_file_cache);
        try (AutoCloseConnectContext r = StatisticsUtil.buildConnectContext(false, useFileCacheForStat)) {
            if (Config.isCloudMode()) {
                r.connectContext.getCloudCluster();
            }
            StmtExecutor stmtExecutor = new StmtExecutor(r.connectContext, sql);
            r.connectContext.setExecutor(stmtExecutor);
            return stmtExecutor.executeInternalQuery();
        }
    }

    public static QueryState execUpdate(String sql) throws Exception {
        StmtExecutor stmtExecutor = null;
        AutoCloseConnectContext r = StatisticsUtil.buildConnectContext();
        try {
            r.connectContext.getSessionVariable().disableNereidsPlannerOnce();
            stmtExecutor = new StmtExecutor(r.connectContext, sql);
            r.connectContext.setExecutor(stmtExecutor);
            stmtExecutor.execute();
            QueryState state = r.connectContext.getState();
            if (state.getStateType().equals(QueryState.MysqlStateType.ERR)) {
                throw new Exception(state.getErrorMessage());
            }
            return state;
        } finally {
            r.close();
            if (stmtExecutor != null) {
                AuditLogHelper.logAuditLog(r.connectContext, stmtExecutor.getOriginStmt().originStmt,
                        stmtExecutor.getParsedStmt(), stmtExecutor.getQueryStatisticsForAuditLog(), true);
            }
        }
    }

    public static ColumnStatistic deserializeToColumnStatistics(List<ResultRow> resultBatches)
            throws Exception {
        if (CollectionUtils.isEmpty(resultBatches)) {
            return null;
        }
        return ColumnStatistic.fromResultRow(resultBatches);
    }

    public static List<Histogram> deserializeToHistogramStatistics(List<ResultRow> resultBatches) {
        return resultBatches.stream().map(Histogram::fromResultRow).collect(Collectors.toList());
    }

    public static PartitionColumnStatistic deserializeToPartitionStatistics(List<ResultRow> resultBatches) {
        if (CollectionUtils.isEmpty(resultBatches)) {
            return null;
        }
        return PartitionColumnStatistic.fromResultRow(resultBatches);
    }

    public static AutoCloseConnectContext buildConnectContext() {
        return buildConnectContext(false, false);
    }

    public static AutoCloseConnectContext buildConnectContext(boolean limitScan, boolean useFileCacheForStat) {
        ConnectContext connectContext = new ConnectContext();
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        sessionVariable.internalSession = true;
        sessionVariable.setMaxExecMemByte(Config.statistics_sql_mem_limit_in_bytes);
        sessionVariable.cpuResourceLimit = Config.cpu_resource_limit_per_analyze_task;
        sessionVariable.setEnableInsertStrict(true);
        sessionVariable.setInsertMaxFilterRatio(1.0);
        sessionVariable.enablePageCache = false;
        sessionVariable.enableProfile = Config.enable_profile_when_analyze;
        sessionVariable.parallelExecInstanceNum = Config.statistics_sql_parallel_exec_instance_num;
        sessionVariable.parallelPipelineTaskNum = Config.statistics_sql_parallel_exec_instance_num;
        sessionVariable.setEnableNereidsPlanner(true);
        sessionVariable.enableScanRunSerial = limitScan;
        sessionVariable.setQueryTimeoutS(StatisticsUtil.getAnalyzeTimeout());
        sessionVariable.insertTimeoutS = StatisticsUtil.getAnalyzeTimeout();
        sessionVariable.enableFileCache = false;
        sessionVariable.forbidUnknownColStats = false;
        sessionVariable.enablePushDownMinMaxOnUnique = true;
        sessionVariable.enablePushDownStringMinMax = true;
        sessionVariable.enableUniqueKeyPartialUpdate = false;
        connectContext.setEnv(Env.getCurrentEnv());
        connectContext.setDatabase(FeConstants.INTERNAL_DB_NAME);
        connectContext.setQualifiedUser(UserIdentity.ROOT.getQualifiedUser());
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        connectContext.setStartTime();
        if (Config.isCloudMode()) {
            AutoCloseConnectContext ctx = new AutoCloseConnectContext(connectContext);
            ctx.connectContext.getCloudCluster();
            sessionVariable.disableFileCache = !useFileCacheForStat;
            return ctx;
        } else {
            return new AutoCloseConnectContext(connectContext);
        }
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
                // no need to check precision and scale, since V2 is fixed point
                return new DecimalLiteral(columnValue);
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
            case DECIMAL256:
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
                case DECIMAL256:
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
        CatalogIf<? extends DatabaseIf<? extends TableIf>> catalogIf =
                Env.getCurrentEnv().getCatalogMgr().getCatalog(tableName.getCtl());
        if (catalogIf == null) {
            throw new IllegalStateException(String.format("Catalog:%s doesn't exist", tableName.getCtl()));
        }
        DatabaseIf<? extends TableIf> databaseIf = catalogIf.getDbNullable(tableName.getDb());
        if (databaseIf == null) {
            throw new IllegalStateException(String.format("DB:%s doesn't exist", tableName.getDb()));
        }
        TableIf tableIf = databaseIf.getTableNullable(tableName.getTbl());
        if (tableIf == null) {
            throw new IllegalStateException(String.format("Table:%s doesn't exist", tableName.getTbl()));
        }
        return new DBObjects(catalogIf, databaseIf, tableIf);
    }

    public static DBObjects convertIdToObjects(long catalogId, long dbId, long tblId) {
        return new DBObjects(findCatalog(catalogId), findDatabase(catalogId, dbId), findTable(catalogId, dbId, tblId));
    }

    public static Column findColumn(long catalogId, long dbId, long tblId, long idxId, String columnName) {
        CatalogIf<? extends DatabaseIf<? extends TableIf>> catalogIf =
                Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogId);
        if (catalogIf == null) {
            return null;
        }
        DatabaseIf<? extends TableIf> db = catalogIf.getDb(dbId).orElse(null);
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

    public static TableIf findTable(long catalogId, long dbId, long tblId) {
        try {
            DatabaseIf<? extends TableIf> db = findDatabase(catalogId, dbId);
            return db.getTableOrException(tblId,
                    t -> new RuntimeException("Table: " + t + " not exists"));
        } catch (Throwable t) {
            throw new RuntimeException("Table: `" + catalogId + "." + dbId + "." + tblId + "` not exists");
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

    public static DatabaseIf<? extends TableIf> findDatabase(long catalogId, long dbId)  {
        CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog = findCatalog(catalogId);
        return catalog.getDbOrException(dbId,
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

    public static CatalogIf<? extends DatabaseIf<? extends TableIf>> findCatalog(long catalogId) {
        return Env.getCurrentEnv().getCatalogMgr().getCatalogOrException(catalogId,
                c -> new RuntimeException("Catalog: " + c + " not exists"));
    }

    public static boolean isNullOrEmpty(String str) {
        return Optional.ofNullable(str)
                .map(String::trim)
                .map(String::toLowerCase)
                .map(s -> "null".equalsIgnoreCase(s) || s.isEmpty())
                .orElse(true);
    }

    public static boolean statsTblAvailable() {
        String dbName = FeConstants.INTERNAL_DB_NAME;
        List<OlapTable> statsTbls = new ArrayList<>();
        try {
            statsTbls.add(
                    (OlapTable) StatisticsUtil
                            .findTable(InternalCatalog.INTERNAL_CATALOG_NAME,
                                    dbName,
                                    StatisticConstants.TABLE_STATISTIC_TBL_NAME));
            // uncomment it when hist is available for user.
            // statsTbls.add(
            //         (OlapTable) StatisticsUtil
            //                 .findTable(InternalCatalog.INTERNAL_CATALOG_NAME,
            //                         dbName,
            //                         StatisticConstants.HISTOGRAM_TBL_NAME));
        } catch (Throwable t) {
            return false;
        }
        if (Config.isCloudMode()) {
            if (!((CloudSystemInfoService) Env.getCurrentSystemInfo()).availableBackendsExists()) {
                return false;
            }
            try (AutoCloseConnectContext r = buildConnectContext()) {
                r.connectContext.getCloudCluster();
                for (OlapTable table : statsTbls) {
                    for (Partition partition : table.getPartitions()) {
                        if (partition.getBaseIndex().getTablets().stream()
                                .anyMatch(t -> t.getNormalReplicaBackendIds().isEmpty())) {
                            return false;
                        }
                    }
                }
            }
        } else {
            for (OlapTable table : statsTbls) {
                for (Partition partition : table.getPartitions()) {
                    if (partition.getBaseIndex().getTablets().stream()
                            .anyMatch(t -> t.getNormalReplicaBackendIds().isEmpty())) {
                        return false;
                    }
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

    public static Set<String> getPartitionIds(TableIf table) {
        if (table instanceof OlapTable) {
            return ((OlapTable) table).getPartitionIds().stream().map(String::valueOf).collect(Collectors.toSet());
        } else if (table instanceof ExternalTable) {
            return table.getPartitionNames();
        }
        throw new RuntimeException(String.format("Not supported Table %s", table.getClass().getName()));
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
     * @param totalRows The current number of rows in the table
     * @return Health, the value range is [0, 100], the larger the value, the healthier the statistics of the table.
     */
    public static int getTableHealth(long totalRows, long updatedRows) {
        // Avoid analyze empty table every time.
        if (totalRows == 0 && updatedRows == 0) {
            return 100;
        }
        if (updatedRows >= totalRows) {
            return 0;
        } else {
            double healthCoefficient = (double) (totalRows - updatedRows) / (double) totalRows;
            return (int) (healthCoefficient * 100.0);
        }
    }

    /**
     * Estimate hive table row count.
     * First get it from remote table parameters. If not found, estimate it : totalSize/estimatedRowSize
     *
     * @param table Hive HMSExternalTable to estimate row count.
     * @return estimated row count
     */
    public static long getHiveRowCount(HMSExternalTable table) {
        Map<String, String> parameters = table.getRemoteTable().getParameters();
        if (parameters == null) {
            return -1;
        }
        // Table parameters contains row count, simply get and return it.
        if (parameters.containsKey(NUM_ROWS)) {
            long rows = Long.parseLong(parameters.get(NUM_ROWS));
            // Sometimes, the NUM_ROWS in hms is 0 but actually is not. Need to check TOTAL_SIZE if NUM_ROWS is 0.
            if (rows != 0) {
                return rows;
            }
        }
        if (!parameters.containsKey(TOTAL_SIZE)) {
            return -1;
        }
        // Table parameters doesn't contain row count but contain total size. Estimate row count : totalSize/rowSize
        long totalSize = Long.parseLong(parameters.get(TOTAL_SIZE));
        long estimatedRowSize = 0;
        for (Column column : table.getFullSchema()) {
            estimatedRowSize += column.getDataType().getSlotSize();
        }
        if (estimatedRowSize == 0) {
            return -1;
        }
        return totalSize / estimatedRowSize;
    }

    /**
     * Get total size parameter from HMS.
     * @param table Hive HMSExternalTable to get HMS total size parameter.
     * @return Long value of table total size, return 0 if not found.
     */
    public static long getTotalSizeFromHMS(HMSExternalTable table) {
        Map<String, String> parameters = table.getRemoteTable().getParameters();
        if (parameters == null) {
            return 0;
        }
        return parameters.containsKey(TOTAL_SIZE) ? Long.parseLong(parameters.get(TOTAL_SIZE)) : 0;
    }

    /**
     * Get Iceberg column statistics.
     *
     * @param colName
     * @param table Iceberg table.
     * @return Optional Column statistic for the given column.
     */
    public static Optional<ColumnStatistic> getIcebergColumnStats(String colName, org.apache.iceberg.Table table) {
        TableScan tableScan = table.newScan().includeColumnStats();
        ColumnStatisticBuilder columnStatisticBuilder = new ColumnStatisticBuilder();
        columnStatisticBuilder.setCount(0);
        columnStatisticBuilder.setMaxValue(Double.POSITIVE_INFINITY);
        columnStatisticBuilder.setMinValue(Double.NEGATIVE_INFINITY);
        columnStatisticBuilder.setDataSize(0);
        columnStatisticBuilder.setAvgSizeByte(0);
        columnStatisticBuilder.setNumNulls(0);
        try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
            for (FileScanTask task : fileScanTasks) {
                processDataFile(task.file(), task.spec(), colName, columnStatisticBuilder);
            }
        } catch (IOException e) {
            LOG.warn("Error to close FileScanTask.", e);
        }
        if (columnStatisticBuilder.getCount() > 0) {
            columnStatisticBuilder.setAvgSizeByte(columnStatisticBuilder.getDataSize()
                    / columnStatisticBuilder.getCount());
        }
        return Optional.of(columnStatisticBuilder.build());
    }

    private static void processDataFile(DataFile dataFile, PartitionSpec partitionSpec,
            String colName, ColumnStatisticBuilder columnStatisticBuilder) {
        int colId = -1;
        for (Types.NestedField column : partitionSpec.schema().columns()) {
            if (column.name().equals(colName)) {
                colId = column.fieldId();
                break;
            }
        }
        if (colId == -1) {
            throw new RuntimeException(String.format("Column %s not exist.", colName));
        }
        // Update the data size, count and num of nulls in columnStatisticBuilder.
        // TODO: Get min max value.
        columnStatisticBuilder.setDataSize(columnStatisticBuilder.getDataSize() + dataFile.columnSizes().get(colId));
        columnStatisticBuilder.setCount(columnStatisticBuilder.getCount() + dataFile.recordCount());
        columnStatisticBuilder.setNumNulls(columnStatisticBuilder.getNumNulls()
                + dataFile.nullValueCounts().get(colId));
    }

    public static boolean isUnsupportedType(Type type) {
        if (ColumnStatistic.UNSUPPORTED_TYPE.contains(type)) {
            return true;
        }
        return type instanceof ArrayType
                || type instanceof StructType
                || type instanceof MapType
                || type instanceof VariantType
                || type instanceof AggStateType;
    }

    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignore) {
            // IGNORE
        }
    }

    public static String quote(String str) {
        return "'" + str + "'";
    }

    public static boolean isMaster(Frontend frontend) {
        InetSocketAddress socketAddress = new InetSocketAddress(frontend.getHost(), frontend.getEditLogPort());
        return Env.getCurrentEnv().getHaProtocol().getLeader().equals(socketAddress);
    }

    public static String escapeSQL(String str) {
        if (str == null) {
            return null;
        }
        return str.replace("'", "''")
                .replace("\\", "\\\\");
    }

    public static String escapeColumnName(String str) {
        if (str == null) {
            return null;
        }
        return str.replace("`", "``");
    }

    public static boolean isExternalTable(String catalogName, String dbName, String tblName) {
        TableIf table;
        try {
            table = StatisticsUtil.findTable(catalogName, dbName, tblName);
        } catch (Throwable e) {
            LOG.warn(e.getMessage());
            return false;
        }
        return table instanceof ExternalTable;
    }

    public static boolean isExternalTable(long catalogId, long dbId, long tblId) {
        TableIf table;
        try {
            table = findTable(catalogId, dbId, tblId);
        } catch (Throwable e) {
            LOG.warn(e.getMessage());
            return false;
        }
        return table instanceof ExternalTable;
    }

    public static boolean inAnalyzeTime(LocalTime now) {
        try {
            Pair<LocalTime, LocalTime> range = findConfigFromGlobalSessionVar();
            if (range == null) {
                return false;
            }
            LocalTime start = range.first;
            LocalTime end = range.second;
            if (start.isAfter(end) && (now.isAfter(start) || now.isBefore(end))) {
                return true;
            } else {
                return now.isAfter(start) && now.isBefore(end);
            }
        } catch (DateTimeParseException e) {
            LOG.warn("Parse analyze start/end time format fail", e);
            return true;
        }
    }

    private static Pair<LocalTime, LocalTime> findConfigFromGlobalSessionVar() {
        try {
            String startTime =
                    findConfigFromGlobalSessionVar(SessionVariable.AUTO_ANALYZE_START_TIME)
                            .autoAnalyzeStartTime;
            // For compatibility
            if (StringUtils.isEmpty(startTime)) {
                startTime = StatisticConstants.FULL_AUTO_ANALYZE_START_TIME;
            }
            String endTime = findConfigFromGlobalSessionVar(SessionVariable.AUTO_ANALYZE_END_TIME)
                    .autoAnalyzeEndTime;
            if (StringUtils.isEmpty(startTime)) {
                endTime = StatisticConstants.FULL_AUTO_ANALYZE_END_TIME;
            }
            DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");
            return Pair.of(LocalTime.parse(startTime, timeFormatter), LocalTime.parse(endTime, timeFormatter));
        } catch (Exception e) {
            return null;
        }
    }

    protected static SessionVariable findConfigFromGlobalSessionVar(String varName) throws Exception {
        SessionVariable sessionVariable =  VariableMgr.getDefaultSessionVariable();
        VariableExpr variableExpr = new VariableExpr(varName, SetType.GLOBAL);
        VariableMgr.getValue(sessionVariable, variableExpr);
        return sessionVariable;
    }

    public static boolean enableAutoAnalyze() {
        try {
            return findConfigFromGlobalSessionVar(SessionVariable.ENABLE_AUTO_ANALYZE).enableAutoAnalyze;
        } catch (Exception e) {
            LOG.warn("Fail to get value of enable auto analyze, return false by default", e);
        }
        return false;
    }

    public static boolean enableAutoAnalyzeInternalCatalog() {
        try {
            return findConfigFromGlobalSessionVar(
                        SessionVariable.ENABLE_AUTO_ANALYZE_INTERNAL_CATALOG).enableAutoAnalyzeInternalCatalog;
        } catch (Exception e) {
            LOG.warn("Fail to get value of enable auto analyze internal catalog, return false by default", e);
        }
        return true;
    }

    public static boolean enablePartitionAnalyze() {
        try {
            return findConfigFromGlobalSessionVar(
                SessionVariable.ENABLE_PARTITION_ANALYZE).enablePartitionAnalyze;
        } catch (Exception e) {
            LOG.warn("Fail to get value of enable partition analyze, return false by default", e);
        }
        return false;
    }

    public static int getInsertMergeCount() {
        try {
            return findConfigFromGlobalSessionVar(SessionVariable.STATS_INSERT_MERGE_ITEM_COUNT)
                    .statsInsertMergeItemCount;
        } catch (Exception e) {
            LOG.warn("Failed to get value of insert_merge_item_count, return default", e);
        }
        return StatisticConstants.INSERT_MERGE_ITEM_COUNT;
    }

    public static long getHugeTableSampleRows() {
        try {
            return findConfigFromGlobalSessionVar(SessionVariable.HUGE_TABLE_DEFAULT_SAMPLE_ROWS)
                    .hugeTableDefaultSampleRows;
        } catch (Exception e) {
            LOG.warn("Failed to get value of huge_table_default_sample_rows, return default", e);
        }
        return StatisticConstants.HUGE_TABLE_DEFAULT_SAMPLE_ROWS;
    }

    public static long getHugeTableLowerBoundSizeInBytes() {
        try {
            return findConfigFromGlobalSessionVar(SessionVariable.HUGE_TABLE_LOWER_BOUND_SIZE_IN_BYTES)
                    .hugeTableLowerBoundSizeInBytes;
        } catch (Exception e) {
            LOG.warn("Failed to get value of huge_table_lower_bound_size_in_bytes, return default", e);
        }
        return StatisticConstants.HUGE_TABLE_LOWER_BOUND_SIZE_IN_BYTES;
    }

    public static long getHugePartitionLowerBoundRows() {
        return GlobalVariable.hugePartitionLowerBoundRows;
    }

    public static int getPartitionAnalyzeBatchSize() {
        return GlobalVariable.partitionAnalyzeBatchSize;
    }

    public static long getHugeTableAutoAnalyzeIntervalInMillis() {
        try {
            return findConfigFromGlobalSessionVar(SessionVariable.HUGE_TABLE_AUTO_ANALYZE_INTERVAL_IN_MILLIS)
                    .hugeTableAutoAnalyzeIntervalInMillis;
        } catch (Exception e) {
            LOG.warn("Failed to get value of huge_table_auto_analyze_interval_in_millis, return default", e);
        }
        return StatisticConstants.HUGE_TABLE_AUTO_ANALYZE_INTERVAL_IN_MILLIS;
    }

    public static long getExternalTableAutoAnalyzeIntervalInMillis() {
        try {
            return findConfigFromGlobalSessionVar(SessionVariable.EXTERNAL_TABLE_AUTO_ANALYZE_INTERVAL_IN_MILLIS)
                .externalTableAutoAnalyzeIntervalInMillis;
        } catch (Exception e) {
            LOG.warn("Failed to get value of externalTableAutoAnalyzeIntervalInMillis, return default", e);
        }
        return StatisticConstants.EXTERNAL_TABLE_AUTO_ANALYZE_INTERVAL_IN_MILLIS;
    }

    public static long getTableStatsHealthThreshold() {
        try {
            return findConfigFromGlobalSessionVar(SessionVariable.TABLE_STATS_HEALTH_THRESHOLD)
                    .tableStatsHealthThreshold;
        } catch (Exception e) {
            LOG.warn("Failed to get value of table_stats_health_threshold, return default", e);
        }
        return StatisticConstants.TABLE_STATS_HEALTH_THRESHOLD;
    }

    public static int getAnalyzeTimeout() {
        try {
            return findConfigFromGlobalSessionVar(SessionVariable.ANALYZE_TIMEOUT)
                    .analyzeTimeoutS;
        } catch (Exception e) {
            LOG.warn("Failed to get value of table_stats_health_threshold, return default", e);
        }
        return StatisticConstants.ANALYZE_TIMEOUT_IN_SEC;
    }

    public static int getAutoAnalyzeTableWidthThreshold() {
        try {
            return findConfigFromGlobalSessionVar(SessionVariable.AUTO_ANALYZE_TABLE_WIDTH_THRESHOLD)
                .autoAnalyzeTableWidthThreshold;
        } catch (Exception e) {
            LOG.warn("Failed to get value of auto_analyze_table_width_threshold, return default", e);
        }
        return StatisticConstants.AUTO_ANALYZE_TABLE_WIDTH_THRESHOLD;
    }

    public static String encodeValue(ResultRow row, int index) {
        if (row == null || row.getValues().size() <= index) {
            return "NULL";
        }
        return encodeString(row.get(index));
    }

    public static String encodeString(String value) {
        if (value == null) {
            return "NULL";
        } else {
            return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
        }
    }

    /**
     * Check if the given column name is a materialized view column.
     * @param table
     * @param columnName
     * @return True for mv column.
     */
    public static boolean isMvColumn(TableIf table, String columnName) {
        return table instanceof OlapTable
            && columnName.startsWith(CreateMaterializedViewStmt.MATERIALIZED_VIEW_NAME_PREFIX)
            || columnName.startsWith(CreateMaterializedViewStmt.MATERIALIZED_VIEW_AGGREGATE_NAME_PREFIX);
    }

    public static boolean isEmptyTable(TableIf table, AnalysisInfo.AnalysisMethod method) {
        int waitRowCountReportedTime = 75;
        if (!(table instanceof OlapTable) || method.equals(AnalysisInfo.AnalysisMethod.FULL)) {
            return false;
        }
        OlapTable olapTable = (OlapTable) table;
        for (int i = 0; i < waitRowCountReportedTime; i++) {
            if (olapTable.getRowCount() > 0) {
                return false;
            }
            // If visible version is 2, table is probably not empty. So we wait row count to be reported.
            // If visible version is not 2 and getRowCount return 0, we assume it is an empty table.
            if (olapTable.getVisibleVersion() != TableAttributes.TABLE_INIT_VERSION + 1) {
                return true;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOG.info("Sleep interrupted.", e);
            }
        }
        return true;
    }

    public static boolean needAnalyzeColumn(TableIf table, Pair<String, String> column) {
        if (column == null) {
            return false;
        }
        AnalysisManager manager = Env.getServingEnv().getAnalysisManager();
        TableStatsMeta tableStatsStatus = manager.findTableStatsStatus(table.getId());
        // Table never been analyzed, need analyze.
        if (tableStatsStatus == null) {
            return true;
        }
        // User injected column stats, don't do auto analyze, avoid overwrite user injected stats.
        if (tableStatsStatus.userInjected) {
            return false;
        }
        ColStatsMeta columnStatsMeta = tableStatsStatus.findColumnStatsMeta(column.first, column.second);
        // Column never been analyzed, need analyze.
        if (columnStatsMeta == null) {
            return true;
        }
        // Partition table partition stats never been collected.
        if (StatisticsUtil.enablePartitionAnalyze() && table.isPartitionedTable()
                && columnStatsMeta.partitionUpdateRows == null) {
            return true;
        }
        if (table instanceof OlapTable) {
            OlapTable olapTable = (OlapTable) table;
            // 0. Check new partition first time loaded flag.
            if (olapTable.isPartitionColumn(column.second) && tableStatsStatus.partitionChanged.get()) {
                return true;
            }
            // 1. Check row count.
            // TODO: One conner case. Last analyze row count is 0, but actually it's not 0 because isEmptyTable waiting.
            long currentRowCount = olapTable.getRowCount();
            long lastAnalyzeRowCount = columnStatsMeta.rowCount;
            // 1.1 Empty table -> non-empty table. Need analyze.
            if (currentRowCount != 0 && lastAnalyzeRowCount == 0) {
                return true;
            }
            // 1.2 Non-empty table -> empty table. Need analyze;
            if (currentRowCount == 0 && lastAnalyzeRowCount != 0) {
                return true;
            }
            // 1.3 Table is still empty. Not need to analyze. lastAnalyzeRowCount == 0 is always true here.
            if (currentRowCount == 0) {
                return false;
            }
            // 1.4 If row count changed more than the threshold, need analyze.
            // lastAnalyzeRowCount == 0 is always false here.
            double changeRate =
                    ((double) Math.abs(currentRowCount - lastAnalyzeRowCount) / lastAnalyzeRowCount) * 100.0;
            if (changeRate > (100 - StatisticsUtil.getTableStatsHealthThreshold())) {
                return true;
            }
            // 2. Check update rows.
            long currentUpdatedRows = tableStatsStatus.updatedRows.get();
            long lastAnalyzeUpdateRows = columnStatsMeta.updatedRows;
            changeRate = ((double) Math.abs(currentUpdatedRows - lastAnalyzeUpdateRows) / lastAnalyzeRowCount) * 100.0;
            if (changeRate > (100 - StatisticsUtil.getTableStatsHealthThreshold())) {
                return true;
            }
            // 3. Check partition
            return needAnalyzePartition(olapTable, tableStatsStatus, columnStatsMeta);
        } else {
            // Now, we only support Hive external table auto analyze.
            if (!(table instanceof HMSExternalTable)) {
                return false;
            }
            HMSExternalTable hmsTable = (HMSExternalTable) table;
            if (!hmsTable.getDlaType().equals(DLAType.HIVE)) {
                return false;
            }
            // External is hard to calculate change rate, use time interval to control analyze frequency.
            return System.currentTimeMillis()
                    - tableStatsStatus.updatedTime > StatisticsUtil.getExternalTableAutoAnalyzeIntervalInMillis();
        }
    }

    public static boolean needAnalyzePartition(OlapTable table, TableStatsMeta tableStatsStatus,
                                               ColStatsMeta columnStatsMeta) {
        if (!StatisticsUtil.enablePartitionAnalyze() || !table.isPartitionedTable()) {
            return false;
        }
        Collection<Partition> partitions = table.getPartitions();
        ConcurrentMap<Long, Long> partitionUpdateRows = columnStatsMeta.partitionUpdateRows;
        if (partitionUpdateRows == null) {
            return true;
        }
        // New partition added or old partition deleted.
        if (partitions.size() != partitionUpdateRows.size()) {
            return true;
        }
        int changedPartitions = 0;
        long hugePartitionLowerBoundRows = getHugePartitionLowerBoundRows();
        for (Partition p : partitions) {
            long id = p.getId();
            // Skip partition that is too large.
            if (p.getBaseIndex().getRowCount() > hugePartitionLowerBoundRows) {
                continue;
            }
            // New partition added.
            if (!partitionUpdateRows.containsKey(id)) {
                return true;
            }
            // Former skipped large partition is not large anymore. Need to analyze it.
            if (partitionUpdateRows.get(id) == -1) {
                return true;
            }
            long currentUpdateRows = tableStatsStatus.partitionUpdateRows.getOrDefault(id, 0L);
            long lastUpdateRows = partitionUpdateRows.get(id);
            long changedRows = currentUpdateRows - lastUpdateRows;
            if (changedRows > 0) {
                changedPartitions++;
                // Too much partition changed, need to reanalyze.
                if (changedPartitions >= UPDATED_PARTITION_THRESHOLD) {
                    return true;
                }
                double changeRate = (((double) changedRows) / currentUpdateRows) * 100;
                // One partition changed too much, need to reanalyze.
                if (changeRate > (100 - StatisticsUtil.getTableStatsHealthThreshold())) {
                    return true;
                }
            }
        }
        return false;
    }
}
