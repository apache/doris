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
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.VariantType;
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.HMSExternalCatalog;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;
import org.apache.doris.datasource.hive.HivePartition;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.Histogram;
import org.apache.doris.statistics.ResultRow;
import org.apache.doris.statistics.StatisticConstants;
import org.apache.doris.system.Frontend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;
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
    private static final Logger LOG = LogManager.getLogger(StatisticsUtil.class);

    private static final String ID_DELIMITER = "-";

    private static final String TOTAL_SIZE = "totalSize";
    private static final String NUM_ROWS = "numRows";

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
        if (!FeConstants.enableInternalSchemaDb) {
            return Collections.emptyList();
        }
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

    public static ColumnStatistic deserializeToColumnStatistics(List<ResultRow> resultBatches)
            throws Exception {
        if (CollectionUtils.isEmpty(resultBatches)) {
            return null;
        }
        return ColumnStatistic.fromResultRow(resultBatches);
    }

    public static List<Histogram> deserializeToHistogramStatistics(List<ResultRow> resultBatches)
            throws Exception {
        return resultBatches.stream().map(Histogram::fromResultRow).collect(Collectors.toList());
    }

    public static AutoCloseConnectContext buildConnectContext() {
        ConnectContext connectContext = new ConnectContext();
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        sessionVariable.internalSession = true;
        sessionVariable.setMaxExecMemByte(Config.statistics_sql_mem_limit_in_bytes);
        sessionVariable.cpuResourceLimit = Config.cpu_resource_limit_per_analyze_task;
        sessionVariable.setEnableInsertStrict(true);
        sessionVariable.parallelExecInstanceNum = Config.statistics_sql_parallel_exec_instance_num;
        sessionVariable.parallelPipelineTaskNum = Config.statistics_sql_parallel_exec_instance_num;
        sessionVariable.setEnableNereidsPlanner(false);
        sessionVariable.enableProfile = false;
        sessionVariable.queryTimeoutS = Config.analyze_task_timeout_in_hours * 60 * 60;
        sessionVariable.insertTimeoutS = Config.analyze_task_timeout_in_hours * 60 * 60;
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
                // no need to check precision and scale, since V2 is fixed point
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
     * @param totalRows The current number of rows in the table
     *         the healthier the statistics of the table
     * @return Health, the value range is [0, 100], the larger the value,
     */
    public static int getTableHealth(long totalRows, long updatedRows) {
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
            return Long.parseLong(parameters.get(NUM_ROWS));
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
            return 1;
        }
        return totalSize / estimatedRowSize;
    }

    /**
     * Estimate iceberg table row count.
     * Get the row count by adding all task file recordCount.
     *
     * @param table Iceberg HMSExternalTable to estimate row count.
     * @return estimated row count
     */
    public static long getIcebergRowCount(HMSExternalTable table) {
        long rowCount = 0;
        try {
            Table icebergTable = Env.getCurrentEnv()
                    .getExtMetaCacheMgr()
                    .getIcebergMetadataCache()
                    .getIcebergTable(table);
            TableScan tableScan = icebergTable.newScan().includeColumnStats();
            for (FileScanTask task : tableScan.planFiles()) {
                rowCount += task.file().recordCount();
            }
            return rowCount;
        } catch (Exception e) {
            LOG.warn("Fail to collect row count for db {} table {}", table.getDbName(), table.getName(), e);
        }
        return -1;
    }

    /**
     * Estimate hive table row count : totalFileSize/estimatedRowSize
     *
     * @param table Hive HMSExternalTable to estimate row count.
     * @return estimated row count
     */
    public static long getRowCountFromFileList(HMSExternalTable table) {
        if (table.isView()) {
            return 0;
        }
        HiveMetaStoreCache cache = Env.getCurrentEnv().getExtMetaCacheMgr()
                .getMetaStoreCache((HMSExternalCatalog) table.getCatalog());
        List<Type> partitionColumnTypes = table.getPartitionColumnTypes();
        HiveMetaStoreCache.HivePartitionValues partitionValues = null;
        List<HivePartition> hivePartitions = Lists.newArrayList();
        int samplePartitionSize = Config.hive_stats_partition_sample_size;
        int totalPartitionSize = 1;
        // Get table partitions from cache.
        if (!partitionColumnTypes.isEmpty()) {
            // It is ok to get partition values from cache,
            // no need to worry that this call will invalid or refresh the cache.
            // because it has enough space to keep partition info of all tables in cache.
            partitionValues = cache.getPartitionValues(table.getDbName(), table.getName(), partitionColumnTypes);
        }
        if (partitionValues != null) {
            Map<Long, PartitionItem> idToPartitionItem = partitionValues.getIdToPartitionItem();
            totalPartitionSize = idToPartitionItem.size();
            Collection<PartitionItem> partitionItems;
            List<List<String>> partitionValuesList;
            // If partition number is too large, randomly choose part of them to estimate the whole table.
            if (samplePartitionSize < totalPartitionSize) {
                List<PartitionItem> items = new ArrayList<>(idToPartitionItem.values());
                Collections.shuffle(items);
                partitionItems = items.subList(0, samplePartitionSize);
                partitionValuesList = Lists.newArrayListWithCapacity(samplePartitionSize);
            } else {
                partitionItems = idToPartitionItem.values();
                partitionValuesList = Lists.newArrayListWithCapacity(totalPartitionSize);
            }
            for (PartitionItem item : partitionItems) {
                partitionValuesList.add(((ListPartitionItem) item).getItems().get(0).getPartitionValuesAsStringList());
            }
            // get partitions without cache, so that it will not invalid the cache when executing
            // non query request such as `show table status`
            hivePartitions = cache.getAllPartitionsWithoutCache(table.getDbName(), table.getName(),
                    partitionValuesList);
        } else {
            hivePartitions.add(new HivePartition(table.getDbName(), table.getName(), true,
                    table.getRemoteTable().getSd().getInputFormat(),
                    table.getRemoteTable().getSd().getLocation(), null));
        }
        // Get files for all partitions.
        List<HiveMetaStoreCache.FileCacheValue> filesByPartitions = cache.getFilesByPartitionsWithoutCache(
                hivePartitions, true);
        long totalSize = 0;
        // Calculate the total file size.
        for (HiveMetaStoreCache.FileCacheValue files : filesByPartitions) {
            for (HiveMetaStoreCache.HiveFileStatus file : files.getFiles()) {
                totalSize += file.getLength();
            }
        }
        // Estimate row count: totalSize/estimatedRowSize
        long estimatedRowSize = 0;
        for (Column column : table.getFullSchema()) {
            estimatedRowSize += column.getDataType().getSlotSize();
        }
        if (estimatedRowSize == 0) {
            return 1;
        }
        if (samplePartitionSize < totalPartitionSize) {
            totalSize = totalSize * totalPartitionSize / samplePartitionSize;
        }
        return totalSize / estimatedRowSize;
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
        columnStatisticBuilder.setMaxValue(Double.MAX_VALUE);
        columnStatisticBuilder.setMinValue(Double.MIN_VALUE);
        columnStatisticBuilder.setDataSize(0);
        columnStatisticBuilder.setAvgSizeByte(0);
        columnStatisticBuilder.setNumNulls(0);
        for (FileScanTask task : tableScan.planFiles()) {
            processDataFile(task.file(), task.spec(), colName, columnStatisticBuilder);
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
                || type instanceof VariantType;
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
        return org.apache.commons.lang3.StringUtils.replace(str, "'", "''");
    }

    public static boolean isExternalTable(String catalogName, String dbName, String tblName) {
        TableIf table;
        try {
            table = StatisticsUtil.findTable(catalogName, dbName, tblName);
        } catch (Throwable e) {
            LOG.warn(e.getMessage());
            return false;
        }
        return table.getType().equals(TableType.HMS_EXTERNAL_TABLE);
    }
}
