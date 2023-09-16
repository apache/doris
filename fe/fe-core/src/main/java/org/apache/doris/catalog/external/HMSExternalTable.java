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

package org.apache.doris.catalog.external;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HiveMetaStoreClientHelper;
import org.apache.doris.catalog.HudiUtils;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.HMSExternalCatalog;
import org.apache.doris.datasource.hive.PooledHiveMetaStoreClient;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.BaseAnalysisTask;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.HMSAnalysisTask;
import org.apache.doris.statistics.TableStatsMeta;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.thrift.THiveTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Hive metastore external table.
 */
@Getter
public class HMSExternalTable extends ExternalTable {
    private static final Logger LOG = LogManager.getLogger(HMSExternalTable.class);

    private static final Set<String> SUPPORTED_HIVE_FILE_FORMATS;
    private static final Set<String> SUPPORTED_HIVE_TRANSACTIONAL_FILE_FORMATS;

    private static final String TBL_PROP_TXN_PROPERTIES = "transactional_properties";
    private static final String TBL_PROP_INSERT_ONLY = "insert_only";
    private static final String NUM_ROWS = "numRows";

    static {
        SUPPORTED_HIVE_FILE_FORMATS = Sets.newHashSet();
        SUPPORTED_HIVE_FILE_FORMATS.add("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
        SUPPORTED_HIVE_FILE_FORMATS.add("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");
        SUPPORTED_HIVE_FILE_FORMATS.add("org.apache.hadoop.mapred.TextInputFormat");

        SUPPORTED_HIVE_TRANSACTIONAL_FILE_FORMATS = Sets.newHashSet();
        SUPPORTED_HIVE_TRANSACTIONAL_FILE_FORMATS.add("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");
    }

    private static final Set<String> SUPPORTED_HUDI_FILE_FORMATS;

    static {
        SUPPORTED_HUDI_FILE_FORMATS = Sets.newHashSet();
        SUPPORTED_HUDI_FILE_FORMATS.add("org.apache.hudi.hadoop.HoodieParquetInputFormat");
        SUPPORTED_HUDI_FILE_FORMATS.add("com.uber.hoodie.hadoop.HoodieInputFormat");
        SUPPORTED_HUDI_FILE_FORMATS.add("org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat");
        SUPPORTED_HUDI_FILE_FORMATS.add("com.uber.hoodie.hadoop.realtime.HoodieRealtimeInputFormat");
    }

    protected volatile org.apache.hadoop.hive.metastore.api.Table remoteTable = null;
    protected List<Column> partitionColumns;

    protected DLAType dlaType = DLAType.UNKNOWN;

    // No as precise as row count in TableStats, but better than none.
    private long estimatedRowCount = -1;

    // record the partition update time when enable hms event listener
    protected volatile long lastPartitionUpdateTime;

    public enum DLAType {
        UNKNOWN, HIVE, HUDI, ICEBERG, DELTALAKE
    }

    /**
     * Create hive metastore external table.
     *
     * @param id Table id.
     * @param name Table name.
     * @param dbName Database name.
     * @param catalog HMSExternalCatalog.
     */
    public HMSExternalTable(long id, String name, String dbName, HMSExternalCatalog catalog) {
        super(id, name, catalog, dbName, TableType.HMS_EXTERNAL_TABLE);
    }

    public HMSExternalTable(long id, String name, String dbName, HMSExternalCatalog catalog, TableType type) {
        super(id, name, catalog, dbName, type);
    }

    public boolean isSupportedHmsTable() {
        makeSureInitialized();
        return dlaType != DLAType.UNKNOWN;
    }

    protected synchronized void makeSureInitialized() {
        super.makeSureInitialized();
        if (!objectCreated) {
            remoteTable = ((HMSExternalCatalog) catalog).getClient().getTable(dbName, name);
            if (remoteTable == null) {
                dlaType = DLAType.UNKNOWN;
            } else {
                if (supportedIcebergTable()) {
                    dlaType = DLAType.ICEBERG;
                } else if (supportedHoodieTable()) {
                    dlaType = DLAType.HUDI;
                } else if (supportedHiveTable()) {
                    dlaType = DLAType.HIVE;
                } else {
                    dlaType = DLAType.UNKNOWN;
                }
            }
            objectCreated = true;
            estimatedRowCount = getRowCountFromExternalSource(true);
        }
    }

    /**
     * Now we only support cow table in iceberg.
     */
    private boolean supportedIcebergTable() {
        Map<String, String> paras = remoteTable.getParameters();
        if (paras == null) {
            return false;
        }
        return paras.containsKey("table_type") && paras.get("table_type").equalsIgnoreCase("ICEBERG");
    }

    /**
     * `HoodieParquetInputFormat`: `Snapshot Queries` on cow and mor table and `Read Optimized Queries` on cow table
     */
    private boolean supportedHoodieTable() {
        if (remoteTable.getSd() == null) {
            return false;
        }
        String inputFormatName = remoteTable.getSd().getInputFormat();
        return inputFormatName != null && SUPPORTED_HUDI_FILE_FORMATS.contains(inputFormatName);
    }

    public boolean isHoodieCowTable() {
        if (remoteTable.getSd() == null) {
            return false;
        }
        String inputFormatName = remoteTable.getSd().getInputFormat();
        return "org.apache.hudi.hadoop.HoodieParquetInputFormat".equals(inputFormatName);
    }

    /**
     * Now we only support three file input format hive tables: parquet/orc/text.
     * Support managed_table and external_table.
     */
    private boolean supportedHiveTable() {
        String inputFileFormat = remoteTable.getSd().getInputFormat();
        boolean supportedFileFormat = inputFileFormat != null && SUPPORTED_HIVE_FILE_FORMATS.contains(inputFileFormat);
        LOG.debug("hms table {} is {} with file format: {}", name, remoteTable.getTableType(), inputFileFormat);
        return supportedFileFormat;
    }

    /**
     * Get the related remote hive metastore table.
     */
    public org.apache.hadoop.hive.metastore.api.Table getRemoteTable() {
        makeSureInitialized();
        return remoteTable;
    }

    public List<Type> getPartitionColumnTypes() {
        makeSureInitialized();
        getFullSchema();
        return partitionColumns.stream().map(c -> c.getType()).collect(Collectors.toList());
    }

    public List<Column> getPartitionColumns() {
        makeSureInitialized();
        getFullSchema();
        return partitionColumns;
    }

    public boolean isHiveTransactionalTable() {
        return dlaType == DLAType.HIVE && AcidUtils.isTransactionalTable(remoteTable)
                && isSupportedTransactionalFileFormat();
    }

    private boolean isSupportedTransactionalFileFormat() {
        // Sometimes we meet "transactional" = "true" but format is parquet, which is not supported.
        // So we need to check the input format for transactional table.
        String inputFormatName = remoteTable.getSd().getInputFormat();
        return inputFormatName != null && SUPPORTED_HIVE_TRANSACTIONAL_FILE_FORMATS.contains(inputFormatName);
    }

    public boolean isFullAcidTable() {
        return dlaType == DLAType.HIVE && AcidUtils.isFullAcidTable(remoteTable);
    }

    @Override
    public boolean isView() {
        makeSureInitialized();
        return remoteTable.isSetViewOriginalText() || remoteTable.isSetViewExpandedText();
    }

    @Override
    public String getMysqlType() {
        return type.name();
    }

    @Override
    public String getEngine() {
        switch (type) {
            case HIVE:
                return "Hive";
            case ICEBERG:
                return "Iceberg";
            case HUDI:
                return "Hudi";
            default:
                return null;
        }
    }

    @Override
    public String getComment() {
        return "";
    }

    @Override
    public long getCreateTime() {
        return 0;
    }

    @Override
    public long getUpdateTime() {
        return 0;
    }

    @Override
    public long getRowCount() {
        makeSureInitialized();
        long rowCount = getRowCountFromExternalSource(false);
        if (rowCount == -1) {
            LOG.debug("Will estimate row count from file list.");
            rowCount = StatisticsUtil.getRowCountFromFileList(this);
        }
        return rowCount;
    }

    private long getRowCountFromExternalSource(boolean isInit) {
        long rowCount;
        switch (dlaType) {
            case HIVE:
                rowCount = StatisticsUtil.getHiveRowCount(this, isInit);
                break;
            case ICEBERG:
                rowCount = StatisticsUtil.getIcebergRowCount(this);
                break;
            default:
                LOG.warn("getRowCount for dlaType {} is not supported.", dlaType);
                rowCount = -1;
        }
        return rowCount;
    }

    @Override
    public long getDataLength() {
        return 0;
    }

    @Override
    public long getAvgRowLength() {
        return 0;
    }

    public long getLastCheckTime() {
        return 0;
    }

    /**
     * get the dla type for scan node to get right information.
     */
    public DLAType getDlaType() {
        makeSureInitialized();
        return dlaType;
    }

    @Override
    public TTableDescriptor toThrift() {
        List<Column> schema = getFullSchema();
        THiveTable tHiveTable = new THiveTable(dbName, name, new HashMap<>());
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.HIVE_TABLE, schema.size(), 0,
                getName(), dbName);
        tTableDescriptor.setHiveTable(tHiveTable);
        return tTableDescriptor;
    }

    @Override
    public BaseAnalysisTask createAnalysisTask(AnalysisInfo info) {
        makeSureInitialized();
        return new HMSAnalysisTask(info);
    }

    public String getViewText() {
        String viewText = getViewExpandedText();
        if (StringUtils.isNotEmpty(viewText)) {
            return viewText;
        }
        return getViewOriginalText();
    }

    public String getViewExpandedText() {
        LOG.debug("View expanded text of hms table [{}.{}.{}] : {}",
                this.getCatalog().getName(), this.getDbName(), this.getName(), remoteTable.getViewExpandedText());
        return remoteTable.getViewExpandedText();
    }

    public String getViewOriginalText() {
        LOG.debug("View original text of hms table [{}.{}.{}] : {}",
                this.getCatalog().getName(), this.getDbName(), this.getName(), remoteTable.getViewOriginalText());
        return remoteTable.getViewOriginalText();
    }

    public String getMetastoreUri() {
        return ((HMSExternalCatalog) catalog).getHiveMetastoreUris();
    }

    public String getHiveVersion() {
        return ((HMSExternalCatalog) catalog).getHiveVersion();
    }

    public Map<String, String> getCatalogProperties() {
        return catalog.getProperties();
    }

    public Map<String, String> getHadoopProperties() {
        return catalog.getCatalogProperty().getHadoopProperties();
    }

    public List<ColumnStatisticsObj> getHiveTableColumnStats(List<String> columns) {
        PooledHiveMetaStoreClient client = ((HMSExternalCatalog) catalog).getClient();
        return client.getTableColumnStatistics(dbName, name, columns);
    }

    public Map<String, List<ColumnStatisticsObj>> getHivePartitionColumnStats(
            List<String> partNames, List<String> columns) {
        PooledHiveMetaStoreClient client = ((HMSExternalCatalog) catalog).getClient();
        return client.getPartitionColumnStatistics(dbName, name, partNames, columns);
    }

    public Partition getPartition(List<String> partitionValues) {
        PooledHiveMetaStoreClient client = ((HMSExternalCatalog) catalog).getClient();
        return client.getPartition(dbName, name, partitionValues);
    }

    @Override
    public Set<String> getPartitionNames() {
        makeSureInitialized();
        PooledHiveMetaStoreClient client = ((HMSExternalCatalog) catalog).getClient();
        List<String> names = client.listPartitionNames(dbName, name);
        return new HashSet<>(names);
    }

    @Override
    public List<Column> initSchema() {
        makeSureInitialized();
        List<Column> columns;
        List<FieldSchema> schema = ((HMSExternalCatalog) catalog).getClient().getSchema(dbName, name);
        if (dlaType.equals(DLAType.ICEBERG)) {
            columns = getIcebergSchema(schema);
        } else if (dlaType.equals(DLAType.HUDI)) {
            columns = getHudiSchema(schema);
        } else {
            List<Column> tmpSchema = Lists.newArrayListWithCapacity(schema.size());
            for (FieldSchema field : schema) {
                tmpSchema.add(new Column(field.getName(),
                        HiveMetaStoreClientHelper.hiveTypeToDorisType(field.getType()), true, null,
                        true, field.getComment(), true, -1));
            }
            columns = tmpSchema;
        }
        initPartitionColumns(columns);
        return columns;
    }

    public List<Column> getHudiSchema(List<FieldSchema> hmsSchema) {
        org.apache.avro.Schema hudiSchema = HiveMetaStoreClientHelper.getHudiTableSchema(this);
        List<Column> tmpSchema = Lists.newArrayListWithCapacity(hmsSchema.size());
        for (org.apache.avro.Schema.Field hudiField : hudiSchema.getFields()) {
            String columnName = hudiField.name().toLowerCase(Locale.ROOT);
            tmpSchema.add(new Column(columnName, HudiUtils.fromAvroHudiTypeToDorisType(hudiField.schema()),
                    true, null, true, null, "", true, null, -1, null));
        }
        return tmpSchema;
    }

    @Override
    public long estimatedRowCount() {
        try {
            TableStatsMeta tableStats = Env.getCurrentEnv().getAnalysisManager().findTableStatsStatus(id);
            if (tableStats != null) {
                long rowCount = tableStats.rowCount;
                LOG.debug("Estimated row count for db {} table {} is {}.", dbName, name, rowCount);
                return rowCount;
            }

            if (estimatedRowCount != -1) {
                return estimatedRowCount;
            }
            // Cache the estimated row count in this structure
            // though the table never get analyzed, since the row estimation might be expensive caused by RPC.
            estimatedRowCount = getRowCount();
            return estimatedRowCount;
        } catch (Exception e) {
            LOG.warn("Fail to get row count for table {}", name, e);
        }
        return 1;
    }

    private List<Column> getIcebergSchema(List<FieldSchema> hmsSchema) {
        Table icebergTable = Env.getCurrentEnv().getExtMetaCacheMgr().getIcebergMetadataCache().getIcebergTable(this);
        Schema schema = icebergTable.schema();
        List<Column> tmpSchema = Lists.newArrayListWithCapacity(hmsSchema.size());
        for (FieldSchema field : hmsSchema) {
            tmpSchema.add(new Column(field.getName(),
                    HiveMetaStoreClientHelper.hiveTypeToDorisType(field.getType(),
                            IcebergExternalTable.ICEBERG_DATETIME_SCALE_MS),
                    true, null, true, false, null, field.getComment(), true, null,
                    schema.caseInsensitiveFindField(field.getName()).fieldId(), null));
        }
        return tmpSchema;
    }

    protected void initPartitionColumns(List<Column> schema) {
        List<String> partitionKeys = remoteTable.getPartitionKeys().stream().map(FieldSchema::getName)
                .collect(Collectors.toList());
        partitionColumns = Lists.newArrayListWithCapacity(partitionKeys.size());
        for (String partitionKey : partitionKeys) {
            // Do not use "getColumn()", which will cause dead loop
            for (Column column : schema) {
                if (partitionKey.equals(column.getName())) {
                    // For partition column, if it is string type, change it to varchar(65535)
                    // to be same as doris managed table.
                    // This is to avoid some unexpected behavior such as different partition pruning result
                    // between doris managed table and external table.
                    if (column.getType().getPrimitiveType() == PrimitiveType.STRING) {
                        column.setType(ScalarType.createVarcharType(ScalarType.MAX_VARCHAR_LENGTH));
                    }
                    partitionColumns.add(column);
                    break;
                }
            }
        }
        LOG.debug("get {} partition columns for table: {}", partitionColumns.size(), name);
    }

    @Override
    public Optional<ColumnStatistic> getColumnStatistic(String colName) {
        makeSureInitialized();
        switch (dlaType) {
            case HIVE:
                return getHiveColumnStats(colName);
            case ICEBERG:
                return StatisticsUtil.getIcebergColumnStats(colName,
                    Env.getCurrentEnv().getExtMetaCacheMgr().getIcebergMetadataCache().getIcebergTable(this));
            default:
                LOG.warn("get column stats for dlaType {} is not supported.", dlaType);
        }
        return Optional.empty();
    }

    private Optional<ColumnStatistic> getHiveColumnStats(String colName) {
        List<ColumnStatisticsObj> tableStats = getHiveTableColumnStats(Lists.newArrayList(colName));
        if (tableStats == null || tableStats.isEmpty()) {
            LOG.debug(String.format("No table stats found in Hive metastore for column %s in table %s.",
                    colName, name));
            return Optional.empty();
        }

        Column column = getColumn(colName);
        if (column == null) {
            LOG.warn(String.format("No column %s in table %s.", colName, name));
            return Optional.empty();
        }
        Map<String, String> parameters = remoteTable.getParameters();
        ColumnStatisticBuilder columnStatisticBuilder = new ColumnStatisticBuilder();
        double count = parameters.containsKey(NUM_ROWS) ? Double.parseDouble(parameters.get(NUM_ROWS)) : 0;
        columnStatisticBuilder.setCount(count);
        // The tableStats length is at most 1.
        for (ColumnStatisticsObj tableStat : tableStats) {
            if (!tableStat.isSetStatsData()) {
                continue;
            }
            ColumnStatisticsData data = tableStat.getStatsData();
            try {
                setStatData(column, data, columnStatisticBuilder, count);
            } catch (AnalysisException e) {
                LOG.debug(e);
                return Optional.empty();
            }
        }

        return Optional.of(columnStatisticBuilder.build());
    }

    private void setStatData(Column col, ColumnStatisticsData data, ColumnStatisticBuilder builder, double count)
            throws AnalysisException {
        long ndv = 0;
        long nulls = 0;
        String min = "";
        String max = "";
        double colSize = 0;
        if (!data.isSetStringStats()) {
            colSize = count * col.getType().getSlotSize();
        }
        // Collect ndv, nulls, min and max for different data type.
        if (data.isSetLongStats()) {
            LongColumnStatsData longStats = data.getLongStats();
            ndv = longStats.getNumDVs();
            nulls = longStats.getNumNulls();
            min = String.valueOf(longStats.getLowValue());
            max = String.valueOf(longStats.getHighValue());
        } else if (data.isSetStringStats()) {
            StringColumnStatsData stringStats = data.getStringStats();
            ndv = stringStats.getNumDVs();
            nulls = stringStats.getNumNulls();
            double avgColLen = stringStats.getAvgColLen();
            colSize = Math.round(avgColLen * count);
        } else if (data.isSetDecimalStats()) {
            DecimalColumnStatsData decimalStats = data.getDecimalStats();
            ndv = decimalStats.getNumDVs();
            nulls = decimalStats.getNumNulls();
            if (decimalStats.isSetLowValue()) {
                Decimal lowValue = decimalStats.getLowValue();
                if (lowValue != null) {
                    BigDecimal lowDecimal = new BigDecimal(new BigInteger(lowValue.getUnscaled()), lowValue.getScale());
                    min = lowDecimal.toString();
                }
            }
            if (decimalStats.isSetHighValue()) {
                Decimal highValue = decimalStats.getHighValue();
                if (highValue != null) {
                    BigDecimal highDecimal =
                            new BigDecimal(new BigInteger(highValue.getUnscaled()), highValue.getScale());
                    max = highDecimal.toString();
                }
            }
        } else if (data.isSetDoubleStats()) {
            DoubleColumnStatsData doubleStats = data.getDoubleStats();
            ndv = doubleStats.getNumDVs();
            nulls = doubleStats.getNumNulls();
            min = String.valueOf(doubleStats.getLowValue());
            max = String.valueOf(doubleStats.getHighValue());
        } else if (data.isSetDateStats()) {
            DateColumnStatsData dateStats = data.getDateStats();
            ndv = dateStats.getNumDVs();
            nulls = dateStats.getNumNulls();
            if (dateStats.isSetLowValue()) {
                org.apache.hadoop.hive.metastore.api.Date lowValue = dateStats.getLowValue();
                if (lowValue != null) {
                    LocalDate lowDate = LocalDate.ofEpochDay(lowValue.getDaysSinceEpoch());
                    min = lowDate.toString();
                }
            }
            if (dateStats.isSetHighValue()) {
                org.apache.hadoop.hive.metastore.api.Date highValue = dateStats.getHighValue();
                if (highValue != null) {
                    LocalDate highDate = LocalDate.ofEpochDay(highValue.getDaysSinceEpoch());
                    max = highDate.toString();
                }
            }
        } else {
            LOG.debug(String.format("Not suitable data type for column %s", col.getName()));
            throw new RuntimeException("Not supported data type.");
        }
        builder.setNdv(ndv);
        builder.setNumNulls(nulls);
        builder.setDataSize(colSize);
        builder.setAvgSizeByte(colSize / count);
        if (!min.equals("")) {
            builder.setMinValue(StatisticsUtil.convertToDouble(col.getType(), min));
            builder.setMinExpr(StatisticsUtil.readableValue(col.getType(), min));
        } else {
            builder.setMinValue(Double.MIN_VALUE);
        }
        if (!max.equals("")) {
            builder.setMaxValue(StatisticsUtil.convertToDouble(col.getType(), max));
            builder.setMaxExpr(StatisticsUtil.readableValue(col.getType(), max));
        } else {
            builder.setMaxValue(Double.MAX_VALUE);
        }
    }

    public void updatePartitionUpdateTime(long updateTime) {
        this.lastPartitionUpdateTime = updateTime;
    }

    @Override
    public void gsonPostProcess() throws IOException {
        super.gsonPostProcess();
        estimatedRowCount = -1;
    }
}


