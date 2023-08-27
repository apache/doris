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

package org.apache.doris.common.util;

import org.apache.doris.analysis.DataSortInfo;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EsResource;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.policy.Policy;
import org.apache.doris.policy.StoragePolicy;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TCompressionType;
import org.apache.doris.thrift.TSortType;
import org.apache.doris.thrift.TStorageFormat;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.thrift.TTabletType;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PropertyAnalyzer {

    public static final String PROPERTIES_SHORT_KEY = "short_key";
    public static final String PROPERTIES_REPLICATION_NUM = "replication_num";
    public static final String PROPERTIES_REPLICATION_ALLOCATION = "replication_allocation";
    public static final String PROPERTIES_STORAGE_TYPE = "storage_type";
    public static final String PROPERTIES_STORAGE_MEDIUM = "storage_medium";
    public static final String PROPERTIES_STORAGE_COOLDOWN_TIME = "storage_cooldown_time";
    // base time for the data in the partition
    public static final String PROPERTIES_DATA_BASE_TIME = "data_base_time_ms";
    // for 1.x -> 2.x migration
    public static final String PROPERTIES_VERSION_INFO = "version_info";
    // for restore
    public static final String PROPERTIES_SCHEMA_VERSION = "schema_version";

    public static final String PROPERTIES_BF_COLUMNS = "bloom_filter_columns";
    public static final String PROPERTIES_BF_FPP = "bloom_filter_fpp";

    public static final String PROPERTIES_COLUMN_SEPARATOR = "column_separator";
    public static final String PROPERTIES_LINE_DELIMITER = "line_delimiter";

    public static final String PROPERTIES_COLOCATE_WITH = "colocate_with";

    public static final String PROPERTIES_TIMEOUT = "timeout";
    public static final String PROPERTIES_COMPRESSION = "compression";

    public static final String PROPERTIES_ENABLE_LIGHT_SCHEMA_CHANGE = "light_schema_change";

    public static final String PROPERTIES_DISTRIBUTION_TYPE = "distribution_type";
    public static final String PROPERTIES_SEND_CLEAR_ALTER_TASK = "send_clear_alter_tasks";
    /*
     * for upgrade alpha rowset to beta rowset, valid value: v1, v2
     * v1: alpha rowset
     * v2: beta rowset
     */
    public static final String PROPERTIES_STORAGE_FORMAT = "storage_format";

    public static final String PROPERTIES_INMEMORY = "in_memory";

    // _auto_bucket can only set in create table stmt rewrite bucket and can not be changed
    public static final String PROPERTIES_AUTO_BUCKET = "_auto_bucket";
    public static final String PROPERTIES_ESTIMATE_PARTITION_SIZE = "estimate_partition_size";

    public static final String PROPERTIES_TABLET_TYPE = "tablet_type";

    public static final String PROPERTIES_STRICT_RANGE = "strict_range";
    public static final String PROPERTIES_USE_TEMP_PARTITION_NAME = "use_temp_partition_name";

    public static final String PROPERTIES_TYPE = "type";
    // This is common prefix for function column
    public static final String PROPERTIES_FUNCTION_COLUMN = "function_column";
    public static final String PROPERTIES_SEQUENCE_TYPE = "sequence_type";
    public static final String PROPERTIES_SEQUENCE_COL = "sequence_col";

    public static final String PROPERTIES_SWAP_TABLE = "swap";

    public static final String TAG_LOCATION = "tag.location";

    public static final String PROPERTIES_DISABLE_QUERY = "disable_query";

    public static final String PROPERTIES_DISABLE_LOAD = "disable_load";

    public static final String PROPERTIES_STORAGE_POLICY = "storage_policy";

    public static final String PROPERTIES_DISABLE_AUTO_COMPACTION = "disable_auto_compaction";

    public static final String PROPERTIES_ENABLE_SINGLE_REPLICA_COMPACTION = "enable_single_replica_compaction";

    public static final String PROPERTIES_STORE_ROW_COLUMN = "store_row_column";

    public static final String PROPERTIES_SKIP_WRITE_INDEX_ON_LOAD = "skip_write_index_on_load";

    public static final String PROPERTIES_COMPACTION_POLICY = "compaction_policy";

    public static final String PROPERTIES_TIME_SERIES_COMPACTION_GOAL_SIZE_MBYTES =
                                                        "time_series_compaction_goal_size_mbytes";

    public static final String PROPERTIES_TIME_SERIES_COMPACTION_FILE_COUNT_THRESHOLD =
                                                        "time_series_compaction_file_count_threshold";

    public static final String PROPERTIES_TIME_SERIES_COMPACTION_TIME_THRESHOLD_SECONDS =
                                                        "time_series_compaction_time_threshold_seconds";
    public static final String PROPERTIES_MUTABLE = "mutable";

    public static final String PROPERTIES_IS_BEING_SYNCED = "is_being_synced";

    // binlog.enable, binlog.ttl_seconds, binlog.max_bytes, binlog.max_history_nums
    public static final String PROPERTIES_BINLOG_PREFIX = "binlog.";
    public static final String PROPERTIES_BINLOG_ENABLE = "binlog.enable";
    public static final String PROPERTIES_BINLOG_TTL_SECONDS = "binlog.ttl_seconds";
    public static final String PROPERTIES_BINLOG_MAX_BYTES = "binlog.max_bytes";
    public static final String PROPERTIES_BINLOG_MAX_HISTORY_NUMS = "binlog.max_history_nums";

    public static final String PROPERTIES_ENABLE_DUPLICATE_WITHOUT_KEYS_BY_DEFAULT =
                                                                        "enable_duplicate_without_keys_by_default";
    // For unique key data model, the feature Merge-on-Write will leverage a primary
    // key index and a delete-bitmap to mark duplicate keys as deleted in load stage,
    // which can avoid the merging cost in read stage, and accelerate the aggregation
    // query performance significantly.
    // For the detail design, see the [DISP-018](https://cwiki.apache.org/confluence/
    // display/DORIS/DSIP-018%3A+Support+Merge-On-Write+implementation+for+UNIQUE+KEY+data+model)
    public static final String ENABLE_UNIQUE_KEY_MERGE_ON_WRITE = "enable_unique_key_merge_on_write";
    private static final Logger LOG = LogManager.getLogger(PropertyAnalyzer.class);
    private static final String COMMA_SEPARATOR = ",";
    private static final double MAX_FPP = 0.05;
    private static final double MIN_FPP = 0.0001;

    // compaction policy
    public static final String SIZE_BASED_COMPACTION_POLICY = "size_based";
    public static final String TIME_SERIES_COMPACTION_POLICY = "time_series";
    public static final long TIME_SERIES_COMPACTION_GOAL_SIZE_MBYTES_DEFAULT_VALUE = 1024;
    public static final long TIME_SERIES_COMPACTION_FILE_COUNT_THRESHOLD_DEFAULT_VALUE = 2000;
    public static final long TIME_SERIES_COMPACTION_TIME_THRESHOLD_SECONDS_DEFAULT_VALUE = 3600;




    /**
     * check and replace members of DataProperty by properties.
     *
     * @param properties      key->value for members to change.
     * @param oldDataProperty old DataProperty
     * @return new DataProperty
     * @throws AnalysisException property has invalid key->value
     */
    public static DataProperty analyzeDataProperty(Map<String, String> properties, final DataProperty oldDataProperty)
            throws AnalysisException {
        if (properties == null || properties.isEmpty()) {
            return oldDataProperty;
        }

        TStorageMedium storageMedium = oldDataProperty.getStorageMedium();
        long cooldownTimestamp = oldDataProperty.getCooldownTimeMs();
        final String oldStoragePolicy = oldDataProperty.getStoragePolicy();
        // When we create one table with table's property set storage policy,
        // the properties wouldn't contain storage policy so the hasStoragePolicy would be false,
        // then we would just set the partition's storage policy the same as the table's
        String newStoragePolicy = oldStoragePolicy;
        boolean hasStoragePolicy = false;
        boolean storageMediumSpecified = false;

        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.equalsIgnoreCase(PROPERTIES_STORAGE_MEDIUM)) {
                if (value.equalsIgnoreCase(TStorageMedium.SSD.name())) {
                    storageMedium = TStorageMedium.SSD;
                    storageMediumSpecified = true;
                } else if (value.equalsIgnoreCase(TStorageMedium.HDD.name())) {
                    storageMedium = TStorageMedium.HDD;
                    storageMediumSpecified = true;
                } else {
                    throw new AnalysisException("Invalid storage medium: " + value);
                }
            } else if (key.equalsIgnoreCase(PROPERTIES_STORAGE_COOLDOWN_TIME)) {
                DateLiteral dateLiteral = new DateLiteral(value, ScalarType.getDefaultDateType(Type.DATETIME));
                cooldownTimestamp = dateLiteral.unixTimestamp(TimeUtils.getTimeZone());
            } else if (!hasStoragePolicy && key.equalsIgnoreCase(PROPERTIES_STORAGE_POLICY)) {
                if (!Strings.isNullOrEmpty(value)) {
                    hasStoragePolicy = true;
                    newStoragePolicy = value;
                }
            }
        } // end for properties

        properties.remove(PROPERTIES_STORAGE_MEDIUM);
        properties.remove(PROPERTIES_STORAGE_COOLDOWN_TIME);
        properties.remove(PROPERTIES_STORAGE_POLICY);
        properties.remove(PROPERTIES_DATA_BASE_TIME);

        Preconditions.checkNotNull(storageMedium);

        if (storageMedium == TStorageMedium.HDD) {
            cooldownTimestamp = DataProperty.MAX_COOLDOWN_TIME_MS;
            LOG.info("Can not assign cool down timestamp to HDD storage medium, ignore user setting.");
        }

        boolean hasCooldown = cooldownTimestamp != DataProperty.MAX_COOLDOWN_TIME_MS;
        long currentTimeMs = System.currentTimeMillis();
        if (storageMedium == TStorageMedium.SSD && hasCooldown) {
            if (cooldownTimestamp <= currentTimeMs) {
                throw new AnalysisException(
                        "Cool down time: " + cooldownTimestamp + " should later than now: " + currentTimeMs);
            }
        }

        if (storageMedium == TStorageMedium.SSD && !hasCooldown) {
            cooldownTimestamp = DataProperty.MAX_COOLDOWN_TIME_MS;
        }

        if (hasStoragePolicy) {
            // check remote storage policy
            StoragePolicy checkedPolicy = StoragePolicy.ofCheck(newStoragePolicy);
            Policy policy = Env.getCurrentEnv().getPolicyMgr().getPolicy(checkedPolicy);
            if (!(policy instanceof StoragePolicy)) {
                throw new AnalysisException("No PolicyStorage: " + newStoragePolicy);
            }

            StoragePolicy storagePolicy = (StoragePolicy) policy;
            // Consider a scenario where if cold data has already been uploaded to resource A,
            // and the user attempts to modify the policy to upload it to resource B,
            // the data needs to be transferred from A to B.
            // However, Doris currently does not support cross-bucket data transfer, therefore,
            // changing the policy to a different policy with different resource is disabled.
            // As for the case where the resource is the same, modifying the cooldown time is allowed,
            // as this will not affect the already cooled data, but only the new data after modifying the policy.
            if (null != oldStoragePolicy && !oldStoragePolicy.equals(newStoragePolicy)) {
                // check remote storage policy
                StoragePolicy oldPolicy = StoragePolicy.ofCheck(oldStoragePolicy);
                Policy p = Env.getCurrentEnv().getPolicyMgr().getPolicy(oldPolicy);
                if ((p instanceof StoragePolicy)) {
                    String newResource = storagePolicy.getStorageResource();
                    String oldResource = ((StoragePolicy) p).getStorageResource();
                    if (!newResource.equals(oldResource)) {
                        throw new AnalysisException("currently do not support change origin "
                                + "storage policy to another one with different resource: ");
                    }
                }
            }
            // check remote storage cool down timestamp
            if (storagePolicy.getCooldownTimestampMs() != -1) {
                if (storagePolicy.getCooldownTimestampMs() <= currentTimeMs) {
                    throw new AnalysisException(
                            "remote storage cool down time: " + storagePolicy.getCooldownTimestampMs()
                                    + " should later than now: " + currentTimeMs);
                }
                if (hasCooldown && storagePolicy.getCooldownTimestampMs() <= cooldownTimestamp) {
                    throw new AnalysisException(
                            "remote storage cool down time: " + storagePolicy.getCooldownTimestampMs()
                                    + " should later than storage cool down time: " + cooldownTimestamp);
                }
            }
        }

        boolean mutable = PropertyAnalyzer.analyzeBooleanProp(properties, PROPERTIES_MUTABLE, true);
        properties.remove(PROPERTIES_MUTABLE);

        DataProperty dataProperty = new DataProperty(storageMedium, cooldownTimestamp, newStoragePolicy, mutable);
        // check the state of data property
        if (storageMediumSpecified) {
            dataProperty.setStorageMediumSpecified(true);
        }
        return dataProperty;
    }

    public static short analyzeShortKeyColumnCount(Map<String, String> properties) throws AnalysisException {
        short shortKeyColumnCount = (short) -1;
        if (properties != null && properties.containsKey(PROPERTIES_SHORT_KEY)) {
            // check and use specified short key
            try {
                shortKeyColumnCount = Short.parseShort(properties.get(PROPERTIES_SHORT_KEY));
            } catch (NumberFormatException e) {
                throw new AnalysisException("Short key: " + e.getMessage());
            }

            if (shortKeyColumnCount <= 0) {
                throw new AnalysisException("Short key column count should larger than 0.");
            }

            properties.remove(PROPERTIES_SHORT_KEY);
        }

        return shortKeyColumnCount;
    }

    private static Short analyzeReplicationNum(Map<String, String> properties, String prefix, short oldReplicationNum)
            throws AnalysisException {
        Short replicationNum = oldReplicationNum;
        String propKey = Strings.isNullOrEmpty(prefix)
                ? PROPERTIES_REPLICATION_NUM
                : prefix + "." + PROPERTIES_REPLICATION_NUM;
        if (properties != null && properties.containsKey(propKey)) {
            try {
                replicationNum = Short.valueOf(properties.get(propKey));
            } catch (Exception e) {
                throw new AnalysisException(e.getMessage());
            }

            if (replicationNum < Config.min_replication_num_per_tablet
                    || replicationNum > Config.max_replication_num_per_tablet) {
                throw new AnalysisException("Replication num should between " + Config.min_replication_num_per_tablet
                        + " and " + Config.max_replication_num_per_tablet);
            }

            properties.remove(propKey);
        }
        return replicationNum;
    }

    public static String analyzeColumnSeparator(Map<String, String> properties, String oldColumnSeparator) {
        String columnSeparator = oldColumnSeparator;
        if (properties != null && properties.containsKey(PROPERTIES_COLUMN_SEPARATOR)) {
            columnSeparator = properties.get(PROPERTIES_COLUMN_SEPARATOR);
            properties.remove(PROPERTIES_COLUMN_SEPARATOR);
        }
        return columnSeparator;
    }

    public static String analyzeLineDelimiter(Map<String, String> properties, String oldLineDelimiter) {
        String lineDelimiter = oldLineDelimiter;
        if (properties != null && properties.containsKey(PROPERTIES_LINE_DELIMITER)) {
            lineDelimiter = properties.get(PROPERTIES_LINE_DELIMITER);
            properties.remove(PROPERTIES_LINE_DELIMITER);
        }
        return lineDelimiter;
    }

    public static TStorageType analyzeStorageType(Map<String, String> properties) throws AnalysisException {
        // default is COLUMN
        TStorageType tStorageType = TStorageType.COLUMN;
        if (properties != null && properties.containsKey(PROPERTIES_STORAGE_TYPE)) {
            String storageType = properties.get(PROPERTIES_STORAGE_TYPE);
            if (storageType.equalsIgnoreCase(TStorageType.COLUMN.name())) {
                tStorageType = TStorageType.COLUMN;
            } else {
                throw new AnalysisException("Invalid storage type: " + storageType);
            }

            properties.remove(PROPERTIES_STORAGE_TYPE);
        }

        return tStorageType;
    }

    public static TTabletType analyzeTabletType(Map<String, String> properties) throws AnalysisException {
        // default is TABLET_TYPE_DISK
        TTabletType tTabletType = TTabletType.TABLET_TYPE_DISK;
        if (properties != null && properties.containsKey(PROPERTIES_TABLET_TYPE)) {
            String tabletType = properties.get(PROPERTIES_TABLET_TYPE);
            if (tabletType.equalsIgnoreCase("memory")) {
                tTabletType = TTabletType.TABLET_TYPE_MEMORY;
            } else if (tabletType.equalsIgnoreCase("disk")) {
                tTabletType = TTabletType.TABLET_TYPE_DISK;
            } else {
                throw new AnalysisException(("Invalid tablet type"));
            }
            properties.remove(PROPERTIES_TABLET_TYPE);
        }
        return tTabletType;
    }

    public static long analyzeVersionInfo(Map<String, String> properties) throws AnalysisException {
        long version = Partition.PARTITION_INIT_VERSION;
        if (properties != null && properties.containsKey(PROPERTIES_VERSION_INFO)) {
            String versionInfoStr = properties.get(PROPERTIES_VERSION_INFO);
            try {
                version = Long.parseLong(versionInfoStr);
            } catch (NumberFormatException e) {
                throw new AnalysisException("version info number format error: " + versionInfoStr);
            }
            properties.remove(PROPERTIES_VERSION_INFO);
        }
        return version;
    }

    public static int analyzeSchemaVersion(Map<String, String> properties) throws AnalysisException {
        int schemaVersion = 0;
        if (properties != null && properties.containsKey(PROPERTIES_SCHEMA_VERSION)) {
            String schemaVersionStr = properties.get(PROPERTIES_SCHEMA_VERSION);
            try {
                schemaVersion = Integer.parseInt(schemaVersionStr);
            } catch (Exception e) {
                throw new AnalysisException("schema version format error");
            }

            properties.remove(PROPERTIES_SCHEMA_VERSION);
        }

        return schemaVersion;
    }

    public static Set<String> analyzeBloomFilterColumns(Map<String, String> properties, List<Column> columns,
            KeysType keysType) throws AnalysisException {
        Set<String> bfColumns = null;
        if (properties != null && properties.containsKey(PROPERTIES_BF_COLUMNS)) {
            bfColumns = Sets.newHashSet();
            String bfColumnsStr = properties.get(PROPERTIES_BF_COLUMNS);
            if (Strings.isNullOrEmpty(bfColumnsStr)) {
                return bfColumns;
            }

            String[] bfColumnArr = bfColumnsStr.split(COMMA_SEPARATOR);
            Set<String> bfColumnSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
            for (String bfColumn : bfColumnArr) {
                bfColumn = bfColumn.trim();
                boolean found = false;
                for (Column column : columns) {
                    if (column.getName().equalsIgnoreCase(bfColumn)) {
                        PrimitiveType type = column.getDataType();

                        // tinyint/float/double columns don't support
                        // key columns and none/replace aggregate non-key columns support
                        if (type == PrimitiveType.TINYINT || type == PrimitiveType.FLOAT
                                || type == PrimitiveType.DOUBLE || type == PrimitiveType.BOOLEAN
                                || type.isComplexType()) {
                            throw new AnalysisException(type + " is not supported in bloom filter index. "
                                    + "invalid column: " + bfColumn);
                        } else if (keysType != KeysType.AGG_KEYS || column.isKey()) {
                            if (!bfColumnSet.add(bfColumn)) {
                                throw new AnalysisException("Reduplicated bloom filter column: " + bfColumn);
                            }

                            bfColumns.add(column.getName());
                            found = true;
                            break;
                        } else {
                            throw new AnalysisException("Bloom filter index only used in columns of"
                                    + " UNIQUE_KEYS/DUP_KEYS table or key columns of AGG_KEYS table."
                                    + " invalid column: " + bfColumn);
                        }
                    }
                }

                if (!found) {
                    throw new AnalysisException("Bloom filter column does not exist in table. invalid column: "
                            + bfColumn);
                }
            }

            properties.remove(PROPERTIES_BF_COLUMNS);
        }

        return bfColumns;
    }

    public static double analyzeBloomFilterFpp(Map<String, String> properties) throws AnalysisException {
        double bfFpp = 0;
        if (properties != null && properties.containsKey(PROPERTIES_BF_FPP)) {
            String bfFppStr = properties.get(PROPERTIES_BF_FPP);
            try {
                bfFpp = Double.parseDouble(bfFppStr);
            } catch (NumberFormatException e) {
                throw new AnalysisException("Bloom filter fpp is not Double");
            }

            // check range
            if (bfFpp < MIN_FPP || bfFpp > MAX_FPP) {
                throw new AnalysisException("Bloom filter fpp should in [" + MIN_FPP + ", " + MAX_FPP + "]");
            }

            properties.remove(PROPERTIES_BF_FPP);
        }

        return bfFpp;
    }

    // analyze the colocation properties of table
    public static String analyzeColocate(Map<String, String> properties) {
        String colocateGroup = null;
        if (properties != null && properties.containsKey(PROPERTIES_COLOCATE_WITH)) {
            colocateGroup = properties.get(PROPERTIES_COLOCATE_WITH);
            properties.remove(PROPERTIES_COLOCATE_WITH);
        }
        return colocateGroup;
    }

    public static long analyzeTimeout(Map<String, String> properties, long defaultTimeout) throws AnalysisException {
        long timeout = defaultTimeout;
        if (properties != null && properties.containsKey(PROPERTIES_TIMEOUT)) {
            String timeoutStr = properties.get(PROPERTIES_TIMEOUT);
            try {
                timeout = Long.parseLong(timeoutStr);
            } catch (NumberFormatException e) {
                throw new AnalysisException("Invalid timeout format: " + timeoutStr);
            }
            properties.remove(PROPERTIES_TIMEOUT);
        }
        return timeout;
    }

    public static Boolean analyzeUseLightSchemaChange(Map<String, String> properties) throws AnalysisException {
        if (properties == null || properties.isEmpty()) {
            return true;
        }
        String value = properties.get(PROPERTIES_ENABLE_LIGHT_SCHEMA_CHANGE);
        // set light schema change true by default
        if (null == value) {
            return true;
        }
        properties.remove(PROPERTIES_ENABLE_LIGHT_SCHEMA_CHANGE);
        if (value.equalsIgnoreCase("true")) {
            return true;
        } else if (value.equalsIgnoreCase("false")) {
            return false;
        }
        throw new AnalysisException(PROPERTIES_ENABLE_LIGHT_SCHEMA_CHANGE + " must be `true` or `false`");
    }

    public static Boolean analyzeDisableAutoCompaction(Map<String, String> properties) throws AnalysisException {
        if (properties == null || properties.isEmpty()) {
            return false;
        }
        String value = properties.get(PROPERTIES_DISABLE_AUTO_COMPACTION);
        // set light schema change false by default
        if (null == value) {
            return false;
        }
        properties.remove(PROPERTIES_DISABLE_AUTO_COMPACTION);
        if (value.equalsIgnoreCase("true")) {
            return true;
        } else if (value.equalsIgnoreCase("false")) {
            return false;
        }
        throw new AnalysisException(PROPERTIES_DISABLE_AUTO_COMPACTION
                + " must be `true` or `false`");
    }

    public static Boolean analyzeEnableSingleReplicaCompaction(Map<String, String> properties)
            throws AnalysisException {
        if (properties == null || properties.isEmpty()) {
            return false;
        }
        String value = properties.get(PROPERTIES_ENABLE_SINGLE_REPLICA_COMPACTION);
        // set enable single replica compaction false by default
        if (null == value) {
            return false;
        }
        properties.remove(PROPERTIES_ENABLE_SINGLE_REPLICA_COMPACTION);
        if (value.equalsIgnoreCase("true")) {
            return true;
        } else if (value.equalsIgnoreCase("false")) {
            return false;
        }
        throw new AnalysisException(PROPERTIES_ENABLE_SINGLE_REPLICA_COMPACTION
                + " must be `true` or `false`");
    }

    public static Boolean analyzeEnableDuplicateWithoutKeysByDefault(Map<String, String> properties)
                            throws AnalysisException {
        if (properties == null || properties.isEmpty()) {
            return false;
        }
        String value = properties.get(PROPERTIES_ENABLE_DUPLICATE_WITHOUT_KEYS_BY_DEFAULT);
        if (null == value) {
            return false;
        }
        properties.remove(PROPERTIES_ENABLE_DUPLICATE_WITHOUT_KEYS_BY_DEFAULT);
        if (value.equalsIgnoreCase("true")) {
            return true;
        } else if (value.equalsIgnoreCase("false")) {
            return false;
        }
        throw new AnalysisException(PROPERTIES_ENABLE_DUPLICATE_WITHOUT_KEYS_BY_DEFAULT
                + " must be `true` or `false`");
    }

    public static Boolean analyzeStoreRowColumn(Map<String, String> properties) throws AnalysisException {
        if (properties == null || properties.isEmpty()) {
            return false;
        }
        String value = properties.get(PROPERTIES_STORE_ROW_COLUMN);
        // set store_row_column false by default
        if (null == value) {
            return false;
        }
        properties.remove(PROPERTIES_STORE_ROW_COLUMN);
        if (value.equalsIgnoreCase("true")) {
            return true;
        } else if (value.equalsIgnoreCase("false")) {
            return false;
        }
        throw new AnalysisException(PROPERTIES_STORE_ROW_COLUMN
                + " must be `true` or `false`");
    }

    public static Boolean analyzeSkipWriteIndexOnLoad(Map<String, String> properties) throws AnalysisException {
        if (properties == null || properties.isEmpty()) {
            return false;
        }
        String value = properties.get(PROPERTIES_SKIP_WRITE_INDEX_ON_LOAD);
        // set skip_write_index_on_load false by default
        if (null == value) {
            return false;
        }
        properties.remove(PROPERTIES_SKIP_WRITE_INDEX_ON_LOAD);
        if (value.equalsIgnoreCase("true")) {
            return true;
        } else if (value.equalsIgnoreCase("false")) {
            return false;
        }
        throw new AnalysisException(PROPERTIES_SKIP_WRITE_INDEX_ON_LOAD
                + " must be `true` or `false`");
    }

    public static String analyzeCompactionPolicy(Map<String, String> properties) throws AnalysisException {
        if (properties == null || properties.isEmpty()) {
            return SIZE_BASED_COMPACTION_POLICY;
        }
        String compactionPolicy = SIZE_BASED_COMPACTION_POLICY;
        if (properties.containsKey(PROPERTIES_COMPACTION_POLICY)) {
            compactionPolicy = properties.get(PROPERTIES_COMPACTION_POLICY);
            properties.remove(PROPERTIES_COMPACTION_POLICY);
            if (compactionPolicy != null && !compactionPolicy.equals(TIME_SERIES_COMPACTION_POLICY)
                                                && !compactionPolicy.equals(SIZE_BASED_COMPACTION_POLICY)) {
                throw new AnalysisException(PROPERTIES_COMPACTION_POLICY
                        + " must be " + TIME_SERIES_COMPACTION_POLICY + " or " + SIZE_BASED_COMPACTION_POLICY);
            }
        }

        return compactionPolicy;
    }

    public static long analyzeTimeSeriesCompactionGoalSizeMbytes(Map<String, String> properties)
                                                                                    throws AnalysisException {
        long goalSizeMbytes = TIME_SERIES_COMPACTION_GOAL_SIZE_MBYTES_DEFAULT_VALUE;
        if (properties == null || properties.isEmpty()) {
            return goalSizeMbytes;
        }
        if (properties.containsKey(PROPERTIES_TIME_SERIES_COMPACTION_GOAL_SIZE_MBYTES)) {
            String goalSizeMbytesStr = properties.get(PROPERTIES_TIME_SERIES_COMPACTION_GOAL_SIZE_MBYTES);
            properties.remove(PROPERTIES_TIME_SERIES_COMPACTION_GOAL_SIZE_MBYTES);
            try {
                goalSizeMbytes = Long.parseLong(goalSizeMbytesStr);
                if (goalSizeMbytes < 10) {
                    throw new AnalysisException("time_series_compaction_goal_size_mbytes can not be"
                                                                + " less than 10: " + goalSizeMbytesStr);
                }
            } catch (NumberFormatException e) {
                throw new AnalysisException("Invalid time_series_compaction_goal_size_mbytes format: "
                        + goalSizeMbytesStr);
            }
        }
        return goalSizeMbytes;
    }

    public static long analyzeTimeSeriesCompactionFileCountThreshold(Map<String, String> properties)
                                                                                    throws AnalysisException {
        long fileCountThreshold = TIME_SERIES_COMPACTION_FILE_COUNT_THRESHOLD_DEFAULT_VALUE;
        if (properties == null || properties.isEmpty()) {
            return fileCountThreshold;
        }
        if (properties.containsKey(PROPERTIES_TIME_SERIES_COMPACTION_FILE_COUNT_THRESHOLD)) {
            String fileCountThresholdStr = properties
                                            .get(PROPERTIES_TIME_SERIES_COMPACTION_FILE_COUNT_THRESHOLD);
            properties.remove(PROPERTIES_TIME_SERIES_COMPACTION_FILE_COUNT_THRESHOLD);
            try {
                fileCountThreshold = Long.parseLong(fileCountThresholdStr);
                if (fileCountThreshold < 10) {
                    throw new AnalysisException("time_series_compaction_file_count_threshold can not be "
                                                            + "less than 10: " + fileCountThresholdStr);
                }
            } catch (NumberFormatException e) {
                throw new AnalysisException("Invalid time_series_compaction_file_count_threshold format: "
                                                                                + fileCountThresholdStr);
            }
        }
        return fileCountThreshold;
    }

    public static long analyzeTimeSeriesCompactionTimeThresholdSeconds(Map<String, String> properties)
                                                                                        throws AnalysisException {
        long timeThresholdSeconds = TIME_SERIES_COMPACTION_TIME_THRESHOLD_SECONDS_DEFAULT_VALUE;
        if (properties == null || properties.isEmpty()) {
            return timeThresholdSeconds;
        }
        if (properties.containsKey(PROPERTIES_TIME_SERIES_COMPACTION_TIME_THRESHOLD_SECONDS)) {
            String timeThresholdSecondsStr = properties.get(PROPERTIES_TIME_SERIES_COMPACTION_TIME_THRESHOLD_SECONDS);
            properties.remove(PROPERTIES_TIME_SERIES_COMPACTION_TIME_THRESHOLD_SECONDS);
            try {
                timeThresholdSeconds = Long.parseLong(timeThresholdSecondsStr);
                if (timeThresholdSeconds < 60) {
                    throw new AnalysisException("time_series_compaction_time_threshold_seconds can not be"
                                                                + " less than 60: " + timeThresholdSecondsStr);
                }
            } catch (NumberFormatException e) {
                throw new AnalysisException("Invalid time_series_compaction_time_threshold_seconds format: "
                                                                                + timeThresholdSecondsStr);
            }
        }
        return timeThresholdSeconds;
    }

    // analyzeCompressionType will parse the compression type from properties
    public static TCompressionType analyzeCompressionType(Map<String, String> properties) throws AnalysisException {
        String compressionType = "";
        if (properties != null && properties.containsKey(PROPERTIES_COMPRESSION)) {
            compressionType = properties.get(PROPERTIES_COMPRESSION);
            properties.remove(PROPERTIES_COMPRESSION);
        } else {
            return TCompressionType.LZ4F;
        }

        if (compressionType.equalsIgnoreCase("no_compression")) {
            return TCompressionType.NO_COMPRESSION;
        } else if (compressionType.equalsIgnoreCase("lz4")) {
            return TCompressionType.LZ4;
        } else if (compressionType.equalsIgnoreCase("lz4f")) {
            return TCompressionType.LZ4F;
        } else if (compressionType.equalsIgnoreCase("lz4hc")) {
            return TCompressionType.LZ4HC;
        } else if (compressionType.equalsIgnoreCase("zlib")) {
            return TCompressionType.ZLIB;
        } else if (compressionType.equalsIgnoreCase("zstd")) {
            return TCompressionType.ZSTD;
        } else if (compressionType.equalsIgnoreCase("snappy")) {
            return TCompressionType.SNAPPY;
        } else if (compressionType.equalsIgnoreCase("default_compression")) {
            return TCompressionType.LZ4F;
        } else {
            throw new AnalysisException("unknown compression type: " + compressionType);
        }
    }

    // analyzeStorageFormat will parse the storage format from properties
    // sql: alter table tablet_name set ("storage_format" = "v2")
    // Use this sql to convert all tablets(base and rollup index) to a new format segment
    public static TStorageFormat analyzeStorageFormat(Map<String, String> properties) throws AnalysisException {
        String storageFormat = "";
        if (properties != null && properties.containsKey(PROPERTIES_STORAGE_FORMAT)) {
            storageFormat = properties.get(PROPERTIES_STORAGE_FORMAT);
            properties.remove(PROPERTIES_STORAGE_FORMAT);
        } else {
            return TStorageFormat.V2;
        }

        if (storageFormat.equalsIgnoreCase("v1")) {
            throw new AnalysisException("Storage format V1 has been deprecated since version 0.14, "
                    + "please use V2 instead");
        } else if (storageFormat.equalsIgnoreCase("v2")) {
            return TStorageFormat.V2;
        } else if (storageFormat.equalsIgnoreCase("default")) {
            return TStorageFormat.V2;
        } else {
            throw new AnalysisException("unknown storage format: " + storageFormat);
        }
    }

    // analyze common boolean properties, such as "in_memory" = "false"
    public static boolean analyzeBooleanProp(Map<String, String> properties, String propKey, boolean defaultVal) {
        if (properties != null && properties.containsKey(propKey)) {
            String val = properties.get(propKey);
            properties.remove(propKey);
            return Boolean.parseBoolean(val);
        }
        return defaultVal;
    }

    public static String analyzeEstimatePartitionSize(Map<String, String> properties) {
        String  estimatePartitionSize = "";
        if (properties != null && properties.containsKey(PROPERTIES_ESTIMATE_PARTITION_SIZE)) {
            estimatePartitionSize = properties.get(PROPERTIES_ESTIMATE_PARTITION_SIZE);
            properties.remove(PROPERTIES_ESTIMATE_PARTITION_SIZE);
        }
        return estimatePartitionSize;
    }

    public static String analyzeStoragePolicy(Map<String, String> properties) {
        String storagePolicy = "";
        if (properties != null && properties.containsKey(PROPERTIES_STORAGE_POLICY)) {
            storagePolicy = properties.get(PROPERTIES_STORAGE_POLICY);
        }

        return storagePolicy;
    }

    // analyze property like : "type" = "xxx";
    public static String analyzeType(Map<String, String> properties) throws AnalysisException {
        String type = null;
        if (properties != null && properties.containsKey(PROPERTIES_TYPE)) {
            type = properties.get(PROPERTIES_TYPE);
            properties.remove(PROPERTIES_TYPE);
        }
        return type;
    }

    public static Type analyzeSequenceType(Map<String, String> properties, KeysType keysType) throws AnalysisException {
        String typeStr = null;
        String propertyName = PROPERTIES_FUNCTION_COLUMN + "." + PROPERTIES_SEQUENCE_TYPE;
        if (properties != null && properties.containsKey(propertyName)) {
            typeStr = properties.get(propertyName);
            properties.remove(propertyName);
        }
        if (typeStr == null) {
            return null;
        }
        if (typeStr != null && keysType != KeysType.UNIQUE_KEYS) {
            throw new AnalysisException("sequence column only support UNIQUE_KEYS");
        }
        PrimitiveType type = PrimitiveType.valueOf(typeStr.toUpperCase());
        if (!type.isFixedPointType() && !type.isDateType()) {
            throw new AnalysisException("sequence type only support integer types and date types");
        }
        return ScalarType.createType(type);
    }

    public static String analyzeSequenceMapCol(Map<String, String> properties, KeysType keysType)
            throws AnalysisException {
        String sequenceCol = null;
        String propertyName = PROPERTIES_FUNCTION_COLUMN + "." + PROPERTIES_SEQUENCE_COL;
        if (properties != null && properties.containsKey(propertyName)) {
            sequenceCol = properties.get(propertyName);
            properties.remove(propertyName);
        }
        if (sequenceCol != null && keysType != KeysType.UNIQUE_KEYS) {
            throw new AnalysisException("sequence column only support UNIQUE_KEYS");
        }
        return sequenceCol;
    }

    public static Boolean analyzeBackendDisableProperties(Map<String, String> properties, String key,
            Boolean defaultValue) {
        if (properties.containsKey(key)) {
            String value = properties.remove(key);
            return Boolean.valueOf(value);
        }
        return defaultValue;
    }

    /**
     * Found property with "tag." prefix and return a tag map, which key is tag type and value is tag value
     * Eg.
     * "tag.location" = "group_a", "tag.compute" = "x1"
     * Returns:
     * [location->group_a] [compute->x1]
     *
     * @param properties
     * @param defaultValue
     * @return
     * @throws AnalysisException
     */
    public static Map<String, String> analyzeBackendTagsProperties(Map<String, String> properties, Tag defaultValue)
            throws AnalysisException {
        Map<String, String> tagMap = Maps.newHashMap();
        Iterator<Map.Entry<String, String>> iter = properties.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, String> entry = iter.next();
            if (!entry.getKey().startsWith("tag.")) {
                continue;
            }
            String[] keyParts = entry.getKey().split("\\.");
            if (keyParts.length != 2) {
                continue;
            }
            String val = entry.getValue().replaceAll(" ", "");
            Tag tag = Tag.create(keyParts[1], val);
            tagMap.put(tag.type, tag.value);
            iter.remove();
        }
        if (tagMap.isEmpty() && defaultValue != null) {
            tagMap.put(defaultValue.type, defaultValue.value);
        }
        return tagMap;
    }

    public static Map<String, String> analyzeBinlogConfig(Map<String, String> properties) throws AnalysisException {
        if (properties == null || properties.isEmpty()) {
            return null;
        }

        Map<String, String> binlogConfigMap = Maps.newHashMap();
        // check PROPERTIES_BINLOG_ENABLE = "binlog.enable";
        if (properties.containsKey(PROPERTIES_BINLOG_ENABLE)) {
            String enable = properties.get(PROPERTIES_BINLOG_ENABLE);
            try {
                binlogConfigMap.put(PROPERTIES_BINLOG_ENABLE, String.valueOf(Boolean.parseBoolean(enable)));
                properties.remove(PROPERTIES_BINLOG_ENABLE);
            } catch (Exception e) {
                throw new AnalysisException("Invalid binlog enable value: " + enable);
            }
        }
        // check PROPERTIES_BINLOG_TTL_SECONDS = "binlog.ttl_seconds";
        if (properties.containsKey(PROPERTIES_BINLOG_TTL_SECONDS)) {
            String ttlSeconds = properties.get(PROPERTIES_BINLOG_TTL_SECONDS);
            try {
                binlogConfigMap.put(PROPERTIES_BINLOG_TTL_SECONDS, String.valueOf(Long.parseLong(ttlSeconds)));
                properties.remove(PROPERTIES_BINLOG_TTL_SECONDS);
            } catch (Exception e) {
                throw new AnalysisException("Invalid binlog ttl_seconds value: " + ttlSeconds);
            }
        }
        // check PROPERTIES_BINLOG_MAX_BYTES = "binlog.max_bytes";
        if (properties.containsKey(PROPERTIES_BINLOG_MAX_BYTES)) {
            String maxBytes = properties.get(PROPERTIES_BINLOG_MAX_BYTES);
            try {
                binlogConfigMap.put(PROPERTIES_BINLOG_MAX_BYTES, String.valueOf(Long.parseLong(maxBytes)));
                properties.remove(PROPERTIES_BINLOG_MAX_BYTES);
            } catch (Exception e) {
                throw new AnalysisException("Invalid binlog max_bytes value: " + maxBytes);
            }
        }
        // check PROPERTIES_BINLOG_MAX_HISTORY_NUMS = "binlog.max_history_nums";
        if (properties.containsKey(PROPERTIES_BINLOG_MAX_HISTORY_NUMS)) {
            String maxHistoryNums = properties.get(PROPERTIES_BINLOG_MAX_HISTORY_NUMS);
            try {
                binlogConfigMap.put(PROPERTIES_BINLOG_MAX_HISTORY_NUMS, String.valueOf(Long.parseLong(maxHistoryNums)));
                properties.remove(PROPERTIES_BINLOG_MAX_HISTORY_NUMS);
            } catch (Exception e) {
                throw new AnalysisException("Invalid binlog max_history_nums value: " + maxHistoryNums);
            }
        }

        return binlogConfigMap;
    }

    public static boolean analyzeIsBeingSynced(Map<String, String> properties, boolean defaultValue) {
        if (properties != null && properties.containsKey(PROPERTIES_IS_BEING_SYNCED)) {
            String value = properties.remove(PROPERTIES_IS_BEING_SYNCED);
            return Boolean.valueOf(value);
        }
        return defaultValue;
    }

    // There are 2 kinds of replication property:
    // 1. "replication_num" = "3"
    // 2. "replication_allocation" = "tag.location.zone1: 2, tag.location.zone2: 1"
    // These 2 kinds of property will all be converted to a ReplicaAllocation and return.
    // Return ReplicaAllocation.NOT_SET if no replica property is set.
    //
    // prefix is for property key such as "dynamic_partition.replication_num", which prefix is "dynamic_partition"
    public static ReplicaAllocation analyzeReplicaAllocation(Map<String, String> properties, String prefix)
            throws AnalysisException {
        if (properties == null || properties.isEmpty()) {
            return ReplicaAllocation.NOT_SET;
        }
        // if give "replication_num" property, return with default backend tag
        Short replicaNum = analyzeReplicationNum(properties, prefix, (short) 0);
        if (replicaNum > 0) {
            return new ReplicaAllocation(replicaNum);
        }

        String propKey = Strings.isNullOrEmpty(prefix) ? PROPERTIES_REPLICATION_ALLOCATION
                : prefix + "." + PROPERTIES_REPLICATION_ALLOCATION;
        // if not set, return default replication allocation
        if (!properties.containsKey(propKey)) {
            return ReplicaAllocation.NOT_SET;
        }

        // analyze user specified replication allocation
        // format is as: "tag.location.zone1: 2, tag.location.zone2: 1"
        ReplicaAllocation replicaAlloc = new ReplicaAllocation();
        String allocationVal = properties.remove(propKey);
        allocationVal = allocationVal.replaceAll(" ", "");
        String[] locations = allocationVal.split(",");
        int totalReplicaNum = 0;
        for (String location : locations) {
            String[] parts = location.split(":");
            if (parts.length != 2) {
                throw new AnalysisException("Invalid replication allocation property: " + location);
            }
            if (!parts[0].startsWith(TAG_LOCATION)) {
                throw new AnalysisException("Invalid replication allocation tag property: " + location);
            }
            String locationVal = parts[0].replace(TAG_LOCATION, "").replace(".", "");
            if (Strings.isNullOrEmpty(locationVal)) {
                throw new AnalysisException("Invalid replication allocation location tag property: " + location);
            }

            Short replicationNum = Short.valueOf(parts[1]);
            replicaAlloc.put(Tag.create(Tag.TYPE_LOCATION, locationVal), replicationNum);
            totalReplicaNum += replicationNum;

            // Check if the current backends satisfy the ReplicaAllocation condition,
            // to avoid user set it success but failed to create table or dynamic partitions
            try {
                SystemInfoService systemInfoService = Env.getCurrentSystemInfo();
                systemInfoService.selectBackendIdsForReplicaCreation(
                        replicaAlloc, null, false, true);
            } catch (DdlException ddlException) {
                throw new AnalysisException(ddlException.getMessage());
            }
        }
        if (totalReplicaNum < Config.min_replication_num_per_tablet
                || totalReplicaNum > Config.max_replication_num_per_tablet) {
            throw new AnalysisException("Total replication num should between " + Config.min_replication_num_per_tablet
                    + " and " + Config.max_replication_num_per_tablet);
        }

        if (replicaAlloc.isEmpty()) {
            throw new AnalysisException("Not specified replica allocation property");
        }
        return replicaAlloc;
    }

    public static DataSortInfo analyzeDataSortInfo(Map<String, String> properties, KeysType keyType,
            int keyCount, TStorageFormat storageFormat) throws AnalysisException {
        if (properties == null || properties.isEmpty()) {
            return new DataSortInfo(TSortType.LEXICAL, keyCount);
        }
        String sortMethod = TSortType.LEXICAL.name();
        if (properties.containsKey(DataSortInfo.DATA_SORT_TYPE)) {
            sortMethod = properties.remove(DataSortInfo.DATA_SORT_TYPE);
        }
        TSortType sortType = TSortType.LEXICAL;
        if (sortMethod.equalsIgnoreCase(TSortType.LEXICAL.name())) {
            sortType = TSortType.LEXICAL;
        } else {
            throw new AnalysisException("only support lexical method now!");
        }

        int colNum = keyCount;
        if (properties.containsKey(DataSortInfo.DATA_SORT_COL_NUM)) {
            try {
                colNum = Integer.valueOf(properties.remove(DataSortInfo.DATA_SORT_COL_NUM));
            } catch (Exception e) {
                throw new AnalysisException("param " + DataSortInfo.DATA_SORT_COL_NUM + " error");
            }
        }
        DataSortInfo dataSortInfo = new DataSortInfo(sortType, colNum);
        return dataSortInfo;
    }

    public static boolean analyzeUniqueKeyMergeOnWrite(Map<String, String> properties) throws AnalysisException {
        if (properties == null || properties.isEmpty()) {
            return false;
        }
        String value = properties.get(PropertyAnalyzer.ENABLE_UNIQUE_KEY_MERGE_ON_WRITE);
        if (value == null) {
            return false;
        }
        properties.remove(PropertyAnalyzer.ENABLE_UNIQUE_KEY_MERGE_ON_WRITE);
        if (value.equals("true")) {
            return true;
        } else if (value.equals("false")) {
            return false;
        }
        throw new AnalysisException(PropertyAnalyzer.ENABLE_UNIQUE_KEY_MERGE_ON_WRITE + " must be `true` or `false`");
    }

    /**
     * Check the type property of the catalog props.
     */
    public static void checkCatalogProperties(Map<String, String> properties, boolean isAlter)
            throws AnalysisException {
        // validate the properties of es catalog
        if ("es".equalsIgnoreCase(properties.get("type"))) {
            try {
                EsResource.valid(properties, true);
            } catch (Exception e) {
                throw new AnalysisException(e.getMessage());
            }
        }
        // validate access controller properties
        // eg:
        // (
        // "access_controller.class" = "org.apache.doris.mysql.privilege.RangerHiveAccessControllerFactory",
        // "access_controller.properties.prop1" = "xxx",
        // "access_controller.properties.prop2" = "yyy",
        // )
        // 1. get access controller class
        String acClass = properties.getOrDefault(CatalogMgr.ACCESS_CONTROLLER_CLASS_PROP, "");
        if (!Strings.isNullOrEmpty(acClass)) {
            // 2. check if class exists
            try {
                Class.forName(acClass);
            } catch (ClassNotFoundException e) {
                throw new AnalysisException("failed to find class " + acClass, e);
            }
        }
    }
}
