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

import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.resource.Tag;
import org.apache.doris.thrift.TStorageFormat;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.thrift.TTabletType;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class PropertyAnalyzer {
    private static final Logger LOG = LogManager.getLogger(PropertyAnalyzer.class);
    private static final String COMMA_SEPARATOR = ",";

    public static final String PROPERTIES_SHORT_KEY = "short_key";
    public static final String PROPERTIES_REPLICATION_NUM = "replication_num";
    public static final String PROPERTIES_REPLICATION_ALLOCATION = "replication_allocation";
    public static final String PROPERTIES_STORAGE_TYPE = "storage_type";
    public static final String PROPERTIES_STORAGE_MEDIUM = "storage_medium";
    public static final String PROPERTIES_STORAGE_COLDOWN_TIME = "storage_cooldown_time";
    // for 1.x -> 2.x migration
    public static final String PROPERTIES_VERSION_INFO = "version_info";
    // for restore
    public static final String PROPERTIES_SCHEMA_VERSION = "schema_version";

    public static final String PROPERTIES_BF_COLUMNS = "bloom_filter_columns";
    public static final String PROPERTIES_BF_FPP = "bloom_filter_fpp";
    private static final double MAX_FPP = 0.05;
    private static final double MIN_FPP = 0.0001;
    
    public static final String PROPERTIES_COLUMN_SEPARATOR = "column_separator";
    public static final String PROPERTIES_LINE_DELIMITER = "line_delimiter";

    public static final String PROPERTIES_COLOCATE_WITH = "colocate_with";
    
    public static final String PROPERTIES_TIMEOUT = "timeout";

    public static final String PROPERTIES_DISTRIBUTION_TYPE = "distribution_type";
    public static final String PROPERTIES_SEND_CLEAR_ALTER_TASK = "send_clear_alter_tasks";
    /*
     * for upgrade alpha rowset to beta rowset, valid value: v1, v2
     * v1: alpha rowset
     * v2: beta rowset
     */
    public static final String PROPERTIES_STORAGE_FORMAT = "storage_format";

    public static final String PROPERTIES_INMEMORY = "in_memory";

    public static final String PROPERTIES_TABLET_TYPE = "tablet_type";

    public static final String PROPERTIES_STRICT_RANGE = "strict_range";
    public static final String PROPERTIES_USE_TEMP_PARTITION_NAME = "use_temp_partition_name";

    public static final String PROPERTIES_TYPE = "type";
    // This is common prefix for function column
    public static final String PROPERTIES_FUNCTION_COLUMN = "function_column";
    public static final String PROPERTIES_SEQUENCE_TYPE = "sequence_type";

    public static final String PROPERTIES_SWAP_TABLE = "swap";

    public static final String TAG_LOCATION = "tag.location";

    public static DataProperty analyzeDataProperty(Map<String, String> properties, DataProperty oldDataProperty)
            throws AnalysisException {
        if (properties == null) {
            return oldDataProperty;
        }

        TStorageMedium storageMedium = null;
        long coolDownTimeStamp = DataProperty.MAX_COOLDOWN_TIME_MS;

        boolean hasMedium = false;
        boolean hasCooldown = false;
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (!hasMedium && key.equalsIgnoreCase(PROPERTIES_STORAGE_MEDIUM)) {
                hasMedium = true;
                if (value.equalsIgnoreCase(TStorageMedium.SSD.name())) {
                    storageMedium = TStorageMedium.SSD;
                } else if (value.equalsIgnoreCase(TStorageMedium.HDD.name())) {
                    storageMedium = TStorageMedium.HDD;
                } else {
                    throw new AnalysisException("Invalid storage medium: " + value);
                }
            } else if (!hasCooldown && key.equalsIgnoreCase(PROPERTIES_STORAGE_COLDOWN_TIME)) {
                hasCooldown = true;
                DateLiteral dateLiteral = new DateLiteral(value, Type.DATETIME);
                coolDownTimeStamp = dateLiteral.unixTimestamp(TimeUtils.getTimeZone());
            }
        } // end for properties

        if (!hasCooldown && !hasMedium) {
            return oldDataProperty;
        }

        properties.remove(PROPERTIES_STORAGE_MEDIUM);
        properties.remove(PROPERTIES_STORAGE_COLDOWN_TIME);

        if (hasCooldown && !hasMedium) {
            throw new AnalysisException("Invalid data property. storage medium property is not found");
        }

        if (storageMedium == TStorageMedium.HDD && hasCooldown) {
            throw new AnalysisException("Can not assign cooldown timestamp to HDD storage medium");
        }

        long currentTimeMs = System.currentTimeMillis();
        if (storageMedium == TStorageMedium.SSD && hasCooldown) {
            if (coolDownTimeStamp <= currentTimeMs) {
                throw new AnalysisException("Cooldown time should later than now");
            }
        }

        if (storageMedium == TStorageMedium.SSD && !hasCooldown) {
            // set default cooldown time
            coolDownTimeStamp = currentTimeMs + Config.storage_cooldown_second * 1000L;
        }

        Preconditions.checkNotNull(storageMedium);
        return new DataProperty(storageMedium, coolDownTimeStamp);
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
        String propKey = Strings.isNullOrEmpty(prefix) ? PROPERTIES_REPLICATION_NUM : prefix + "." + PROPERTIES_REPLICATION_NUM;
        if (properties != null && properties.containsKey(propKey)) {
            try {
                replicationNum = Short.valueOf(properties.get(propKey));
            } catch (Exception e) {
                throw new AnalysisException(e.getMessage());
            }

            if (replicationNum <= 0) {
                throw new AnalysisException("Replication num should larger than 0. (suggested 3)");
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

    public static Pair<Long, Long> analyzeVersionInfo(Map<String, String> properties) throws AnalysisException {
        Pair<Long, Long> versionInfo = new Pair<>(Partition.PARTITION_INIT_VERSION,
                Partition.PARTITION_INIT_VERSION_HASH);
        if (properties != null && properties.containsKey(PROPERTIES_VERSION_INFO)) {
            String versionInfoStr = properties.get(PROPERTIES_VERSION_INFO);
            String[] versionInfoArr = versionInfoStr.split(COMMA_SEPARATOR);
            if (versionInfoArr.length == 2) {
                try {
                    versionInfo.first = Long.parseLong(versionInfoArr[0]);
                    versionInfo.second = Long.parseLong(versionInfoArr[1]);
                } catch (NumberFormatException e) {
                    throw new AnalysisException("version info number format error");
                }
            } else {
                throw new AnalysisException("version info format error. format: version,version_hash");
            }

            properties.remove(PROPERTIES_VERSION_INFO);
        }

        return versionInfo;
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
                                || type == PrimitiveType.DOUBLE || type == PrimitiveType.BOOLEAN) {
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
                            throw new AnalysisException("Bloom filter index only used in columns of" +
                                " UNIQUE_KEYS/DUP_KEYS table or key columns of AGG_KEYS table." +
                                " invalid column: " + bfColumn);
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

    public static String analyzeColocate(Map<String, String> properties) throws AnalysisException {
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
            if (!Config.enable_alpha_rowset) {
                throw new AnalysisException("Storage format V1 has been deprecated since version 0.14," +
                        " please use V2 instead");
            }
            return TStorageFormat.V1;
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

    // analyze property like : "type" = "xxx";
    public static String analyzeType(Map<String, String> properties) throws AnalysisException {
        String type = null;
        if (properties != null && properties.containsKey(PROPERTIES_TYPE)) {
            type = properties.get(PROPERTIES_TYPE);
            properties.remove(PROPERTIES_TYPE);
        }
        return type;
    }

    public static Type analyzeSequenceType(Map<String, String> properties, KeysType keysType) throws  AnalysisException{
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

    public static Tag analyzeBackendTagProperties(Map<String, String> properties) throws AnalysisException {
        if (properties.containsKey(TAG_LOCATION)) {
            String tagVal = properties.remove(TAG_LOCATION);
            return Tag.create(Tag.TYPE_LOCATION, tagVal);
        }
        return Tag.DEFAULT_BACKEND_TAG;
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
        for (String location : locations) {
            String[] parts = location.split(":");
            if (parts.length != 2) {
                throw new AnalysisException("Invalid replication allocation property: " + location);
            }
            if (!parts[0].startsWith(TAG_LOCATION)) {
                throw new AnalysisException("Invalid replication allocation tag property: " + location);
            }
            String locationVal = parts[0].substring(TAG_LOCATION.length() + 1); // +1 to skip dot.
            if (Strings.isNullOrEmpty(locationVal)) {
                throw new AnalysisException("Invalid replication allocation location tag property: " + location);
            }

            replicaAlloc.put(Tag.create(Tag.TYPE_LOCATION, locationVal), Short.valueOf(parts[1]));
        }

        if (replicaAlloc.isEmpty()) {
            throw new AnalysisException("Not specified replica allocation property");
        }
        return replicaAlloc;
    }

}
