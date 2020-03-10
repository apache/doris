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
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStorageType;

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
    
    public static final String PROPERTIES_KUDU_MASTER_ADDRS = "kudu_master_addrs";

    public static final String PROPERTIES_COLUMN_SEPARATOR = "column_separator";
    public static final String PROPERTIES_LINE_DELIMITER = "line_delimiter";

    public static final String PROPERTIES_COLOCATE_WITH = "colocate_with";
    
    public static final String PROPERTIES_TIMEOUT = "timeout";

    public static final String PROPERTIES_DISTRIBUTION_TYPE = "distribution_type";
    public static final String PROPERTIES_SEND_CLEAR_ALTER_TASK = "send_clear_alter_tasks";

    public static final String PROPERTIES_TYPE = "type";

    public static DataProperty analyzeDataProperty(Map<String, String> properties, DataProperty oldDataProperty)
            throws AnalysisException {
        DataProperty dataProperty = oldDataProperty;

        if (properties == null) {
            return dataProperty;
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
                coolDownTimeStamp = dateLiteral.getLongValue();
            }
        } // end for properties

        if (!hasCooldown && !hasMedium) {
            return dataProperty;
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
            // check and use speciefied short key
            try {
                shortKeyColumnCount = Short.valueOf(properties.get(PROPERTIES_SHORT_KEY));
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
    
    public static Short analyzeReplicationNum(Map<String, String> properties, short oldReplicationNum)
            throws AnalysisException {
        Short replicationNum = oldReplicationNum;
        if (properties != null && properties.containsKey(PROPERTIES_REPLICATION_NUM)) {
            try {
                replicationNum = Short.valueOf(properties.get(PROPERTIES_REPLICATION_NUM));
            } catch (Exception e) {
                throw new AnalysisException(e.getMessage());
            }

            if (replicationNum <= 0) {
                throw new AnalysisException("Replication num should larger than 0. (suggested 3)");
            }

            properties.remove(PROPERTIES_REPLICATION_NUM);
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
                schemaVersion = Integer.valueOf(schemaVersionStr);
            } catch (Exception e) {
                throw new AnalysisException("schema version format error");
            }

            properties.remove(PROPERTIES_SCHEMA_VERSION);
        }

        return schemaVersion;
    }

    public static Set<String> analyzeBloomFilterColumns(Map<String, String> properties, List<Column> columns)
            throws AnalysisException {
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
                                || type == PrimitiveType.DOUBLE) {
                            throw new AnalysisException(type + " is not supported in bloom filter index. "
                                    + "invalid column: " + bfColumn);
                        } else if (column.isKey()
                                || column.getAggregationType() == AggregateType.NONE
                                || column.getAggregationType() == AggregateType.REPLACE
                                || column.getAggregationType() == AggregateType.REPLACE_IF_NOT_NULL) {
                            if (!bfColumnSet.add(bfColumn)) {
                                throw new AnalysisException("Reduplicated bloom filter column: " + bfColumn);
                            }

                            bfColumns.add(column.getName());
                            found = true;
                            break;
                        } else {
                            // althrough the implemention supports bf for replace non-key column,
                            // for simplicity and unity, we don't expose that to user.
                            throw new AnalysisException("Bloom filter index only used in columns of DUP_KEYS table or "
                                    + "key columns of UNIQUE_KEYS/AGG_KEYS table. invalid column: " + bfColumn);
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

    public static String analyzeKuduMasterAddr(Map<String, String> properties, String kuduMasterAddr)
            throws AnalysisException {
        String returnAddr = kuduMasterAddr;
        if (properties != null && properties.containsKey(PROPERTIES_KUDU_MASTER_ADDRS)) {
            String addrsStr = properties.get(PROPERTIES_KUDU_MASTER_ADDRS);
            String[] addrArr = addrsStr.split(",");
            if (addrArr.length == 0) {
                throw new AnalysisException("Kudu master address is set empty");
            }

            returnAddr = "";
            for (int i = 0; i < addrArr.length; i++) {
                Pair<String, Integer> hostPort = SystemInfoService.validateHostAndPort(addrArr[i]);
                returnAddr += hostPort.first + ":" + hostPort.second + ",";
            }
            // remove last comma
            returnAddr = returnAddr.substring(0, returnAddr.length() - 1);

            properties.remove(PROPERTIES_KUDU_MASTER_ADDRS);
        }

        return returnAddr;
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
                timeout = Long.valueOf(timeoutStr);
            } catch (NumberFormatException e) {
                throw new AnalysisException("Invalid timeout format: " + timeoutStr);
            }
            properties.remove(PROPERTIES_TIMEOUT);
        }
        return timeout;
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
}
