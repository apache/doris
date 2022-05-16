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

package org.apache.doris.external.hudi;

import org.apache.doris.common.DdlException;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Hudi utils.
 */
public class HudiUtils {

    private static final String PROPERTY_MISSING_MSG =
            "Hudi table %s is null. Please add properties('%s'='xxx') when create table";

    /**
     * check hudi table properties.
     */
    public static void validateCreateTable(HudiTable table) throws DdlException {

        if (table.getTableProperties() == null) {
            throw new DdlException("Please set properties of hudi table, "
                    + "they are: database, table and 'hive.metastore.uris'");
        }

        Map<String, String> copiedProps = Maps.newHashMap(table.getTableProperties());
        String hiveDb = copiedProps.get(HudiProperty.HUDI_DATABASE);
        if (Strings.isNullOrEmpty(hiveDb)) {
            throw new DdlException(String.format(PROPERTY_MISSING_MSG,
                    HudiProperty.HUDI_DATABASE, HudiProperty.HUDI_DATABASE));
        }
        copiedProps.remove(HudiProperty.HUDI_DATABASE);

        String hiveTable = copiedProps.get(HudiProperty.HUDI_TABLE);
        if (Strings.isNullOrEmpty(hiveTable)) {
            throw new DdlException(String.format(PROPERTY_MISSING_MSG,
                    HudiProperty.HUDI_TABLE, HudiProperty.HUDI_TABLE));
        }
        copiedProps.remove(HudiProperty.HUDI_TABLE);

        // check hive properties
        // hive.metastore.uris
        String hiveMetastoreUris = copiedProps.get(HudiProperty.HUDI_HIVE_METASTORE_URIS);
        if (Strings.isNullOrEmpty(hiveMetastoreUris)) {
            throw new DdlException(String.format(PROPERTY_MISSING_MSG,
                    HudiProperty.HUDI_HIVE_METASTORE_URIS, HudiProperty.HUDI_HIVE_METASTORE_URIS));
        }
        copiedProps.remove(HudiProperty.HUDI_HIVE_METASTORE_URIS);

        if (!copiedProps.isEmpty()) {
            throw new DdlException("Unknown table properties: " + copiedProps.toString());
        }
    }

    /**
     * check a hiveTable is hudi table or not.
     *
     * @param hiveTable hive metastore table
     * @return true when hiveTable is hudi table, false when it is not
     */
    public static boolean isHudiTable(org.apache.hadoop.hive.metastore.api.Table hiveTable) {
        String inputFormat = hiveTable.getSd().getInputFormat();
        if (HoodieParquetInputFormat.class.getName().equals(inputFormat)
                || HoodieParquetRealtimeInputFormat.class.getName().equals(inputFormat)) {
            return true;
        }
        return false;
    }

    /**
     * check whether the table is hudi realtime table.
     *
     * @param hiveTable hive metastore table
     * @return true when table is hudi table
     */
    public static boolean isHudiRealtimeTable(org.apache.hadoop.hive.metastore.api.Table hiveTable) {
        String inputFormat = hiveTable.getSd().getInputFormat();
        if (HoodieParquetRealtimeInputFormat.class.getName().equals(inputFormat)) {
            return true;
        }
        return false;
    }

    /**
     * Check if there are duplicate columns in hudi table.
     * check if columns of hudi table exist in hive table.
     *
     * @param table hudi table to be checked
     * @param hiveTable the corresponding hive table
     * @return non return value
     * @throws DdlException when hudi table's column(s) didn't exist in hive table
     */
    public static void validateColumns(HudiTable table,
                                       org.apache.hadoop.hive.metastore.api.Table hiveTable) throws DdlException {
        Set<String> hudiColumnNames = table.getFullSchema().stream()
                .map(x -> x.getName()).collect(Collectors.toSet());

        Set<String> hiveTableColumnNames = hiveTable.getSd().getCols()
                .stream().map(x -> x.getName()).collect(Collectors.toSet());
        hudiColumnNames.removeAll(hiveTableColumnNames);
        if (hudiColumnNames.size() > 0) {
            throw new DdlException(String.format("Hudi table's column(s): {%s} didn't exist in hive table. ",
                    String.join(", ", hudiColumnNames)));
        }
    }
}
