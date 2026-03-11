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

package org.apache.doris.datasource.systable;

import org.apache.doris.info.TableValuedFunctionRefInfo;
import org.apache.doris.nereids.trees.expressions.functions.table.PartitionValues;
import org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * System table type for Hive partition tables (table$partitions).
 *
 * <p>This system table uses the TVF path with partition_values function.
 */
public class PartitionsSysTable extends TvfSysTable {

    private static final Logger LOG = LogManager.getLogger(PartitionsSysTable.class);

    public static final PartitionsSysTable INSTANCE = new PartitionsSysTable();

    /**
     * Supported system tables for Hive tables (only partitions).
     * Key is the system table name.
     */
    public static final Map<String, SysTable> HIVE_SUPPORTED_SYS_TABLES =
            Collections.singletonMap(INSTANCE.getSysTableName(), INSTANCE);

    private PartitionsSysTable() {
        super("partitions", "partition_values");
    }

    @Override
    public TableValuedFunction createFunction(String ctlName, String dbName, String sourceNameWithMetaName) {
        List<String> nameParts = Lists.newArrayList(ctlName, dbName,
                getSourceTableName(sourceNameWithMetaName));
        return PartitionValues.create(nameParts);
    }

    @Override
    public TableValuedFunctionRefInfo createFunctionRef(String ctlName, String dbName, String sourceNameWithMetaName) {
        Map<String, String> params = Maps.newHashMap();
        params.put("catalog", ctlName);
        params.put("database", dbName);
        params.put("table", getSourceTableName(sourceNameWithMetaName));
        try {
            return new TableValuedFunctionRefInfo(tvfName, null, params);
        } catch (org.apache.doris.common.AnalysisException e) {
            LOG.warn("should not happen. {}.{}.{}", ctlName, dbName, sourceNameWithMetaName);
            return null;
        }
    }
}
