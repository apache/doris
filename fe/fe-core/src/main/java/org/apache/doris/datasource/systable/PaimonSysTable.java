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

import org.apache.doris.analysis.TableValuedFunctionRef;
import org.apache.doris.nereids.trees.expressions.functions.table.PaimonMeta;
import org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction;
import org.apache.doris.tablefunction.PaimonTableValuedFunction;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.table.system.SystemTableLoader;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * System table implementation for Paimon metadata tables.
 */
public class PaimonSysTable extends SysTable {
    private static final Logger LOG = LogManager.getLogger(PaimonSysTable.class);

    private static final Set<String> EXCLUDED_SYS_TABLES = new HashSet<>(Arrays.asList("binlog", "ro", "audit_log"));

    private static final List<PaimonSysTable> SUPPORTED_PAIMON_SYS_TABLES = SystemTableLoader.SYSTEM_TABLES
            .stream()
            .filter(table -> !EXCLUDED_SYS_TABLES.contains(table))
            .map(PaimonSysTable::new)
            .collect(Collectors.toList());

    private final String tableName;

    /**
     * Creates a new Paimon system table instance.
     *
     * @param tableName the name of the system table
     */
    protected PaimonSysTable(String tableName) {
        super(tableName, "paimon_meta");
        this.tableName = tableName;
    }

    public static List<PaimonSysTable> getSupportedPaimonSysTables() {
        return SUPPORTED_PAIMON_SYS_TABLES;
    }

    @Override
    public TableValuedFunction createFunction(String ctlName, String dbName, String sourceNameWithMetaName) {
        List<String> nameParts = Lists.newArrayList(ctlName, dbName,
                getSourceTableName(sourceNameWithMetaName));
        return PaimonMeta.createPaimonMeta(nameParts, tableName);
    }

    @Override
    public TableValuedFunctionRef createFunctionRef(String ctlName, String dbName, String sourceNameWithMetaName) {
        List<String> nameParts = Lists.newArrayList(ctlName, dbName,
                getSourceTableName(sourceNameWithMetaName));
        Map<String, String> params = Maps.newHashMap();
        params.put(PaimonTableValuedFunction.TABLE, Joiner.on(".").join(nameParts));
        params.put(PaimonTableValuedFunction.QUERY_TYPE, tableName);
        try {
            return new TableValuedFunctionRef(tvfName, null, params);
        } catch (org.apache.doris.common.AnalysisException e) {
            LOG.warn("should not happen. {}.{}.{}", ctlName, dbName, sourceNameWithMetaName, e);
            return null;
        }
    }
}
