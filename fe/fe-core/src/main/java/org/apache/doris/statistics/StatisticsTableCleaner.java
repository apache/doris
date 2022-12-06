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

package org.apache.doris.statistics;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

/**
 * Maintenance the internal statistics table.
 *  Delete rows that corresponding DB/Table/Column not exists anymore.
 */
public class StatisticsTableCleaner extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(StatisticsTableCleaner.class);

    public StatisticsTableCleaner() {
        super("Statistics Table Cleaner",
                StatisticConstants.STATISTIC_CLEAN_INTERVAL_IN_HOURS * 3600 * 1000);
    }

    @Override
    protected void runAfterCatalogReady() {
        if (Env.getCurrentEnv().isMaster()) {
            deleteExpiredStatistics();
        }
    }

    private void deleteExpiredStatistics() {
        List<Database> databases = Env.getCurrentEnv().getInternalCatalog().getDbs();
        deleteByDB(databases.stream().map(Database::getId).map(String::valueOf).collect(Collectors.toList()));
        List<String> tblIds = new ArrayList<>();
        List<String> colIds = new ArrayList<>();
        List<String> partitionIds = new ArrayList<>();
        for (Database database : databases) {
            List<Table> tables = database.getTables();
            for (Table table : tables) {
                tblIds.add(String.valueOf(table.getId()));
                if (table instanceof OlapTable) {
                    OlapTable olapTable = (OlapTable) table;
                    partitionIds.addAll(olapTable.getPartitionIds()
                            .stream().map(String::valueOf).collect(Collectors.toList()));
                }
                colIds.addAll(table.getColumns().stream().map(Column::getName)
                        .map(String::valueOf).collect(Collectors.toList()));
            }
        }
        deleteByTblId(tblIds);
        deleteByColId(colIds);
        deleteByPartitionId(partitionIds);
    }

    private void deleteByPartitionId(List<String> partitionIds) {
        deleteExpired("part_id", partitionIds);
    }

    private void deleteByTblId(List<String> tblIds) {
        deleteExpired("tbl_id", tblIds);
    }

    private void deleteByDB(List<String> dbIds) {
        deleteExpired("db_id", dbIds);
    }

    private void deleteByColId(List<String> colId) {
        deleteExpired("col_id", colId);
    }

    private void deleteExpired(String colName, List<String> constants) {
        // TODO: must promise count of children of predicate is less than the FE limits.
        String deleteTemplate = "DELETE FROM " + FeConstants.INTERNAL_DB_NAME
                + "." + StatisticConstants.STATISTIC_TBL_NAME + "WHERE ${colName} NOT IN ${predicate}";
        StringJoiner predicateBuilder = new StringJoiner(",", "(", ")");
        constants.forEach(predicateBuilder::add);
        Map<String, String> map = new HashMap<String, String>() {
            {
                put("colName", colName);
                put("predicate", predicateBuilder.toString());
            }
        };
        StringSubstitutor stringSubstitutor = new StringSubstitutor(map);
        try {
            StatisticsUtil.execUpdate(stringSubstitutor.replace(deleteTemplate));
        } catch (Exception e) {
            LOG.warn("Remove expired statistics failed", e);
        }
    }

}
