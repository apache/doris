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

package org.apache.doris.task;

import org.apache.commons.lang.StringUtils;
import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.util.Daemon;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.system.HeartbeatMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class DynamicPartitionTask extends Daemon {
    private static final Logger LOG = LogManager.getLogger(HeartbeatMgr.class);

    private static final String TIMESTAMP_FORMAT = "yyyyMMdd";
    private static final String DATE_FORMAT = "yyyy-MM-dd";
    private static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    @Override
    protected void runOneCycle() {
        if (!Catalog.getInstance().isMaster()) {
            LOG.info("Only master can do auto add partition job");
        }
        LOG.info("Start check if need to add partition");
        List<Long> dbIds = Catalog.getInstance().getDbIds();
        for (Long dbId : dbIds) {
            Database db = Catalog.getInstance().getDb(dbId);
            for (Table table : db.getTables()) {
                if (table instanceof OlapTable) {
                    try {
                        autoAddPartition(db.getClusterName(), db.getFullName(), (OlapTable) table);
                    } catch (Exception e) {
                        LOG.error("Failed auto add partition: " + e.getMessage());
                    }
                }
            }
        }
    }

    private void autoAddPartition(String clusterName, String fullDbName, OlapTable table) throws Exception {
        // Only support Single-column range partition
        if (!table.getPartitionInfo().getType().equals(PartitionType.RANGE)) {
            return;
        }
        RangePartitionInfo partitionInfo = (RangePartitionInfo) table.getPartitionInfo();
        if (partitionInfo.getPartitionColumns().size() != 1) {
            return;
        }
        String rangePartitionFormat;
        Column partitionColumn = partitionInfo.getPartitionColumns().get(0);
        if (partitionColumn.getDataType().equals(PrimitiveType.DATE)) {
            rangePartitionFormat = DATE_FORMAT;
        } else if (partitionColumn.getDataType().equals(PrimitiveType.DATETIME)) {
            rangePartitionFormat = DATETIME_FORMAT;
        } else {
            rangePartitionFormat = TIMESTAMP_FORMAT;
        }

        String[] dbNameArr = fullDbName.split(":");
        String dbName = dbNameArr[dbNameArr.length - 1];
        HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) table.getDefaultDistributionInfo();
        List<String> hashKey = new ArrayList<>();
        for (Column distributionColumn : hashDistributionInfo.getDistributionColumns()) {
            hashKey.add(distributionColumn.getName());
        }

        Calendar calendar = Calendar.getInstance();
        for (int i=1; i<=table.getDynamicPartitionEnd(); ++i) {
            String partitionNamePrefix = table.getDynamicPartitionTemplate();
            String partitionName = partitionNamePrefix + getPartitionName(table.getDynamicPartitionTimeUnit(),
                    i, (Calendar) calendar.clone(), rangePartitionFormat);
            String validPartitionName = partitionName.replace("-", "");
            if (table.getPartition(validPartitionName) != null) {
                continue;
            }

            if (!(table.getDefaultDistributionInfo() instanceof HashDistributionInfo)) {
                LOG.warn("Auto add partition task should only support hash distribution buckets");
            }

            String nextBorder = getPartitionName(table.getDynamicPartitionTimeUnit(),
                    i + 1, (Calendar) calendar.clone(), rangePartitionFormat);
            String dynamicPartitionSql = String.format("ALTER TABLE %s.%s ADD PARTITION `%s` VALUES LESS THAN(\"%s\") " +
                    "(\"replication_num\" = \"%d\") DISTRIBUTED BY HASH(`%s`) BUCKETS %d;", dbName, table.getName(),
                    validPartitionName, nextBorder, estimateReplicateNum(table), StringUtils.join(hashKey, ", "),
                    table.getDynamicPartitionBuckets());
            try {
                SqlScanner sqlScanner = new SqlScanner(new StringReader(dynamicPartitionSql));
                SqlParser sqlParser = new SqlParser(sqlScanner);
                AlterTableStmt alterTableStmt = (AlterTableStmt) sqlParser.parse().value;
                alterTableStmt.setClusterName(clusterName);
                alterTableStmt.getTbl().setDb(fullDbName);
                Catalog.getInstance().getAlterInstance().processAlterTable(alterTableStmt);
            } catch (Exception e) {
                LOG.error("Dynamic partition failed, sql: {}, error: {}", dynamicPartitionSql, e);
            }
        }
    }

    private String getPartitionName(String timeUnit, int offset, Calendar calendar, String format) {
        if (timeUnit.equalsIgnoreCase(PropertyAnalyzer.DAY)) {
            calendar.add(Calendar.DAY_OF_MONTH, offset);
        } else if (timeUnit.equalsIgnoreCase(PropertyAnalyzer.WEEK)){
            calendar.add(Calendar.WEEK_OF_MONTH, offset);
        } else if (timeUnit.equalsIgnoreCase(PropertyAnalyzer.MONTH)) {
            calendar.add(Calendar.MONTH, offset);
        } else {
            LOG.error("Unsupported time unit: " + timeUnit);
            return null;
        }
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        return dateFormat.format(calendar.getTime());
    }

    private int estimateReplicateNum(OlapTable table) {
        int replicateNum = 3;
        long maxPartitionId = 0;
        for (Partition partition: table.getPartitions()) {
            if (partition.getId() > maxPartitionId) {
                maxPartitionId = partition.getId();
                replicateNum = table.getPartitionInfo().getReplicationNum(partition.getId());
            }
        }
        return replicateNum;
    }
}
