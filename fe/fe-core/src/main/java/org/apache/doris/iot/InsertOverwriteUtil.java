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

package org.apache.doris.iot;

import org.apache.doris.analysis.AddPartitionLikeClause;
import org.apache.doris.analysis.DropPartitionClause;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.ReplacePartitionClause;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.DdlException;

import com.google.common.collect.Maps;
import static org.apache.doris.common.util.PropertyAnalyzer.PROPERTIES_USE_TEMP_PARTITION_NAME;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class InsertOverwriteUtil {
    private static final Logger LOG = LogManager.getLogger(InsertOverwriteUtil.class);

    /**
     * add temp partitions
     *
     * @param olapTable
     * @param partitionNames
     * @param tempPartitionNames
     * @throws DdlException
     */
    public static void addTempPartitions(OlapTable olapTable, List<String> partitionNames,
            List<String> tempPartitionNames) throws DdlException {
        for (int i = 0; i < partitionNames.size(); i++) {
            Env.getCurrentEnv().addPartitionLike((Database) olapTable.getDatabase(), olapTable.getQualifiedName(),
                    new AddPartitionLikeClause(partitionNames.get(i), tempPartitionNames.get(i), true));
        }
    }

    /**
     * replace partitions
     *
     * @param olapTable
     * @param partitionNames
     * @param tempPartitionNames
     * @throws DdlException
     */
    public static void replacePartition(OlapTable olapTable, List<String> partitionNames,
            List<String> tempPartitionNames) throws DdlException {
        try {
            if (!olapTable.writeLockIfExist()) {
                return;
            }
            Map<String, String> properties = Maps.newHashMap();
            properties.put(PROPERTIES_USE_TEMP_PARTITION_NAME, "false");
            ReplacePartitionClause replacePartitionClause = new ReplacePartitionClause(
                    new PartitionNames(false, partitionNames),
                    new PartitionNames(true, tempPartitionNames), properties);
            Env.getCurrentEnv()
                    .replaceTempPartition((Database) olapTable.getDatabase(), olapTable, replacePartitionClause);
        } finally {
            olapTable.writeUnlock();
        }

    }

    /**
     * generate temp partitionName
     *
     * @param partitionNames
     * @return
     */
    public static List<String> generateTempPartitionNames(List<String> partitionNames) {
        List<String> tempPartitionNames = new ArrayList<String>(partitionNames.size());
        for (String partitionName : partitionNames) {
            tempPartitionNames.add("iot_temp_" + partitionName);
        }
        return tempPartitionNames;
    }

    /**
     * drop temp partitions
     *
     * @param olapTable
     * @param tempPartitionNames
     * @return
     */
    public static boolean dropPartitions(OlapTable olapTable, List<String> tempPartitionNames) {
        try {
            if (!olapTable.writeLockIfExist()) {
                return true;
            }
            for (String partitionName : tempPartitionNames) {
                if (olapTable.getPartition(partitionName, true) == null) {
                    continue;
                }
                Env.getCurrentEnv().dropPartition(
                        (Database) olapTable.getDatabase(), olapTable,
                        new DropPartitionClause(true, partitionName, true, true));
                LOG.info("successfully drop temp partition [{}] for [{}]", partitionName, olapTable.getName());
            }
        } catch (DdlException e) {
            LOG.info("drop partition failed for [{}]", olapTable.getName(), e);
            return false;
        } finally {
            olapTable.writeUnlock();
        }
        return true;
    }
}
