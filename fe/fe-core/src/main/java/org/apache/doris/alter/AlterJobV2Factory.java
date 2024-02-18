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

package org.apache.doris.alter;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.qe.OriginStatement;

import java.util.List;

public class AlterJobV2Factory {
    public static SchemaChangeJobV2 createSchemaChangeJobV2(String rawSql, long jobId, long dbId,
            long tableId, String tableName, long timeoutMs) {
        if (Config.isCloudMode()) {
            return new CloudSchemaChangeJobV2(rawSql, jobId, dbId, tableId, tableName, timeoutMs);
        } else {
            return new SchemaChangeJobV2(rawSql, jobId, dbId, tableId, tableName, timeoutMs, false);
        }
    }

    public static RollupJobV2 createRollupJobV2(String rawSql, long jobId, long dbId, long tableId,
            String tableName, long timeoutMs, long baseIndexId,
            long rollupIndexId, String baseIndexName, String rollupIndexName, List<Column> rollupSchema,
            Column whereColumn,
            int baseSchemaHash, int rollupSchemaHash, KeysType rollupKeysType,
            short rollupShortKeyColumnCount,
            OriginStatement origStmt) throws AnalysisException {
        if (Config.isCloudMode()) {
            return new CloudRollupJobV2(rawSql, jobId, dbId, tableId, tableName, timeoutMs, baseIndexId,
                    rollupIndexId, baseIndexName, rollupIndexName, rollupSchema, whereColumn,
                    baseSchemaHash, rollupSchemaHash, rollupKeysType, rollupShortKeyColumnCount, origStmt);
        } else {
            return new RollupJobV2(rawSql, jobId, dbId, tableId, tableName, timeoutMs, baseIndexId,
                    rollupIndexId, baseIndexName, rollupIndexName, rollupSchema, whereColumn,
                    baseSchemaHash, rollupSchemaHash, rollupKeysType, rollupShortKeyColumnCount, origStmt, false);
        }
    }

    public static AlterJobV2 rebuildAlterJobV2(AlterJobV2 job) throws AnalysisException {
        if (Config.isCloudMode()) {
            if (job.getType() == AlterJobV2.JobType.SCHEMA_CHANGE && !((SchemaChangeJobV2) job).isCloudSchemaChange()) {
                job = new CloudSchemaChangeJobV2((SchemaChangeJobV2) job);
            } else if (job.getType() == AlterJobV2.JobType.ROLLUP && !((RollupJobV2) job).isCloudRollup()) {
                job = new CloudRollupJobV2((RollupJobV2) job);
            }
        }
        return job;
    }
}
