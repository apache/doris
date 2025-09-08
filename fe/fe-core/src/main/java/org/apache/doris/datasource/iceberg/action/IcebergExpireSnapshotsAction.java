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

package org.apache.doris.datasource.iceberg.action;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ArgumentParsers;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionNamesInfo;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.Optional;

/**
 * Iceberg expire snapshots action implementation.
 * This action removes old snapshots from Iceberg tables to free up storage
 * space
 * and improve metadata performance.
 */
public class IcebergExpireSnapshotsAction extends BaseIcebergAction {
    public static final String OLDER_THAN = "older_than";
    public static final String RETAIN_LAST = "retain_last";
    public static final String MAX_CONCURRENT_DELETES = "max_concurrent_deletes";
    public static final String STREAM_RESULTS = "stream_results";
    public static final String SNAPSHOT_IDS = "snapshot_ids";
    public static final String CLEAN_EXPIRED_METADATA = "clean_expired_metadata";

    public IcebergExpireSnapshotsAction(Map<String, String> properties,
            Optional<PartitionNamesInfo> partitionNamesInfo,
            Optional<Expression> whereCondition,
            IcebergExternalTable icebergTable) {
        super("expire_snapshots", properties, partitionNamesInfo, whereCondition, icebergTable);
    }

    @Override
    protected void registerIcebergArguments() {
        // Register optional arguments for expire_snapshots
        namedArguments.registerOptionalArgument(OLDER_THAN,
                "Timestamp before which snapshots will be removed",
                null, ArgumentParsers.nonEmptyString(OLDER_THAN));
        namedArguments.registerOptionalArgument(RETAIN_LAST,
                "Number of ancestor snapshots to preserve regardless of older_than",
                null, ArgumentParsers.positiveInt(RETAIN_LAST));
        namedArguments.registerOptionalArgument(MAX_CONCURRENT_DELETES,
                "Size of the thread pool used for delete file actions",
                null, ArgumentParsers.positiveInt(MAX_CONCURRENT_DELETES));
        namedArguments.registerOptionalArgument(STREAM_RESULTS,
                "When true, deletion files will be sent to Spark driver by RDD partition",
                null, ArgumentParsers.booleanValue(STREAM_RESULTS));
        namedArguments.registerOptionalArgument(SNAPSHOT_IDS,
                "Array of snapshot IDs to expire",
                null, ArgumentParsers.nonEmptyString(SNAPSHOT_IDS));
        namedArguments.registerOptionalArgument(CLEAN_EXPIRED_METADATA,
                "When true, cleans up metadata such as partition specs and schemas",
                null, ArgumentParsers.booleanValue(CLEAN_EXPIRED_METADATA));
    }

    @Override
    protected void validateIcebergAction() throws UserException {
        // Validate older_than parameter (timestamp)
        String olderThan = namedArguments.getString(OLDER_THAN);
        if (olderThan != null) {
            try {
                // Try to parse as ISO datetime format
                LocalDateTime.parse(olderThan, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            } catch (DateTimeParseException e) {
                try {
                    // Try to parse as timestamp (milliseconds since epoch)
                    long timestamp = Long.parseLong(olderThan);
                    if (timestamp < 0) {
                        throw new AnalysisException("older_than timestamp must be non-negative");
                    }
                } catch (NumberFormatException nfe) {
                    throw new AnalysisException("Invalid older_than format. Expected ISO datetime "
                            + "(yyyy-MM-ddTHH:mm:ss) or timestamp in milliseconds: " + olderThan);
                }
            }
        }

        // Validate retain_last parameter
        Integer retainLast = namedArguments.getInt(RETAIN_LAST);
        if (retainLast != null && retainLast < 1) {
            throw new AnalysisException("retain_last must be at least 1");
        }

        // At least one of older_than or retain_last must be specified for validation
        if (olderThan == null && retainLast == null) {
            throw new AnalysisException("At least one of 'older_than' or 'retain_last' must be specified");
        }

        // Iceberg procedures don't support partitions or where conditions
        validateNoPartitions();
        validateNoWhereCondition();
    }

    @Override
    public void execute(TableIf table) throws UserException {
        throw new DdlException("Iceberg expire_snapshots procedure is not implemented yet");
    }

    @Override
    public String getDescription() {
        return "Expire old Iceberg snapshots to free up storage space and improve metadata performance";
    }
}
