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
 * This action removes old snapshots from Iceberg tables to free up storage space
 * and improve metadata performance.
 */
public class IcebergExpireSnapshotsAction extends BaseIcebergAction {
    public static final String OLDER_THAN = "older_than";
    public static final String RETAIN_LAST = "retain_last";
    public static final String MAX_CONCURRENT_DELETES = "max_concurrent_deletes";
    public static final String STREAM_RESULTS = "stream_results";

    public IcebergExpireSnapshotsAction(Map<String, String> properties,
            Optional<PartitionNamesInfo> partitionNamesInfo,
            Optional<Expression> whereCondition,
            IcebergExternalTable icebergTable) {
        super("expire_snapshots", properties, partitionNamesInfo, whereCondition, icebergTable);
    }

    @Override
    protected void validateIcebergAction() throws UserException {
        // Validate older_than parameter (timestamp)
        if (properties.containsKey(OLDER_THAN)) {
            String olderThan = properties.get(OLDER_THAN);
            try {
                // Try to parse as ISO datetime format
                LocalDateTime.parse(olderThan, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            } catch (DateTimeParseException e) {
                try {
                    // Try to parse as timestamp (milliseconds since epoch)
                    long timestamp = Long.parseLong(olderThan);
                    if (timestamp < 0) {
                        throw new DdlException("older_than timestamp must be non-negative");
                    }
                } catch (NumberFormatException nfe) {
                    throw new DdlException("Invalid older_than format. Expected ISO datetime "
                            + "(yyyy-MM-ddTHH:mm:ss) or timestamp in milliseconds: " + olderThan);
                }
            }
        }

        // Validate retain_last parameter
        if (properties.containsKey(RETAIN_LAST)) {
            try {
                int retainLast = Integer.parseInt(properties.get(RETAIN_LAST));
                if (retainLast < 1) {
                    throw new DdlException("retain_last must be at least 1");
                }
            } catch (NumberFormatException e) {
                throw new DdlException("Invalid retain_last format: " + properties.get(RETAIN_LAST));
            }
        }

        // At least one of older_than or retain_last must be specified
        if (!properties.containsKey(OLDER_THAN) && !properties.containsKey(RETAIN_LAST)) {
            throw new DdlException("At least one of 'older_than' or 'retain_last' must be specified");
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
