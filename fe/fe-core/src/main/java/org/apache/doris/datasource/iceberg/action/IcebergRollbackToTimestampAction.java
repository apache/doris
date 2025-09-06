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
 * Iceberg rollback to timestamp action implementation.
 * This action rolls back the Iceberg table to a specific point in time.
 */
public class IcebergRollbackToTimestampAction extends BaseIcebergAction {
    public static final String TIMESTAMP = "timestamp";

    public IcebergRollbackToTimestampAction(Map<String, String> properties,
            Optional<PartitionNamesInfo> partitionNamesInfo,
            Optional<Expression> whereCondition,
            IcebergExternalTable icebergTable) {
        super("rollback_to_timestamp", properties, partitionNamesInfo, whereCondition, icebergTable);
    }

    @Override
    protected void validateIcebergAction() throws UserException {
        // Validate required properties for Iceberg rollback procedure
        String timestamp = getRequiredProperty(TIMESTAMP);

        // Validate timestamp format
        try {
            // Try to parse as ISO datetime first
            LocalDateTime.parse(timestamp, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        } catch (DateTimeParseException e) {
            try {
                // Try to parse as timestamp (milliseconds since epoch)
                long timestampMs = Long.parseLong(timestamp);
                if (timestampMs < 0) {
                    throw new DdlException("Timestamp must be non-negative: " + timestamp);
                }
            } catch (NumberFormatException nfe) {
                throw new DdlException("Invalid timestamp format. Expected ISO datetime "
                        + "(yyyy-MM-ddTHH:mm:ss) or timestamp in milliseconds: " + timestamp);
            }
        }

        // Iceberg procedures don't support partitions or where conditions
        validateNoPartitions();
        validateNoWhereCondition();
    }

    @Override
    public void execute(TableIf table) throws UserException {
        throw new DdlException("Iceberg rollback_to_timestamp procedure is not implemented yet");
    }

    @Override
    public String getDescription() {
        return "Rollback Iceberg table to a specific timestamp";
    }
}
