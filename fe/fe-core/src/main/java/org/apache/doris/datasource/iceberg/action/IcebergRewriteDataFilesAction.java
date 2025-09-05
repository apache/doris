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

import java.util.Map;
import java.util.Optional;

/**
 * Iceberg rewrite data files action implementation.
 * This action rewrites data files in Iceberg tables to optimize file sizes
 * and improve query performance.
 */
public class IcebergRewriteDataFilesAction extends BaseIcebergAction {
    public static final String TARGET_FILE_SIZE = "target_file_size";
    public static final String MIN_INPUT_FILES = "min_input_files";
    public static final String MAX_CONCURRENT_FILE_GROUP_REWRITES = "max_concurrent_file_group_rewrites";
    public static final String PARTIAL_PROGRESS_ENABLED = "partial_progress_enabled";

    public IcebergRewriteDataFilesAction(Map<String, String> properties,
            Optional<PartitionNamesInfo> partitionNamesInfo,
            Optional<Expression> whereCondition,
            IcebergExternalTable icebergTable) {
        super("rewrite_data_files", properties, partitionNamesInfo, whereCondition, icebergTable);
    }

    @Override
    protected void validateIcebergAction() throws UserException {
        // Validate target_file_size parameter
        if (properties.containsKey(TARGET_FILE_SIZE)) {
            try {
                long targetFileSize = Long.parseLong(properties.get(TARGET_FILE_SIZE));
                if (targetFileSize <= 0) {
                    throw new DdlException("target_file_size must be positive");
                }
            } catch (NumberFormatException e) {
                throw new DdlException("Invalid target_file_size format: " + properties.get(TARGET_FILE_SIZE));
            }
        }

        // Validate min_input_files parameter
        if (properties.containsKey(MIN_INPUT_FILES)) {
            try {
                int minInputFiles = Integer.parseInt(properties.get(MIN_INPUT_FILES));
                if (minInputFiles < 1) {
                    throw new DdlException("min_input_files must be at least 1");
                }
            } catch (NumberFormatException e) {
                throw new DdlException("Invalid min_input_files format: " + properties.get(MIN_INPUT_FILES));
            }
        }

        // Validate partial_progress_enabled parameter
        if (properties.containsKey(PARTIAL_PROGRESS_ENABLED)) {
            String partialProgressEnabled = properties.get(PARTIAL_PROGRESS_ENABLED);
            if (!"true".equalsIgnoreCase(partialProgressEnabled) && !"false".equalsIgnoreCase(partialProgressEnabled)) {
                throw new DdlException("partial_progress_enabled must be 'true' or 'false'");
            }
        }

        // Iceberg procedures don't support partitions or where conditions
        validateNoPartitions();
        validateNoWhereCondition();
    }

    @Override
    public void execute(TableIf table) throws UserException {
        throw new DdlException("Iceberg rewrite_data_files procedure is not implemented yet");
    }

    @Override
    public String getDescription() {
        return "Rewrite Iceberg data files to optimize file sizes and improve query performance";
    }
}