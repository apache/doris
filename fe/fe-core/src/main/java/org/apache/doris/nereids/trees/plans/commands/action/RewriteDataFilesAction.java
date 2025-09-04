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

package org.apache.doris.nereids.trees.plans.commands.action;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionNamesInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Optional;

/**
 * Rewrite data files action for OPTIMIZE TABLE command.
 * Supports rewriting data files for Iceberg tables to optimize file layout and size.
 */
public class RewriteDataFilesAction extends OptimizeAction {
    private static final Logger LOG = LogManager.getLogger(RewriteDataFilesAction.class);

    // Rewrite-specific properties
    public static final String TARGET_FILE_SIZE = "target_file_size";
    public static final String MIN_FILE_SIZE = "min_file_size";
    public static final String MAX_FILE_SIZE = "max_file_size";
    public static final String MIN_INPUT_FILES = "min_input_files";
    public static final String REWRITE_JOB_ORDER = "rewrite_job_order";

    public RewriteDataFilesAction(Map<String, String> properties,
                                 Optional<PartitionNamesInfo> partitionNamesInfo,
                                 Optional<Expression> whereCondition,
                                 ExternalTable table) throws DdlException {
        super(ACTION_REWRITE_DATA_FILES, properties, partitionNamesInfo, whereCondition);
        validateIcebergTable(table);
    }

    @Override
    public void validate(TableNameInfo tableNameInfo, UserIdentity currentUser) throws UserException {
        validateCommon(tableNameInfo, currentUser);
        
        // Validate file size properties if specified
        validateFileSizeProperty(TARGET_FILE_SIZE);
        validateFileSizeProperty(MIN_FILE_SIZE);
        validateFileSizeProperty(MAX_FILE_SIZE);
        
        // Validate min input files
        String minInputFiles = properties.get(MIN_INPUT_FILES);
        if (minInputFiles != null) {
            try {
                int minFiles = Integer.parseInt(minInputFiles);
                if (minFiles <= 0) {
                    throw new DdlException("'min_input_files' must be a positive integer");
                }
            } catch (NumberFormatException e) {
                throw new DdlException("'min_input_files' must be a valid integer");
            }
        }
        
        // Validate rewrite job order
        String jobOrder = properties.get(REWRITE_JOB_ORDER);
        if (jobOrder != null && !jobOrder.matches("(?i)(bytes-asc|bytes-desc|files-asc|files-desc|none)")) {
            throw new DdlException("'rewrite_job_order' must be one of: bytes-asc, bytes-desc, files-asc, files-desc, none");
        }
    }

    @Override
    public void execute(ExternalTable table) throws UserException {
        IcebergExternalTable icebergTable = (IcebergExternalTable) table;
        
        LOG.info("Executing rewrite data files action for table: {}.{}.{}", 
                icebergTable.getCatalog().getName(), icebergTable.getDbName(), icebergTable.getName());

        try {
            org.apache.iceberg.Table icebergTableInstance = icebergTable.getIcebergTable();
            
            // TODO: Implement actual Iceberg rewrite data files logic
            // This would involve:
            // 1. Creating RewriteFiles action
            // 2. Applying partition filters if specified
            // 3. Applying WHERE conditions if specified
            // 4. Setting rewrite parameters (file sizes, etc.)
            // 5. Executing the rewrite
            
            // Example structure:
            // RewriteFiles rewriteFiles = icebergTableInstance.newRewrite();
            // 
            // if (partitionNamesInfo.isPresent()) {
            //     // Apply partition filters
            //     for (String partition : partitionNamesInfo.get().getPartitionNames()) {
            //         // Add partition filter
            //     }
            // }
            // 
            // if (whereCondition.isPresent()) {
            //     // Apply row-level filters
            //     // Convert Expression to Iceberg filter
            // }
            // 
            // // Configure rewrite parameters
            // String targetFileSize = getProperty(TARGET_FILE_SIZE, null);
            // if (targetFileSize != null) {
            //     rewriteFiles = rewriteFiles.option("target-file-size-bytes", targetFileSize);
            // }
            // 
            // String minInputFiles = getProperty(MIN_INPUT_FILES, "5");
            // rewriteFiles = rewriteFiles.option("min-input-files", minInputFiles);
            // 
            // rewriteFiles.commit();
            
            throw new DdlException("Rewrite data files action implementation is not yet complete");
            
        } catch (Exception e) {
            LOG.error("Failed to execute rewrite data files action for Iceberg table: {}", 
                     icebergTable.getName(), e);
            throw new DdlException("Failed to rewrite data files for Iceberg table: " + e.getMessage());
        }
    }

    @Override
    public boolean isSupported(ExternalTable table) {
        return table instanceof IcebergExternalTable;
    }

    @Override
    public String getDescription() {
        StringBuilder desc = new StringBuilder("Rewrite data files");
        
        String targetFileSize = getProperty(TARGET_FILE_SIZE, null);
        if (targetFileSize != null) {
            desc.append(" with target file size: ").append(targetFileSize);
        }
        
        if (partitionNamesInfo.isPresent()) {
            desc.append(" for partitions: ").append(partitionNamesInfo.get().getPartitionNames());
        }
        
        if (whereCondition.isPresent()) {
            desc.append(" with filter condition");
        }
        
        return desc.toString();
    }

    private void validateFileSizeProperty(String propertyName) throws DdlException {
        String value = properties.get(propertyName);
        if (value != null) {
            try {
                long size = Long.parseLong(value);
                if (size <= 0) {
                    throw new DdlException(propertyName + " must be a positive number");
                }
            } catch (NumberFormatException e) {
                throw new DdlException(propertyName + " must be a valid number");
            }
        }
    }
}
