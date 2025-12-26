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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.ArgumentParsers;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.datasource.iceberg.rewrite.RewriteDataFileExecutor;
import org.apache.doris.datasource.iceberg.rewrite.RewriteDataFilePlanner;
import org.apache.doris.datasource.iceberg.rewrite.RewriteDataGroup;
import org.apache.doris.datasource.iceberg.rewrite.RewriteResult;
import org.apache.doris.info.PartitionNamesInfo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import org.apache.iceberg.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Action for rewriting Iceberg data files to compact and optimize table data
 *
 * Execution Flow:
 * 1. Validate rewrite parameters and get Iceberg table
 * 2. Use RewriteDataFilePlanner to plan and organize file scan tasks into rewrite groups
 * 3. Create and start rewrite job concurrently
 * 4. Wait for job completion and return result
 */
public class IcebergRewriteDataFilesAction extends BaseIcebergAction {
    private static final Logger LOG = LogManager.getLogger(IcebergRewriteDataFilesAction.class);
    // File size parameters
    public static final String TARGET_FILE_SIZE_BYTES = "target-file-size-bytes";
    public static final String MIN_FILE_SIZE_BYTES = "min-file-size-bytes";
    public static final String MAX_FILE_SIZE_BYTES = "max-file-size-bytes";

    // Input files parameters
    public static final String MIN_INPUT_FILES = "min-input-files";
    public static final String REWRITE_ALL = "rewrite-all";
    public static final String MAX_FILE_GROUP_SIZE_BYTES = "max-file-group-size-bytes";

    // Delete files parameters
    public static final String DELETE_FILE_THRESHOLD = "delete-file-threshold";
    public static final String DELETE_RATIO_THRESHOLD = "delete-ratio-threshold";

    // Output specification parameter
    public static final String OUTPUT_SPEC_ID = "output-spec-id";

    // Parameters with special default handling
    private long minFileSizeBytes;
    private long maxFileSizeBytes;

    public IcebergRewriteDataFilesAction(Map<String, String> properties,
            Optional<PartitionNamesInfo> partitionNamesInfo,
            Optional<Expression> whereCondition) {
        super("rewrite_data_files", properties, partitionNamesInfo, whereCondition);
    }

    /**
     * Register all arguments supported by rewrite_data_files action.
     */
    @Override
    protected void registerIcebergArguments() {
        // File size arguments
        namedArguments.registerOptionalArgument(TARGET_FILE_SIZE_BYTES,
                "Target file size in bytes for output files",
                536870912L,
                ArgumentParsers.positiveLong(TARGET_FILE_SIZE_BYTES));

        namedArguments.registerOptionalArgument(MIN_FILE_SIZE_BYTES,
                "Minimum file size in bytes for files to be rewritten",
                0L,
                ArgumentParsers.positiveLong(MIN_FILE_SIZE_BYTES));

        namedArguments.registerOptionalArgument(MAX_FILE_SIZE_BYTES,
                "Maximum file size in bytes for files to be rewritten",
                0L,
                ArgumentParsers.positiveLong(MAX_FILE_SIZE_BYTES));

        // Input files arguments
        namedArguments.registerOptionalArgument(MIN_INPUT_FILES,
                "Minimum number of input files to rewrite together",
                5,
                ArgumentParsers.intRange(MIN_INPUT_FILES, 1, 10000));

        namedArguments.registerOptionalArgument(REWRITE_ALL,
                "Whether to rewrite all files regardless of size",
                false,
                ArgumentParsers.booleanValue(REWRITE_ALL));

        namedArguments.registerOptionalArgument(MAX_FILE_GROUP_SIZE_BYTES,
                "Maximum size in bytes for a file group to be rewritten",
                107374182400L,
                ArgumentParsers.positiveLong(MAX_FILE_GROUP_SIZE_BYTES));

        // Delete files arguments
        namedArguments.registerOptionalArgument(DELETE_FILE_THRESHOLD,
                "Minimum number of delete files to trigger rewrite",
                Integer.MAX_VALUE,
                ArgumentParsers.intRange(DELETE_FILE_THRESHOLD, 1, Integer.MAX_VALUE));

        namedArguments.registerOptionalArgument(DELETE_RATIO_THRESHOLD,
                "Minimum ratio of delete records to total records to trigger rewrite",
                0.3,
                ArgumentParsers.doubleRange(DELETE_RATIO_THRESHOLD, 0.0, 1.0));

        // Output specification argument
        namedArguments.registerOptionalArgument(OUTPUT_SPEC_ID,
                "Partition specification ID for output files",
                2L,
                ArgumentParsers.positiveLong(OUTPUT_SPEC_ID));
    }

    @Override
    protected List<Column> getResultSchema() {
        return Lists.newArrayList(
                new Column("rewritten_data_files_count", Type.INT, false,
                        "Number of data which were re-written by this command"),
                new Column("added_data_files_count", Type.INT, false,
                        "Number of new data files which were written by this command"),
                new Column("rewritten_bytes_count", Type.INT, false,
                        "Number of bytes which were written by this command"),
                new Column("removed_delete_files_count", Type.BIGINT, false,
                        "Number of delete files removed by this command"));
    }

    @Override
    protected void validateIcebergAction() throws UserException {
        // Validate min and max file size parameters
        long targetFileSizeBytes = namedArguments.getLong(TARGET_FILE_SIZE_BYTES);
        // min-file-size-bytes default to 75% of target file size
        this.minFileSizeBytes = namedArguments.getLong(MIN_FILE_SIZE_BYTES);
        if (this.minFileSizeBytes == 0) {
            this.minFileSizeBytes = (long) (targetFileSizeBytes * 0.75);
        }
        // max-file-size-bytes default to 180% of target file size
        this.maxFileSizeBytes = namedArguments.getLong(MAX_FILE_SIZE_BYTES);
        if (this.maxFileSizeBytes == 0) {
            this.maxFileSizeBytes = (long) (targetFileSizeBytes * 1.8);
        }
        validateNoPartitions();
    }

    @Override
    protected List<String> executeAction(TableIf table) throws UserException {
        try {
            Table icebergTable = IcebergUtils.getIcebergTable((IcebergExternalTable) table);

            if (icebergTable.currentSnapshot() == null) {
                LOG.info("Table {} has no data, skipping rewrite", table.getName());
                // return empty result
                return Lists.newArrayList("0", "0", "0", "0");
            }

            RewriteDataFilePlanner.Parameters parameters = buildRewriteParameters();

            ConnectContext connectContext = ConnectContext.get();
            if (connectContext == null) {
                throw new UserException("No active connection context found");
            }

            // Step 1: Plan and organize file scan tasks into groups
            RewriteDataFilePlanner fileManager = new RewriteDataFilePlanner(parameters);
            Iterable<RewriteDataGroup> allGroups = fileManager.planAndOrganizeTasks(icebergTable);

            // Step 2: Execute rewrite groups concurrently
            List<RewriteDataGroup> groupsList = Lists.newArrayList(allGroups);

            // Create executor and execute groups concurrently
            RewriteDataFileExecutor executor = new RewriteDataFileExecutor(
                    (IcebergExternalTable) table, connectContext);
            long targetFileSizeBytes = namedArguments.getLong(TARGET_FILE_SIZE_BYTES);
            RewriteResult totalResult = executor.executeGroupsConcurrently(groupsList, targetFileSizeBytes);
            return totalResult.toStringList();
        } catch (Exception e) {
            LOG.warn("Failed to rewrite data files for table: " + table.getName(), e);
            throw new UserException("Rewrite data files failed: " + e.getMessage());
        }
    }

    private RewriteDataFilePlanner.Parameters buildRewriteParameters() {
        return new RewriteDataFilePlanner.Parameters(
                namedArguments.getLong(TARGET_FILE_SIZE_BYTES),
                this.minFileSizeBytes,
                this.maxFileSizeBytes,
                namedArguments.getInt(MIN_INPUT_FILES),
                namedArguments.getBoolean(REWRITE_ALL),
                namedArguments.getLong(MAX_FILE_GROUP_SIZE_BYTES),
                namedArguments.getInt(DELETE_FILE_THRESHOLD),
                namedArguments.getDouble(DELETE_RATIO_THRESHOLD),
                namedArguments.getLong(OUTPUT_SPEC_ID),
                whereCondition);
    }

    @Override
    public String getDescription() {
        return "Rewrite Iceberg data files to optimize file sizes and improve query performance";
    }
}
