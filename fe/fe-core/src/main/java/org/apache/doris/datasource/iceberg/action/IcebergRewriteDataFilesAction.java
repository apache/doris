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
import org.apache.doris.datasource.iceberg.rewrite.RewriteDataFileManager;
import org.apache.doris.datasource.iceberg.rewrite.RewriteDataGroup;
import org.apache.doris.datasource.iceberg.rewrite.RewriteResult;
import org.apache.doris.datasource.iceberg.rewrite.RewriteJob;
import org.apache.doris.datasource.iceberg.rewrite.RewriteJobManager;
import org.apache.doris.datasource.iceberg.rewrite.RewriteJobState;
import org.apache.doris.info.PartitionNamesInfo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Action for rewriting Iceberg data files by executing INSERT-SELECT operations.
 *
 * Execution Flow:
 * 1. Validate rewrite parameters and get Iceberg table
 * 2. Use RewriteDataFileManager to scan files and organize them into groups
 * 3. Create RewriteDataFileExecutor to execute rewrite for each group
 * 4. Manage IcebergTransaction directly to commit all changes atomically
 * 5. Collect and return execution results including statistics
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

    // Concurrency control
    private static final String PARALLELISM = "parallelism";
    private int parallelism = 1;

    public IcebergRewriteDataFilesAction(Map<String, String> properties,
            Optional<PartitionNamesInfo> partitionNamesInfo,
            Optional<Expression> whereCondition,
            IcebergExternalTable icebergTable) {
        super("rewrite_data_files", properties, partitionNamesInfo, whereCondition, icebergTable);
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

        // Parallelism argument
        namedArguments.registerOptionalArgument(PARALLELISM,
                        "Number of parallel tasks for rewrite execution",
                        1,
                        ArgumentParsers.intRange(PARALLELISM, 1, 32));
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

        // Get parallelism parameter
        this.parallelism = namedArguments.getInt(PARALLELISM);
    }

    @Override
    protected List<String> executeAction(TableIf table) throws UserException {
        try {
            org.apache.iceberg.Table icebergTable = IcebergUtils.getIcebergTable(this.icebergTable);

            if (icebergTable.currentSnapshot() == null) {
                LOG.info("Table {} has no data, skipping rewrite", table.getName());
                return Lists.newArrayList("0", "0", "0", "0");
            }

            RewriteDataFileManager.Parameters parameters = buildRewriteParameters();

            ConnectContext connectContext = ConnectContext.get();
            if (connectContext == null) {
                throw new UserException("No active connection context found");
            }

            // Step 1: Plan and organize file scan tasks into groups
            RewriteDataFileManager fileManager = new RewriteDataFileManager(icebergTable, parameters);
            fileManager.planAndOrganizeTasks();

            if (fileManager.getTotalGroupCount() == 0) {
                LOG.info("No file groups need rewriting for table: {}", table.getName());
                return Lists.newArrayList("0", "0", "0", "0");
            }

            // Step 2: Create and start concurrent rewrite job
            RewriteJobManager jobManager = new RewriteJobManager();
            String jobLabel = "rewrite_" + table.getName() + "_" + System.currentTimeMillis();

            RewriteJob rewriteJob = jobManager.createAndStartJob(
                            jobLabel, this.icebergTable, parameters, connectContext, parallelism);

            // Get all groups for concurrent processing
            List<RewriteDataGroup> allGroups = fileManager.getAllGroups();

            // Start the job with groups
            jobManager.startRewriteJob(rewriteJob, allGroups, parallelism);

            // Wait for job completion (in a real implementation, this would be
            // asynchronous)
            // For now, we'll simulate waiting by checking job state
            RewriteResult totalResult = waitForJobCompletion(rewriteJob, jobManager);

            LOG.info("Rewrite data files completed for table: {}, result: {}",
                    table.getName(), totalResult);
            return totalResult.toStringList();

        } catch (Exception e) {
            LOG.error("Failed to rewrite data files for table: " + table.getName(), e);
            throw new UserException("Rewrite data files failed: " + e.getMessage());
        }
    }

    private RewriteDataFileManager.Parameters buildRewriteParameters() {
        return new RewriteDataFileManager.Parameters(
                namedArguments.getLong(TARGET_FILE_SIZE_BYTES),
                this.minFileSizeBytes,
                this.maxFileSizeBytes,
                namedArguments.getInt(MIN_INPUT_FILES),
                namedArguments.getBoolean(REWRITE_ALL),
                namedArguments.getLong(MAX_FILE_GROUP_SIZE_BYTES),
                namedArguments.getInt(DELETE_FILE_THRESHOLD),
                namedArguments.getDouble(DELETE_RATIO_THRESHOLD),
                namedArguments.getLong(OUTPUT_SPEC_ID),
                partitionNamesInfo,
                whereCondition);
    }

    /**
     * Wait for job completion and return result
     */
    private RewriteResult waitForJobCompletion(RewriteJob job, RewriteJobManager jobManager)
                    throws UserException {
            LOG.info("Waiting for rewrite job {} to complete", job.getId());

            // In a real implementation, this would use proper synchronization
            // For now, we'll use a simple polling approach
            int maxWaitTime = 300; // 5 minutes
            int waitInterval = 1000; // 1 second
            int waited = 0;

            while (!job.isFinalState() && waited < maxWaitTime * 1000) {
                    try {
                            Thread.sleep(waitInterval);
                            waited += waitInterval;

                            // Check job state periodically
                            RewriteJob currentJob = jobManager.getJob(job.getId());
                            if (currentJob != null) {
                                    LOG.debug("Job {} state: {}, progress: {}%",
                                                    currentJob.getId(), currentJob.getState(),
                                                    currentJob.getProgress());
                            }
                    } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new UserException("Wait for job completion was interrupted");
                    }
            }

            if (!job.isFinalState()) {
                    throw new UserException("Rewrite job did not complete within timeout");
            }

            if (job.getState() == RewriteJobState.CANCELLED) {
                    throw new UserException("Rewrite job was cancelled: " + job.getFailMsg());
            }

            RewriteResult result = job.getTotalResult();
            if (result == null) {
                    result = new RewriteResult();
            }

            LOG.info("Rewrite job {} completed with result: {}", job.getId(), result);
            return result;
    }

    @Override
    public String getDescription() {
        return "Rewrite Iceberg data files to optimize file sizes and improve query performance";
    }
}
