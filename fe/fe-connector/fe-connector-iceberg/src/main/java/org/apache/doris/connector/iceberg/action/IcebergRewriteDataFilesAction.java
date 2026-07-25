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

package org.apache.doris.connector.iceberg.action;

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.pushdown.ConnectorPredicate;
import org.apache.doris.connector.iceberg.rewrite.RewriteDataFilePlanner;
import org.apache.doris.foundation.util.ArgumentParsers;

import org.apache.iceberg.Table;

import java.util.List;
import java.util.Map;

/**
 * Argument spec + planning-parameter builder for iceberg {@code ALTER TABLE EXECUTE rewrite_data_files(...)}.
 *
 * <p>Connector port of the argument half of fe-core {@code datasource/iceberg/action/IcebergRewriteDataFilesAction}
 * (P6.4-T05/T06, WS-REWRITE R3). It registers and validates the ten rewrite arguments (reusing the shared
 * fe-foundation {@link org.apache.doris.foundation.util.NamedArguments} framework, exactly as the engine did,
 * so the error strings stay byte-identical) and turns the validated arguments into a neutral
 * {@link RewriteDataFilePlanner.Parameters}.</p>
 *
 * <p><b>Not a {@code SINGLE_CALL} body.</b> {@code rewrite_data_files} is a {@code DISTRIBUTED} procedure: the
 * actual rewrite is N per-group {@code INSERT-SELECT} writes driven by the engine, not a synchronous SDK call.
 * So this action is NOT reachable through {@link IcebergExecuteActionFactory#createAction} (which deliberately
 * rejects {@code rewrite_data_files}) and {@link #executeAction} is never invoked — it is built directly by
 * {@code IcebergProcedureOps.planRewrite}, which calls {@link #buildRewriteParameters()} and runs the
 * connector {@link RewriteDataFilePlanner}. Live since the iceberg SPI cutover.</p>
 */
public class IcebergRewriteDataFilesAction extends BaseIcebergAction {

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

    // Parameters with special default handling (resolved in validateIcebergAction, read by buildRewriteParameters)
    private long minFileSizeBytes;
    private long maxFileSizeBytes;

    public IcebergRewriteDataFilesAction(Map<String, String> properties, List<String> partitionNames,
            ConnectorPredicate whereCondition) {
        super("rewrite_data_files", properties, partitionNames, whereCondition);
    }

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
    protected void validateIcebergAction() {
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
        if (this.minFileSizeBytes > this.maxFileSizeBytes) {
            throw new DorisConnectorException(
                    "min-file-size-bytes must be less than or equal to max-file-size-bytes");
        }
        validateNoPartitions();
    }

    /**
     * Builds the neutral planner parameters from the validated arguments. Must be called after
     * {@link #validate()} (which resolves the min/max file-size defaults). Mirrors the legacy
     * {@code buildRewriteParameters}; the engine-lowered {@link ConnectorPredicate} {@code WHERE} is threaded
     * straight through (the planner lowers it to iceberg expressions).
     */
    public RewriteDataFilePlanner.Parameters buildRewriteParameters() {
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
    protected List<String> executeAction(Table table, ConnectorSession session) {
        // rewrite_data_files is DISTRIBUTED: it is planned via buildRewriteParameters + the engine rewrite
        // driver, never run as a synchronous single-call body. This is unreachable (the factory rejects the
        // name); guard loudly in case a future caller wires it wrong.
        throw new DorisConnectorException(
                "rewrite_data_files is a distributed procedure and has no single-call body; "
                        + "plan it via IcebergProcedureOps.planRewrite");
    }
}
