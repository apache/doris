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

package org.apache.doris.tablefunction;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.trinoconnector.TrinoConnectorExternalCatalog;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;
import lombok.Getter;

import java.util.List;
import java.util.Map;

/**
 * Kafka Table-Valued Function implementation.
 * 
 * This TVF provides a way to read from Kafka topics via a Trino Kafka Connector catalog.
 * It is designed primarily for use with streaming jobs (CREATE JOB ... ON STREAMING).
 * 
 * Parameters:
 * - catalog: Name of the Trino Kafka Catalog (required)
 * - database: Database/schema name, default "default"
 * - table: Kafka topic name (required)
 * - kafka_default_offsets: Initial offset position, "OFFSET_BEGINNING" or "OFFSET_END"
 * - max_batch_rows: Maximum rows per batch
 * 
 * Note: When used in a streaming job, the KafkaSourceOffsetProvider will rewrite
 * this TVF to a direct table reference with partition and offset filtering.
 * Therefore, getScanNode() is not implemented as it won't be called directly.
 */
@Getter
public class KafkaTableValuedFunction extends TableValuedFunctionIf {
    
    public static final String NAME = "kafka";
    
    // Parameter names
    public static final String PARAM_CATALOG = "catalog";
    public static final String PARAM_DATABASE = "database";
    public static final String PARAM_TABLE = "table";
    public static final String PARAM_DEFAULT_OFFSETS = "kafka_default_offsets";
    public static final String PARAM_MAX_BATCH_ROWS = "max_batch_rows";
    
    // Default values
    public static final String DEFAULT_DATABASE = "default";
    public static final String DEFAULT_OFFSETS = "OFFSET_END";
    public static final long DEFAULT_MAX_BATCH_ROWS = 100000L;
    
    private final String catalogName;
    private final String databaseName;
    private final String tableName;
    private final String defaultOffsets;
    private final long maxBatchRows;
    
    private final Map<String, String> properties;
    
    public KafkaTableValuedFunction(Map<String, String> params) throws AnalysisException {
        this.properties = params;
        
        // Parse required parameters
        this.catalogName = getRequiredParam(params, PARAM_CATALOG);
        this.tableName = getRequiredParam(params, PARAM_TABLE);
        
        // Parse optional parameters with defaults
        this.databaseName = params.getOrDefault(PARAM_DATABASE, DEFAULT_DATABASE);
        this.defaultOffsets = params.getOrDefault(PARAM_DEFAULT_OFFSETS, DEFAULT_OFFSETS);
        
        String maxBatchRowsStr = params.get(PARAM_MAX_BATCH_ROWS);
        if (maxBatchRowsStr != null) {
            try {
                this.maxBatchRows = Long.parseLong(maxBatchRowsStr);
            } catch (NumberFormatException e) {
                throw new AnalysisException(PARAM_MAX_BATCH_ROWS + " must be a valid number");
            }
        } else {
            this.maxBatchRows = DEFAULT_MAX_BATCH_ROWS;
        }
        
        // Validate the catalog exists and is a Kafka connector
        validateKafkaCatalog();
        
        // Validate default offsets
        validateDefaultOffsets();
    }
    
    /**
     * Validate that the specified catalog exists and is a Trino Kafka connector.
     */
    private void validateKafkaCatalog() throws AnalysisException {
        CatalogIf<?> catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogName);
        if (catalog == null) {
            throw new AnalysisException("Catalog not found: " + catalogName);
        }
        
        if (!(catalog instanceof TrinoConnectorExternalCatalog)) {
            throw new AnalysisException(
                    "Catalog '" + catalogName + "' must be a Trino Connector catalog. "
                    + "Please create a Trino Kafka catalog first.");
        }
        
        TrinoConnectorExternalCatalog trinoCatalog = (TrinoConnectorExternalCatalog) catalog;
        String connectorName = trinoCatalog.getConnectorName() != null 
                ? trinoCatalog.getConnectorName().toString() : "";
        
        if (!"kafka".equalsIgnoreCase(connectorName)) {
            throw new AnalysisException(
                    "Catalog '" + catalogName + "' must be a Kafka connector, but found: " + connectorName);
        }
    }
    
    /**
     * Validate the kafka_default_offsets parameter.
     */
    private void validateDefaultOffsets() throws AnalysisException {
        String upper = defaultOffsets.toUpperCase();
        if (!"OFFSET_BEGINNING".equals(upper) && !"OFFSET_END".equals(upper)) {
            // Try to parse as a number
            try {
                Long.parseLong(defaultOffsets);
            } catch (NumberFormatException e) {
                throw new AnalysisException(
                        "Invalid " + PARAM_DEFAULT_OFFSETS + ": " + defaultOffsets 
                        + ". Expected: OFFSET_BEGINNING, OFFSET_END, or a numeric offset value.");
            }
        }
    }
    
    private String getRequiredParam(Map<String, String> params, String key) throws AnalysisException {
        String value = params.get(key);
        if (value == null || value.isEmpty()) {
            throw new AnalysisException("Missing required parameter: " + key);
        }
        return value;
    }
    
    @Override
    public String getTableName() {
        return "KafkaTableValuedFunction";
    }
    
    @Override
    public List<Column> getTableColumns() throws AnalysisException {
        // Return a placeholder schema since the actual columns come from the Trino Kafka table.
        // When used in streaming jobs, the TVF is rewritten to reference the actual table.
        // This placeholder allows the TVF to be parsed and validated.
        return ImmutableList.of(
                new Column("_placeholder", ScalarType.createStringType())
        );
    }
    
    @Override
    public ScanNode getScanNode(PlanNodeId id, TupleDescriptor desc, SessionVariable sv) {
        // This TVF is designed for streaming jobs where it gets rewritten by
        // KafkaSourceOffsetProvider.rewriteTvfParams() to a direct table reference.
        // Direct scanning via ScanNode is not supported.
        throw new UnsupportedOperationException(
                "kafka() TVF does not support direct scanning. "
                + "It should be used with CREATE JOB ... ON STREAMING.");
    }
    
    /**
     * Get the full table identifier as: catalog.database.table
     */
    public String getFullTableName() {
        return catalogName + "." + databaseName + "." + tableName;
    }
}
