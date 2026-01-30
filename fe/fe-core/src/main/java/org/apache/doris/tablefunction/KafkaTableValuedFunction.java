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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.trinoconnector.TrinoConnectorExternalCatalog;
import org.apache.doris.datasource.trinoconnector.TrinoConnectorExternalDatabase;
import org.apache.doris.datasource.trinoconnector.TrinoConnectorExternalTable;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.SessionVariable;

import com.google.common.base.Joiner;
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
    
    private final String catalogName;
    private final String databaseName;
    private final String tableName;
    private final Map<String, String> properties;

    private TrinoConnectorExternalCatalog trinoCatalog;
    private TrinoConnectorExternalDatabase trinoDatabase;
    private TrinoConnectorExternalTable trinoTable;
    
    public KafkaTableValuedFunction(Map<String, String> params) throws AnalysisException {
        this.properties = params;
        
        // Parse required parameters
        this.catalogName = getRequiredParam(params, PARAM_CATALOG);
        this.databaseName = getRequiredParam(params, PARAM_DATABASE);
        this.tableName = getRequiredParam(params, PARAM_TABLE);

        // Validate the catalog exists and is a Kafka connector
        validateKafkaCatalog();
    }
    
    /**
     * Validate that the specified catalog exists and is a Trino Kafka connector.
     */
    private void validateKafkaCatalog() throws AnalysisException {
        CatalogIf<?> catalog = Env.getCurrentEnv().getCatalogMgr().getCatalogOrAnalysisException(catalogName);
        if (!(catalog instanceof TrinoConnectorExternalCatalog)) {
            throw new AnalysisException(
                    "Catalog '" + catalogName + "' must be a Trino Connector catalog. "
                    + "Please create a Trino Kafka catalog first.");
        }

        this.trinoCatalog = (TrinoConnectorExternalCatalog) catalog;
        String connectorName = trinoCatalog.getConnectorName() != null
                ? trinoCatalog.getConnectorName().toString() : "";
        
        if (!"kafka".equalsIgnoreCase(connectorName)) {
            throw new AnalysisException(
                    "Catalog '" + catalogName + "' must be a Kafka connector, but found: " + connectorName);
        }

        this.trinoDatabase = (TrinoConnectorExternalDatabase) catalog.getDbOrAnalysisException(this.databaseName);
        this.trinoTable = this.trinoDatabase.getTableOrAnalysisException(this.tableName);
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
        return Joiner.on(".").join(this.catalogName, this.databaseName, this.tableName);
    }

    @Override
    public List<Column> getTableColumns() throws AnalysisException {
        return trinoTable.getFullSchema();
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
}
