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

package org.apache.doris.datasource.deltalake;

import org.apache.doris.catalog.info.CreateOrReplaceBranchInfo;
import org.apache.doris.catalog.info.CreateOrReplaceTagInfo;
import org.apache.doris.catalog.info.DropBranchInfo;
import org.apache.doris.catalog.info.DropTagInfo;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.operations.ExternalMetadataOps;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;

import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * Abstract base class for Delta Lake metadata operations.
 * Provides common functionality shared by different metadata source implementations
 * (HMS-based and Unity Catalog-based).
 *
 * <p>Subclasses must implement:
 * <ul>
 *   <li>{@link #listDatabaseNames()} - discover databases</li>
 *   <li>{@link #listTableNames(String)} - discover tables in a database</li>
 *   <li>{@link #tableExist(String, String)} - check table existence</li>
 *   <li>{@link #databaseExist(String)} - check database existence</li>
 *   <li>{@link #getTableLocation(String, String)} - get the storage path of a Delta table</li>
 *   <li>{@link #close()} - release resources</li>
 * </ul>
 *
 * <p>Common functionality provided:
 * <ul>
 *   <li>Delta Kernel Engine management ({@link #getEngine()})</li>
 *   <li>Read-only DDL stubs (all DDL operations throw exceptions)</li>
 * </ul>
 */
public abstract class AbstractDeltaLakeMetadataOps implements ExternalMetadataOps {
    private static final Logger LOG = LogManager.getLogger(AbstractDeltaLakeMetadataOps.class);

    protected final ExternalCatalog catalog;
    protected volatile Engine engine;

    protected AbstractDeltaLakeMetadataOps(ExternalCatalog catalog) {
        this.catalog = catalog;
    }

    /**
     * Get or create the Delta Kernel Engine instance.
     * Uses Hadoop Configuration from the catalog properties.
     */
    public synchronized Engine getEngine() {
        if (engine == null) {
            Configuration hadoopConf = catalog.getConfiguration();
            engine = DefaultEngine.create(hadoopConf);
        }
        return engine;
    }

    /**
     * Get the storage location of a Delta Lake table.
     *
     * @param dbName  the database (schema) name
     * @param tblName the table name
     * @return the storage path (e.g., s3://bucket/path/to/table)
     */
    public abstract String getTableLocation(String dbName, String tblName);

    // ========== Read-only catalog: DDL operations not supported ==========

    @Override
    public boolean createDbImpl(String dbName, boolean ifNotExists, Map<String, String> properties)
            throws DdlException {
        throw new DdlException("Create database is not supported for Delta Lake catalog.");
    }

    @Override
    public void dropDbImpl(String dbName, boolean ifExists, boolean force) throws DdlException {
        throw new DdlException("Drop database is not supported for Delta Lake catalog.");
    }

    @Override
    public void afterDropDb(String dbName) {
        // no-op
    }

    @Override
    public boolean createTableImpl(CreateTableInfo createTableInfo) throws UserException {
        throw new DdlException("Create table is not supported for Delta Lake catalog.");
    }

    @Override
    public void dropTableImpl(ExternalTable dorisTable, boolean ifExists) throws DdlException {
        throw new DdlException("Drop table is not supported for Delta Lake catalog.");
    }

    @Override
    public void truncateTableImpl(ExternalTable dorisTable, List<String> partitions) throws DdlException {
        throw new DdlException("Truncate table is not supported for Delta Lake catalog.");
    }

    @Override
    public void createOrReplaceBranchImpl(ExternalTable dorisTable, CreateOrReplaceBranchInfo branchInfo)
            throws UserException {
        throw new UserException("Create or replace branch is not supported for Delta Lake catalog.");
    }

    @Override
    public void createOrReplaceTagImpl(ExternalTable dorisTable, CreateOrReplaceTagInfo tagInfo)
            throws UserException {
        throw new UserException("Create or replace tag is not supported for Delta Lake catalog.");
    }

    @Override
    public void dropTagImpl(ExternalTable dorisTable, DropTagInfo tagInfo) throws UserException {
        throw new UserException("Drop tag is not supported for Delta Lake catalog.");
    }

    @Override
    public void dropBranchImpl(ExternalTable dorisTable, DropBranchInfo branchInfo) throws UserException {
        throw new UserException("Drop branch is not supported for Delta Lake catalog.");
    }
}
