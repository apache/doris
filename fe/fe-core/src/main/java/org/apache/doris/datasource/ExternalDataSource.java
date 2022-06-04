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

package org.apache.doris.datasource;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.external.ExternalDatabase;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.external.ExternalScanRange;

import com.google.common.collect.Maps;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * The abstract class for all types of external data sources.
 */
public abstract class ExternalDataSource implements DataSourceIf {

    // Unique id of this data source, will be assigned after data source is loaded.
    private long id;

    // save properties of this data source, such as hive meta store url.
    private Map<String, String> properties = Maps.newHashMap();

    private Map<Long, ExternalDatabase> idToDbs = Maps.newConcurrentMap();
    private Map<String, ExternalDatabase> nameToDbs = Maps.newConcurrentMap();

    public void setId(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }

    public String getProperty(String key) {
        return properties.get(key);
    }

    public String getPropertyOrException(String key) throws DataSourceException {
        if (properties.containsKey(key)) {
            return properties.get(key);
        }
        throw new DataSourceException("Not found property " + key + " in data source " + getName());
    }


    /**
     * @return names of database in this data source.
     */
    public abstract List<String> listDatabaseNames(SessionContext ctx);

    /**
     * @param dbName
     * @return names of tables in specified database
     */
    public abstract List<String> listTableNames(SessionContext ctx, String dbName);

    /**
     * check if the specified table exist.
     *
     * @param dbName
     * @param tblName
     * @return true if table exists, false otherwise
     */
    public abstract boolean tableExist(SessionContext ctx, String dbName, String tblName);

    /**
     * get schema of the specified table
     *
     * @param dbName
     * @param tblName
     * @return list of columns as table's schema
     */
    public abstract List<Column> getSchema(SessionContext ctx, String dbName, String tblName);

    /**
     * @return list of ExternalScanRange
     */
    public abstract List<ExternalScanRange> getExternalScanRanges(SessionContext ctx);

    @Override
    public String getType() {
        return null;
    }

    @Override
    public String getName() {
        return null;
    }

    @Nullable
    @Override
    public DatabaseIf getDbNullable(String dbName) {
        return null;
    }

    @Nullable
    @Override
    public DatabaseIf getDbNullable(long dbId) {
        return null;
    }

    @Override
    public Optional<DatabaseIf> getDb(String dbName) {
        return Optional.empty();
    }

    @Override
    public Optional<DatabaseIf> getDb(long dbId) {
        return Optional.empty();
    }

    @Override
    public <E extends Exception> DatabaseIf getDbOrException(String dbName, Function<String, E> e) throws E {
        return null;
    }

    @Override
    public <E extends Exception> DatabaseIf getDbOrException(long dbId, Function<Long, E> e) throws E {
        return null;
    }

    @Override
    public DatabaseIf getDbOrMetaException(String dbName) throws MetaNotFoundException {
        return null;
    }

    @Override
    public DatabaseIf getDbOrMetaException(long dbId) throws MetaNotFoundException {
        return null;
    }

    @Override
    public DatabaseIf getDbOrDdlException(String dbName) throws DdlException {
        return null;
    }

    @Override
    public DatabaseIf getDbOrDdlException(long dbId) throws DdlException {
        return null;
    }

    @Override
    public DatabaseIf getDbOrAnalysisException(String dbName) throws AnalysisException {
        return null;
    }

    @Override
    public DatabaseIf getDbOrAnalysisException(long dbId) throws AnalysisException {
        return null;
    }
}
