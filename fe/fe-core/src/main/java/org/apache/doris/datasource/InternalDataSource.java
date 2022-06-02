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

import org.apache.doris.catalog.Database;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;

import org.jetbrains.annotations.Nullable;

import java.util.Optional;
import java.util.function.Function;

/**
 * The Internal data source will manage all self-managed meta object in a Doris cluster.
 * Such as Database, tables, etc.
 * There is only one internal data source in a cluster.
 */
public class InternalDataSource implements DataSourceIf {
    @Override
    public String getType() {
        return "internal";
    }

    @Override
    public String getName() {
        return "_internal_";
    }

    @Nullable
    @Override
    public Database getDbNullable(String dbName) {
        return null;
    }

    @Nullable
    @Override
    public Database getDbNullable(long dbId) {
        return null;
    }

    @Override
    public Optional<Database> getDb(String dbName) {
        return Optional.empty();
    }

    @Override
    public Optional<Database> getDb(long dbId) {
        return Optional.empty();
    }

    @Override
    public <E extends Exception> Database getDbOrException(String dbName, Function<String, E> e) throws E {
        return null;
    }

    @Override
    public <E extends Exception> Database getDbOrException(long dbId, Function<Long, E> e) throws E {
        return null;
    }

    @Override
    public Database getDbOrMetaException(String dbName) throws MetaNotFoundException {
        return null;
    }

    @Override
    public Database getDbOrMetaException(long dbId) throws MetaNotFoundException {
        return null;
    }

    @Override
    public Database getDbOrDdlException(String dbName) throws DdlException {
        return null;
    }

    @Override
    public Database getDbOrDdlException(long dbId) throws DdlException {
        return null;
    }

    @Override
    public Database getDbOrAnalysisException(String dbName) throws AnalysisException {
        return null;
    }

    @Override
    public Database getDbOrAnalysisException(long dbId) throws AnalysisException {
        return null;
    }
}
