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

package org.apache.doris.catalog;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * InternalDatabase interface for the current Database class.
 *
 * @param <T> current Database table type, subclass of Table.
 */
public interface InternalDatabase<T extends Table> extends DatabaseIf<T> {

    @Override
    public List<T> getTables();

    @Override
    public List<T> getTablesOnIdOrder();

    @Override
    public List<T> getViews();

    @Override
    public List<T> getTablesOnIdOrderIfExist(List<Long> tableIdList);

    @Override
    public List<T> getTablesOnIdOrderOrThrowException(List<Long> tableIdList) throws MetaNotFoundException;

    @Override
    public T getTableNullable(String tableName);

    @Override
    public Optional<T> getTable(String tableName);

    @Override
    public Optional<T> getTable(long tableId);

    @Override
    public <E extends Exception> T getTableOrException(String tableName, Function<String, E> e) throws E;

    @Override
    public <E extends Exception> T getTableOrException(long tableId, Function<Long, E> e) throws E;

    @Override
    public T getTableOrMetaException(String tableName) throws MetaNotFoundException;

    @Override
    public T getTableOrMetaException(long tableId) throws MetaNotFoundException;

    @Override
    public T getTableOrMetaException(String tableName, Table.TableType tableType) throws MetaNotFoundException;

    @Override
    public T getTableOrMetaException(long tableId, Table.TableType tableType) throws MetaNotFoundException;

    @Override
    public T getTableOrDdlException(String tableName) throws DdlException;

    @Override
    public T getTableOrDdlException(long tableId) throws DdlException;

    @Override
    public T getTableOrAnalysisException(String tableName) throws AnalysisException;

    @Override
    public T getTableOrAnalysisException(long tableId) throws AnalysisException;

    @Override
    public OlapTable getOlapTableOrAnalysisException(String tableName) throws AnalysisException;
}
