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

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * The interface of DataSource(catalog).
 */
public interface DataSourceIf<T extends DatabaseIf> {

    // Type of this data source
    String getType();

    long getId();

    // Name of this data source
    String getName();

    List<String> getDbNames();

    List<Long> getDbIds();

    @Nullable
    T getDbNullable(String dbName);

    @Nullable
    T getDbNullable(long dbId);

    Optional<T> getDb(String dbName);

    Optional<T> getDb(long dbId);

    <E extends Exception> T getDbOrException(String dbName, java.util.function.Function<String, E> e) throws E;

    <E extends Exception> T getDbOrException(long dbId, java.util.function.Function<Long, E> e) throws E;

    T getDbOrMetaException(String dbName) throws MetaNotFoundException;

    T getDbOrMetaException(long dbId) throws MetaNotFoundException;

    T getDbOrDdlException(String dbName) throws DdlException;

    T getDbOrDdlException(long dbId) throws DdlException;

    T getDbOrAnalysisException(String dbName) throws AnalysisException;

    T getDbOrAnalysisException(long dbId) throws AnalysisException;

    Map<String, String> getProperties();

    void modifyDatasourceName(String name);

    void modifyDatasourceProps(Map<String, String> props);
}
